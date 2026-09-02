# Activities and workflow macros
_Exported on 9/2/2026 at 13:07:38 EDT from Cursor (3.18.9)_

---

**User**

can you help me understand the #[activities] and #[workflow] macros more? i see they decorate the implementations of corresponding structs, but what is happening under the hood?

i think temporal is open source, so we should be able to find this info https://github.com/temporalio/temporal 

---

**Cursor**

The Temporal server repo is Go; these macros live in this Rust workspace. I'll look at how they're used in the hello-world crate, then follow the implementations.

The macros are in the Rust SDK (`temporalio-macros`), not the Go server. I'll pull the crate source and the expansion so we can see what they generate.

I have the macro codegen. Next I’ll expand hello-world and see how the worker/client consume those generated types.

The repo you linked, [temporalio/temporal](https://github.com/temporalio/temporal), is the **Go server**. It never sees these macros. On the wire, a workflow/activity is a **string name plus protobuf payloads**. The server stores history and hands out tasks; it does not run your Rust.

The macros live in the **Rust SDK**: [`temporalio-macros` 0.7.0](https://github.com/temporalio/sdk-rust/tree/main/crates/macros), used by [`temporalio-sdk`](https://github.com/temporalio/sdk-rust). Docs are at [docs.rs/temporalio-macros](https://docs.rs/temporalio-macros/latest/temporalio_macros/).

They are ordinary proc-macros. At compile time they rewrite your `impl` into typed marker types plus trait impls the Worker and `execute_activity` already know how to call.

## The inner attributes are tags, not macros that do work

`#[activity]`, `#[run]`, `#[signal]`, `#[query]`, `#[update]` are pass-through. Used alone they do nothing. `#[activities]` and `#[workflow_methods]` scan the `impl` for those tags and generate everything.

`#[workflow]` on the struct is almost a no-op too: it only rejects `#[workflow(name = "...")]`. The type name comes from `#[run]`, or defaults to the struct name (`HelloWorldWorkflow`).

## The trick: steal the method name for a typed handle

After expansion, `GreetingActivities::greet` is **not a function**. It is a `const` of a zero-sized marker type. That is why this compiles and is type-checked:

```rust
ctx.execute_activity(GreetingActivities::greet, name, opts)
```

`execute_activity` is generic over `ActivityDefinition`. The marker carries `Input`, `Output`, and the string name. Your real function is renamed to `__greet` so the original identifier is free for that const.

Same pattern for workflows: `HelloWorldWorkflow::run` becomes a marker; the body is `__run`.

## What `#[activities]` emits for hello-world

From this:

```29:41:crates/hello-world/src/lib.rs
#[activities]
impl GreetingActivities {
    #[activity]
    pub async fn greet(_ctx: ActivityContext, name: String) -> Result<String, ActivityError> {
        Ok(format!("Hello, {name}!"))
    }
}
```

the macro roughly produces:

```rust
impl GreetingActivities {
    // your method, renamed so `greet` can be a const
    pub async fn __greet(_ctx: ActivityContext, name: String) -> Result<String, ActivityError> {
        Ok(format!("Hello, {name}!"))
    }

    pub const greet: greeting_activities::Greet = greeting_activities::Greet;
}

mod greeting_activities {
    pub struct Greet; // ZST marker — this is what you pass to execute_activity
}

// Typed contract used by the Workflow side (schedule by name + serde types)
impl temporalio_common::ActivityDefinition for greeting_activities::Greet {
    type Input = String;
    type Output = String;
    fn name(&self) -> &str { "GreetingActivities::greet" }
}

// Worker-side: deserialize payloads, call __greet, serialize the result
impl temporalio_sdk::activities::ExecutableActivity for greeting_activities::Greet {
    type Implementer = GreetingActivities;
    const REQUIRES_INSTANCE: bool = false; // no `self: Arc<Self>`
    fn execute(receiver, ctx, input) -> BoxFuture<'static, Result<String, ActivityError>> {
        async move { GreetingActivities::__greet(ctx, input).await }.boxed()
    }
}

// So WorkerOptions::register_activities(GreetingActivities) can find every method
impl temporalio_sdk::activities::ActivityImplementer for GreetingActivities {
    fn register_all(self: Arc<Self>, defs: &mut ActivityDefinitions) {
        defs.register_activity::<greeting_activities::Greet>(self.clone());
    }
}
```

A few details that match the sharp edges in this repo:

- **Default activity type name** is `"StructName::method"`. Override with `#[activity(name = "compose_greeting")]`.
- **Static vs instance.** No `self` → `REQUIRES_INSTANCE = false`. Stateful methods must be `self: Arc<Self>` (not `&self`). That is how `OrderActivities` injects `Deps`.
- **At most 6 inputs** after `ActivityContext`; more than one becomes `MultiArgsN<T1, …>`.
- Generated code names `::temporalio_workflow`, `::temporalio_common`, and `::futures` by absolute path, which is why those crates must be **direct** dependencies even if you never `use` them.

On the Worker, registration is a hashmap from that string name to a closure that deserializes payloads and calls `ExecutableActivity::execute`. When the Workflow does `execute_activity(GreetingActivities::greet, name, opts)`, it only sends the name `"GreetingActivities::greet"` plus the serialized `String`. The server never sees the marker type.

## What `#[workflow]` + `#[workflow_methods]` emit

`#[workflow]` leaves the struct alone. `#[workflow_methods]` is the real generator. For hello-world it:

1. Renames `run` → `__run`.
2. Adds `HelloWorldWorkflow::run` as a marker const, plus `HelloWorldWorkflow::name()`.
3. Implements `WorkflowDefinition` on both the struct and the `Run` marker (`Input = String`, `Output = String`, name `"HelloWorldWorkflow"`).
4. Implements `WorkflowImplementation` — the trait the Worker actually calls.

That last impl is the dispatcher:

```rust
impl WorkflowImplementation for HelloWorldWorkflow {
    type Run = hello_world_workflow::Run;
    const HAS_INIT: bool = false;

    fn init(...) -> Self { Self::default() }   // or your #[init], or panic if factory_only

    fn run(mut ctx, input) -> LocalBoxFuture<...> {
        async move { Self::__run(&mut ctx, input.expect("run takes input")).await ... }
    }

    fn decode_signal_input(...) { match name { ... } }
    fn dispatch_signal(...)     { match name { ... } }
    // same for query and update
}
```

That is why `register_workflow::<HelloWorldWorkflow>()` only needs the type: the macro already built a factory (`Default` or `#[init]`), a name, and match-arms for every `#[signal]` / `#[query]` / `#[update]`. Incoming messages arrive as string + bytes; the generated `decode_*` turns them into `Box<dyn Any>` of the right input type, then `dispatch_*` downcasts and calls `__cancel` / `__status` / etc.

Order-pipeline uses the extra pieces:

- `#[init]` — input goes to `new`, not `run`. `HAS_INIT = true`. `init()` calls `__new` instead of `Default`.
- `#[signal]` / `#[query]` — extra markers (`OrderWorkflow::cancel`, `::status`) plus decode/dispatch arms. Sync signals take `&mut self`; async ones must **not** take `self` so a signal cannot mutate state across an `.await` in `run`.

## Why two macros for workflows

| Macro | Target | Job |
|---|---|---|
| `#[workflow]` | struct | Marker + reject `name =` on the struct |
| `#[workflow_methods]` | `impl` | Rename methods, emit markers, `WorkflowDefinition`, `WorkflowImplementation` |
| `#[activities]` | `impl` | Same idea for activities: markers, `ActivityDefinition`, `ExecutableActivity`, `ActivityImplementer` |

Activities do not need a struct-level attribute because the struct is just a bag of methods (and optional `Arc` deps). Workflows need the struct because **fields are the durable in-memory state** rebuilt on replay; `WorkflowImplementation::init` constructs that struct, and `run` / signals mutate it through `WorkflowContext`.

There is also `#[activity_definitions]`: same markers and `ActivityDefinition`, but bodies must be `unimplemented!()` and there is no `ExecutableActivity`. That is for a workflow crate that only *schedules* activities implemented in another worker (or another language).

## The loop, end to end

```
Starter:  start_workflow::<HelloWorldWorkflow>("Ada")
            → type "HelloWorldWorkflow" + payload

Server:   records history, puts a Workflow Task on the queue

Worker:   looks up WorkflowImplementation by name
            → init() (Default / #[init])
            → run() → your __run
            → execute_activity(GreetingActivities::greet, ...)
                 only sends name + payload; does not call greet

Server:   Activity Task on the queue

Worker:   looks up "GreetingActivities::greet"
            → deserialize String
            → GreetingActivities::__greet(ctx, name)
            → serialize result into history

Worker:   replays __run; execute_activity returns the recorded String
```

The macros are a typed façade over that stringly-typed protocol. That is also why diagnostics often point at `#[activities]` rather than the method: the compiler is type-checking generated trait impls, not the function you wrote.

If you want to see the exact expansion locally:

```bash
cargo install cargo-expand
cargo expand -p hello-world --lib
```

---

**User**

this is a dumb question, but how do i find the  generated source from a proc macro?

---

**Cursor**

Not a dumb question — proc macros do not write a file you can open. The expansion exists only in the compiler’s head unless you ask a tool to dump it.

**In Cursor (fastest for one attribute)**

1. Put the cursor on `#[activities]` or `#[workflow_methods]`.
2. Command Palette (`Cmd+Shift+P`) → **Rust Analyzer: Expand macro recursively**.

That is the generated source for that item. Inner tags like `#[activity]` and `#[run]` expand to themselves; expand the **outer** attribute.

**Whole crate, as text**

```bash
cargo install cargo-expand
cargo expand -p hello-world --lib
```

`cargo-expand` temporarily uses nightly just for `-Zunpretty=expanded`. Filter if the dump is huge:

```bash
cargo expand -p hello-world --lib GreetingActivities
```

**Without extra tools (nightly)**

```bash
cargo +nightly rustc -p hello-world --lib -- -Zunpretty=expanded
```

You will not find this under `target/` on a normal build. rustc throws the tokens away after type-checking.

