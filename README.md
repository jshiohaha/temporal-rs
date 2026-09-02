# Temporal + Rust: from zero to durable execution

A worked introduction to the [Temporal Rust SDK](https://github.com/temporalio/sdk-rust)
(`temporalio-sdk` **0.7.0**, Public Preview). Two crates, both compiling and
commented line by line:

| Crate | What it teaches |
|---|---|
| [`crates/hello-world`](crates/hello-world) | The whole model in ~60 lines: one Workflow, one Activity, a Worker, a Starter. |
| [`crates/order-pipeline`](crates/order-pipeline) | The real job: dependency injection, idempotency, retry policies, heartbeats, signals, queries, saga compensation. |

Visual walkthrough: **[`docs/architecture.html`](docs/architecture.html)** — open it in a browser.

---

## 1. The mental model

Temporal is a way to write a function that **cannot lose its place**. If the
process running it is killed halfway through, another process picks it up at the
exact line it stopped on, with local variables intact — hours or weeks later.

It does this by splitting your code in two:

**Workflows** are the orchestration. They are *replayed*: to recover state,
Temporal re-runs your Workflow function from the top and feeds it the recorded
results of everything it did last time. This is why Workflow code must be
deterministic — same history in, same sequence of calls out. No clocks, no
randomness, no network, no threads.

**Activities** are the side effects. Each one runs exactly once *per successful
attempt*, and its result is written to durable history. They can do anything:
HTTP, SQL, file IO. They are retried automatically when they fail.

Three processes are involved, and it helps to be precise about who does what:

```
┌───────────┐  start_workflow   ┌──────────────────┐   poll    ┌────────────┐
│  Client   │ ────────────────► │ Temporal Server  │ ◄──────── │   Worker   │
│ (starter) │ ◄──────────────── │  (history + task │ ────────► │ (your code)│
└───────────┘   workflow result │     queues)      │   tasks   └────────────┘
                                └──────────────────┘
```

**The server never runs your code.** It stores Event History and hands out
tasks. Your Worker polls a *task queue*, executes Workflow and Activity code,
and reports results back. If no Worker is polling the queue your Starter named,
the Workflow will start and sit in "Running" forever, doing nothing. That is the
number one first-day confusion.

---

## 2. Prerequisites

```bash
# Rust. The SDK is edition 2024; it is developed against 1.94.
rustup update stable

# protoc — REQUIRED. See the gotchas section; the build fails without it.
sudo apt-get install -y protobuf-compiler   # Debian/Ubuntu
brew install protobuf                       # macOS

# The Temporal CLI, which bundles a zero-config dev server.
curl -sSf https://temporal.download/cli.sh | sh
# or: brew install temporal
```

## 3. Run it

Three terminals.

```bash
# ── Terminal 1: the dev server (in-memory; nothing to configure) ──
temporal server start-dev
#   gRPC on localhost:7233, Web UI on http://localhost:8233
```

```bash
# ── Terminal 2: the Worker. Leave it running. ──
cargo run --bin hello-worker
```

```bash
# ── Terminal 3: start a Workflow ──
cargo run --bin hello-starter -- Ada
#   started workflow, run_id: Some("...")
#   workflow result: Hello, Ada!
```

Now open <http://localhost:8233> and click into the execution. The Event History
is the thing to look at — every decision the Workflow made is a durable, replayable
event. That list *is* the durability.

### The interesting one

```bash
cargo run --bin order-worker                        # terminal 2

cargo run --bin order-starter -- ord-1001           # happy path
cargo run --bin order-starter -- ord-flaky-1002     # gateway fails twice, retries, succeeds
cargo run --bin order-starter -- ord-decline-1003   # non-retryable failure → saga compensation
```

Watch `ord-flaky-1002` in the UI: the Activity fails, backs off, and retries
without the Workflow knowing anything happened. Then **kill the worker
mid-shipment** (`ord-1001` ships three parcels with a heartbeat) and restart it —
the Workflow resumes where it left off rather than starting over.

Connection settings come from `temporal.toml` + `TEMPORAL_*` env vars. Against a
local dev server on the default port you need neither.

---

## 4. What the code is doing

### The Workflow

```rust
#[workflow]
#[derive(Default)]
pub struct HelloWorldWorkflow;

#[workflow_methods]
impl HelloWorldWorkflow {
    #[run]
    pub async fn run(ctx: &mut WorkflowContext<Self>, name: String) -> WorkflowResult<String> {
        let greeting = ctx.execute_activity(
            GreetingActivities::greet,
            name,
            ActivityOptions::start_to_close_timeout(Duration::from_secs(10)),
        ).await?;
        Ok(greeting)
    }
}
```

The Workflow **struct's fields are durable state**. They are not written to the
server; they are rebuilt by replaying history. From your code's perspective they
behave like a process that never crashes.

Note that `execute_activity` takes the Activity *method*, not a string name. The
SDK derives the type name and type-checks input and output at compile time —
a genuine advantage over the stringly-typed SDKs in other languages.

### The `.await` that is not really an await

When a Workflow awaits an Activity, the Worker does not block a thread for ten
seconds. It **suspends the Workflow, evicts it from memory, and returns the
task**. When the Activity result arrives, the Workflow is replayed from the
start of history up to that point and continues. This is why a Workflow can
sleep for 30 days with `ctx.timer(Duration::from_days(30))` at no cost — and why
determinism is non-negotiable.

### Where state lives

| Thing | Lives where | Survives a crash? |
|---|---|---|
| Workflow struct fields | Rebuilt by replay | Yes |
| Activity struct fields (`Deps`) | Worker process memory | No — rebuilt at startup |
| Anything in a `static`/global | Worker process memory | No, and it breaks replay |

---

## 5. Dependency injection

This is the question everyone asks, and the answer is refreshingly boring:
**the Activity struct is the container.** There is no framework, no registry, no
macro magic.

```rust
pub struct Deps {
    pub db: Database,          // in real life: sqlx::PgPool
    pub payments: PaymentGateway,  // in real life: a reqwest::Client wrapper
}

pub struct OrderActivities {
    deps: Arc<Deps>,
}

#[activities]
impl OrderActivities {
    #[activity]
    pub async fn charge_payment(
        self: Arc<Self>,              // ← required receiver for stateful activities
        ctx: ActivityContext,         // ← must be second when `self` is present
        input: ChargePayment,
    ) -> Result<String, ActivityError> {
        self.deps.db.find_charge(&input.idempotency_key).await;
        // ...
    }
}
```

and in `main`:

```rust
let deps = Deps::from_env().await?;            // build it however you like

let worker_options = WorkerOptions::new(TASK_QUEUE)
    .register_workflow::<OrderWorkflow>()?
    .register_activities(OrderActivities::new(Arc::clone(&deps)))  // ← inject
    .build();
```

Rules worth knowing:

- **The receiver must be exactly `self: Arc<Self>`.** Not `&self`, not `self`.
  The macro rejects anything else, because the SDK shares one instance across all
  concurrent Activity invocations.
- **Parameter order is fixed**: `self: Arc<Self>`, then `ActivityContext`, then
  your input. Stateless Activities just drop the `self`.
- **You can call `register_activities` more than once** with different structs.
  That is the natural way to group Activities by bounded context while sharing a
  pool between them.
- **Never inject dependencies into a Workflow.** Workflow structs are replayed;
  a connection pool in one is a determinism bug waiting to happen. If a Workflow
  needs data, it gets it by calling an Activity.
- Interior mutability is fine (`Arc<Mutex<...>>`, `AtomicUsize`), but remember it
  is *per worker process* and vanishes on restart. Durable state belongs in the
  Workflow struct or a real database.

For test doubles, make `Deps` hold trait objects (`Arc<dyn PaymentGateway>`) and
inject a fake — the Activity code does not change.

---

## 6. Idempotency, retries, and the rest of the bread and butter

### Retries: the default is "forever"

Temporal retries a failed Activity **indefinitely** by default
(`maximum_attempts: 0` means unlimited). For a transient dependency that is
exactly right. For a malformed request it is a disaster — you will retry a
guaranteed-failing call until someone notices.

The fix is to classify your errors:

```rust
ActivityOptions::with_start_to_close_timeout(Duration::from_secs(30))
    .retry_policy(
        RetryPolicy::builder()
            .initial_interval(Duration::from_secs(1))
            .backoff_coefficient(2.0)
            .maximum_interval(Duration::from_secs(30))
            .maximum_attempts(5)
            .non_retryable_error_types(["CardDeclined"])   // ← matches type_name
            .build(),
    )
    .build()
```

and on the Activity side:

```rust
// Permanent — fails immediately, no backoff.
ApplicationFailure::builder(msg)
    .type_name("CardDeclined".to_string())
    .non_retryable(true)
    .build()

// Transient — plain failure, retried per the policy.
ApplicationFailure::new(msg)
```

`non_retryable(true)` on the error and `non_retryable_error_types` on the policy
are two routes to the same outcome. Use the error when the *code* knows the
failure is permanent; use the policy when the *caller* decides.

**Timeouts are the other half of retries:**

| Timeout | Means | Use it for |
|---|---|---|
| `start_to_close` | Budget for one attempt | Always set this |
| `schedule_to_close` | Budget for all attempts including backoff | Overall deadlines |
| `heartbeat` | Max gap between heartbeats | Long Activities — detects dead workers fast |
| `schedule_to_start` | Time waiting in the queue | Rarely; only for host-specific queues |

Without a heartbeat timeout, a worker that dies mid-Activity is not noticed until
the full `start_to_close` elapses. With one, it is noticed in seconds.

### Idempotency: the one rule

Temporal guarantees your Activity is **attempted** at least once, not that it
runs exactly once. A worker can complete a charge and die before reporting
success — the retry then charges again. Preventing that is your job, and the
whole trick is one sentence:

> **Derive the idempotency key in the Workflow, pass it into the Activity.**

```rust
// In the WORKFLOW. Stable across every retry and every replay.
let payment_key = format!("{}:charge", ctx.workflow_id());
```

Generating the key inside the Activity defeats the entire purpose: each attempt
invents a new one and the dedupe check never hits. Workflow-derived values are
recorded in history, so attempt #4 sees the same key attempt #1 did.

`ctx.uuid4()` and `ctx.random()` are also safe — they are seeded deterministically
from the run and return the same values on replay. `Uuid::new_v4()` and
`rand::random()` are **not** safe and will corrupt your history.

Then defend in two layers:

```rust
// Layer 1: your own dedupe table.
if let Some(existing) = self.deps.db.find_charge(&input.idempotency_key).await {
    return Ok(existing);   // a previous attempt already did this
}

// Layer 2: the provider's idempotency key. Covers the gap between their
// success and your database write.
let charge_id = self.deps.payments
    .charge(&input.order_id, input.amount_cents, &input.idempotency_key)
    .await?;

self.deps.db.upsert_charge(&input.idempotency_key, &charge_id).await;
```

### Idempotency at the front door

The same problem exists one level up: a duplicated queue message or a
double-clicked button starting the same Workflow twice. Derive the **Workflow ID**
from your business key and let the server dedupe:

```rust
WorkflowStartOptions::new(TASK_QUEUE, format!("order-{order_id}"))
    .id_reuse_policy(WorkflowIdReusePolicy::RejectDuplicate)
    .build()
```

Workflow ID is unique per namespace among *running* executions. `RejectDuplicate`
extends that to completed ones too. This is free, server-side deduplication, and
it is the single highest-value line in the starter.

### Sagas: undoing what you cannot roll back

There is no distributed transaction. When step 3 fails you must actively undo
steps 1 and 2, in reverse order:

```rust
let charge_id = match charge_result {
    Ok(id) => id,
    Err(e) => {
        Self::release_inventory(ctx, &reservation_id).await;  // compensate
        return Err(e.into());
    }
};
```

Two things that are easy to get wrong:

- **Compensations must themselves be idempotent and near-unconditional.** Give
  them a generous retry policy. A failed refund is money lost, so it should try
  much harder than the forward path did.
- **Never `?` your way past a step that needs compensation.** The moment you take
  an action that must be undone, switch to explicit `match`.

### Heartbeats and resumable Activities

```rust
let start_at: u32 = ctx.heartbeat_details().deserialize().ok().flatten().unwrap_or(0);

for parcel in start_at..input.parcel_count {
    if ctx.is_cancelled() { return Err(ActivityError::Cancelled { details: None }); }
    do_work(parcel).await;
    let _ = ctx.record_heartbeat(parcel + 1).await;   // survives into the next attempt
}
```

Heartbeat details are handed to the *next attempt*, so a retry resumes instead of
restarting. For a batch job over 100k rows this is the difference between a
30-second recovery and a 6-hour one.

### Signals, queries, updates

| | Direction | Mutates state? | Blocks? |
|---|---|---|---|
| **Signal** | in | yes | no — fire and forget |
| **Query** | out | no (must be sync, side-effect free) | yes |
| **Update** | both | yes | yes — returns a result |

```rust
handle.signal(OrderWorkflow::request_cancel, (), WorkflowSignalOptions::default()).await?;
let status = handle.query(OrderWorkflow::status, (), WorkflowQueryOptions::default()).await?;
```

Signals are delivered *between* Workflow Tasks, so the natural place to react to
one is at a step boundary (`ctx.state(|s| s.cancel_requested)`) or by blocking on
`ctx.wait_condition(|s| ...)`.

### Local activities

For sub-second, low-value work (a formatting call, a cache read), `execute_local_activity`
runs on the same worker without a separate task-queue round trip. Much faster,
but no independent scheduling. Use it for things where the round trip would cost
more than the work.

---

## 7. Gotchas

**`protoc` is required to build, and the error is confusing.** A transitive
dependency (`prost-wkt-types`) shells out to `protoc` at build time. Without it:

```
error: failed to run custom build command for `prost-wkt-types v0.7.2`
  Could not find `protoc`...
```

Install `protobuf-compiler`. This is not mentioned prominently anywhere, and it
is the first wall you hit.

**The macros need crates you never `use` yourself.** `#[activities]` and
`#[workflow_methods]` expand to code referencing `temporalio_workflow`,
`temporalio_common`, and `futures` by absolute path. If they are not *direct*
dependencies of your crate you get:

```
error[E0433]: failed to resolve: could not find `temporalio_workflow` in the list of imported crates
```

pointing at the macro attribute rather than the real problem. This repo's
`Cargo.toml` declares all three with a comment explaining why.

**`ConfigError` is not `Sync`, so `?` into `anyhow::Error` fails.** Both binaries
return `Result<(), Box<dyn std::error::Error>>` for exactly this reason. Using
`anyhow::Result` on a `main` that calls `load_from_config` produces a wall of
trait-bound errors.

**Two different `WorkflowIdReusePolicy` types exist.** `temporalio_sdk::WorkflowIdReusePolicy`
is for child workflows; client starts want the protobuf one at
`temporalio_common::protos::temporal::api::enums::v1::WorkflowIdReusePolicy`.
The compiler's "expected `WorkflowIdReusePolicy`, found a different
`WorkflowIdReusePolicy`" is at least honest about it.

**Sync signal handlers take `&mut SyncWorkflowContext<Self>`**, not
`&mut WorkflowContext<Self>`. The SDK README currently shows the latter.

**The SDK README has drifted from the shipped API.** As of 0.7.0 it shows
`WorkflowOptions` (really `WorkflowStartOptions`), `GetWorkflowResultOptions`
(really `WorkflowGetResultOptions`), and `execute_activity(...)?.await?` (there is
no `?` before the `.await`). The `crates/sdk/examples/` directory in the SDK repo
is accurate; trust it over the prose.

**Query and signal handlers called from another crate must be `pub`.** The macro
generates an associated constant that inherits the method's visibility, so a
private handler produces "associated constant `status` is private" at the *call*
site.

**Silence usually means a task-queue mismatch.** If a Workflow starts and nothing
happens, the Starter's queue name and the Worker's queue name do not match, or no
Worker is running.

**Determinism failures show up on the *next* Workflow Task.** The runtime
nondeterminism detector watches async wake sources, and because those fire
asynchronously, the task that *introduced* the bad code completes fine and the
*following* one fails. Don't look at the last line of the trace; look at what the
previous task started. `tokio::time::sleep`, `tokio::spawn`, and `tokio::sync`
channels are all caught. Use `ctx.timer()` and `workflows::{select!, join!}`
instead of the `tokio`/`futures` equivalents.

**Changing Workflow code breaks running Workflows.** Replay of an old history
against new code that makes different calls is a nondeterminism error. Use
`ctx.patched("my-change-v2")` to branch on old vs. new, or version the task queue.
This bites in production, not in development, which is what makes it dangerous.

**Adding a field to a struct payload is safe; changing a tuple's arity is not.**
Prefer named structs for Activity inputs.

---

## 8. Next steps

**Test what you have written.** Enable the `testing` feature and you get both
halves:

```toml
[dev-dependencies]
temporalio-sdk = { version = "0.7.0", features = ["testing"] }
```

```rust
// Activities are just async functions — run them directly.
let env = ActivityEnvironment::builder()
    .register_activities(OrderActivities::new(test_deps()))
    .build();
assert_eq!(env.run(OrderActivities::charge_payment, input).await?, "ch_...");

// Workflows get a real ephemeral server, started and torn down per test.
let env = WorkflowEnvironment::start_local(Default::default()).await?;
```

**Add replay tests to CI.** `WorkflowReplayer` checks new code against recorded
histories from production. Export a history as JSON, commit it, and replay it on
every build — this is how you catch the determinism break *before* it reaches
running Workflows.

```rust
let replayer = WorkflowReplayer::new(
    WorkflowReplayerOptions::new().register_workflow::<OrderWorkflow>()?.build(),
)?;
replayer.replay_workflow(WorkflowHistory::from_json(&saved)?).await?;
```

**Then, roughly in order of value:**

- **Split the task queues.** One queue for Workflows, one per Activity class.
  Lets you scale the parts independently and stops a slow Activity starving
  Workflow progress.
- **Turn `Deps` into traits** so Activities can be unit-tested against fakes.
- **Set worker tuning knobs**: `max_cached_workflows`, poller behaviour,
  `graceful_shutdown_period`. The defaults are fine until they suddenly are not.
- **Export metrics.** The `prometheus` feature is on by default; wire it up and
  watch `workflow_task_schedule_to_start_latency` — it is the first thing to move
  when workers are undersized.
- **Adopt `ctx.patched()` before your first production change**, not after.
- **Continue-as-new for long loops.** Histories have a practical size limit;
  `ctx.continue_as_new()` starts a fresh execution with carried-over state.
  `ctx.continue_as_new_suggested()` tells you when.
- **Child workflows** for fan-out where each unit deserves its own history and
  retry policy.
- **Search attributes** (`upsert_search_attributes`) so you can find executions by
  business key in the UI instead of scrolling.

## Reference

- SDK source and examples: <https://github.com/temporalio/sdk-rust> — `crates/sdk/examples/`
  is the most reliable documentation available today
- API docs: <https://docs.rs/temporalio-sdk>
- Concepts (language-agnostic, all correct): <https://docs.temporal.io>

> The Rust SDK is **Public Preview**. The API will change before 1.0. Pin exact
> versions and read the changelog on upgrade.

---

*Verified in this repo: `cargo build --workspace`, `cargo clippy --workspace --all-targets`
(clean), and `cargo test --workspace` (6 activity tests passing) against
temporalio-sdk 0.7.0 on rustc 1.94.*
