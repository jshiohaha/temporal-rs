# Workflow execution model
_Exported on 9/2/2026 at 13:55:03 EDT from Cursor (3.18.9)_

---

**User**

@crates/order-pipeline/src/workflow.rs:128-129 so 1 thing i don't really get is that there's a single workflow run function with mutliple activity calls. how do we ensure that activities are not re-called for a given workflow if it partially fails part way through? what's the right mental model i should use to think about execution? is this evident in the ui what's happening? i think i've seen a timeline of events, so maybe i can see each activity executed until a terminal state?

---

**Cursor**

I'll read the workflow and how activities are invoked so I can explain the execution model against your actual code.

The `run` function is not “start over from scratch.” It is a **deterministic replay of event history**. Temporal records every command and result on the server; on the next Workflow Task it re-runs `run()` from the top, but completed activities are filled in from history instead of being scheduled again.

## Mental model: a script over a log

Think of `run` as a script that walks a growing event log:

1. First execution: `execute_activity(reserve_inventory)` is a **command**. The workflow yields. A worker actually runs the activity. The result is appended to history as `ActivityTaskCompleted`.
2. Then `execute_activity(charge_payment)` does the same.
3. If the worker dies after reserve succeeded but before charge finishes, Temporal **re-runs `run()` from line 128**. When it hits reserve again, the SDK sees that event already in history and **returns the recorded `reservation_id` without scheduling anything**. Charge has no completion event yet, so that one is scheduled for real.

So the function looking linear is the point: you write “do A, then B, then C,” and the runtime turns that into “replay A from history, then do B if it has not completed.”

The yield points are the Temporal `.await`s (`execute_activity`, timers, signals). Everything between them is local CPU that must be deterministic, which is why your file comments say no clocks, no `Uuid::new_v4()`, no IO in the workflow.

## Two different “don’t run it twice”

These get mixed together a lot.

**Replay (Temporal’s job).** A completed activity is never re-scheduled. After reserve is in history, later replays skip it. Your `reservation_id` / `charge_id` / `status` fields are **not** stored as a row on the server; they are rebuilt by walking history, which is why `ctx.state_mut` “survives” crashes.

**Retries of an in-flight activity (your job).** If `charge_payment` hits the gateway, succeeds, then the activity worker dies **before** reporting success, Temporal has **no** completion event. It will retry that same activity. History does not protect you there — **idempotency keys** do. That is why the keys are derived in the workflow (`{workflow_id}:charge`) and passed in, not generated inside the activity.

So: history = “don’t schedule a finished step again.” Idempotency = “if this attempt is retried, don’t double-charge.”

## What “partially fails part way through” actually means

| What happened | What Temporal does |
|---|---|
| Worker crash / deploy after reserve completed | Replay; reserve is not re-called; charge is the next command |
| Activity fails with a **retryable** error | Same activity is retried (attempts 2, 3, …) until policy gives up |
| Activity fails **non-retryable** (`CardDeclined`, `OutOfStock`) | That `execute_activity` returns `Err`; your compensation path runs (`release_inventory`, `refund`) |
| Shipping fails after charge succeeded | Reserve and charge stay completed in history; you **explicitly** schedule refund + release — new events, not a rewind |

Failure does not rewind the log. Completed steps stay completed. Compensation is more workflow code that schedules **new** activities.

## The UI

Yes. In Temporal UI, open the workflow run and look at **History** (and **Timeline** if you have it).

You should see a sequence like:

- `WorkflowExecutionStarted`
- `ActivityTaskScheduled` / `Started` / `Completed` for `reserve_inventory`
- same trio for `charge_payment`
- same for `ship_order`
- `WorkflowExecutionCompleted` (or `Failed`)

If the worker crashed after reserve, you will also see extra **WorkflowTask** events (scheduled / started / timed out / completed) — that is replay — and you will **not** see a second `ActivityTaskScheduled` for reserve.

Retries of one activity show as multiple `ActivityTaskStarted` / `Failed` (or `TimedOut`) under the **same** scheduled activity, with an attempt count. Compensations show up as later, separate activities (`refund_payment`, `release_inventory`).

Pending vs done is also visible: an activity that is scheduled but not completed sits in Pending Activities until it completes, fails terminally, or times out.

**Bottom line:** treat `run` as “replay this log, then continue from the first command that has no result yet.” The timeline in the UI is that log. Completed activities are facts in history; they are not re-executed. The activity code only runs again if that specific step never recorded a completion (retry) or you scheduled a new one (compensation).

---

**User**

and how do activities actually get executed? is it essentially a lambda function or smth that is given some serialized state? and i assume state is preserved through across activity boundaries? what's the right way to think about how it's modelee

---

**Cursor**

I'll look at how the worker registers activities so the execution model matches your code, not just the Temporal docs.

Activities are closer to **RPC jobs on a queue** than to lambdas that capture the workflow. The workflow never ships its struct to the activity. It ships a **name + serialized arguments**. The activity process already has its own long-lived dependencies.

## Two different runtimes, same `run()` function

When `run()` hits this:

```151:162:crates/order-pipeline/src/workflow.rs
        let reservation_id = ctx
            .execute_activity(
                OrderActivities::reserve_inventory,
                ReserveInventory { /* ... */ },
                inventory_options(),
            )
            .await?;
```

the workflow worker does **not** call `reserve_inventory`. It records a command: “schedule activity `reserve_inventory` with this payload.” Then it yields.

Temporal writes that to history and puts an **Activity Task** on the task queue (`order-pipeline`). A worker process polls it, deserializes `ReserveInventory`, and calls the method on the `OrderActivities` instance you built at process start:

```43:45:crates/order-pipeline/src/bin/worker.rs
    let worker_options = WorkerOptions::new(TASK_QUEUE)
        .register_workflow::<OrderWorkflow>()?
        .register_activities(OrderActivities::new(Arc::clone(&deps)))
```

That instance is a normal Rust object: `Arc<Deps>` with a DB and payment client. It lives for the **worker process**, not for the workflow. Every activity invocation is `Arc::clone` of that same struct plus the payload from history.

When the method returns, the result (or error) is serialized back to Temporal, appended as `ActivityTaskCompleted` / `Failed`, and the workflow is woken with a new **Workflow Task**. Replay walks history up to that result, then continues after the `.await`.

So:

| | Workflow | Activity |
|---|---|---|
| What it is | Deterministic orchestrator | Ordinary async function with IO |
| Input | Event history + your args | Serialized payload only |
| Holds | `OrderWorkflow` fields, rebuilt by replay | `OrderActivities` + `Deps`, process-local |
| Can do IO? | No | Yes |

Not a lambda with a captured closure. More like: “enqueue `reserve_inventory(ReserveInventory { ... })`; I’ll resume when the result is in the log.”

## What is and isn’t preserved across activity boundaries

**Workflow fields look preserved.** After reserve completes you do:

```163:166:crates/order-pipeline/src/workflow.rs
        ctx.state_mut(|s| {
            s.reservation_id = Some(reservation_id.clone());
            s.status = OrderStatus::Charging;
        });
```

Those fields are still there when you reach charge. That is not because Temporal checkpointed the struct into the activity and back. The activity never saw `OrderWorkflow` at all. On the next Workflow Task, Temporal **replays `run()`**, `execute_activity(reserve)` returns the recorded id, then your `state_mut` runs again and the struct looks the same.

Mental model: workflow state is **derived from history**, not a blob passed around. `reservation_id` survives because “reserve completed with `resv_…`” is in the log, and your code writes it into the struct on every replay.

**Activities do not see workflow state.** They only get what you put in the input struct (`order_id`, `sku`, `idempotency_key`, …). If you need something on the other side of the await, pass it explicitly, or keep it in workflow fields and use it after the activity returns.

**Activity process state is a different thing.** `Deps` (pools, HTTP clients, your in-memory fake DB) is shared across all activity calls on that worker. That is service-level state, not per-workflow state. A new worker process starts with empty fake DB unless you pointed it at a real one.

**The one Temporal-kept activity-side crumb is heartbeats.** `ship_order` records `parcel` progress; on retry, `heartbeat_details()` gives that back. That is retry-resume for **one activity attempt chain**, not workflow state.

## How to model it in your head

```
Starter  --start(OrderRequest)-->  Temporal server (event history)
                                         ^
                                         |  Workflow Task: "replay run() until next command"
                                         v
                                   Workflow worker
                                   (deterministic, no IO)
                                         |
                                         |  command: ScheduleActivity(name, bytes)
                                         v
                                   Temporal server
                                         |
                                         |  Activity Task on the queue
                                         v
                                   Activity worker
                                   OrderActivities { deps }
                                   deserialize args → call method → serialize result
```

Three layers of “state,” don’t conflate them:

1. **Event history** (Temporal server) — source of truth: what was scheduled, what completed, with what payload.
2. **Workflow struct** (`request`, `status`, `reservation_id`, …) — a cache you rebuild by replaying that history. Convenient, durable from *your* point of view, not a snapshot sent to activities.
3. **Worker / `Deps`** — real IO handles. Exist only while that process is up. Activities use these; workflows must not.

The payload on the wire is just serde: `ReserveInventory` in, `String` reservation id out. Same as calling a service, except Temporal is the bus, retries and history are built in, and the caller is a replayable script rather than a normal process.

---

**User**

so is the call to an activity kind of like a kv cache fronted function call in that the first call will produce a cache miss, execute the activity, and associate a durable id with the output? then the next attempt will lookup the id and see the output?

---

**Cursor**

Yes — **for replay**, that is the right picture, with one important difference in how the key works.

On the first pass, `execute_activity(reserve_inventory)` is a miss. Temporal schedules the work, the worker runs it, and the **result is written into event history**. The next time `run()` hits that same call (worker crash, new Workflow Task, continue-as-new is a different story), the SDK looks at history, finds the recorded completion, and returns it **without calling the activity**.

What is *not* like a typical KV cache: the key is **not** `(function, args)`. It is **position in this run’s command log**.

- First `execute_activity` in `run()` matches the first `ActivityTaskScheduled` / `Completed` pair.
- Second call matches the second pair, even if the payloads look identical.
- Reordering calls, inserting a new activity in the middle, or changing a recorded argument is a **non-determinism** error, not a cache miss that re-runs. History and code no longer line up.

So it is more “log-backed memoization by call slot” than “hash the inputs and reuse.” Two charges with the same payload are two slots. A later replay does not skip the second because the first already stored that output.

The other place the analogy breaks is **retries of an activity that never completed**. If `charge_payment` dies before Temporal records `ActivityTaskCompleted`, that slot is still empty. The next attempt is another miss: the activity function runs again. History is not a cache of “we tried this,” only of “this slot finished.”

That gap is why you have a **second**, explicit KV cache inside the activity: `idempotency_key` → `charge_id` in your DB / payment provider. Temporal’s history cache and your idempotency table cover different failures:

| Layer | Key | Hit means |
|---|---|---|
| Event history (replay) | call slot in this workflow run | do not schedule that step again |
| Your `idempotency_key` | `{workflow_id}:charge` | activity code may run again, but do not double-charge |

So: treat `execute_activity` as a memoized RPC keyed by **history index**, filled only when the result is durably recorded. Treat the idempotency key as the cache for “the RPC ran, but the memo never got written.”

---

**User**

what's allowable time between activities? like what happens if we get the order, charge the card, and need to wait an arbitrary time (possibly many days) for the shipping to initiate

---

**Cursor**

I'll check how this SDK models timers and timeouts so the answer matches what you'd actually call between charge and ship.

There is no required gap and no “activity must follow activity soon.” Days or weeks between charge and ship is a normal Temporal pattern. The wait is a **durable timer or a signal**, not a thread sitting on a worker and not a long-running activity.

## What actually happens

After charge completes, you would write something like:

```rust
ctx.timer(Duration::from_days(14)).await; // or wait on a signal

ctx.execute_activity(OrderActivities::ship_order, ...).await?;
```

That `.await` is the same kind of yield as `execute_activity`. The worker **does not sleep**. Temporal records `TimerStarted`, evicts the workflow from memory, and your process is free. Fourteen days later the server fires the timer, a new Workflow Task is queued, `run()` is replayed, charge comes back from history, the timer is already fired, then `ship_order` is scheduled for the first time.

Cost of waiting is essentially zero: no worker thread, no polling loop, no “keep the process alive.” The file comment at the top of `workflow.rs` is describing exactly this.

Your current `ship_order` `start_to_close` of 300s is a budget for **one activity attempt**, not for the idle time before shipping starts. Do not park a multi-day wait inside the activity (`tokio::time::sleep` for days). That holds a worker, burns the activity timeout, and dies if the process restarts.

## What actually limits you

**Workflow execution timeout**, if you set one on start. Unset, Temporal will let the execution run indefinitely. That is the knob for “this order may not sit forever,” not the gap between two activities.

**History size**, not wall-clock time. A timer plus a few activities is a handful of events; months of idle is fine. You only need `continue_as_new` if the workflow keeps appending events (a daily loop for a year, etc.).

**Business constraints Temporal does not know about.** A card authorization can expire in ~7 days even if the workflow is happy to wait 30. If capture must happen near ship time, charge-then-wait-then-ship may be the wrong *payments* model, even though it is a fine *Temporal* model.

## Two ways to “wait until we can ship”

| If you know | Use |
|---|---|
| A deadline (“don’t ship before Friday”) | `ctx.timer(...)` |
| An external event (“warehouse marked ready”) | a **signal**, same as `request_cancel` — workflow parks until the signal, then ships |

You can combine them: timer *or* signal, whichever comes first. Either way the workflow is dormant in history, not running on a machine.

**Bottom line:** time between activities is not a timeout you have to stay under. Park on `ctx.timer` or a signal; Temporal will wake the workflow later, replay past charge, and only then execute ship. The UI will show the timer (or signal) sitting between the charge and ship events, often for days, with the workflow in Running the whole time.

---

**User**

ok nice! i'm still not clear on timer vs signal. can you explain more

---

**Cursor**

I'll use your existing `request_cancel` signal as the concrete example and contrast it with a timer.

They are two different answers to “what are we waiting for?”

A **timer** is the workflow telling Temporal: wake me at a time I already know. A **signal** is the outside world telling the workflow: something happened that I could not know in advance.

## Timer — time is the event

```rust
ctx.timer(Duration::from_days(2)).await;
// now schedule ship
```

You set the alarm when you start waiting. Nothing else has to call in. Two days later Temporal appends `TimerFired` to history, the workflow is replayed, and execution continues after the `.await`.

Use it when the wake-up condition is **the clock**: cooling-off period, “don’t ship before Friday,” “retry this check tomorrow,” “auth hold expires in 7 days.”

The worker is not sleeping. Same eviction story as waiting on an activity.

## Signal — an external event

You already have one:

```242:245:crates/order-pipeline/src/workflow.rs
    #[signal]
    pub fn request_cancel(&mut self, _ctx: &mut SyncWorkflowContext<Self>) {
        self.cancel_requested = true;
    }
```

A client (or another service) does `handle.signal(OrderWorkflow::request_cancel, ...)`. Temporal writes `WorkflowExecutionSignaled` into history. That is a message **into** a running workflow: fire-and-forget, may carry a payload, does not return a result.

Use it when the wake-up condition is **someone else’s fact**: warehouse marked packed, user cancelled, payment webhook arrived, ops unblocked a hold.

The workflow does not know *when* that will happen. It only knows *what* it means.

## How the workflow actually “waits” on a signal

A signal handler usually just mutates state. It does not, by itself, pause `run()`. Something in `run()` has to notice.

Your pipeline uses the simple form: **set a flag, check at the next step boundary** (after charge, before ship). If cancel arrived while charging, you see it when you get back. If it arrived earlier, it is already in history and replay sets the flag before you check.

The other form is **block until the flag is set**:

```rust
ctx.wait_condition(|s| s.ready_to_ship).await;
```

That parks the workflow the same way a timer does, except the wake-up is a signal instead of the clock. For “wait until the warehouse is ready, then ship,” that is the usual pattern — plus a `ready_to_ship` signal analogous to `request_cancel`.

## Side by side

| | Timer | Signal |
|---|---|---|
| Who decides the moment? | The workflow, up front | Some other process, later |
| What is in history? | `TimerStarted` / `TimerFired` | `WorkflowExecutionSignaled` |
| Payload? | Just a duration / timestamp | Optional data (tracking number, reason, …) |
| If nobody ever fires it? | It *will* fire | Workflow waits until timeout, cancel, or forever |
| Example in this order | “Ship in 48h” | “User cancelled” / “WMS says packed” |

Queries are a third thing: **read** state (`status`, `charge_id`) without advancing history. They do not wait and they do not change anything.

## The combination you usually want

Shipping is rarely *only* a timer or *only* a signal.

- **Timer only:** you will ship at T even if the warehouse is not ready.
- **Signal only:** if the warehouse never signals, the workflow sits in Running until some other timeout you set.
- **Both:** ship when ready, or give up after 14 days.

That last one is `select` on a timer and `wait_condition` (or a cancel flag). Same idea as your current cancel check, except you are willing to sit idle between charge and ship instead of racing straight into `ship_order`.

**Rule of thumb:** if you can write the wake-up as a `Duration` or a deadline, it is a timer. If you need a human, another service, or a webhook to tell you, it is a signal. The clock is not an external system; the warehouse is.

---

**User**

how is a signal actually fired though

---

**Cursor**

I'll look at how this repo and SDK actually send `request_cancel` so the explanation matches the real call path.

A signal is an RPC to the **Temporal server**, not to your worker. Anything with a Temporal client and the workflow id can fire it. Your worker only finds out later, when it gets the next Workflow Task.

## The call

Same client you already use in `order-starter`. After start you have a handle; signaling looks like the query you already run:

```rust
handle
    .signal(
        OrderWorkflow::request_cancel,
        (), // payload — this signal has none
        WorkflowSignalOptions::default(),
    )
    .await?;
```

`OrderWorkflow::request_cancel` is the typed method, same idea as `OrderWorkflow::run` on start. The `()` is the argument list; a `ready_to_ship(tracking: String)` signal would pass a `String` here and it would be serde’d just like activity input.

You do **not** need the handle from `start_workflow`. A cancel button, a webhook handler, or a warehouse service can connect later and address the run by id:

```rust
let handle = client.workflow_handle::<OrderWorkflow>(format!("order-{order_id}"));
handle.signal(OrderWorkflow::request_cancel, (), Default::default()).await?;
```

That id is the one you chose at start (`order-ord-1001`). That is why deriving workflow id from the business key matters: every other system can find the run without storing a Temporal-specific handle.

You can also fire it from the UI (Signal button on the workflow) or the CLI (`temporal workflow signal --name request_cancel --workflow-id order-ord-1001`). Same server API.

## What happens after `.await`

`signal(...).await` only waits until Temporal **accepts** the message and appends `WorkflowExecutionSignaled` to history. It does **not** wait for `request_cancel` to run, and it does not call your worker.

Then:

1. Server has a new event on that workflow’s history.
2. If the workflow is idle (waiting on an activity, a timer, `wait_condition`), Temporal schedules a Workflow Task.
3. A worker polls that task, **replays** `run()` from the start, and while replaying applies signal handlers when it hits those events in history.
4. Your handler runs in the workflow process:

```243:245:crates/order-pipeline/src/workflow.rs
    pub fn request_cancel(&mut self, _ctx: &mut SyncWorkflowContext<Self>) {
        self.cancel_requested = true;
    }
```

5. `run()` continues. In your code, that just means the next time you check `cancel_requested`, it is true.

If a signal arrives **while** an activity is running, it sits in history until that activity completes (or you cancel the activity). Your comment about “delivered between Workflow Tasks” is that: the handler does not interrupt `charge_payment` mid-flight; you see the flag at the next workflow step.

## Who fires it in a real system

| Source | Typical use |
|---|---|
| HTTP handler / another service | “Cancel this order”, “warehouse packed” |
| Temporal UI / CLI | ops, debugging |
| Another workflow | parent tells child, or you signal by workflow id |

The activity worker is not in this path. Activities talk to Stripe/DB; signals are how **other Temporal clients** poke the orchestrator.

**Short version:** firing a signal is `client` + `workflow_id` + signal name + optional payload. Temporal writes it to history and later wakes the workflow. Your `#[signal]` fn is the handler, not the trigger.

