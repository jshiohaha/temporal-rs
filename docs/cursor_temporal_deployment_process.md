# Temporal deployment process
_Exported on 9/2/2026 at 14:09:23 EDT from Cursor (3.18.9)_

---

links

https://docs.temporal.io/production-deployment/worker-deployments/worker-versioning
https://www.youtube.com/watch?v=rm4BlD9WXqc

---

**User**

what does temporal deploy process usually look like? aka when i have an updated workflow or smth, how should i think about that being deployed to remote infra? presumably it shoulnd't just hot overwrite what exists b/c that could disrupt what's there, etc

---

**Cursor**

I’ll look at how this repo frames Temporal deploys, then map that onto the usual worker/versioning model so the “don’t hot-overwrite in-flight workflows” part is concrete.

The repo already flags replay and worker versioning as the reason a deploy isn’t a live overwrite. I’ll pull those bits next so the remote-infra picture matches how Temporal actually works.

You’re right that you should not think of this as “replace the running program.” Temporal deploys are about **workers**, and the dangerous part is **replaying old histories against new workflow code**, not the restart itself.

## What actually gets deployed

The Temporal server never runs your code. It stores Event History and hands out tasks. A deploy is: **build a new worker process, have it poll a task queue, take the old ones down**.

Worker processes coming and going is the happy path. Kill a worker mid-run and another one picks the same execution up by **replaying history from the top**. That is recovery, not a special deploy feature.

So the restart is fine. The question is whether the **new binary still produces the same command sequence** for in-flight runs.

## Why a hot overwrite can break things

On the next Workflow Task, the new worker re-runs the workflow function against the existing history. If the code now:

- schedules a different activity,
- awaits in a different order,
- adds/removes a timer, signal wait, or child workflow,
- takes a different branch,

the server sees a **nondeterminism** error and the run wedges. That shows up in production, not in a fresh start of the new version.

This is why this repo’s README says: *changing workflow code breaks running workflows* — use `ctx.patched("my-change-v2")` or version the task queue.

**Activities are different.** Their results are already in history; the activity function is not replayed. You can usually ship an activity bugfix (new HTTP client, extra logging, a real retry) by rolling workers. The constraint is input/output compatibility and timeouts, not replay.

## How to think about a change

Ask: **would an in-flight execution, replayed on this binary, make the same Temporal commands it already recorded?**

| Kind of change | Typical deploy |
|---|---|
| Activity implementation, deps, infra | Rolling restart. Old and new workers can coexist. |
| Additive payload field (named struct + default) | Usually safe. |
| Workflow logic that changes command sequence | Must version: patch, or pin old runs to old workers. |
| Tuple arity / renamed activity type / incompatible payload | Breaking. Version or migrate carefully. |

## Three playbooks people actually use

**1. Compatible roll (most deploys)**  
New workers join the queue, old ones drain (`graceful_shutdown_period`), then exit. In-flight workflows keep running. This is the default for activity-only and truly compatible workflow edits.

**2. Patches (`patched` / `GetVersion`)**  
Same task queue, same workflow type, one binary that can replay **both** histories:

- old runs take the old branch,
- new runs take the new branch,
- once old executions have drained, you delete the old branch.

Adopt this **before** the first production workflow change, not after.

**3. Worker generations (task-queue versioning or Worker Versioning)**  
When the change is too big for a patch, or you do not want old and new logic in one binary:

- Classic: `order-v1` and `order-v2` queues. Keep v1 workers until those runs finish; starters send **new** work to v2.
- Modern: [Worker Versioning](https://docs.temporal.io/production-deployment/worker-deployments/worker-versioning) — each worker announces a build/version; the server pins an execution to a compatible worker and you **ramp** *new* starts (0% → 1% → 100%). Existing runs stay on the version they started on until they complete (or you explicitly move them, e.g. via continue-as-new).

The Rust SDK inherits this from Core; you are not waiting on a Rust-only feature.

Long-running loops often **continue-as-new** onto the new version so they hop without carrying an infinite history.

## What a remote deploy usually looks like

A typical pipeline is not “kubectl set image and hope”:

1. **CI replay tests** — `WorkflowReplayer` against recorded production histories. That is how you catch a determinism break *before* it hits open runs.
2. **Build a worker image** (git SHA as the version).
3. **Roll workers** with a graceful shutdown: stop polling, finish the current Workflow Task, exit. The workflow is not killed; it waits for the next poller.
4. **If the workflow graph changed**: either the binary contains `patched()` branches, or the new version is a new Worker Deployment / queue and new starts are ramped.
5. **Drain old versions** — watch open execution count (and workflow-task failure rate). Retire v1 only when nothing is pinned to it.

The server stays up the whole time. You are not redeploying Temporal; you are replacing pollers.

## One-line mental model

**Replacing workers is crash recovery. Replacing workflow *logic* is a compatibility problem.** Activities you can usually overwrite. Workflows you version — with patches for small evolutions, and with worker/task-queue generations when you want a clean cut.

---

**User**

how would you achieve this?

1. Compatible roll (most deploys)
New workers join the queue, old ones drain (graceful_shutdown_period), then exit. In-flight workflows keep running. This is the default for activity-only and truly compatible workflow edits.

---

**Cursor**

I’ll look at how this repo’s workers start and shut down, then map that onto a concrete compatible-roll (new pollers in, old ones drain).

I’ll check the SDK’s worker shutdown APIs so the roll can be described in terms of what `Worker::run` actually does, not generic k8s folklore.

A compatible roll is **overlap plus a polite SIGTERM**, not waiting for every workflow to finish. In-flight *executions* stay on the server; you only drain the *tasks currently in that process*.

This repo’s workers do not do that yet. `order-worker` just blocks on `worker.run()` with no shutdown handle, so a deploy today is “kill the process.” The rest of the machinery is already there in the SDK.

## What has to be true first

Same task queue name (`TASK_QUEUE`), same workflow/activity type names, and new workflow code that still replays old histories. If that last part is false, this pattern *will* wedge open runs — that is the patched / versioned-worker path, not this one.

CI should prove it with `WorkflowReplayer` against recorded histories before the image ships.

## What “drain” actually means

When an old worker gets SIGTERM:

1. It **stops polling**. New workflow/activity tasks go to whoever is still polling (the new pods).
2. The **current Workflow Task** finishes — that is usually milliseconds of CPU between awaits, not the whole order.
3. **In-flight activities** get `graceful_shutdown_period` to complete. After that the SDK cancels them. A cancelled/crashed activity retries on another worker if the retry policy allows it.
4. Workflows parked on a timer or signal are not on the worker at all. They sit in history until some other poller takes the next task.

You are draining *slots on this machine*, not draining the business process.

Sticky cache is the only extra wrinkle: the dying worker owned some cached executions. After `sticky_queue_schedule_to_start_timeout` (default 10s) those tasks bounce back to the shared queue and a new worker replays them.

## Worker process

Grab `shutdown_handle()` **before** `run()`, wire it to SIGTERM/SIGINT, and set a grace period:

```rust
use std::time::Duration;
use tokio::signal;

let worker_options = WorkerOptions::new(TASK_QUEUE)
    .register_workflow::<OrderWorkflow>()?
    .register_activities(OrderActivities::new(Arc::clone(&deps)))
    .graceful_shutdown_period(Duration::from_secs(30))
    .build();

let mut worker = Worker::new(&runtime, client, worker_options)?;
let shutdown = worker.shutdown_handle();

tokio::spawn(async move {
    let _ = signal::ctrl_c().await;
    shutdown();
});

worker.run().await?;
```

On Linux in a container you also want `SIGTERM` (`tokio::signal::unix`). `ctrl_c` is SIGINT; Kubernetes sends SIGTERM.

`graceful_shutdown_period` is the SDK’s “let running activities finish, then cancel.” After that, Core still waits for the current workflow task. Hung local activities can block shutdown — keep those short.

Long activities should already heartbeat and honor cancel, which `ship_parcels` already does:

```224:226:crates/order-pipeline/src/activities.rs
            if ctx.is_cancelled() {
```

Without heartbeats, a killed activity just looks like a crash until `start_to_close` / heartbeat timeout, then it retries. That is recoverable, just slower and noisier.

## Orchestrator

The Temporal server does not participate. Your scheduler has to **start new pollers before it stops old ones**.

On Kubernetes that is a rolling update, not Recreate:

```yaml
spec:
  replicas: 3
  strategy:
    type: RollingUpdate
    rollingUpdate:
      maxUnavailable: 0   # never drop below 3 pollers
      maxSurge: 1         # one extra pod of the new image first
  template:
    spec:
      terminationGracePeriodSeconds: 45  # > graceful_shutdown_period
```

Sequence for one old pod:

1. New pod starts, connects, polls the **same** queue.
2. Kubelet SIGTERM’s the old pod.
3. `shutdown_handle()` runs → stop polling → 30s for activities → `run()` returns → process exits.
4. If it is still alive at 45s, SIGKILL. Those activities then retry elsewhere.

`terminationGracePeriodSeconds` must be **greater** than `graceful_shutdown_period`, or kubelet kills the process while the SDK still thinks it is draining.

Same idea on ECS/Nomad/systemd: min healthy percent 100%, stop timeout > grace period, SIGTERM not SIGKILL.

One replica can still work if you surge first (`maxUnavailable: 0`, `maxSurge: 1`): there is a moment with two pollers, then the old one drains. With Recreate or `maxUnavailable: 100%` you get a gap where nothing polls and tasks just sit.

## What you do *not* change

- Task queue name
- Workflow type string
- A Worker Deployment / Build ID ramp — that is the incompatible-change path
- Waiting for open workflow count to hit zero — that could be days

You ship a new image of the same worker, pointed at the same queue, and let the replica set overlap.

## What to watch during the roll

- `workflow_task_schedule_to_start_latency` — spikes if you dropped all pollers
- Workflow task failure rate — the first place a stealthy nondeterminism error shows up
- Activity retries / cancellations — expected if you SIGTERM mid-activity; should recover on the new pods

If task-failure rate jumps and stays up, the change was not compatible. Roll back the image (old binary can still replay; that is why rollback works on this path) and treat the next attempt as a patch or a new worker generation.

