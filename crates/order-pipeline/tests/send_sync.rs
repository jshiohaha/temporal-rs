//! Where the `Send + Sync` boundary actually sits.
//!
//! These are compile-time assertions: if the SDK changes what is thread-safe,
//! this test file stops compiling. It exists to document the boundary, because
//! "workflow contexts are `!Send`" is easy to over-generalize into "nothing in
//! this SDK is `Send`", which is not true.
//!
//! The rule: **everything outside a running workflow is `Send + Sync`. The
//! workflow context and its view are not, because workflows are replayed on a
//! single thread.**

use std::sync::Arc;

use order_pipeline::{
    activities::OrderActivities,
    deps::{Database, Deps, PaymentGateway},
    workflow::{OrderRequest, OrderStatus},
};

fn assert_send<T: Send>() {}
fn assert_sync<T: Sync>() {}
fn assert_send_sync<T: Send + Sync>() {}

#[test]
fn client_and_worker_types_are_thread_safe() {
    // The whole client/worker layer is ordinary multi-threaded Rust. You can
    // put a Client in an Arc, share it across tasks, hold it in axum state.
    assert_send_sync::<temporalio_client::Client>();
    assert_send_sync::<temporalio_client::Connection>();
    assert_send_sync::<temporalio_sdk::Runtime>();
    assert_send_sync::<temporalio_sdk::WorkerOptions>();

    // `Worker` is the one asymmetric case: Send but NOT Sync. It owns the
    // single-threaded workflow cache (a `RefCell<HashMap<_, WorkflowData>>`),
    // so you can MOVE a worker into a spawned task, but you cannot share one
    // behind an `&` across threads. In practice you call `worker.run()` from
    // one task and that is the whole lifecycle, so this rarely bites.
    assert_send::<temporalio_sdk::Worker>();
    assert_send_sync::<temporalio_client::WorkflowStartOptions>();
}

#[test]
fn activity_side_is_thread_safe() {
    // Activities run on the multi-threaded tokio runtime, so the context and
    // everything you inject must cross threads.
    assert_send_sync::<temporalio_sdk::activities::ActivityContext>();
    assert_send_sync::<temporalio_sdk::activities::ActivityInfo>();

    // Your injected dependencies -- the DI container -- must be Send + Sync,
    // which is exactly why `Deps` holds `tokio::sync::Mutex` and atomics rather
    // than `RefCell` and `Cell`.
    assert_send_sync::<Deps>();
    assert_send_sync::<Arc<Deps>>();
    assert_send_sync::<Database>();
    assert_send_sync::<PaymentGateway>();
    assert_send_sync::<OrderActivities>();
}

#[test]
fn payload_types_are_thread_safe() {
    // Anything that crosses the wire is a plain serde type, so it is as
    // thread-safe as you make it.
    assert_send_sync::<OrderRequest>();
    assert_send_sync::<OrderStatus>();
    assert_send::<temporalio_common::RetryPolicy>();
    assert_sync::<temporalio_common::RetryPolicy>();
}

// ---------------------------------------------------------------------------
// The other side of the boundary
// ---------------------------------------------------------------------------
//
// `WorkflowContext<W>` and `WorkflowContextView` are NOT Send or Sync -- they
// are built on `Rc<RefCell<..>>`. Uncommenting either line below is a compile
// error, which is the point:
//
//     assert_send::<temporalio_sdk::WorkflowContext<OrderWorkflow>>();
//     assert_send::<temporalio_sdk::WorkflowContextView>();
//
// The consequence is confined but real: a future that holds the workflow
// context across an await is itself `!Send`, so inside workflow code you must
// use `LocalBoxFuture` rather than `BoxFuture`, and you cannot call a helper
// that demands a `Send` future. Everything above this line is unaffected.
