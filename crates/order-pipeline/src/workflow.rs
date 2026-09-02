//! The Workflow: durable orchestration, zero IO.
//!
//! Everything here is replayed from history on every Workflow Task, so it must
//! be deterministic. What it *does* get in exchange: the state in this struct
//! survives worker crashes, deploys, and multi-day waits, with no database of
//! your own.

use std::time::Duration;

use serde::{Deserialize, Serialize};
use temporalio_common::RetryPolicy;
use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{
    ActivityOptions, SyncWorkflowContext, WorkflowContext, WorkflowContextView, WorkflowResult,
};

use crate::activities::{ChargePayment, OrderActivities, ReserveInventory, ShipOrder, error_types};

pub const TASK_QUEUE: &str = "order-pipeline";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderRequest {
    pub order_id: String,
    pub sku: String,
    pub quantity: u32,
    pub amount_cents: u64,
    pub parcel_count: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderStatus {
    Reserving,
    Charging,
    Shipping,
    Completed,
    Compensating,
    Failed,
}

// ---------------------------------------------------------------------------
// Retry policies
// ---------------------------------------------------------------------------
//
// Read these as: "under what circumstances is trying again the right move?"
// Temporal retries Activities *forever* by default (maximum_attempts = 0 means
// unlimited). That is usually what you want for a transient dependency, and
// almost never what you want for a request that is simply invalid -- which is
// what `non_retryable_error_types` is for.

fn payment_options() -> ActivityOptions {
    ActivityOptions::with_start_to_close_timeout(Duration::from_secs(30))
        .retry_policy(
            RetryPolicy::builder()
                .initial_interval(Duration::from_secs(1))
                .backoff_coefficient(2.0)
                .maximum_interval(Duration::from_secs(30))
                // Give up eventually: a payment that has failed 5 times is a
                // business problem, not a blip.
                .maximum_attempts(5)
                // These match `ApplicationFailure::type_name`. A declined card
                // fails the Activity immediately, no backoff, no retries.
                .non_retryable_error_types([error_types::CARD_DECLINED])
                .build(),
        )
        .build()
}

fn inventory_options() -> ActivityOptions {
    ActivityOptions::with_start_to_close_timeout(Duration::from_secs(10))
        .retry_policy(
            RetryPolicy::builder()
                .maximum_attempts(3)
                .non_retryable_error_types([error_types::OUT_OF_STOCK])
                .build(),
        )
        .build()
}

fn shipping_options() -> ActivityOptions {
    ActivityOptions::with_start_to_close_timeout(Duration::from_secs(300))
        // With a heartbeat timeout, a dead worker is detected in 5s rather than
        // after the full 300s start-to-close budget.
        .heartbeat_timeout(Duration::from_secs(5))
        .build()
}

/// Compensations should be near-unconditional, so they retry indefinitely with
/// a modest cap rather than giving up and leaving money on the floor.
fn compensation_options() -> ActivityOptions {
    ActivityOptions::with_start_to_close_timeout(Duration::from_secs(30))
        .retry_policy(RetryPolicy::builder().maximum_attempts(10).build())
        .build()
}

// ---------------------------------------------------------------------------
// Workflow
// ---------------------------------------------------------------------------

/// Fields are durable state. They are *not* serialized to the server -- they
/// are rebuilt by replaying history -- but from your code's point of view they
/// behave like a process that never dies.
#[workflow]
pub struct OrderWorkflow {
    request: OrderRequest,
    status: OrderStatus,
    reservation_id: Option<String>,
    charge_id: Option<String>,
    cancel_requested: bool,
}

#[workflow_methods]
impl OrderWorkflow {
    /// `#[init]` receives the Workflow input and builds the initial state.
    /// When a Workflow has an `#[init]`, the input goes here rather than to
    /// `#[run]`.
    #[init]
    fn new(_ctx: &WorkflowContextView, request: OrderRequest) -> Self {
        Self {
            request,
            status: OrderStatus::Reserving,
            reservation_id: None,
            charge_id: None,
            cancel_requested: false,
        }
    }

    #[run]
    pub async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<String> {
        let request = ctx.state(|s| s.request.clone());

        // ---- Idempotency keys ------------------------------------------
        //
        // THE key insight. These are derived from the Workflow ID, which means
        // they are:
        //   * identical across every retry of the Activity,
        //   * identical across replay after a worker crash,
        //   * different for a genuinely different order.
        //
        // Generating the key *inside* the Activity would defeat the whole
        // point: each attempt would invent a new one and the dedupe check would
        // never hit. Derive it in the Workflow, pass it down.
        //
        // `ctx.uuid4()` is also safe here -- it is seeded deterministically from
        // the run, so it returns the same value on replay. `Uuid::new_v4()` is
        // NOT safe and will corrupt your history.
        let workflow_id = ctx.workflow_id().to_string();
        let reservation_key = format!("{workflow_id}:reserve");
        let payment_key = format!("{workflow_id}:charge");

        // ---- Step 1: reserve inventory ---------------------------------
        let reservation_id = ctx
            .execute_activity(
                OrderActivities::reserve_inventory,
                ReserveInventory {
                    order_id: request.order_id.clone(),
                    sku: request.sku.clone(),
                    quantity: request.quantity,
                    idempotency_key: reservation_key,
                },
                inventory_options(),
            )
            .await?;
        ctx.state_mut(|s| {
            s.reservation_id = Some(reservation_id.clone());
            s.status = OrderStatus::Charging;
        });

        // ---- Step 2: charge payment ------------------------------------
        //
        // From here on a failure means we owe the customer a compensation, so
        // we stop using `?` and handle the error explicitly.
        let charge_result = ctx
            .execute_activity(
                OrderActivities::charge_payment,
                ChargePayment {
                    order_id: request.order_id.clone(),
                    amount_cents: request.amount_cents,
                    idempotency_key: payment_key,
                },
                payment_options(),
            )
            .await;

        let charge_id = match charge_result {
            Ok(id) => id,
            Err(e) => {
                // Payment never succeeded, so only inventory needs undoing.
                ctx.state_mut(|s| s.status = OrderStatus::Compensating);
                Self::release_inventory(ctx, &reservation_id).await;
                ctx.state_mut(|s| s.status = OrderStatus::Failed);
                return Err(e.into());
            }
        };
        ctx.state_mut(|s| {
            s.charge_id = Some(charge_id.clone());
            s.status = OrderStatus::Shipping;
        });

        // A signal may have arrived while we were charging. Signals are
        // delivered between Workflow Tasks, so checking state at step
        // boundaries is the natural place to react.
        if ctx.state(|s| s.cancel_requested) {
            ctx.state_mut(|s| s.status = OrderStatus::Compensating);
            Self::refund(ctx, &charge_id).await;
            Self::release_inventory(ctx, &reservation_id).await;
            ctx.state_mut(|s| s.status = OrderStatus::Failed);
            return Ok("cancelled by request; fully compensated".to_string());
        }

        // ---- Step 3: ship ----------------------------------------------
        let ship_result = ctx
            .execute_activity(
                OrderActivities::ship_order,
                ShipOrder {
                    order_id: request.order_id.clone(),
                    parcel_count: request.parcel_count,
                },
                shipping_options(),
            )
            .await;

        match ship_result {
            Ok(summary) => {
                ctx.state_mut(|s| s.status = OrderStatus::Completed);
                Ok(summary)
            }
            Err(e) => {
                // Compensate in reverse order: refund first, then release.
                ctx.state_mut(|s| s.status = OrderStatus::Compensating);
                Self::refund(ctx, &charge_id).await;
                Self::release_inventory(ctx, &reservation_id).await;
                ctx.state_mut(|s| s.status = OrderStatus::Failed);
                Err(e.into())
            }
        }
    }

    // -- message handlers ------------------------------------------------

    /// A signal is a fire-and-forget message into a running Workflow. Sync
    /// handlers may mutate `&mut self` directly.
    #[signal]
    pub fn request_cancel(&mut self, _ctx: &mut SyncWorkflowContext<Self>) {
        self.cancel_requested = true;
    }

    /// A query reads state without advancing history. It must be side-effect
    /// free and synchronous -- you cannot await an Activity in here.
    #[query]
    pub fn status(&self, _ctx: &WorkflowContextView) -> OrderStatus {
        self.status
    }

    #[query]
    pub fn charge_id(&self, _ctx: &WorkflowContextView) -> Option<String> {
        self.charge_id.clone()
    }

    // -- compensation helpers (plain associated fns, not handlers) -------

    async fn refund(ctx: &mut WorkflowContext<Self>, charge_id: &str) {
        if let Err(e) = ctx
            .execute_activity(
                OrderActivities::refund_payment,
                charge_id.to_string(),
                compensation_options(),
            )
            .await
        {
            // A failed compensation is an operational alarm, not a reason to
            // crash the Workflow -- it would only lose the rest of the cleanup.
            tracing::error!(error = %e, "refund failed; manual intervention required");
        }
    }

    async fn release_inventory(ctx: &mut WorkflowContext<Self>, reservation_id: &str) {
        if let Err(e) = ctx
            .execute_activity(
                OrderActivities::release_inventory,
                reservation_id.to_string(),
                compensation_options(),
            )
            .await
        {
            tracing::error!(error = %e, "inventory release failed");
        }
    }
}
