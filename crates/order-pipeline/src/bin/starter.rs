//! Start an order and watch it. Try the magic order ids:
//!
//!   cargo run --bin order-starter -- ord-1001          # happy path
//!   cargo run --bin order-starter -- ord-flaky-1002    # gateway fails twice, then succeeds
//!   cargo run --bin order-starter -- ord-decline-1003  # non-retryable; compensates

use order_pipeline::workflow::{OrderRequest, OrderStatus, OrderWorkflow, TASK_QUEUE};
use temporalio_client::{
    Client, ClientOptions, Connection, WorkflowGetResultOptions, WorkflowQueryOptions,
    WorkflowStartOptions, envconfig::LoadClientConfigProfileOptions,
};
// Note: this is the *protobuf* enum, not `temporalio_sdk::WorkflowIdReusePolicy`.
// The two are distinct types with the same name -- the SDK one is for child
// workflows, this one for client starts.
use temporalio_common::protos::temporal::api::enums::v1::WorkflowIdReusePolicy;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (conn_opts, client_opts) =
        ClientOptions::load_from_config(LoadClientConfigProfileOptions::default())?;
    let connection = Connection::connect(conn_opts).await?;
    let client = Client::new(connection, client_opts)?;

    let order_id = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "ord-1001".to_string());

    let request = OrderRequest {
        order_id: order_id.clone(),
        sku: "WIDGET-1".to_string(),
        quantity: 2,
        amount_cents: 4_999,
        parcel_count: 3,
    };

    let handle = client
        .start_workflow(
            OrderWorkflow::run,
            request,
            // Idempotency at the *entry point*. Deriving the Workflow ID from
            // the business key means that if this starter is itself retried --
            // a duplicated queue message, a client timeout, a double-click --
            // you get one order, not two. `RejectDuplicate` turns a repeat start
            // into an error instead of a second execution.
            WorkflowStartOptions::new(TASK_QUEUE, format!("order-{order_id}"))
                .id_reuse_policy(WorkflowIdReusePolicy::RejectDuplicate)
                .build(),
        )
        .await?;

    println!("started order-{order_id}, run_id: {:?}", handle.run_id());

    // Queries read live state without disturbing the Workflow.
    let status: OrderStatus = handle
        .query(OrderWorkflow::status, (), WorkflowQueryOptions::default())
        .await?;
    println!("status right after start: {status:?}");

    match handle.get_result(WorkflowGetResultOptions::default()).await {
        Ok(summary) => println!("completed: {summary}"),
        Err(e) => println!("workflow failed (this is expected for decline/): {e}"),
    }

    Ok(())
}
