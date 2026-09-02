//! The Starter: an ordinary Client that kicks off a Workflow and waits.
//!
//! In a real system this is your HTTP handler, CLI, or cron job. It does not
//! need to be the same process (or even the same language) as the Worker.

use hello_world::{HelloWorldWorkflow, TASK_QUEUE};
use temporalio_client::{
    Client, ClientOptions, Connection, WorkflowGetResultOptions, WorkflowStartOptions,
    envconfig::LoadClientConfigProfileOptions,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (conn_opts, client_opts) =
        ClientOptions::load_from_config(LoadClientConfigProfileOptions::default())?;
    let connection = Connection::connect(conn_opts).await?;
    let client = Client::new(connection, client_opts)?;

    let name = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "Temporal".to_string());

    let handle = client
        .start_workflow(
            // Note this is the *method*, not a string. The SDK derives the
            // Workflow type name and type-checks the input and output for you.
            HelloWorldWorkflow::run,
            name,
            // The Workflow ID is your business identifier and your dedupe key.
            // Reusing an ID is how Temporal gives you start-idempotency --
            // see `order-pipeline` for the full story.
            WorkflowStartOptions::new(TASK_QUEUE, "hello-world-workflow-id").build(),
        )
        .await?;

    println!("started workflow, run_id: {:?}", handle.run_id());

    // `start_workflow` returns as soon as the server has durably recorded the
    // start. Waiting for the result is a separate, resumable call: you can drop
    // this process and re-attach later with `client.get_workflow_handle`.
    let result: String = handle
        .get_result(WorkflowGetResultOptions::default())
        .await?;
    println!("workflow result: {result}");

    Ok(())
}
