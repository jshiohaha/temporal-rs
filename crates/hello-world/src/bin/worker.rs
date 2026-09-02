//! The Worker: a long-lived process that polls a task queue and runs your code.
//!
//! Nothing executes without a Worker. The Temporal Server never runs your code;
//! it only stores history and hands out tasks.

use hello_world::{GreetingActivities, HelloWorldWorkflow, TASK_QUEUE};
use temporalio_client::{
    Client, ClientOptions, Connection, envconfig::LoadClientConfigProfileOptions,
};
use temporalio_sdk::{Runtime, Worker, WorkerOptions};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    // The Runtime owns the Core SDK's own tokio threads and telemetry. Create
    // exactly one per process. `new_assume_tokio` reuses the ambient runtime
    // started by `#[tokio::main]`.
    let runtime = Runtime::new_assume_tokio(Default::default())?;

    // Connection settings come from `temporal.toml` and/or TEMPORAL_* env vars,
    // so the same binary points at a dev server or at Temporal Cloud with no
    // code change. See the repo-root `temporal.toml`.
    let (conn_opts, client_opts) =
        ClientOptions::load_from_config(LoadClientConfigProfileOptions::default())?;
    let connection = Connection::connect(conn_opts).await?;
    let client = Client::new(connection, client_opts)?;

    let worker_options = WorkerOptions::new(TASK_QUEUE)
        // Workflows are registered by type: the Worker needs the code.
        .register_workflow::<HelloWorldWorkflow>()?
        // Activities are registered by *instance*: the value you pass here is
        // what the Activity methods will see. This is the dependency-injection
        // seam -- see the `order-pipeline` crate.
        .register_activities(GreetingActivities)
        .build();

    let mut worker = Worker::new(&runtime, client, worker_options)?;
    tracing::info!(task_queue = TASK_QUEUE, "worker started; waiting for tasks");

    // Blocks until shutdown. Poll, execute, report, repeat.
    worker.run().await?;

    Ok(())
}
