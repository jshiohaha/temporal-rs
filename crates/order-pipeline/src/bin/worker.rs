//! The composition root: build dependencies once, inject them into Activities.

use std::sync::Arc;

use order_pipeline::{
    activities::OrderActivities,
    deps::Deps,
    workflow::{OrderWorkflow, TASK_QUEUE},
};
use temporalio_client::{
    Client, ClientOptions, Connection, envconfig::LoadClientConfigProfileOptions,
};
use temporalio_sdk::{Runtime, Worker, WorkerOptions};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,temporalio=warn".into()),
        )
        .init();

    // 1. Build your dependencies exactly as you would in any Rust service.
    //    Connection pools, HTTP clients, config -- Temporal has no opinion.
    let deps: Arc<Deps> = Deps::from_env().await?;

    // 2. Standard Temporal plumbing.
    let runtime = Runtime::new_assume_tokio(Default::default())?;
    let (conn_opts, client_opts) =
        ClientOptions::load_from_config(LoadClientConfigProfileOptions::default())?;
    let connection = Connection::connect(conn_opts).await?;
    let client = Client::new(connection, client_opts)?;

    // 3. Inject. `register_activities` takes an *instance*, and the SDK wraps
    //    it in an `Arc` so every Activity invocation gets a cheap clone of the
    //    same shared state. This is the whole DI story -- there is no container,
    //    no macro, no registry. It is just a struct you construct.
    //
    //    You can call `register_activities` more than once with different
    //    structs, which is the natural way to group Activities by bounded
    //    context while sharing a pool between them.
    let worker_options = WorkerOptions::new(TASK_QUEUE)
        .register_workflow::<OrderWorkflow>()?
        .register_activities(OrderActivities::new(Arc::clone(&deps)))
        .build();

    let mut worker = Worker::new(&runtime, client, worker_options)?;
    tracing::info!(task_queue = TASK_QUEUE, "order worker started");
    worker.run().await?;

    Ok(())
}
