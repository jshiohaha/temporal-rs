//! The smallest useful Temporal program: one Workflow that calls one Activity.
//!
//! Read this file top to bottom -- it is the whole mental model.

use std::time::Duration;

use temporalio_macros::{activities, workflow, workflow_methods};
use temporalio_sdk::{
    ActivityOptions, WorkflowContext, WorkflowResult,
    activities::{ActivityContext, ActivityError},
};

/// The task queue both the Worker and the Starter must agree on.
///
/// A task queue is just a name. The Client writes tasks to it, the Worker polls
/// it. If these two strings ever disagree, the Workflow will start and then sit
/// forever in "Running" with nothing picking it up -- that is the single most
/// common first-time mistake.
pub const TASK_QUEUE: &str = "hello-world";

// ---------------------------------------------------------------------------
// Activity
// ---------------------------------------------------------------------------

/// Activities live on a plain struct. The struct is the unit of registration,
/// and (see the `order-pipeline` crate) the place dependencies get injected.
pub struct GreetingActivities;

#[activities]
impl GreetingActivities {
    /// An Activity is where anything non-deterministic belongs: network calls,
    /// database writes, clocks, randomness, file IO.
    ///
    /// Its result is written into the Workflow's Event History, so on replay
    /// the Workflow gets the recorded answer instead of running this again.
    #[activity]
    pub async fn greet(_ctx: ActivityContext, name: String) -> Result<String, ActivityError> {
        // Pretend this is an HTTP call to a greeting service.
        Ok(format!("Hello, {name}!"))
    }
}

// ---------------------------------------------------------------------------
// Workflow
// ---------------------------------------------------------------------------

/// A Workflow is a struct; its fields are the durable state that survives
/// process restarts. This one is stateless, hence the unit struct.
#[workflow]
#[derive(Default)]
pub struct HelloWorldWorkflow;

#[workflow_methods]
impl HelloWorldWorkflow {
    /// `#[run]` is the entry point. The signature -- input type and output type
    /// -- is what the Client is type-checked against when it starts the Workflow.
    ///
    /// This code must be deterministic: given the same Event History it must
    /// make the same sequence of calls every time. No `std::time::SystemTime`,
    /// no `rand`, no `tokio::spawn`, no direct IO. Use the `ctx` helpers instead.
    #[run]
    pub async fn run(ctx: &mut WorkflowContext<Self>, name: String) -> WorkflowResult<String> {
        let greeting = ctx
            .execute_activity(
                GreetingActivities::greet,
                name,
                // A close timeout is mandatory. `start_to_close` is the wall-clock
                // budget for one attempt of the Activity.
                ActivityOptions::start_to_close_timeout(Duration::from_secs(10)),
            )
            .await?;

        Ok(greeting)
    }
}
