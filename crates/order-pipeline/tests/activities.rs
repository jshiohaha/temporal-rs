//! Activity tests. No Temporal server required.
//!
//! `ActivityEnvironment` invokes the Activity exactly as a Worker would --
//! including injecting the registered instance as `self: Arc<Self>` -- so this
//! exercises the real dependency-injection path, not a mock of it.

use std::sync::Arc;

use order_pipeline::{
    activities::{ChargePayment, OrderActivities, ReserveInventory, error_types},
    deps::{Database, Deps, PaymentGateway},
};
use temporalio_sdk::{activities::ActivityError, testing::ActivityEnvironment};

async fn test_deps() -> Arc<Deps> {
    Arc::new(Deps {
        db: Database::connect("memory://test").await.unwrap(),
        payments: PaymentGateway::new("test-key".to_string()),
    })
}

#[tokio::test]
async fn charges_once_and_returns_a_charge_id() {
    let deps = test_deps().await;
    let env = ActivityEnvironment::builder()
        .register_activities(OrderActivities::new(Arc::clone(&deps)))
        .build();

    let charge_id = env
        .run(
            OrderActivities::charge_payment,
            ChargePayment {
                order_id: "ord-1".into(),
                amount_cents: 4_999,
                idempotency_key: "order-ord-1:charge".into(),
            },
        )
        .await
        .expect("charge should succeed");

    assert!(charge_id.starts_with("ch_4999_"));
    assert_eq!(deps.payments.call_count(), 1);
}

/// The important one: this is what makes a retried Activity safe.
#[tokio::test]
async fn repeated_attempts_with_the_same_key_do_not_double_charge() {
    let deps = test_deps().await;
    let env = ActivityEnvironment::builder()
        .register_activities(OrderActivities::new(Arc::clone(&deps)))
        .build();

    let input = ChargePayment {
        order_id: "ord-2".into(),
        amount_cents: 1_200,
        idempotency_key: "order-ord-2:charge".into(),
    };

    // Simulate the Worker crashing after the charge and Temporal retrying:
    // same input, same key, three attempts.
    let first = env
        .run(OrderActivities::charge_payment, input.clone())
        .await
        .unwrap();
    let second = env
        .run(OrderActivities::charge_payment, input.clone())
        .await
        .unwrap();
    let third = env
        .run(OrderActivities::charge_payment, input)
        .await
        .unwrap();

    assert_eq!(first, second);
    assert_eq!(second, third);
    // Only the first attempt ever reached the payment gateway.
    assert_eq!(deps.payments.call_count(), 1);
}

/// A *different* key must produce a genuinely separate charge -- otherwise the
/// dedupe would be swallowing real orders.
#[tokio::test]
async fn different_keys_produce_different_charges() {
    let deps = test_deps().await;
    let env = ActivityEnvironment::builder()
        .register_activities(OrderActivities::new(Arc::clone(&deps)))
        .build();

    let a = env
        .run(
            OrderActivities::charge_payment,
            ChargePayment {
                order_id: "ord-3".into(),
                amount_cents: 100,
                idempotency_key: "order-ord-3:charge".into(),
            },
        )
        .await
        .unwrap();
    let b = env
        .run(
            OrderActivities::charge_payment,
            ChargePayment {
                order_id: "ord-4".into(),
                amount_cents: 200,
                idempotency_key: "order-ord-4:charge".into(),
            },
        )
        .await
        .unwrap();

    assert_ne!(a, b);
    assert_eq!(deps.payments.call_count(), 2);
}

/// A declined card must be reported as non-retryable, or Temporal will back off
/// and retry a request that can never succeed.
#[tokio::test]
async fn declined_card_is_non_retryable() {
    let deps = test_deps().await;
    let env = ActivityEnvironment::builder()
        .register_activities(OrderActivities::new(Arc::clone(&deps)))
        .build();

    let err = env
        .run(
            OrderActivities::charge_payment,
            ChargePayment {
                order_id: "ord-decline-5".into(),
                amount_cents: 999,
                idempotency_key: "order-ord-decline-5:charge".into(),
            },
        )
        .await
        .expect_err("a declined card must fail");

    let msg = format!("{err:?}");
    assert!(
        msg.contains(error_types::CARD_DECLINED),
        "expected the failure to carry the {} type name, got: {msg}",
        error_types::CARD_DECLINED
    );
}

/// The flaky gateway fails twice then succeeds -- this is what the retry policy
/// is there to ride out. Note each `env.run` is one *attempt*; the environment
/// does not apply retry policies for you.
#[tokio::test]
async fn flaky_gateway_succeeds_on_the_third_attempt() {
    let deps = test_deps().await;
    let env = ActivityEnvironment::builder()
        .register_activities(OrderActivities::new(Arc::clone(&deps)))
        .build();

    let input = ChargePayment {
        order_id: "ord-flaky-6".into(),
        amount_cents: 500,
        idempotency_key: "order-ord-flaky-6:charge".into(),
    };

    assert!(
        env.run(OrderActivities::charge_payment, input.clone())
            .await
            .is_err()
    );
    assert!(
        env.run(OrderActivities::charge_payment, input.clone())
            .await
            .is_err()
    );

    let ok = env
        .run(OrderActivities::charge_payment, input)
        .await
        .expect("third attempt should succeed");
    assert!(ok.starts_with("ch_500_"));
}

#[tokio::test]
async fn zero_quantity_is_rejected_without_touching_the_database() {
    let deps = test_deps().await;
    let env = ActivityEnvironment::builder()
        .register_activities(OrderActivities::new(Arc::clone(&deps)))
        .build();

    let err = env
        .run(
            OrderActivities::reserve_inventory,
            ReserveInventory {
                order_id: "ord-7".into(),
                sku: "WIDGET-1".into(),
                quantity: 0,
                idempotency_key: "order-ord-7:reserve".into(),
            },
        )
        .await
        .expect_err("zero quantity must be rejected");

    assert!(matches!(
        err,
        temporalio_sdk::testing::ActivityEnvironmentError::Activity(ActivityError::Application(_))
    ));
}
