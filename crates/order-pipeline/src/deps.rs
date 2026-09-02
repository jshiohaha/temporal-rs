//! Dependencies: the things your business logic needs and Temporal knows nothing about.
//!
//! These are deliberately fake so the repo builds with no external services. The
//! *shapes* are what matter: replace [`Database`] with `sqlx::PgPool`, and
//! [`PaymentGateway`] with a `reqwest::Client` wrapper, and nothing else in this
//! crate has to change.
//!
//! The rule of thumb: dependencies belong to the **Activity struct**, never to
//! the Workflow. A Workflow struct is replayed from history and must stay
//! deterministic, so it cannot hold a connection pool.

use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    },
};

use tokio::sync::Mutex;

/// Everything the Activities need, constructed once at process start.
///
/// Real code would build this in `main` from config/env, exactly as it does
/// here -- Temporal imposes no framework on it.
pub struct Deps {
    pub db: Database,
    pub payments: PaymentGateway,
}

impl Deps {
    pub async fn from_env() -> anyhow::Result<Arc<Self>> {
        let db = Database::connect(
            &std::env::var("DATABASE_URL")
                .unwrap_or_else(|_| "postgres://localhost/orders".to_string()),
        )
        .await?;

        let payments = PaymentGateway::new(
            std::env::var("PAYMENTS_API_KEY").unwrap_or_else(|_| "test-key".to_string()),
        );

        Ok(Arc::new(Self { db, payments }))
    }
}

// ---------------------------------------------------------------------------
// Database
// ---------------------------------------------------------------------------

/// Stand-in for a real pool. Note it is `Clone` and cheap to clone, because it
/// is `Arc` inside -- this is exactly how `sqlx::PgPool` behaves.
#[derive(Clone)]
pub struct Database {
    inner: Arc<Mutex<DbState>>,
}

#[derive(Default)]
struct DbState {
    /// `idempotency_key -> charge_id`. In Postgres this is a table with a
    /// UNIQUE constraint on the key column.
    charges: HashMap<String, String>,
    reservations: HashMap<String, String>,
}

impl Database {
    pub async fn connect(_url: &str) -> anyhow::Result<Self> {
        Ok(Self {
            inner: Arc::new(Mutex::new(DbState::default())),
        })
    }

    /// The idempotent-write primitive every side-effecting Activity should have.
    ///
    /// Postgres equivalent:
    /// ```sql
    /// INSERT INTO charges (idempotency_key, charge_id) VALUES ($1, $2)
    ///   ON CONFLICT (idempotency_key) DO NOTHING;
    /// SELECT charge_id FROM charges WHERE idempotency_key = $1;
    /// ```
    /// Returns the stored value, which is the *pre-existing* one if the key was
    /// already present.
    pub async fn upsert_charge(&self, key: &str, charge_id: &str) -> String {
        let mut db = self.inner.lock().await;
        db.charges
            .entry(key.to_string())
            .or_insert_with(|| charge_id.to_string())
            .clone()
    }

    pub async fn find_charge(&self, key: &str) -> Option<String> {
        self.inner.lock().await.charges.get(key).cloned()
    }

    pub async fn upsert_reservation(&self, key: &str, reservation_id: &str) -> String {
        let mut db = self.inner.lock().await;
        db.reservations
            .entry(key.to_string())
            .or_insert_with(|| reservation_id.to_string())
            .clone()
    }

    pub async fn delete_reservation(&self, reservation_id: &str) {
        self.inner
            .lock()
            .await
            .reservations
            .retain(|_, v| v != reservation_id);
    }
}

// ---------------------------------------------------------------------------
// Payment gateway
// ---------------------------------------------------------------------------

#[derive(Debug, thiserror::Error)]
pub enum PaymentError {
    /// Permanent: retrying will never help. The card is bad.
    #[error("card declined: {0}")]
    Declined(String),
    /// Transient: the gateway was unreachable. Retrying is exactly right.
    #[error("payment gateway unavailable: {0}")]
    Unavailable(String),
}

/// Stand-in for an HTTP client against Stripe/Adyen/etc.
#[derive(Clone)]
pub struct PaymentGateway {
    _api_key: Arc<String>,
    /// Only here to simulate a flaky upstream in the demo.
    attempts: Arc<AtomicU32>,
    /// Counts every call that actually reached the "gateway". Tests assert on
    /// this to prove the idempotency check really did prevent a second charge.
    calls: Arc<AtomicU32>,
}

impl PaymentGateway {
    pub fn new(api_key: String) -> Self {
        Self {
            _api_key: Arc::new(api_key),
            attempts: Arc::new(AtomicU32::new(0)),
            calls: Arc::new(AtomicU32::new(0)),
        }
    }

    /// Number of charge attempts that reached the gateway.
    pub fn call_count(&self) -> u32 {
        self.calls.load(Ordering::SeqCst)
    }

    /// Charge a card.
    ///
    /// `idempotency_key` is passed through to the provider. Every serious
    /// payments API accepts one, and it is the outermost layer of the
    /// exactly-once story: even if our own dedupe row is lost, the provider
    /// will not double-charge.
    pub async fn charge(
        &self,
        order_id: &str,
        amount_cents: u64,
        idempotency_key: &str,
    ) -> Result<String, PaymentError> {
        self.calls.fetch_add(1, Ordering::SeqCst);

        // --- demo behaviour, keyed off the order id -------------------------
        if order_id.contains("decline") {
            return Err(PaymentError::Declined("insufficient funds".into()));
        }
        if order_id.contains("flaky") {
            // Fail the first two calls, then succeed -- so you can watch
            // Temporal's retry policy do its job in the UI.
            let n = self.attempts.fetch_add(1, Ordering::SeqCst);
            if n < 2 {
                return Err(PaymentError::Unavailable(format!("timeout (call {n})")));
            }
        }
        // -------------------------------------------------------------------

        Ok(format!(
            "ch_{}_{}",
            amount_cents,
            &idempotency_key[..8.min(idempotency_key.len())]
        ))
    }

    pub async fn refund(&self, charge_id: &str) -> Result<(), PaymentError> {
        tracing::info!(charge_id, "refunded");
        Ok(())
    }
}
