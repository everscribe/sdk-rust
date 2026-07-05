//! The set of Event field names accepted by the mint endpoint.
//!
//! Hard-coded because the wire shape is owned by
//! this SDK. Expanding `event::Event` requires updating this list and the
//! server's allowlist too.

/// Valid `allowed_columns` values.
pub const ALLOWED_COLUMNS: [&str; 11] = [
    "id",
    "tenant_id",
    "occurred_at",
    "actor",
    "action",
    "target",
    "metadata",
    "origin",
    "result",
    "change",
    "idempotency_key",
];
