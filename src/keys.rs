//! Logical key scheme. rārangi composes structured string keys and hands them to the `Store`; kete hashes each to a 32-byte vault address (domain-separated by app). Every structure a normal DB discovers by scanning is reachable here by *computing a key from data the caller already holds* — the adaptation that lets a query DB live on a non-enumerable keyed store.

/// The DB root: schema version, committed txn seq, table directory, WAL head.
pub fn superroot() -> String {
    "meta/superroot".to_string()
}

/// One transaction's intent log — the single atomic commit unit.
pub fn wal(seq: u64) -> String {
    format!("wal/{seq}")
}

/// A table's field + index schema.
pub fn schema(table: &str) -> String {
    format!("schema/{table}")
}

/// Directory over a table's catalog shards (shard count, per-shard live counts).
pub fn catmeta(table: &str) -> String {
    format!("catmeta/{table}")
}

/// One shard of a table's primary-key catalog (the enumeration substrate).
pub fn catalog(table: &str, shard: u64) -> String {
    format!("catalog/{table}/{shard}")
}

/// One row, by table + primary key.
pub fn row(table: &str, pk: &str) -> String {
    format!("row/{table}/{pk}")
}

/// A large-media manifest (chunk count, per-chunk hash, total length, mime).
pub fn blob_manifest(id: &str) -> String {
    format!("blob/{id}/manifest")
}

/// One chunk of a large-media blob.
pub fn blob_chunk(id: &str, chunk: u64) -> String {
    format!("blob/{id}/{chunk}")
}
