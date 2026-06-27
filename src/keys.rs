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

// ============================================================================ Byte-native addressing ====================================================
// A byte-keyed table can't go through the string scheme above (the table is raw bytes — a 32-byte friendship id — and text-encoding binary into keys is exactly what's being removed). Instead rārangi derives the 32-byte vault address directly from the bytes with `blake3::derive_key`, the same primitive kete uses for its own entry addresses, then writes via the `*_addr` Store path. The derive context is domain-separated per logical kind (row vs catalog) so a row and its catalog never collide even for the same table.

const ROW_CONTEXT: &str = "rarangi.byte-table.row.v0";
const CATALOG_CONTEXT: &str = "rarangi.byte-table.catalog.v0";

/// The 32-byte vault address for one row in a byte-keyed table: derived from `table_bytes ++ pk_bytes` under the row context. The pk's bytes come from `Pk::addr_bytes()` (a counter's 8 big-endian bytes, a string's/hash's literal bytes), so distinct (table, pk) pairs land on distinct addresses.
pub fn row_addr(table: &[u8], pk_bytes: &[u8]) -> [u8; 32] {
    // Length-prefix the table so `(table, pk)` is unambiguous for any byte lengths — without it `("ab", "c")` and `("a", "bc")` would hash identically.
    let mut material = Vec::with_capacity(8 + table.len() + pk_bytes.len());
    material.extend_from_slice(&(table.len() as u64).to_be_bytes());
    material.extend_from_slice(table);
    material.extend_from_slice(pk_bytes);
    blake3::derive_key(ROW_CONTEXT, &material)
}

/// The 32-byte vault address for a byte-keyed table's catalog shard, derived from the table bytes under the catalog context.
pub fn catalog_addr(table: &[u8], shard: u64) -> [u8; 32] {
    let mut material = Vec::with_capacity(table.len() + 8);
    material.extend_from_slice(table);
    material.extend_from_slice(&shard.to_be_bytes());
    blake3::derive_key(CATALOG_CONTEXT, &material)
}

/// A large-media manifest (chunk count, per-chunk hash, total length, mime).
pub fn blob_manifest(id: &str) -> String {
    format!("blob/{id}/manifest")
}

/// One chunk of a large-media blob.
pub fn blob_chunk(id: &str, chunk: u64) -> String {
    format!("blob/{id}/{chunk}")
}
