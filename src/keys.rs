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

// ============================================================================ Byte-native addressing ====================================================
// A byte-keyed table can't go through the string scheme above (the table is raw bytes — a 32-byte friendship id — and text-encoding binary into keys is exactly what's being removed). Instead rārangi derives the 32-byte vault address directly from the bytes with `blake3::derive_key`, the same primitive kete uses for its own entry addresses, then writes via the `*_addr` Store path. The derive context is domain-separated per logical kind (row vs catalog) so a row and its catalog never collide even for the same table.

const ROW_CONTEXT: &str = "rarangi.byte-table.row.v0";
const CATALOG_CONTEXT: &str = "rarangi.byte-table.catalog.v0";

/// The 32-byte vault address for one row in a byte-keyed table: derived from `table_bytes ++ pk_bytes` under the row context. The pk's bytes come from `Pk::addr_bytes()` (a counter's 8 big-endian bytes, a hash's literal bytes, a string's VSF `x` wire form), so distinct (table, pk) pairs land on distinct addresses.
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

/// The table bytes for a text-named table (the `&str` row API): its VSF `x` wire form, the same encoding a `Pk::Str` contributes to a row address.
/// Text reaches a hash only through the x encoder (NFC inside), never as raw string bytes, so "café" and "cafe\u{301}" name one table (LIB-14).
pub fn text_table(table: &str) -> Vec<u8> {
    vsf::VsfType::x(table.to_string()).flatten()
}

#[cfg(test)]
mod tests {
    use super::row_addr;
    use crate::Pk;

    fn hex(a: &[u8; 32]) -> String {
        a.iter().map(|b| format!("{b:02x}")).collect()
    }

    /// Known-answer pins for the row address: `Int` and `Bytes` are frozen exactly as they were before LIB-14 (values captured from the pre-change code), because moving them would re-address every existing row in every vault.
    /// The `Str` pin is the post-LIB-14 wire-form address; drift there means the x encoder changed and every text-keyed row moved, which must never happen silently.
    #[test]
    fn row_addr_known_answers() {
        let table = [9u8; 32];
        let cases: [(Pk, &str); 6] = [
            (Pk::Int(0), "250a200a5a08075d3239907b3b8b83e20c772cab49df4bc20d67078d157c6480"),
            (Pk::Int(1), "f4a505d905bdf958c38f11eaaf0963a55436dee06e32910af9eae345d2571d69"),
            (Pk::Int(5_000_000_000), "2b10d876ce4bc4476ce08fcd0af94bc8abe6ba512b74a6d04b89b4c5cb0161a9"),
            (Pk::bytes(&[0xABu8; 32]), "95d7c96e8e0abebec9bb3eefc96ee49931aab5a0a6feddf1526515a022cafa67"),
            (Pk::bytes(b"k0"), "727b8d2dea4a6e70fe40be92df531c9ddb3f417a056f069bc6b80eee0a666db2"),
            (Pk::str("café"), "d6bd5ba651b6c11e97370ba8c345bb98d0cad61ec2fe1c5984429a5cc598afeb"),
        ];
        for (pk, want) in &cases {
            assert_eq!(&hex(&row_addr(&table, &pk.addr_bytes())), want, "row address drifted for {pk:?}");
        }
        // The pre-LIB-14 raw-byte address of "café" is no longer where that row lives.
        assert_ne!(
            hex(&row_addr(&table, &Pk::str("café").addr_bytes())),
            "52e848edabce94524b3741930902122505faa348bb674cfbb947d59177c6a256"
        );
        assert_eq!(
            row_addr(&table, &Pk::str("café").addr_bytes()),
            row_addr(&table, &Pk::str("cafe\u{301}").addr_bytes())
        );
    }

    /// The `&str` row API's address is the byte-keyed `Str` address with the table as its x wire form: both table and pk NFC-fold, and the pin catches any drift in that composition.
    #[test]
    fn text_table_address_is_the_x_wire_form() {
        use super::{catalog_addr, text_table};
        assert_eq!(text_table("café"), Pk::str("café").to_vsf().flatten());
        assert_eq!(text_table("café"), text_table("cafe\u{301}"));
        assert_ne!(text_table("track"), b"track".to_vec(), "a text table never hashes as its raw bytes");
        let addr = row_addr(&text_table("track"), &Pk::str("café").addr_bytes());
        assert_eq!(addr, row_addr(&text_table("track"), &Pk::str("cafe\u{301}").addr_bytes()));
        assert_eq!(hex(&addr), "f5cc5e1bd1006791518c70fdf5e92cdfddff43700755e552191b11906c010b00");
        assert_eq!(catalog_addr(&text_table("café"), 0), catalog_addr(&text_table("cafe\u{301}"), 0));
    }
}

/// A large-media manifest (chunk count, per-chunk hash, total length, mime).
pub fn blob_manifest(id: &str) -> String {
    format!("blob/{id}/manifest")
}

/// One chunk of a large-media blob.
pub fn blob_chunk(id: &str, chunk: u64) -> String {
    format!("blob/{id}/{chunk}")
}
