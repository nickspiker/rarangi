<p align="center">
  <img src="rarangi.webp" alt="rārangi" width="400"/>
</p>

# rārangi

**The register — durable, encrypted, queryable rows on a crash-proof vault.**

*rārangi* (te reo Māori): a list, row, register, catalogue, index. The literal shape of a relational database — rows in a register, found by an index.

`rārangi` is an **embedded** query database: you link it into your program and it keeps structured records in a local vault. It's [SQLite](https://sqlite.org)'s category, not MySQL's — in-process, single-writer, file-backed, no server or socket. The difference is the stack underneath: records are [VSF](https://crates.io/crates/vsf) documents, stored and (optionally) encrypted by [kete](https://crates.io/crates/kete) over a crash-proof [manifestus](https://crates.io/crates/manifestus) dual-ring vault.

```
your app
  └── rārangi   rows · catalog · indexes · queries · transactions
        └── kete       per-key encryption + opaque addressing
              └── manifestus   durable, mirrored, COW keyed store
                    └── vsf · tohu   record format · device identity
```

rārangi carries no storage code of its own — no pages, no allocator, no journal. manifestus already is a durable, crash-proof keyed store, so rārangi builds only the query layers — rows, catalog, indexes, transactions — on top of it.

## What it gives you

- **Typed rows.** A `Record` is named fields of typed `Value`s (text, uint, bytes, eagle-time — no Unix time anywhere). `put_row` / `get_row` / `delete_row` by primary key. Each row is a complete, self-verifying VSF file on disk.
- **Enumeration on a non-enumerable store.** The store underneath answers one key at a time and won't list itself; rārangi maintains its own per-table catalog, so `list("table")` works and "scan" means "read the catalog, then point-fetch the rows."
- **Content-addressed blobs.** `put_blob(&bytes) -> id` (the BLAKE3 of the content) chunks large media under the value cap and writes a hash-verified manifest; `get_blob(id)` reassembles and verifies it. Idempotent and deduplicating — store the same audio twice, it's one blob.
- **Crash-safe transactions.** A multi-key change is staged, written as one atomic commit (a redo-WAL whose commit point is a single durable `put`), then applied with the superroot flipped last. Kill the process mid-write and recovery on open completes or discards it — all-or-nothing, never a torn row.
- **Crypto-native, by inheritance.** Integrity on every block (manifestus BLAKE3-seals them) and optional confidentiality (kete encrypts per key, under a secret you choose — machine-bound or portable). The on-disk vault leaks nothing about what's stored.

## Usage

```rust
use rarangi::{Db, Record, Value};

let mut db = Db::open(storage)?;   // storage = a kete vault, or MemoryStore for tests

db.put_row("track", "5vYA1mW9g2Coh1HUFUSmlb", &Record::new()
    .set("title",  "3AM")
    .set("artist", "Matchbox Twenty")
    .set("year",   1996u64)
    .set("added",  Value::Time(osc)))?;        // eagle-time

let rec = db.get_row("track", "5vYA1mW9g2Coh1HUFUSmlb")?;   // Option<Record>
rec.unwrap().text("title");                    // typed getters: text / uint / bytes / time
db.list("track")?;                             // Vec<pk> — the catalog

let id = db.put_blob(&audio_bytes)?;           // content-addressed media
let audio = db.get_blob(&id)?;                 // verified against its hash
```

## Status

Young and honest about it. **Built and tested** (including end-to-end on a real encrypted on-disk vault: store a record + a 3 MB blob, close, reopen, read back): the transaction spine + crash recovery, typed rows, the catalog, and content-addressed blobs.

**Still landing:** the query engine that earns the name — secondary indexes (find rows *by field*, not just primary key), a filter/select executor, and joins. Until those exist you query by primary key or by listing a table and filtering in your own code.

So: solid bones — durable, encrypted, content-addressed, integrity-checked — and the `SELECT`-shaped flesh is the next work. Next to SQLite's 25 years it's a newborn; what it brings that SQLite doesn't is the crypto-native, content-addressed, non-enumerable substrate and VSF's type system.

## License

MIT OR Apache-2.0, at your option.
