//! rārangi — a query database on the manifestus keyed object store.
//!
//! rārangi carries no storage code of its own — no pages, allocator, or journal. manifestus already is a durable, crash-proof, mirrored, COW-HAMT keyed store, so rārangi builds only the query layers (records, catalog, indexes, joins, transactions) on top, reaching storage through `kete` (encryption + addressing).
//!
//! Two properties of the substrate shape everything:
//!
//! Non-enumerable: no scan/range/list. rārangi materializes its own row catalog and indexes as ordinary keyed entries; every "scan" is "read a catalog/index value, then point-fetch the rows it names."
//!
//! Commit-per-write, single-writer, ~4MB per value: multi-row atomicity is a redo-WAL whose commit point is one atomic `put`; posting lists, catalog shards, and large media are chunked under the cap.
//!
//! Public surface: open a `Db` on any `Store`, then `put_row`/`get_row`/`delete_row`/`list` for structured rows (`Record` of typed `Value`s) and `put_blob`/`get_blob` for content-addressed media.

mod blob;
mod catalog;
mod db;
pub mod keys;
pub mod record;
pub mod store;
pub mod value;
pub mod wal;

pub use record::Record;
pub use store::{MemoryStore, Store, StoreError};
pub use value::Value;
pub use wal::{Db, DbError, Write};
