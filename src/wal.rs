//! The transaction spine.
//!
//! manifestus's only atomic durable unit is a single keyed `put`. So a multi-key transaction is staged in RAM, serialized into ONE complete VSF file holding the full post-images of every key it changes, and written with ONE `put("wal/{seq}")` — that put is the COMMIT POINT. Apply then blindly redoes those post-images (idempotent), writing the new superroot LAST. Recovery-on-open replays any live `wal/{seq}` and retires it; an absent WAL entry means the txn never committed.
//!
//! Single-writer: a new transaction must not be issued until the previous one's WAL entry is retired, so at most one committed-unapplied entry exists and recovery is a simple ascending replay.

use vsf::{Tensor, VsfBuilder, VsfField, VsfHeader, VsfSection, VsfType};

use crate::keys;
use crate::store::{Store, StoreError};

const FORMAT_VERSION: u8 = 0;

#[derive(Debug)]
pub enum DbError {
    Store(String),
    /// A stored record didn't decode as expected — corruption or a version skew.
    Corrupt(String),
}

impl std::fmt::Display for DbError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DbError::Store(s) => write!(f, "store: {s}"),
            DbError::Corrupt(s) => write!(f, "corrupt: {s}"),
        }
    }
}

impl std::error::Error for DbError {}

impl From<StoreError> for DbError {
    fn from(e: StoreError) -> Self {
        DbError::Store(e.0)
    }
}

pub type Result<T> = std::result::Result<T, DbError>;

/// Where a post-image lands: a logical string key (the backend hashes it to a vault address), or a pre-computed 32-byte vault address (the byte-keyed table path, which derives its address from raw bytes and must not text-encode them). Both name the same kind of vault slot, so the WAL carries them as one uniform target type and recovery replays either without caring which API produced it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Target {
    Str(String),
    Addr([u8; 32]),
}

impl From<String> for Target {
    fn from(s: String) -> Self {
        Target::Str(s)
    }
}

impl From<[u8; 32]> for Target {
    fn from(a: [u8; 32]) -> Self {
        Target::Addr(a)
    }
}

/// One post-image: write a value, or delete the target (`None`).
pub type Write = (Target, Option<Vec<u8>>);

/// The database handle. Owns the store and the last committed transaction seq.
pub struct Db<S: Store> {
    store: S,
    committed_seq: u64,
}

impl<S: Store> Db<S> {
    /// Open (or initialize) the DB on `store`, running recovery first.
    pub fn open(store: S) -> Result<Self> {
        let committed_seq = match store.get(&keys::superroot())? {
            Some(bytes) => decode_superroot(&bytes)?,
            None => {
                // Fresh DB: establish a superroot at seq 0 so recovery has an anchor.
                store.put(&keys::superroot(), &encode_superroot(0))?;
                0
            }
        };
        let mut db = Db { store, committed_seq };
        db.recover()?;
        Ok(db)
    }

    /// Replay any committed-but-unretired WAL entries (ascending) and retire them. Idempotent: re-putting a key already at its target value is harmless.
    fn recover(&mut self) -> Result<()> {
        loop {
            let seq = self.committed_seq + 1;
            let Some(bytes) = self.store.get(&keys::wal(seq))? else {
                break; // no live WAL entry beyond the committed seq → done
            };
            let rec = decode_wal(&bytes)?;
            self.apply(seq, &rec)?;
        }
        Ok(())
    }

    /// Read a value by logical key (the committed state — readers never see a half-applied transaction because the superroot flips last).
    pub fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        Ok(self.store.get(key)?)
    }

    /// Read a value by 32-byte vault address — the byte-keyed read path, mirroring `get` for targets that were written via the address API.
    pub fn get_addr(&self, addr: &[u8; 32]) -> Result<Option<Vec<u8>>> {
        Ok(self.store.get_addr(addr)?)
    }

    /// Direct store access for content-addressed blobs (immutable, so they're written outside the WAL — see db.rs::put_blob).
    pub(crate) fn store(&self) -> &S {
        &self.store
    }

    /// Commit a set of post-images as one atomic logical transaction.
    /// The committed transaction counter: how many transactions have landed since the store was empty.
    /// Each transaction is two durable commits, so this is the number a caller watches to prove a batched write costs ONE transaction rather than one per row.
    pub fn seq(&self) -> u64 {
        self.committed_seq
    }

    pub fn transact(&mut self, writes: Vec<Write>) -> Result<()> {
        let seq = self.committed_seq + 1;
        let new_superroot = encode_superroot(seq);
        let rec = WalRecord { writes, superroot: new_superroot };

        // COMMIT POINT: one atomic durable put. Before this, nothing happened.
        self.store.put(&keys::wal(seq), &encode_wal(seq, &rec))?;
        self.apply(seq, &rec)?;
        Ok(())
    }

    /// Apply a committed WAL record as ONE ordered batch: user post-images first, then the superroot flip, then the WAL retire — the exact order the sequential form wrote, but riding the store's group-commit (kete lands the lot under one manifestus commit instead of 4-5; the 2026-09-02 write-amplification fix). Order inside the batch is the store's contract — a fence-pressure prefix commit can land early ops sooner, never later ops first — so the superroot still only becomes visible after every post-image, and the retire only after the superroot. Safe to call twice (idempotent redo): the WAL entry this retires carries the full post-images, so a crash anywhere mid-batch replays the whole set.
    fn apply(&mut self, seq: u64, rec: &WalRecord) -> Result<()> {
        let mut ops: Vec<(Target, Option<Vec<u8>>)> = Vec::with_capacity(rec.writes.len() + 2);
        for (target, value) in &rec.writes {
            ops.push((target.clone(), value.clone()));
        }
        // The superroot flip is the visible "this transaction happened" edge.
        ops.push((Target::Str(keys::superroot()), Some(rec.superroot.clone())));
        ops.push((Target::Str(keys::wal(seq)), None));
        self.store.apply_batch(ops)?;
        self.committed_seq = seq;
        Ok(())
    }
}

// ============================================================================ Records ===================================================================

struct WalRecord {
    writes: Vec<Write>,
    /// The new superroot bytes, applied last.
    superroot: Vec<u8>,
}

/// Superroot: format version + last committed transaction seq. (Table registry, index roots, etc. join this in later phases.)
fn encode_superroot(committed_seq: u64) -> Vec<u8> {
    VsfBuilder::new()
        .creation_time_oscillations(vsf::eagle_time_oscillations())
        .add_section(
            "superroot",
            vec![
                ("version".to_string(), VsfType::u3(FORMAT_VERSION)),
                ("committed_seq".to_string(), VsfType::u6(committed_seq)),
            ],
        )
        .build()
        .expect("superroot encode")
}

fn decode_superroot(bytes: &[u8]) -> Result<u64> {
    let (_, mut ptr) = VsfHeader::decode(bytes).map_err(DbError::Corrupt)?;
    let sec = VsfSection::parse(bytes, &mut ptr).map_err(DbError::Corrupt)?;
    match sec.get_field("committed_seq").and_then(|f| f.values.first()) {
        Some(VsfType::u6(v)) => Ok(*v),
        _ => Err(DbError::Corrupt("superroot missing committed_seq".into())),
    }
}

/// WAL record: parallel `key`/`op`/`val` arrays for the user post-images, plus the new superroot bytes (applied last). A complete VSF file (envelope + provenance).
fn encode_wal(seq: u64, rec: &WalRecord) -> Vec<u8> {
    let mut kinds_v = Vec::with_capacity(rec.writes.len());
    let mut keys_v = Vec::with_capacity(rec.writes.len());
    let mut ops_v = Vec::with_capacity(rec.writes.len());
    let mut vals_v = Vec::with_capacity(rec.writes.len());
    for (target, value) in &rec.writes {
        // Target as (kind, bytes): kind 0 = string key, kind 1 = 32-byte address. The target bytes go in a binary-safe `t_u3` so an address (raw bytes, not ascii) round-trips losslessly alongside string keys.
        let (kind, tbytes) = match target {
            Target::Str(s) => (0u8, s.as_bytes().to_vec()),
            Target::Addr(a) => (1u8, a.to_vec()),
        };
        kinds_v.push(VsfType::u3(kind));
        keys_v.push(VsfType::t_u3(Tensor::new(vec![tbytes.len()], tbytes)));
        match value {
            Some(v) => {
                ops_v.push(VsfType::u3(1));
                vals_v.push(VsfType::t_u3(Tensor::new(vec![v.len()], v.clone())));
            }
            None => {
                ops_v.push(VsfType::u3(0));
                vals_v.push(VsfType::t_u3(Tensor::new(vec![0], Vec::new())));
            }
        }
    }

    let mut sec = VsfSection::new("wal");
    sec.fields.push(VsfField {
        name: "seq".to_string(),
        values: vec![VsfType::u6(seq)],
    });
    sec.fields.push(VsfField { name: "kind".to_string(), values: kinds_v });
    sec.fields.push(VsfField { name: "key".to_string(), values: keys_v });
    sec.fields.push(VsfField { name: "op".to_string(), values: ops_v });
    sec.fields.push(VsfField { name: "val".to_string(), values: vals_v });
    sec.fields.push(VsfField {
        name: "superroot".to_string(),
        values: vec![VsfType::t_u3(Tensor::new(vec![rec.superroot.len()], rec.superroot.clone()))],
    });

    VsfBuilder::new()
        .creation_time_oscillations(vsf::eagle_time_oscillations())
        .add_section_direct(sec)
        .build()
        .expect("wal encode")
}

fn decode_wal(bytes: &[u8]) -> Result<WalRecord> {
    let (_, mut ptr) = VsfHeader::decode(bytes).map_err(DbError::Corrupt)?;
    let sec = VsfSection::parse(bytes, &mut ptr).map_err(DbError::Corrupt)?;

    let kinds_v = field_values(&sec, "kind")?;
    let keys_v = field_values(&sec, "key")?;
    let ops_v = field_values(&sec, "op")?;
    let vals_v = field_values(&sec, "val")?;
    if keys_v.len() != ops_v.len() || keys_v.len() != vals_v.len() || keys_v.len() != kinds_v.len() {
        return Err(DbError::Corrupt("wal kind/key/op/val arrays misaligned".into()));
    }

    let mut writes = Vec::with_capacity(keys_v.len());
    for i in 0..keys_v.len() {
        let tbytes = match &keys_v[i] {
            VsfType::t_u3(t) => t.data.clone(),
            _ => return Err(DbError::Corrupt("wal key not opaque bytes".into())),
        };
        let target = match &kinds_v[i] {
            VsfType::u3(0) => Target::Str(
                String::from_utf8(tbytes).map_err(|_| DbError::Corrupt("wal string key not utf8".into()))?,
            ),
            VsfType::u3(1) => {
                let a: [u8; 32] = tbytes
                    .try_into()
                    .map_err(|_| DbError::Corrupt("wal address not 32 bytes".into()))?;
                Target::Addr(a)
            }
            _ => return Err(DbError::Corrupt("wal target kind unknown".into())),
        };
        let value = match (&ops_v[i], &vals_v[i]) {
            (VsfType::u3(1), VsfType::t_u3(t)) => Some(t.data.clone()),
            (VsfType::u3(0), _) => None,
            _ => return Err(DbError::Corrupt("wal op/val type mismatch".into())),
        };
        writes.push((target, value));
    }

    let superroot = match sec.get_field("superroot").and_then(|f| f.values.first()) {
        Some(VsfType::t_u3(t)) => t.data.clone(),
        _ => return Err(DbError::Corrupt("wal missing superroot".into())),
    };

    Ok(WalRecord { writes, superroot })
}

fn field_values<'a>(sec: &'a VsfSection, name: &str) -> Result<&'a [VsfType]> {
    sec.get_field(name)
        .map(|f| f.values.as_slice())
        .ok_or_else(|| DbError::Corrupt(format!("wal missing field {name}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::MemoryStore;

    #[test]
    fn commit_and_read_back() {
        let mut db = Db::open(MemoryStore::new()).unwrap();
        db.transact(vec![
            ("row/t/a".to_string().into(), Some(b"alice".to_vec())),
            ("row/t/b".to_string().into(), Some(b"bob".to_vec())),
        ])
        .unwrap();
        assert_eq!(db.get("row/t/a").unwrap().as_deref(), Some(&b"alice"[..]));
        assert_eq!(db.get("row/t/b").unwrap().as_deref(), Some(&b"bob"[..]));
        assert_eq!(db.committed_seq, 1);
        // WAL entry retired after apply.
        assert!(db.store.get(&keys::wal(1)).unwrap().is_none());
    }

    #[test]
    fn delete_post_image() {
        let mut db = Db::open(MemoryStore::new()).unwrap();
        db.transact(vec![("row/t/a".to_string().into(), Some(b"x".to_vec()))]).unwrap();
        db.transact(vec![("row/t/a".to_string().into(), None)]).unwrap();
        assert_eq!(db.get("row/t/a").unwrap(), None);
        assert_eq!(db.committed_seq, 2);
    }

    #[test]
    fn recovery_replays_committed_unapplied() {
        // Simulate a crash AFTER the WAL commit point but BEFORE apply: the store holds an old superroot (seq 0) and a live wal/1, with the row NOT yet written. Opening must replay it.
        let store = MemoryStore::new();
        store.put(&keys::superroot(), &encode_superroot(0)).unwrap();
        let rec = WalRecord {
            writes: vec![("row/t/a".to_string().into(), Some(b"recovered".to_vec()))],
            superroot: encode_superroot(1),
        };
        store.put(&keys::wal(1), &encode_wal(1, &rec)).unwrap();
        // (row/t/a deliberately absent — apply never ran)

        let db = Db::open(store).unwrap();
        assert_eq!(db.committed_seq, 1);
        assert_eq!(db.get("row/t/a").unwrap().as_deref(), Some(&b"recovered"[..]));
        assert!(db.store.get(&keys::wal(1)).unwrap().is_none()); // retired
    }

    #[test]
    fn recovery_is_idempotent_after_full_commit() {
        // A fully-committed txn (WAL already retired) must not be re-applied or error.
        let store = MemoryStore::new();
        {
            let mut db = Db::open(&store).unwrap();
            db.transact(vec![("k".to_string().into(), Some(b"v".to_vec()))]).unwrap();
        }
        // Reopen on the same store — recovery finds no live WAL, committed_seq holds.
        let db = Db::open(&store).unwrap();
        assert_eq!(db.committed_seq, 1);
        assert_eq!(db.get("k").unwrap().as_deref(), Some(&b"v"[..]));
    }
}

// Allow `Db::open(&store)` in tests (and callers that keep the store) by implementing Store for shared references.
impl<S: Store> Store for &S {
    fn get(&self, key: &str) -> std::result::Result<Option<Vec<u8>>, StoreError> {
        (**self).get(key)
    }
    fn put(&self, key: &str, value: &[u8]) -> std::result::Result<(), StoreError> {
        (**self).put(key, value)
    }
    fn delete(&self, key: &str) -> std::result::Result<(), StoreError> {
        (**self).delete(key)
    }
    fn get_addr(&self, addr: &[u8; 32]) -> std::result::Result<Option<Vec<u8>>, StoreError> {
        (**self).get_addr(addr)
    }
    fn put_addr(&self, addr: &[u8; 32], value: &[u8]) -> std::result::Result<(), StoreError> {
        (**self).put_addr(addr, value)
    }
    fn delete_addr(&self, addr: &[u8; 32]) -> std::result::Result<(), StoreError> {
        (**self).delete_addr(addr)
    }
    // Forward explicitly — the trait default on `&S` would re-dispatch thru the per-op methods and silently LOSE a backend's group-commit override.
    fn apply_batch(&self, ops: Vec<(Target, Option<Vec<u8>>)>) -> std::result::Result<(), StoreError> {
        (**self).apply_batch(ops)
    }
}
