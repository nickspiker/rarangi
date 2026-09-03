//! The keyed-storage interface rārangi is built on.
//!
//! rārangi never talks to manifestus or a cipher directly — it reads and writes VSF-encoded byte values by logical string key through a `Store`. The real backend is `kete::FlatStorage` (per-key ChaCha20-Poly1305 over a durable manifestus vault). `MemoryStore` is an in-RAM cleartext implementation for fast logic tests of the transaction spine (durability tests use kete).

use std::collections::HashMap;
use std::sync::Mutex;

#[derive(Debug)]
pub struct StoreError(pub String);

impl std::fmt::Display for StoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "store: {}", self.0)
    }
}

impl std::error::Error for StoreError {}

/// Point-lookup keyed storage. No iteration — rārangi maintains its own catalog to enumerate (manifestus is non-enumerable, and so is this).
///
/// Two address spaces sit behind the same store: logical **string** keys (`get`/`put`/`delete`), which the backend hashes to a vault address itself, and pre-computed **byte addresses** (`get_addr`/`put_addr`/`delete_addr`), for callers (the byte-keyed table path) that derive a 32-byte address from raw bytes and must not text-encode them first. Both resolve to the same kind of 32-byte vault slot, so the WAL can carry either as a uniform target.
pub trait Store {
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>, StoreError>;
    fn put(&self, key: &str, value: &[u8]) -> Result<(), StoreError>;
    fn delete(&self, key: &str) -> Result<(), StoreError>;

    fn get_addr(&self, addr: &[u8; 32]) -> Result<Option<Vec<u8>>, StoreError>;
    fn put_addr(&self, addr: &[u8; 32], value: &[u8]) -> Result<(), StoreError>;
    fn delete_addr(&self, addr: &[u8; 32]) -> Result<(), StoreError>;

    /// Apply many put-or-delete ops IN ORDER. The default is the sequential per-op loop (semantics unchanged for plain stores); a backend with a group-commit primitive (kete) overrides it to land the whole list under ONE durable commit. The caller is the transaction's apply phase — [user post-images + superroot + WAL retire] riding one commit instead of one each is the 4-5x write-amplification cut behind the 2026-09-02 field day (40s message sends). Order is the contract: the superroot flip and the WAL retire only ever land after everything before them.
    fn apply_batch(&self, ops: Vec<(crate::wal::Target, Option<Vec<u8>>)>) -> Result<(), StoreError> {
        for (target, value) in ops {
            match (target, value) {
                (crate::wal::Target::Str(k), Some(v)) => self.put(&k, &v)?,
                (crate::wal::Target::Str(k), None) => self.delete(&k)?,
                (crate::wal::Target::Addr(a), Some(v)) => self.put_addr(&a, &v)?,
                (crate::wal::Target::Addr(a), None) => self.delete_addr(&a)?,
            }
        }
        Ok(())
    }
}

impl Store for kete::FlatStorage {
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>, StoreError> {
        self.read(key).map_err(|e| StoreError(e.to_string()))
    }
    fn put(&self, key: &str, value: &[u8]) -> Result<(), StoreError> {
        self.write(key, value).map_err(|e| StoreError(e.to_string()))
    }
    fn delete(&self, key: &str) -> Result<(), StoreError> {
        kete::FlatStorage::delete(self, key).map_err(|e| StoreError(e.to_string()))
    }
    fn get_addr(&self, addr: &[u8; 32]) -> Result<Option<Vec<u8>>, StoreError> {
        self.read_addr(addr).map_err(|e| StoreError(e.to_string()))
    }
    fn put_addr(&self, addr: &[u8; 32], value: &[u8]) -> Result<(), StoreError> {
        self.write_addr(addr, value).map_err(|e| StoreError(e.to_string()))
    }
    fn delete_addr(&self, addr: &[u8; 32]) -> Result<(), StoreError> {
        kete::FlatStorage::delete_addr(self, addr).map_err(|e| StoreError(e.to_string()))
    }
    /// The group-commit override: string keys derive to their vault addresses (the same derivation `write` uses, so batched and single writes hit identical rows), then the whole ordered list posts as ONE librarian batch — (usually) one manifestus commit for the entire transaction apply.
    fn apply_batch(&self, ops: Vec<(crate::wal::Target, Option<Vec<u8>>)>) -> Result<(), StoreError> {
        let items: Vec<([u8; 32], Option<Vec<u8>>)> = ops
            .into_iter()
            .map(|(target, value)| {
                let addr = match target {
                    crate::wal::Target::Str(s) => self.entry_addr(&s),
                    crate::wal::Target::Addr(a) => a,
                };
                (addr, value)
            })
            .collect();
        self.apply_addr_batch(items).map_err(|e| StoreError(e.to_string()))
    }
}

/// In-RAM cleartext store for tests. Not durable — a process kill loses it, so it's for exercising transaction/recovery *logic*, not crash safety.
#[derive(Default)]
pub struct MemoryStore {
    map: Mutex<HashMap<String, Vec<u8>>>,
    /// The byte-addressed half of the store (the `*_addr` API), keyed directly by the 32-byte address — a separate map so a string key and a byte address can never alias.
    addr_map: Mutex<HashMap<[u8; 32], Vec<u8>>>,
}

impl MemoryStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Store for MemoryStore {
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>, StoreError> {
        Ok(self.map.lock().unwrap().get(key).cloned())
    }
    fn put(&self, key: &str, value: &[u8]) -> Result<(), StoreError> {
        self.map.lock().unwrap().insert(key.to_string(), value.to_vec());
        Ok(())
    }
    fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.map.lock().unwrap().remove(key);
        Ok(())
    }
    fn get_addr(&self, addr: &[u8; 32]) -> Result<Option<Vec<u8>>, StoreError> {
        Ok(self.addr_map.lock().unwrap().get(addr).cloned())
    }
    fn put_addr(&self, addr: &[u8; 32], value: &[u8]) -> Result<(), StoreError> {
        self.addr_map.lock().unwrap().insert(*addr, value.to_vec());
        Ok(())
    }
    fn delete_addr(&self, addr: &[u8; 32]) -> Result<(), StoreError> {
        self.addr_map.lock().unwrap().remove(addr);
        Ok(())
    }
}
