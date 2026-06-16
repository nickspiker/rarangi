//! The query-database surface on top of the transaction spine: rows + a maintained catalog (a row and its catalog entry move in one transaction, so they can't drift), and content-addressed blobs for large media.

use crate::store::Store;
use crate::wal::{Db, DbError, Result};
use crate::{blob, catalog, keys};
use crate::record::{self, Record};

impl<S: Store> Db<S> {
    /// Insert or replace a row, keeping the table catalog in the same transaction.
    pub fn put_row(&mut self, table: &str, pk: &str, rec: &Record) -> Result<()> {
        let mut writes = vec![(keys::row(table, pk), Some(record::encode(rec)))];
        let mut pks = self.read_catalog(table)?;
        if !pks.iter().any(|p| p == pk) {
            pks.push(pk.to_string());
            writes.push((keys::catalog(table, 0), Some(catalog::encode(&pks))));
        }
        self.transact(writes)
    }

    /// Fetch a row by primary key.
    pub fn get_row(&self, table: &str, pk: &str) -> Result<Option<Record>> {
        match self.get(&keys::row(table, pk))? {
            Some(b) => Ok(Some(record::decode(&b)?)),
            None => Ok(None),
        }
    }

    /// Delete a row, dropping it from the catalog in the same transaction.
    pub fn delete_row(&mut self, table: &str, pk: &str) -> Result<()> {
        let mut writes = vec![(keys::row(table, pk), None)];
        let mut pks = self.read_catalog(table)?;
        if let Some(i) = pks.iter().position(|p| p == pk) {
            pks.remove(i);
            writes.push((keys::catalog(table, 0), Some(catalog::encode(&pks))));
        }
        self.transact(writes)
    }

    /// Every primary key in a table, in insertion order (read from the catalog).
    pub fn list(&self, table: &str) -> Result<Vec<String>> {
        self.read_catalog(table)
    }

    fn read_catalog(&self, table: &str) -> Result<Vec<String>> {
        match self.get(&keys::catalog(table, 0))? {
            Some(b) => catalog::decode(&b),
            None => Ok(Vec::new()),
        }
    }

    /// Store large media, returning its content id. Idempotent and deduplicating (content-addressed). Writes chunks directly, then the manifest as the commit point — blobs are immutable, so they don't ride the WAL.
    pub fn put_blob(&self, content: &[u8]) -> Result<String> {
        let id = blob::id(content);
        if self.store().get(&keys::blob_manifest(&id))?.is_some() {
            return Ok(id); // already stored
        }
        let mut chunks = 0u64;
        for (i, chunk) in content.chunks(blob::CHUNK_SIZE.max(1)).enumerate() {
            self.store()
                .put(&keys::blob_chunk(&id, i as u64), &blob::encode_chunk(chunk))?;
            chunks += 1;
        }
        let manifest = blob::Manifest { len: content.len() as u64, chunks };
        self.store()
            .put(&keys::blob_manifest(&id), &blob::encode_manifest(&manifest))?;
        Ok(id)
    }

    /// Reassemble a blob by id, verifying length and content hash. None if absent.
    pub fn get_blob(&self, id: &str) -> Result<Option<Vec<u8>>> {
        let Some(mbytes) = self.store().get(&keys::blob_manifest(id))? else {
            return Ok(None);
        };
        let manifest = blob::decode_manifest(&mbytes)?;
        let mut out = Vec::with_capacity(manifest.len as usize);
        for i in 0..manifest.chunks {
            let Some(cbytes) = self.store().get(&keys::blob_chunk(id, i))? else {
                return Err(DbError::Corrupt(format!("blob {id} missing chunk {i}")));
            };
            out.extend_from_slice(&blob::decode_chunk(&cbytes)?);
        }
        if out.len() as u64 != manifest.len || blob::id(&out) != id {
            return Err(DbError::Corrupt(format!("blob {id} failed content verification")));
        }
        Ok(Some(out))
    }
}

#[cfg(test)]
mod tests {
    use crate::store::MemoryStore;
    use crate::value::Value;
    use crate::{Db, Record};

    #[test]
    fn row_round_trip_and_catalog() {
        let mut db = Db::open(MemoryStore::new()).unwrap();
        let rec = Record::new()
            .set("title", "3AM")
            .set("artist", "Matchbox Twenty")
            .set("year", 1996u64)
            .set("added", Value::Time(42));
        db.put_row("track", "5vYA1mW9g2Coh1HUFUSmlb", &rec).unwrap();

        let got = db.get_row("track", "5vYA1mW9g2Coh1HUFUSmlb").unwrap().unwrap();
        assert_eq!(got.text("title"), Some("3AM"));
        assert_eq!(got.text("artist"), Some("Matchbox Twenty"));
        assert_eq!(got.uint("year"), Some(1996));
        assert_eq!(got.time("added"), Some(42));

        assert_eq!(db.list("track").unwrap(), vec!["5vYA1mW9g2Coh1HUFUSmlb"]);
        assert!(db.get_row("track", "nope").unwrap().is_none());
    }

    #[test]
    fn delete_drops_row_and_catalog() {
        let mut db = Db::open(MemoryStore::new()).unwrap();
        db.put_row("t", "a", &Record::new().set("x", 1u64)).unwrap();
        db.put_row("t", "b", &Record::new().set("x", 2u64)).unwrap();
        db.delete_row("t", "a").unwrap();
        assert!(db.get_row("t", "a").unwrap().is_none());
        assert_eq!(db.list("t").unwrap(), vec!["b"]);
    }

    #[test]
    fn blob_round_trip_multichunk() {
        let db = Db::open(MemoryStore::new()).unwrap();
        // 2.x chunks so we exercise multi-chunk + a partial final chunk.
        let content: Vec<u8> = (0..crate::blob::CHUNK_SIZE * 2 + 12345)
            .map(|i| (i % 251) as u8)
            .collect();
        let id = db.put_blob(&content).unwrap();
        let got = db.get_blob(&id).unwrap().unwrap();
        assert_eq!(got, content);
        // Content-addressed + idempotent: same bytes → same id, second put is a no-op.
        assert_eq!(db.put_blob(&content).unwrap(), id);
        assert!(db.get_blob("deadbeef").unwrap().is_none());
    }
}
