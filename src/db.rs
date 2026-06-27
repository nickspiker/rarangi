//! The query-database surface on top of the transaction spine: rows + a maintained catalog (a row and its catalog entry move in one transaction, so they can't drift), and content-addressed blobs for large media.

use crate::pk::Pk;
use crate::store::Store;
use crate::wal::{Db, DbError, Result, Target};
use crate::{blob, catalog, keys};
use crate::record::{self, Record};

impl<S: Store> Db<S> {
    /// Insert or replace a row, keeping the table catalog in the same transaction.
    pub fn put_row(&mut self, table: &str, pk: &str, rec: &Record) -> Result<()> {
        let mut writes = vec![(Target::Str(keys::row(table, pk)), Some(record::encode(rec)))];
        let mut pks = self.read_catalog(table)?;
        if !pks.iter().any(|p| p == pk) {
            pks.push(pk.to_string());
            writes.push((Target::Str(keys::catalog(table, 0)), Some(catalog::encode(&pks))));
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
        let mut writes = vec![(Target::Str(keys::row(table, pk)), None)];
        let mut pks = self.read_catalog(table)?;
        if let Some(i) = pks.iter().position(|p| p == pk) {
            pks.remove(i);
            writes.push((Target::Str(keys::catalog(table, 0)), Some(catalog::encode(&pks))));
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

    // ======================================================================== Byte-keyed table API ======================================================
    // Same row+catalog-in-one-transaction guarantee as the string API, but the table is raw bytes (a 32-byte friendship id) and the pk is typed (`Pk`): the row address is derived from `table ++ pk.addr_bytes()` and the pk round-trips through the catalog by its native VSF type. No binary is ever text-encoded into a key.

    /// Insert or replace a row in a byte-keyed table, keeping the table's typed catalog in the same transaction.
    pub fn put_row_in(&mut self, table: &[u8], pk: Pk, rec: &Record) -> Result<()> {
        let row = keys::row_addr(table, &pk.addr_bytes());
        let mut writes = vec![(Target::Addr(row), Some(record::encode(rec)))];
        let mut pks = self.read_catalog_in(table)?;
        if !pks.iter().any(|p| p == &pk) {
            pks.push(pk.into_owned());
            writes.push((Target::Addr(keys::catalog_addr(table, 0)), Some(catalog::encode_pks(&pks))));
        }
        self.transact(writes)
    }

    /// Fetch a row from a byte-keyed table by typed primary key.
    pub fn get_row_in(&self, table: &[u8], pk: Pk) -> Result<Option<Record>> {
        match self.get_addr(&keys::row_addr(table, &pk.addr_bytes()))? {
            Some(b) => Ok(Some(record::decode(&b)?)),
            None => Ok(None),
        }
    }

    /// Delete a row from a byte-keyed table, dropping it from the typed catalog in the same transaction.
    pub fn delete_row_in(&mut self, table: &[u8], pk: Pk) -> Result<()> {
        let row = keys::row_addr(table, &pk.addr_bytes());
        let mut writes = vec![(Target::Addr(row), None)];
        let mut pks = self.read_catalog_in(table)?;
        if let Some(i) = pks.iter().position(|p| p == &pk) {
            pks.remove(i);
            writes.push((Target::Addr(keys::catalog_addr(table, 0)), Some(catalog::encode_pks(&pks))));
        }
        self.transact(writes)
    }

    /// Every typed primary key in a byte-keyed table, in insertion order (read from the typed catalog).
    pub fn list_in(&self, table: &[u8]) -> Result<Vec<Pk<'static>>> {
        self.read_catalog_in(table)
    }

    fn read_catalog_in(&self, table: &[u8]) -> Result<Vec<Pk<'static>>> {
        match self.get_addr(&keys::catalog_addr(table, 0))? {
            Some(b) => catalog::decode_pks(&b),
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
    fn byte_table_int_counter_pks() {
        use crate::Pk;
        let mut db = Db::open(MemoryStore::new()).unwrap();
        let table = [9u8; 32]; // a raw 32-byte table id (e.g. a friendship id)

        // Monotonic counter pks: first message 0, then 1, 2…
        for (i, body) in ["hi", "yo", "sup"].iter().enumerate() {
            db.put_row_in(&table, Pk::Int(i as u64), &Record::new().set("body", *body)).unwrap();
        }

        assert_eq!(db.get_row_in(&table, Pk::Int(0)).unwrap().unwrap().text("body"), Some("hi"));
        assert_eq!(db.get_row_in(&table, Pk::Int(2)).unwrap().unwrap().text("body"), Some("sup"));
        assert!(db.get_row_in(&table, Pk::Int(3)).unwrap().is_none());

        assert_eq!(db.list_in(&table).unwrap(), vec![Pk::Int(0), Pk::Int(1), Pk::Int(2)]);

        db.delete_row_in(&table, Pk::Int(1)).unwrap();
        assert!(db.get_row_in(&table, Pk::Int(1)).unwrap().is_none());
        assert_eq!(db.list_in(&table).unwrap(), vec![Pk::Int(0), Pk::Int(2)]);
    }

    #[test]
    fn byte_table_distinct_tables_and_typed_pks() {
        use crate::Pk;
        let mut db = Db::open(MemoryStore::new()).unwrap();
        let t1 = [1u8; 32];
        let t2 = [2u8; 32];
        let hash_pk = [0xABu8; 32];

        db.put_row_in(&t1, Pk::Int(0), &Record::new().set("x", 1u64)).unwrap();
        db.put_row_in(&t1, Pk::str("alpha"), &Record::new().set("x", 2u64)).unwrap();
        db.put_row_in(&t1, Pk::bytes(&hash_pk), &Record::new().set("x", 3u64)).unwrap();
        // Same pk value in a different table is a different row (table is part of the address).
        db.put_row_in(&t2, Pk::Int(0), &Record::new().set("x", 99u64)).unwrap();

        assert_eq!(db.get_row_in(&t1, Pk::Int(0)).unwrap().unwrap().uint("x"), Some(1));
        assert_eq!(db.get_row_in(&t1, Pk::str("alpha")).unwrap().unwrap().uint("x"), Some(2));
        assert_eq!(db.get_row_in(&t1, Pk::bytes(&hash_pk)).unwrap().unwrap().uint("x"), Some(3));
        assert_eq!(db.get_row_in(&t2, Pk::Int(0)).unwrap().unwrap().uint("x"), Some(99));

        // The catalog round-trips each pk by its native type.
        assert_eq!(
            db.list_in(&t1).unwrap(),
            vec![Pk::Int(0), Pk::str("alpha"), Pk::bytes(&hash_pk)]
        );
        assert_eq!(db.list_in(&t2).unwrap(), vec![Pk::Int(0)]);
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
