//! End-to-end proof of the real stack: rarangi rows + blobs through kete's encrypted per-key ChaCha20-Poly1305 onto a durable manifestus dual-ring vault on disk. Closes the DB and reopens from the vault file to prove on-disk durability + decryption, not just in-RAM round-trip.

use kete::{App, FlatStorage};
use rarangi::{Db, Pk, Record};

const APP: App = App {
    id: "rarangi-test",
    dir: "rarangi-test",
};

#[test]
fn music_vault_on_real_encrypted_vault() {
    let device_secret = [7u8; 32];
    // Vault seed is 32 bytes (an identity seed in production); fixed here so both sessions open the same vault.
    let vault_seed = *blake3::hash(b"rarangi-integration").as_bytes();

    // --- session 1: write a track + its audio blob ---
    {
        let storage = FlatStorage::new(APP, vault_seed, device_secret).unwrap();
        let mut db = Db::open(storage).unwrap();

        let audio = vec![0xABu8; 3 * (1 << 20) + 4242]; // ~3 MB → multi-chunk
        let blob_id = db.put_blob(&audio).unwrap();

        let rec = Record::new()
            .set("title", "3AM")
            .set("artist", "Matchbox Twenty")
            .set("year", 1996u64)
            .set("audio_ref", blob_id);
        db.put_row("track", "trk1", &rec).unwrap();
    } // db dropped → vault file closed

    // --- session 2: reopen the on-disk vault, read everything back ---
    let storage = FlatStorage::new(APP, vault_seed, device_secret).unwrap();
    let db = Db::open(storage).unwrap();

    let got = db.get_row("track", "trk1").unwrap().expect("row survived restart");
    assert_eq!(got.text("title"), Some("3AM"));
    assert_eq!(got.text("artist"), Some("Matchbox Twenty"));
    assert_eq!(got.uint("year"), Some(1996));

    let ref_id = got.text("audio_ref").expect("audio_ref present");
    let audio_back = db.get_blob(ref_id).unwrap().expect("blob survived restart");
    assert_eq!(audio_back.len(), 3 * (1 << 20) + 4242);
    assert!(audio_back.iter().all(|&b| b == 0xAB));

    assert_eq!(db.list("track").unwrap(), vec!["trk1"]);
}

#[test]
fn byte_table_int_counter_on_real_encrypted_vault() {
    const APP2: App = App {
        id: "rarangi-test-bytes",
        dir: "rarangi-test-bytes",
    };
    let device_secret = [11u8; 32];
    let vault_seed = *blake3::hash(b"rarangi-integration-bytes").as_bytes();
    // A raw 32-byte table id (a friendship id in production) keyed by a monotonic message counter — never text-encoded into a key.
    let friendship_id = *blake3::hash(b"alice<->bob").as_bytes();

    // --- session 1: append three messages by counter 0,1,2 ---
    {
        let storage = FlatStorage::new(APP2, vault_seed, device_secret).unwrap();
        let mut db = Db::open(storage).unwrap();
        for (i, body) in ["first", "second", "third"].iter().enumerate() {
            db.put_row_in(&friendship_id, Pk::Int(i as u64), &Record::new().set("body", *body))
                .unwrap();
        }
    } // vault file closed

    // --- session 2: reopen the on-disk encrypted vault, read everything back ---
    let storage = FlatStorage::new(APP2, vault_seed, device_secret).unwrap();
    let db = Db::open(storage).unwrap();

    assert_eq!(
        db.get_row_in(&friendship_id, Pk::Int(0)).unwrap().expect("msg 0 survived").text("body"),
        Some("first")
    );
    assert_eq!(
        db.get_row_in(&friendship_id, Pk::Int(2)).unwrap().expect("msg 2 survived").text("body"),
        Some("third")
    );
    assert_eq!(
        db.list_in(&friendship_id).unwrap(),
        vec![Pk::Int(0), Pk::Int(1), Pk::Int(2)]
    );
}
