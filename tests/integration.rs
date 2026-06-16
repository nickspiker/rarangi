//! End-to-end proof of the real stack: rarangi rows + blobs through kete's encrypted per-key ChaCha20-Poly1305 onto a durable manifestus dual-ring vault on disk. Closes the DB and reopens from the vault file to prove on-disk durability + decryption, not just in-RAM round-trip.

use kete::{App, FlatStorage};
use rarangi::{Db, Record};

const APP: App = App {
    id: "rarangi-test",
    dir: "rarangi-test",
};

#[test]
fn music_vault_on_real_encrypted_vault() {
    let device_secret = [7u8; 32];
    let handle = "rarangi-integration";

    // --- session 1: write a track + its audio blob ---
    {
        let storage = FlatStorage::new(APP, handle, device_secret).unwrap();
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
    let storage = FlatStorage::new(APP, handle, device_secret).unwrap();
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
