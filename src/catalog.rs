//! The per-table primary-key catalog — the enumeration substrate over a non-enumerable store. One shard for now (the music-vault scale is tens of KB; splitting a shard as it nears the value cap is a later change that doesn't touch the public API). A complete VSF file with one multi-valued `pk` field.
//!
//! Each pk is stored by its native VSF type — a counter as an unsigned int, a string id as text, a hash as opaque bytes — so `list`/`list_in` reconstruct the exact typed `Pk` that was put. The string catalog (`encode`/`decode`) is the all-text special case of the typed one (`encode_pks`/`decode_pks`).

use vsf::{VsfBuilder, VsfField, VsfHeader, VsfSection};

use crate::pk::Pk;
use crate::wal::{DbError, Result};

/// Encode a list of typed pks as the catalog's `pk` field, each by its native VSF type.
pub fn encode_pks(pks: &[Pk]) -> Vec<u8> {
    let mut sec = VsfSection::new("catalog");
    sec.fields.push(VsfField {
        name: "pk".to_string(),
        values: pks.iter().map(|p| p.to_vsf()).collect(),
    });
    VsfBuilder::new()
        .creation_time_oscillations(vsf::eagle_time_oscillations())
        .add_section_direct(sec)
        .build()
        .expect("catalog encode")
}

/// Decode the catalog into typed owned pks (an int decodes to `Pk::Int`, text to `Pk::Str`, opaque bytes to `Pk::Bytes`).
pub fn decode_pks(bytes: &[u8]) -> Result<Vec<Pk<'static>>> {
    let (_, mut ptr) = VsfHeader::decode(bytes).map_err(DbError::Corrupt)?;
    let sec = VsfSection::parse(bytes, &mut ptr).map_err(DbError::Corrupt)?;
    let Some(field) = sec.get_field("pk") else {
        return Ok(Vec::new());
    };
    let mut pks = Vec::with_capacity(field.values.len());
    for v in &field.values {
        pks.push(Pk::from_vsf(v)?);
    }
    Ok(pks)
}

/// Encode a string-keyed catalog — the all-text case of [`encode_pks`], used by the string row API.
pub fn encode(pks: &[String]) -> Vec<u8> {
    let typed: Vec<Pk> = pks.iter().map(|p| Pk::str(p)).collect();
    encode_pks(&typed)
}

/// Decode a string-keyed catalog. Every entry must be text (the string API only ever stores `Pk::Str`); a non-string entry means a byte-keyed table was read through the string API and is rejected.
pub fn decode(bytes: &[u8]) -> Result<Vec<String>> {
    let mut pks = Vec::new();
    for p in decode_pks(bytes)? {
        match p {
            Pk::Str(s) => pks.push(s.into_owned()),
            _ => return Err(DbError::Corrupt("catalog pk not a string".into())),
        }
    }
    Ok(pks)
}
