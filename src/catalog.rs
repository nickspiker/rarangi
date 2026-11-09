//! The per-table primary-key catalog — the enumeration substrate over a non-enumerable store. One shard for now (the music-vault scale is tens of KB; splitting a shard as it nears the value cap is a later change that doesn't touch the public API). A complete VSF file with one multi-valued `pk` field.
//!
//! Each pk is stored by its native VSF type — a counter as an unsigned int, a string id as text, a hash as opaque bytes — so `list`/`list_in` reconstruct the exact typed `Pk` that was put. A text table (the `&str` row API) is a byte table named by its x wire form, so it shares this one catalog codec.

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
