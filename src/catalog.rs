//! The per-table primary-key catalog — the enumeration substrate over a non-enumerable store. One shard for now (the music-vault scale is tens of KB; splitting a shard as it nears the value cap is a later change that doesn't touch the public API). A complete VSF file with one multi-valued `pk` field.

use vsf::{VsfBuilder, VsfField, VsfHeader, VsfSection, VsfType};

use crate::wal::{DbError, Result};

pub fn encode(pks: &[String]) -> Vec<u8> {
    let mut sec = VsfSection::new("catalog");
    sec.fields.push(VsfField {
        name: "pk".to_string(),
        values: pks.iter().map(|p| VsfType::x(p.clone())).collect(),
    });
    VsfBuilder::new()
        .creation_time_oscillations(vsf::eagle_time_oscillations())
        .add_section_direct(sec)
        .build()
        .expect("catalog encode")
}

pub fn decode(bytes: &[u8]) -> Result<Vec<String>> {
    let (_, mut ptr) = VsfHeader::decode(bytes).map_err(DbError::Corrupt)?;
    let sec = VsfSection::parse(bytes, &mut ptr).map_err(DbError::Corrupt)?;
    let Some(field) = sec.get_field("pk") else {
        return Ok(Vec::new());
    };
    let mut pks = Vec::with_capacity(field.values.len());
    for v in &field.values {
        match v {
            VsfType::x(s) | VsfType::a(s) => pks.push(s.clone()),
            _ => return Err(DbError::Corrupt("catalog pk not a string".into())),
        }
    }
    Ok(pks)
}
