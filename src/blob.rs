//! Content-addressed large media. A blob is split into fixed chunks, each a complete VSF file under `blob/{id}/{n}`, with a `blob/{id}/manifest` recording total length and chunk count. The id is `blake3(content)`, so writes are idempotent and dedup for free, and the manifest's presence is the blob's commit point. Blobs are immutable, so they're written directly rather than through the WAL — an interrupted write leaves only orphan chunks under keys nothing references.

use vsf::{Tensor, VsfBuilder, VsfHeader, VsfSection, VsfType};

use crate::wal::{DbError, Result};

/// One chunk: 1 MiB. Comfortably under manifestus's ~4 MB single-value cap after the VSF envelope and kete's encryption framing.
pub const CHUNK_SIZE: usize = 1 << 20;

/// Content id: lowercase hex of BLAKE3(content).
pub fn id(content: &[u8]) -> String {
    blake3::hash(content).to_hex().to_string()
}

pub fn encode_chunk(data: &[u8]) -> Vec<u8> {
    VsfBuilder::new()
        .creation_time_oscillations(vsf::eagle_time_oscillations())
        .add_section(
            "chunk",
            vec![("data".to_string(), VsfType::t_u3(Tensor::new(vec![data.len()], data.to_vec())))],
        )
        .build()
        .expect("chunk encode")
}

pub fn decode_chunk(bytes: &[u8]) -> Result<Vec<u8>> {
    let (_, mut ptr) = VsfHeader::decode(bytes).map_err(DbError::Corrupt)?;
    let sec = VsfSection::parse(bytes, &mut ptr).map_err(DbError::Corrupt)?;
    match sec.get_field("data").and_then(|f| f.values.first()) {
        Some(VsfType::t_u3(t)) => Ok(t.data.clone()),
        _ => Err(DbError::Corrupt("blob chunk missing data".into())),
    }
}

pub struct Manifest {
    pub len: u64,
    pub chunks: u64,
}

pub fn encode_manifest(m: &Manifest) -> Vec<u8> {
    VsfBuilder::new()
        .creation_time_oscillations(vsf::eagle_time_oscillations())
        .add_section(
            "blob",
            vec![
                ("len".to_string(), VsfType::u6(m.len)),
                ("chunks".to_string(), VsfType::u6(m.chunks)),
            ],
        )
        .build()
        .expect("manifest encode")
}

pub fn decode_manifest(bytes: &[u8]) -> Result<Manifest> {
    let (_, mut ptr) = VsfHeader::decode(bytes).map_err(DbError::Corrupt)?;
    let sec = VsfSection::parse(bytes, &mut ptr).map_err(DbError::Corrupt)?;
    let len = match sec.get_field("len").and_then(|f| f.values.first()) {
        Some(VsfType::u6(v)) => *v,
        _ => return Err(DbError::Corrupt("blob manifest missing len".into())),
    };
    let chunks = match sec.get_field("chunks").and_then(|f| f.values.first()) {
        Some(VsfType::u6(v)) => *v,
        _ => return Err(DbError::Corrupt("blob manifest missing chunks".into())),
    };
    Ok(Manifest { len, chunks })
}
