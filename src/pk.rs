//! A typed primary key for byte-keyed tables.
//!
//! The row ADDRESS is always derived from the pk's raw bytes (so a binary pk like a hash or a counter never gets text-encoded into a key), while the catalog round-trips the pk by its native VSF type — a counter stays an unsigned int, a string stays text, opaque bytes stay opaque bytes — so `list_in()` returns the same typed value that was put.
//!
//! `Pk` borrows its `Str`/`Bytes` payloads via `Cow`, so a caller can pass a cheap `Pk::Str("trk1")` / `Pk::Bytes(&hash)` on the write path, and `list_in()` can hand back owned `Pk<'static>` values without leaking. (The spec sketches the borrow variants as `&'a str` / `&'a [u8]`; `Cow` is the lossless superset that also satisfies the `Pk<'static>` return type, so it's used here.)

use std::borrow::Cow;

use vsf::{Tensor, VsfType};

use crate::wal::{DbError, Result};

/// A primary key. The row address is derived from `addr_bytes()`; the catalog stores it by `to_vsf()` and reconstructs it with `from_vsf()`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Pk<'a> {
    /// A monotonic counter / sequence number — stored in the catalog as a VSF unsigned int (`u6` on the wire, narrowed by EWE on encode).
    Int(u64),
    /// A genuine string id — stored as VSF text (`x`).
    Str(Cow<'a, str>),
    /// Raw bytes (e.g. a 32-byte hash) — stored as VSF opaque bytes (`t_u3`), the same opaque-bytes type `Value::Bytes` uses in this crate.
    Bytes(Cow<'a, [u8]>),
}

impl<'a> Pk<'a> {
    /// Borrow a string id as a pk without allocating.
    pub fn str(s: &'a str) -> Self {
        Pk::Str(Cow::Borrowed(s))
    }

    /// Borrow raw bytes as a pk without allocating.
    pub fn bytes(b: &'a [u8]) -> Self {
        Pk::Bytes(Cow::Borrowed(b))
    }

    /// The pk's contribution to the row/catalog address — its raw bytes, never a text rendering. An `Int` contributes its 8 big-endian bytes (a stable, collision-free encoding of the counter); `Str`/`Bytes` contribute their literal bytes.
    pub fn addr_bytes(&self) -> Cow<'_, [u8]> {
        match self {
            Pk::Int(n) => Cow::Owned(n.to_be_bytes().to_vec()),
            Pk::Str(s) => Cow::Borrowed(s.as_bytes()),
            Pk::Bytes(b) => Cow::Borrowed(b.as_ref()),
        }
    }

    /// Encode the pk as the VSF type stored in the catalog. `Int` is narrowed to the smallest unsigned width that holds it (EWE), matching how the rest of the crate stores unsigned values.
    pub fn to_vsf(&self) -> VsfType {
        match self {
            Pk::Int(n) => uint_to_vsf(*n),
            Pk::Str(s) => VsfType::x(s.clone().into_owned()),
            Pk::Bytes(b) => VsfType::t_u3(Tensor::new(vec![b.len()], b.clone().into_owned())),
        }
    }

    /// Reconstruct an owned pk from a catalog VSF value. Tolerant of any unsigned width (all read back as `Int`) and either string type, mirroring `Value::from_vsf`.
    pub fn from_vsf(v: &VsfType) -> Result<Pk<'static>> {
        Ok(match v {
            VsfType::u3(n) => Pk::Int(*n as u64),
            VsfType::u4(n) => Pk::Int(*n as u64),
            VsfType::u5(n) => Pk::Int(*n as u64),
            VsfType::u6(n) => Pk::Int(*n),
            VsfType::x(s) | VsfType::a(s) => Pk::Str(Cow::Owned(s.clone())),
            VsfType::t_u3(t) => Pk::Bytes(Cow::Owned(t.data.clone())),
            _ => return Err(DbError::Corrupt("catalog pk has unsupported VSF type".into())),
        })
    }

    /// Take ownership, detaching from the borrowed lifetime so the pk can outlive its source (e.g. to be returned from `list_in`).
    pub fn into_owned(self) -> Pk<'static> {
        match self {
            Pk::Int(n) => Pk::Int(n),
            Pk::Str(s) => Pk::Str(Cow::Owned(s.into_owned())),
            Pk::Bytes(b) => Pk::Bytes(Cow::Owned(b.into_owned())),
        }
    }
}

/// The narrowest VSF unsigned width that holds `n` (EWE) — same policy as `value::uint_to_vsf`, duplicated here to keep that one private.
fn uint_to_vsf(n: u64) -> VsfType {
    if n <= u8::MAX as u64 {
        VsfType::u3(n as u8)
    } else if n <= u16::MAX as u64 {
        VsfType::u4(n as u16)
    } else if n <= u32::MAX as u64 {
        VsfType::u5(n as u32)
    } else {
        VsfType::u6(n)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip(pk: Pk) {
        let v = pk.to_vsf();
        let back = Pk::from_vsf(&v).unwrap();
        assert_eq!(pk.into_owned(), back);
    }

    #[test]
    fn pk_vsf_round_trip_all_kinds() {
        round_trip(Pk::Int(0));
        round_trip(Pk::Int(1));
        round_trip(Pk::Int(255));
        round_trip(Pk::Int(70_000));
        round_trip(Pk::Int(5_000_000_000));
        round_trip(Pk::str("trk1"));
        round_trip(Pk::str(""));
        round_trip(Pk::bytes(&[0u8; 32]));
        round_trip(Pk::bytes(&[1, 2, 3, 0xAB, 0xFF]));
    }

    #[test]
    fn int_addr_bytes_are_eight_be_bytes() {
        assert_eq!(Pk::Int(0).addr_bytes().as_ref(), &[0u8; 8]);
        assert_eq!(Pk::Int(1).addr_bytes().as_ref(), &[0, 0, 0, 0, 0, 0, 0, 1]);
        // Distinct counters give distinct address contributions.
        assert_ne!(Pk::Int(1).addr_bytes(), Pk::Int(2).addr_bytes());
    }
}
