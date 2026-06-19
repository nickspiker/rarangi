//! Typed field values — the clean abstraction over `VsfType` so callers (and rows) never touch the wire types directly. Each `Value` maps to exactly one self-describing VSF type; decoding matches on the type marker (never on position), tolerating wider/narrower encodings of the same kind.

use vsf::{EtType, Tensor, VsfType};

/// A field value in a row.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    /// Unicode text (titles, lyrics, names) — VSF `x`.
    Text(String),
    /// Unsigned integer — VSF `u6`.
    Uint(u64),
    /// Opaque bytes (hashes, small binary) — VSF `t_u3`. Large media goes through blobs, not a field.
    Bytes(Vec<u8>),
    /// Eagle-time, in oscillations since the lunar landing — VSF `e` (`e6`). The only timestamp type; no Unix time anywhere.
    Time(i64),
}

impl Value {
    /// Encode to the matching self-describing VSF type. Unsigned integers take the narrowest VSF width that holds the value (EWE) — `1996` is 2 bytes (`u4`), not 8 — so the caller passes one `u64` and storage stays minimal.
    pub fn to_vsf(&self) -> VsfType {
        match self {
            Value::Text(s) => VsfType::x(s.clone()),
            Value::Uint(u) => uint_to_vsf(*u),
            Value::Bytes(b) => VsfType::t_u3(Tensor::new(vec![b.len()], b.clone())),
            Value::Time(t) => VsfType::e(EtType::e6(*t)),
        }
    }

    /// Recover a `Value` from a VSF type by its marker. Tolerant: any unsigned width reads as `Uint`, either string type as `Text`, any eagle-time width as `Time`.
    pub fn from_vsf(v: &VsfType) -> Option<Value> {
        Some(match v {
            VsfType::x(s) => Value::Text(s.clone()),
            VsfType::a(s) => Value::Text(s.clone()),
            VsfType::u3(u) => Value::Uint(*u as u64),
            VsfType::u4(u) => Value::Uint(*u as u64),
            VsfType::u5(u) => Value::Uint(*u as u64),
            VsfType::u6(u) => Value::Uint(*u),
            VsfType::t_u3(t) => Value::Bytes(t.data.clone()),
            VsfType::e(et) => Value::Time(eagle_oscillations(et)?),
            _ => return None,
        })
    }
}

/// The narrowest VSF unsigned width that holds `u` (EWE): `u3` ≤ 255, `u4` ≤ 65535, `u5` ≤ 2³²-1, else `u6`.
fn uint_to_vsf(u: u64) -> VsfType {
    if u <= u8::MAX as u64 {
        VsfType::u3(u as u8)
    } else if u <= u16::MAX as u64 {
        VsfType::u4(u as u16)
    } else if u <= u32::MAX as u64 {
        VsfType::u5(u as u32)
    } else {
        VsfType::u6(u)
    }
}

fn eagle_oscillations(et: &EtType) -> Option<i64> {
    match et {
        EtType::e5(v) => Some(*v as i64),
        EtType::e6(v) => Some(*v),
        EtType::e7(v) => Some(*v as i64),
        _ => None,
    }
}

impl From<&str> for Value {
    fn from(s: &str) -> Self {
        Value::Text(s.to_string())
    }
}
impl From<String> for Value {
    fn from(s: String) -> Self {
        Value::Text(s)
    }
}
impl From<u64> for Value {
    fn from(u: u64) -> Self {
        Value::Uint(u)
    }
}
impl From<Vec<u8>> for Value {
    fn from(b: Vec<u8>) -> Self {
        Value::Bytes(b)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uint_uses_minimal_ewe_width() {
        assert!(matches!(uint_to_vsf(200), VsfType::u3(200)));
        assert!(matches!(uint_to_vsf(1996), VsfType::u4(1996)));
        assert!(matches!(uint_to_vsf(70_000), VsfType::u5(70_000)));
        assert!(matches!(uint_to_vsf(5_000_000_000), VsfType::u6(5_000_000_000)));
        // ...and every width decodes back to the same u64.
        for n in [0u64, 200, 1996, 70_000, 5_000_000_000] {
            assert_eq!(Value::from_vsf(&uint_to_vsf(n)), Some(Value::Uint(n)));
        }
    }
}
