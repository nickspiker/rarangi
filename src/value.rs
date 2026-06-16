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
    /// Encode to the matching self-describing VSF type.
    pub fn to_vsf(&self) -> VsfType {
        match self {
            Value::Text(s) => VsfType::x(s.clone()),
            Value::Uint(u) => VsfType::u6(*u),
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
