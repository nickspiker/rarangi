//! A row: an ordered set of named typed fields, encoded as one complete VSF file (magic, version, eagle-time, provenance hash, one `row` section). Complete-file-per-record is the storage rule — every stored value is inspectable and integrity-checked.

use vsf::{VsfBuilder, VsfHeader, VsfSection, VsfType};

use crate::value::Value;
use crate::wal::{DbError, Result};

const SECTION: &str = "row";

/// An ordered collection of named field values. Built fluently, queried by typed getters.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Record {
    fields: Vec<(String, Value)>,
}

impl Record {
    pub fn new() -> Self {
        Self::default()
    }

    /// Set (or append) a field. Builder-style; last write to a name wins on read.
    pub fn set(mut self, name: impl Into<String>, value: impl Into<Value>) -> Self {
        self.fields.push((name.into(), value.into()));
        self
    }

    /// The raw value for a field (the last one set under that name).
    pub fn get(&self, name: &str) -> Option<&Value> {
        self.fields.iter().rev().find(|(n, _)| n == name).map(|(_, v)| v)
    }

    pub fn text(&self, name: &str) -> Option<&str> {
        match self.get(name)? {
            Value::Text(s) => Some(s),
            _ => None,
        }
    }
    pub fn uint(&self, name: &str) -> Option<u64> {
        match self.get(name)? {
            Value::Uint(u) => Some(*u),
            _ => None,
        }
    }
    pub fn bytes(&self, name: &str) -> Option<&[u8]> {
        match self.get(name)? {
            Value::Bytes(b) => Some(b),
            _ => None,
        }
    }
    pub fn time(&self, name: &str) -> Option<i64> {
        match self.get(name)? {
            Value::Time(t) => Some(*t),
            _ => None,
        }
    }

    pub fn fields(&self) -> &[(String, Value)] {
        &self.fields
    }
}

/// Encode a record as a complete VSF file.
pub fn encode(rec: &Record) -> Vec<u8> {
    let fields: Vec<(String, VsfType)> = rec
        .fields
        .iter()
        .map(|(name, value)| (name.clone(), value.to_vsf()))
        .collect();
    VsfBuilder::new()
        .creation_time_oscillations(vsf::eagle_time_oscillations())
        .add_section(SECTION, fields)
        .build()
        .expect("record encode")
}

/// Decode a record from a complete VSF file.
pub fn decode(bytes: &[u8]) -> Result<Record> {
    let (_, mut ptr) = VsfHeader::decode(bytes).map_err(DbError::Corrupt)?;
    let sec = VsfSection::parse(bytes, &mut ptr).map_err(DbError::Corrupt)?;
    let mut fields = Vec::with_capacity(sec.fields.len());
    for f in &sec.fields {
        let Some(v) = f.values.first() else { continue };
        let value = Value::from_vsf(v)
            .ok_or_else(|| DbError::Corrupt(format!("row field {} has unsupported type", f.name)))?;
        fields.push((f.name.clone(), value));
    }
    Ok(Record { fields })
}
