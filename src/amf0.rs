// In-house AMF0 codec for RTMP, vendored from rml_amf0 0.3.0.
// Upstream: rust-media-libs, MIT, Copyright 2017 Matthew Shapiro.
// See README.md ("Changes from RML") for provenance.
// Hardened for proxy ingest with depth and collection caps,
// exact reads, strict truncated arrays, property name length enforcement,
// slice based serialize, borrowed getters, single value helper,
// and direct conversion to and from Amf3Value.
use crate::amf_common as common;
use std::io::{self};
use thiserror::Error;

/// The property map behind an AMF0 object.
///
/// Order preserving on purpose. AMF0 writes properties in sequence, so the map
/// order *is* the wire order; a `std::collections::HashMap` randomises
/// iteration per process, which makes the bytes emitted for the same logical
/// value differ between runs. That defeats byte-stable relaying, golden-byte
/// tests, and anything that hashes or caches an encoded payload.
pub type Amf0Object = indexmap::IndexMap<String, Amf0Value>;
/// An AMF0 value.
///
/// Like [`crate::amf3::Amf3Value`] this is a tree. The AMF0 `reference` marker
/// (`0x07`) is consequently not supported and decodes as
/// [`Amf0DeserializationError::UnknownMarker`]; no RTMP encoder in practice
/// emits it, and a typed error is preferable to inventing sharing.
#[derive(PartialEq, Debug, Clone)]
pub enum Amf0Value {
    Number(f64),
    Boolean(bool),
    Utf8String(String),
    Object(Amf0Object),
    StrictArray(Vec<Amf0Value>),
    Null,
    Undefined,
    /// `date-marker`. `timezone` is reserved by the spec and should be zero;
    /// it is preserved so relayed values re-encode byte-identically.
    Date {
        millis: f64,
        timezone: i16,
    },
    /// `xml-document-marker`. Carries the document as an unparsed string.
    XmlDocument(String),
    /// `typed-object-marker`: an object tagged with a class name.
    TypedObject {
        class_name: String,
        properties: Amf0Object,
    },
    /// A single AMF3 value carried inside an AMF0 stream behind the
    /// `avmplus-object-marker` (`0x11`).
    ///
    /// The escape is per-value and non-sticky: the byte after the nested AMF3
    /// value returns to AMF0. This is how an `objectEncoding` 3 peer mixes AMF3
    /// values into the AMF0-framed payload of a type 15/17 message, and it is
    /// also where [`crate::amf3::Amf3Value::to_amf0`] parks values that have no
    /// AMF0 equivalent rather than silently degrading them.
    AvmPlus(Box<crate::amf3::Amf3Value>),
}
impl Amf0Value {
    pub fn get_number(&self) -> Option<f64> {
        match self {
            Amf0Value::Number(value) => Some(*value),
            _ => None,
        }
    }
    pub fn get_boolean(&self) -> Option<bool> {
        match self {
            Amf0Value::Boolean(value) => Some(*value),
            _ => None,
        }
    }
    pub fn get_string(&self) -> Option<String> {
        match self {
            Amf0Value::Utf8String(value) => Some(value.clone()),
            _ => None,
        }
    }
    pub fn get_object_properties(&self) -> Option<Amf0Object> {
        match self {
            Amf0Value::Object(properties) => Some(properties.clone()),
            _ => None,
        }
    }
    pub fn get_integer(&self) -> Option<i32> {
        match self {
            Amf0Value::Number(n) => {
                if n.is_finite()
                    && n.fract() == 0.0
                    && *n >= i32::MIN as f64
                    && *n <= i32::MAX as f64
                {
                    Some(*n as i32)
                } else {
                    None
                }
            }
            _ => None,
        }
    }
    pub fn get_double(&self) -> Option<f64> {
        match self {
            Amf0Value::Number(value) => Some(*value),
            _ => None,
        }
    }
}
pub(crate) mod markers {
    pub const NUMBER_MARKER: u8 = 0;
    pub const BOOLEAN_MARKER: u8 = 1;
    pub const STRING_MARKER: u8 = 2;
    pub const OBJECT_MARKER: u8 = 3;
    pub const NULL_MARKER: u8 = 5;
    pub const UNDEFINED_MARKER: u8 = 6;
    pub const ECMA_ARRAY_MARKER: u8 = 8;
    pub const OBJECT_END_MARKER: u8 = 9;
    pub const STRICT_ARRAY_MARKER: u8 = 10;
    pub const DATE_MARKER: u8 = 11;
    pub const LONG_STRING_MARKER: u8 = 12;
    pub const XML_DOCUMENT_MARKER: u8 = 15;
    pub const TYPED_OBJECT_MARKER: u8 = 16;
    /// Switches the *next single value* to AMF3. Non-sticky.
    pub const AVMPLUS_OBJECT_MARKER: u8 = 17;
    pub const UTF_8_EMPTY_MARKER: u16 = 0;
}
#[derive(Debug, Error)]
pub enum Amf0DeserializationError {
    #[error("AMF0 unknown marker: {marker}")]
    UnknownMarker { marker: u8 },
    #[error("AMF0 unexpected empty object property name")]
    UnexpectedEmptyObjectPropertyName,
    #[error("AMF0 hit end of buffer while expecting more data")]
    UnexpectedEof,
    #[error("AMF0 nesting too deep")]
    DepthLimit,
    #[error("AMF0 collection too large: {0} items")]
    CollectionTooLarge(usize),
    #[error("AMF0 embedded AMF3 value: {0}")]
    EmbeddedAmf3(String),
    #[error("AMF0 failed to read byte buffer: {0}")]
    BufferReadError(#[from] io::Error),
    #[error("AMF0 failed to read utf8 string: {0}")]
    StringParseError(#[from] std::string::FromUtf8Error),
}
#[derive(Debug, Error)]
pub enum Amf0SerializationError {
    #[error("AMF0 string length greater than 65535")]
    NormalStringTooLong,
    #[error("AMF0 nesting too deep")]
    DepthLimit,
    #[error("AMF0 collection too large: {0} items")]
    CollectionTooLarge(usize),
    #[error("AMF0 embedded AMF3 value: {0}")]
    EmbeddedAmf3(String),
    #[error("AMF0 failed to write byte buffer")]
    BufferWriteError(#[from] io::Error),
}
pub fn serialize(values: &[Amf0Value]) -> Result<Vec<u8>, Amf0SerializationError> {
    let mut bytes = Vec::new();
    for value in values {
        serialize_value(value, &mut bytes, 0)?;
    }
    Ok(bytes)
}
fn ensure_ser_depth(depth: usize) -> Result<(), Amf0SerializationError> {
    if common::check_depth(depth) {
        Ok(())
    } else {
        Err(Amf0SerializationError::DepthLimit)
    }
}
fn ensure_ser_collection(len: usize) -> Result<(), Amf0SerializationError> {
    if !common::check_collection_len(len) {
        Err(Amf0SerializationError::CollectionTooLarge(len))
    } else {
        Ok(())
    }
}
fn serialize_value(
    value: &Amf0Value,
    bytes: &mut Vec<u8>,
    depth: usize,
) -> Result<(), Amf0SerializationError> {
    ensure_ser_depth(depth)?;
    match value {
        Amf0Value::Boolean(val) => {
            bytes.push(markers::BOOLEAN_MARKER);
            bytes.push(u8::from(*val));
            Ok(())
        }
        Amf0Value::Null => {
            bytes.push(markers::NULL_MARKER);
            Ok(())
        }
        Amf0Value::Undefined => {
            bytes.push(markers::UNDEFINED_MARKER);
            Ok(())
        }
        Amf0Value::Number(val) => {
            bytes.push(markers::NUMBER_MARKER);
            common::write_f64_be(bytes, *val);
            Ok(())
        }
        Amf0Value::Utf8String(val) => serialize_string(val, bytes),
        Amf0Value::Object(val) => serialize_object(val, bytes, depth),
        Amf0Value::StrictArray(val) => serialize_strict_array(val, bytes, depth),
        Amf0Value::Date { millis, timezone } => {
            bytes.push(markers::DATE_MARKER);
            common::write_f64_be(bytes, *millis);
            bytes.extend_from_slice(&timezone.to_be_bytes());
            Ok(())
        }
        Amf0Value::XmlDocument(val) => {
            bytes.push(markers::XML_DOCUMENT_MARKER);
            common::write_u32_be(bytes, val.len() as u32);
            bytes.extend_from_slice(val.as_bytes());
            Ok(())
        }
        Amf0Value::TypedObject {
            class_name,
            properties,
        } => {
            if class_name.len() > common::AMF0_MAX_STRING_LEN {
                return Err(Amf0SerializationError::NormalStringTooLong);
            }
            bytes.push(markers::TYPED_OBJECT_MARKER);
            common::write_u16_be(bytes, class_name.len() as u16);
            bytes.extend_from_slice(class_name.as_bytes());
            serialize_object_body(properties, bytes, depth)
        }
        Amf0Value::AvmPlus(inner) => {
            bytes.push(markers::AVMPLUS_OBJECT_MARKER);
            let encoded = crate::amf3::serialize(std::slice::from_ref(inner.as_ref()))
                .map_err(|e| Amf0SerializationError::EmbeddedAmf3(e.to_string()))?;
            bytes.extend_from_slice(&encoded);
            Ok(())
        }
    }
}
/// Emits `string-marker` when the value fits a `u16` length and
/// `long-string-marker` otherwise, so a long string decoded from the wire
/// re-encodes instead of failing.
fn serialize_string(value: &str, bytes: &mut Vec<u8>) -> Result<(), Amf0SerializationError> {
    if value.len() > common::AMF0_MAX_STRING_LEN {
        bytes.push(markers::LONG_STRING_MARKER);
        common::write_u32_be(bytes, value.len() as u32);
    } else {
        bytes.push(markers::STRING_MARKER);
        common::write_u16_be(bytes, value.len() as u16);
    }
    bytes.extend_from_slice(value.as_bytes());
    Ok(())
}
fn serialize_object(
    properties: &Amf0Object,
    bytes: &mut Vec<u8>,
    depth: usize,
) -> Result<(), Amf0SerializationError> {
    ensure_ser_depth(depth)?;
    ensure_ser_collection(properties.len())?;
    bytes.push(markers::OBJECT_MARKER);
    serialize_object_body(properties, bytes, depth)
}
/// Property list plus terminator, shared by `object-marker` and
/// `typed-object-marker`. Property names ride on a bare `u16` in both cases,
/// so unlike string *values* they have no long form.
fn serialize_object_body(
    properties: &Amf0Object,
    bytes: &mut Vec<u8>,
    depth: usize,
) -> Result<(), Amf0SerializationError> {
    ensure_ser_depth(depth)?;
    ensure_ser_collection(properties.len())?;
    for (name, value) in properties {
        if name.len() > common::AMF0_MAX_STRING_LEN {
            return Err(Amf0SerializationError::NormalStringTooLong);
        }
        common::write_u16_be(bytes, name.len() as u16);
        bytes.extend_from_slice(name.as_bytes());
        serialize_value(value, bytes, depth + 1)?;
    }
    common::write_u16_be(bytes, markers::UTF_8_EMPTY_MARKER);
    bytes.push(markers::OBJECT_END_MARKER);
    Ok(())
}
fn serialize_strict_array(
    array: &Vec<Amf0Value>,
    bytes: &mut Vec<u8>,
    depth: usize,
) -> Result<(), Amf0SerializationError> {
    ensure_ser_depth(depth)?;
    ensure_ser_collection(array.len())?;
    bytes.push(markers::STRICT_ARRAY_MARKER);
    common::write_u32_be(bytes, array.len() as u32);
    for value in array {
        serialize_value(value, bytes, depth + 1)?;
    }
    Ok(())
}
struct ObjectProperty {
    label: String,
    value: Amf0Value,
}
pub fn deserialize<R: common::AmfRead>(
    bytes: &mut R,
) -> Result<Vec<Amf0Value>, Amf0DeserializationError> {
    let mut results = Vec::new();
    while let Some(value) = read_next_value(bytes, 0)? {
        results.push(value);
    }
    Ok(results)
}
pub fn deserialize_single<R: common::AmfRead>(
    bytes: &mut R,
) -> Result<Amf0Value, Amf0DeserializationError> {
    match read_next_value(bytes, 0)? {
        Some(value) => Ok(value),
        None => Err(Amf0DeserializationError::UnexpectedEof),
    }
}
fn ensure_de_depth(depth: usize) -> Result<(), Amf0DeserializationError> {
    if common::check_depth(depth) {
        Ok(())
    } else {
        Err(Amf0DeserializationError::DepthLimit)
    }
}
fn ensure_de_collection(len: usize) -> Result<(), Amf0DeserializationError> {
    if !common::check_collection_len(len) {
        Err(Amf0DeserializationError::CollectionTooLarge(len))
    } else {
        Ok(())
    }
}
fn read_next_value<R: common::AmfRead>(
    bytes: &mut R,
    depth: usize,
) -> Result<Option<Amf0Value>, Amf0DeserializationError> {
    ensure_de_depth(depth)?;
    let mut buffer = [0u8; 1];
    let n = bytes
        .read(&mut buffer)
        .map_err(Amf0DeserializationError::BufferReadError)?;
    if n == 0 {
        return Ok(None);
    }
    if buffer[0] == markers::OBJECT_END_MARKER {
        return Ok(None);
    }
    match buffer[0] {
        markers::BOOLEAN_MARKER => parse_bool(bytes).map(Some),
        markers::NULL_MARKER => Ok(Some(Amf0Value::Null)),
        markers::UNDEFINED_MARKER => Ok(Some(Amf0Value::Undefined)),
        markers::NUMBER_MARKER => parse_number(bytes).map(Some),
        markers::OBJECT_MARKER => parse_object(bytes, depth).map(Some),
        markers::ECMA_ARRAY_MARKER => parse_ecma_array(bytes, depth).map(Some),
        markers::STRING_MARKER => parse_string(bytes).map(Some),
        markers::STRICT_ARRAY_MARKER => parse_strict_array(bytes, depth).map(Some),
        markers::LONG_STRING_MARKER => parse_long_string(bytes).map(Some),
        markers::DATE_MARKER => parse_date(bytes).map(Some),
        markers::XML_DOCUMENT_MARKER => parse_xml_document(bytes).map(Some),
        markers::TYPED_OBJECT_MARKER => parse_typed_object(bytes, depth).map(Some),
        markers::AVMPLUS_OBJECT_MARKER => parse_avmplus(bytes, depth).map(Some),
        other => Err(Amf0DeserializationError::UnknownMarker { marker: other }),
    }
}
/// `long-string-marker`: same payload as a string but with a `u32` length.
fn parse_long_string<R: common::AmfRead>(
    bytes: &mut R,
) -> Result<Amf0Value, Amf0DeserializationError> {
    let length = common::read_u32_be(bytes)? as usize;
    let buffer = read_checked(bytes, length)?;
    Ok(Amf0Value::Utf8String(String::from_utf8(buffer)?))
}
/// `date-marker`: milliseconds since the epoch plus a reserved timezone field.
fn parse_date<R: common::AmfRead>(bytes: &mut R) -> Result<Amf0Value, Amf0DeserializationError> {
    let millis = common::read_f64_be(bytes)?;
    let timezone = common::read_u16_be(bytes)? as i16;
    Ok(Amf0Value::Date { millis, timezone })
}
/// `xml-document-marker`: a `u32`-prefixed document, kept unparsed.
fn parse_xml_document<R: common::AmfRead>(
    bytes: &mut R,
) -> Result<Amf0Value, Amf0DeserializationError> {
    let length = common::read_u32_be(bytes)? as usize;
    let buffer = read_checked(bytes, length)?;
    Ok(Amf0Value::XmlDocument(String::from_utf8(buffer)?))
}
/// `typed-object-marker`: a class name followed by an ordinary property list.
fn parse_typed_object<R: common::AmfRead>(
    bytes: &mut R,
    depth: usize,
) -> Result<Amf0Value, Amf0DeserializationError> {
    ensure_de_depth(depth)?;
    let name_length = common::read_u16_be(bytes)? as usize;
    let name_buffer = read_checked(bytes, name_length)?;
    let class_name = String::from_utf8(name_buffer)?;
    match parse_object(bytes, depth)? {
        Amf0Value::Object(properties) => Ok(Amf0Value::TypedObject {
            class_name,
            properties,
        }),
        _ => Err(Amf0DeserializationError::UnexpectedEof),
    }
}
/// `avmplus-object-marker`: exactly one AMF3 value, after which the stream
/// returns to AMF0. The escape is per-value, so no AMF3 state is carried across
/// it and each occurrence gets fresh reference tables.
fn parse_avmplus<R: common::AmfRead>(
    bytes: &mut R,
    depth: usize,
) -> Result<Amf0Value, Amf0DeserializationError> {
    ensure_de_depth(depth)?;
    let value = crate::amf3::deserialize_single(bytes)
        .map_err(|e| Amf0DeserializationError::EmbeddedAmf3(e.to_string()))?;
    Ok(Amf0Value::AvmPlus(Box::new(value)))
}
/// Read `len` bytes, rejecting a length the input cannot satisfy before
/// allocating for it.
fn read_checked<R: common::AmfRead>(
    bytes: &mut R,
    len: usize,
) -> Result<Vec<u8>, Amf0DeserializationError> {
    if !common::length_is_plausible(bytes, len) {
        return Err(Amf0DeserializationError::UnexpectedEof);
    }
    common::read_exact_vec(bytes, len).map_err(|e| {
        if e.kind() == std::io::ErrorKind::UnexpectedEof {
            Amf0DeserializationError::UnexpectedEof
        } else {
            Amf0DeserializationError::BufferReadError(e)
        }
    })
}
fn parse_number<R: common::AmfRead>(bytes: &mut R) -> Result<Amf0Value, Amf0DeserializationError> {
    let number = common::read_f64_be(bytes)?;
    Ok(Amf0Value::Number(number))
}
fn parse_bool<R: common::AmfRead>(bytes: &mut R) -> Result<Amf0Value, Amf0DeserializationError> {
    let value = common::read_u8(bytes)?;
    if value == 1 {
        Ok(Amf0Value::Boolean(true))
    } else {
        Ok(Amf0Value::Boolean(false))
    }
}
fn parse_string<R: common::AmfRead>(bytes: &mut R) -> Result<Amf0Value, Amf0DeserializationError> {
    let length = common::read_u16_be(bytes)? as usize;
    let buffer = read_checked(bytes, length)?;
    let value = String::from_utf8(buffer)?;
    Ok(Amf0Value::Utf8String(value))
}
fn parse_object<R: common::AmfRead>(
    bytes: &mut R,
    depth: usize,
) -> Result<Amf0Value, Amf0DeserializationError> {
    ensure_de_depth(depth)?;
    let mut properties = Amf0Object::new();
    while let Some(property) = parse_object_property(bytes, depth)? {
        ensure_de_collection(properties.len() + 1)?;
        properties.insert(property.label, property.value);
    }
    Ok(Amf0Value::Object(properties))
}
fn parse_ecma_array<R: common::AmfRead>(
    bytes: &mut R,
    depth: usize,
) -> Result<Amf0Value, Amf0DeserializationError> {
    let count = common::read_u32_be(bytes)? as usize;
    ensure_de_collection(count)?;
    parse_object(bytes, depth)
}
fn parse_strict_array<R: common::AmfRead>(
    bytes: &mut R,
    depth: usize,
) -> Result<Amf0Value, Amf0DeserializationError> {
    let count = common::read_u32_be(bytes)? as usize;
    ensure_de_collection(count)?;
    let mut values = Vec::new();
    for _ in 0..count {
        match read_next_value(bytes, depth + 1)? {
            Some(value) => values.push(value),
            None => return Err(Amf0DeserializationError::UnexpectedEof),
        }
    }
    Ok(Amf0Value::StrictArray(values))
}
fn parse_object_property<R: common::AmfRead>(
    bytes: &mut R,
    depth: usize,
) -> Result<Option<ObjectProperty>, Amf0DeserializationError> {
    let label_length = common::read_u16_be(bytes)?;
    if label_length == 0 {
        let byte = common::read_u8(bytes)?;
        if byte != markers::OBJECT_END_MARKER {
            return Err(Amf0DeserializationError::UnexpectedEmptyObjectPropertyName);
        }
        return Ok(None);
    }
    let label_buffer = read_checked(bytes, label_length as usize)?;
    let label = String::from_utf8(label_buffer)?;
    match read_next_value(bytes, depth + 1)? {
        None => Err(Amf0DeserializationError::UnexpectedEof),
        Some(property_value) => Ok(Some(ObjectProperty {
            label,
            value: property_value,
        })),
    }
}
impl Amf0Value {
    /// Project into the AMF3 value model. Total and lossless: every AMF0 type
    /// has an AMF3 counterpart, and [`Amf0Value::AvmPlus`] simply unwraps.
    pub fn to_amf3(&self) -> crate::amf3::Amf3Value {
        use crate::amf3::Amf3Value as B;
        use crate::amf3::{MAX_AMF3_INTEGER, MIN_AMF3_INTEGER};
        match self {
            Amf0Value::Undefined => B::Undefined,
            Amf0Value::Null => B::Null,
            Amf0Value::Boolean(b) => B::Boolean(*b),
            Amf0Value::Number(n) => {
                if n.fract() == 0.0
                    && n.is_finite()
                    && *n >= MIN_AMF3_INTEGER as f64
                    && *n <= MAX_AMF3_INTEGER as f64
                {
                    B::Integer(*n as i32)
                } else {
                    B::Double(*n)
                }
            }
            Amf0Value::Utf8String(s) => B::String(s.clone()),
            Amf0Value::Object(map) => B::Object {
                class_name: None,
                sealed: Vec::new(),
                dynamic: Some(map.iter().map(|(k, v)| (k.clone(), v.to_amf3())).collect()),
            },
            Amf0Value::StrictArray(items) => B::Array {
                dense: items.iter().map(|v| v.to_amf3()).collect(),
                associative: Vec::new(),
            },
            Amf0Value::Date { millis, .. } => B::Date(*millis),
            Amf0Value::XmlDocument(s) => B::XmlDoc(s.clone()),
            Amf0Value::TypedObject {
                class_name,
                properties,
            } => B::Object {
                class_name: Some(class_name.clone()),
                sealed: Vec::new(),
                dynamic: Some(
                    properties
                        .iter()
                        .map(|(k, v)| (k.clone(), v.to_amf3()))
                        .collect(),
                ),
            },
            Amf0Value::AvmPlus(inner) => (**inner).clone(),
        }
    }
}

impl From<crate::amf3::Amf3Value> for Amf0Value {
    fn from(value: crate::amf3::Amf3Value) -> Self {
        value.to_amf0()
    }
}
