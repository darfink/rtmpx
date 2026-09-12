// In-house AMF0 codec for RTMP, vendored from rml_amf0 0.3.0.
// Upstream: rust-media-libs, MIT, Copyright 2017 Matthew Shapiro.
// See README.md ("Changes from RML") for provenance.
// Hardened for proxy ingest with depth and collection caps,
// exact reads, strict truncated arrays, property name length enforcement,
// slice based serialize, borrowed getters, single value helper,
// and direct conversion to and from Amf3Value.
use crate::amf::common;
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
/// Inline values form a tree. `Reference` values belong to an [`Amf0Document`],
/// which owns complex objects once and supports cycles. The convenience decoder
/// expands acyclic graphs under a budget. Use [`deserialize_document`] to preserve identity.
#[derive(PartialEq, Debug, Clone)]
#[non_exhaustive]
pub enum Amf0Value {
    /// A document-local complex-object identity.
    Reference(crate::amf::ObjectId),
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
    /// Borrow string storage without allocating.
    pub fn as_str(&self) -> Option<&str> {
        if let Self::Utf8String(value) = self {
            Some(value)
        } else {
            None
        }
    }

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
    /// Borrow anonymous or typed object properties without cloning them.
    pub fn as_object(&self) -> Option<&Amf0Object> {
        match self {
            Self::Object(properties) | Self::TypedObject { properties, .. } => Some(properties),
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
#[non_exhaustive]
pub enum Amf0DeserializationError {
    #[error("AMF0 graph expansion exceeds the tree budget; use deserialize_document")]
    ExpansionLimit,
    #[error("AMF0 invalid reference {0}")]
    BadReference(u16),
    #[error("AMF0 cyclic reference {0}; use deserialize_document")]
    CyclicReference(u16),
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
#[non_exhaustive]
pub enum Amf0SerializationError {
    #[error("AMF0 invalid or unbound reference {0:?}")]
    InvalidReference(crate::amf::ObjectId),
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
    let mut output = Vec::new();
    serialize_into(values, &mut output)?;
    Ok(output)
}
/// Append one encoding context. On error the caller's output is unchanged.
pub fn serialize_into(
    values: &[Amf0Value],
    output: &mut Vec<u8>,
) -> Result<(), Amf0SerializationError> {
    encode_document(values, &[], &[], output)
}
fn encode_document(
    roots: &[Amf0Value],
    objects: &[Amf0Value],
    embedded: &[crate::amf3::Amf3Value],
    output: &mut Vec<u8>,
) -> Result<(), Amf0SerializationError> {
    let mut ctx = EncodeContext {
        objects,
        embedded,
        refs: Default::default(),
        next: 0,
    };
    let start = output.len();
    for value in roots {
        if let Err(error) = serialize_value(value, output, 0, &mut ctx) {
            output.truncate(start);
            return Err(error);
        }
    }
    Ok(())
}
struct EncodeContext<'a> {
    objects: &'a [Amf0Value],
    embedded: &'a [crate::amf3::Amf3Value],
    refs: std::collections::HashMap<crate::amf::ObjectId, u16>,
    next: usize,
}
#[derive(Default)]
struct DecodeContext {
    objects: Vec<Amf0Value>,
    embedded: Vec<crate::amf3::Amf3Value>,
}
fn is_complex(value: &Amf0Value) -> bool {
    matches!(
        value,
        Amf0Value::Object(_) | Amf0Value::StrictArray(_) | Amf0Value::TypedObject { .. }
    )
}
/// AMF0 arena, with a separate AMF3 arena for embedded AVM+ values.
#[derive(Clone, Debug, PartialEq)]
pub struct Amf0Document {
    values: crate::amf::Document<Amf0Value>,
    embedded: Vec<crate::amf3::Amf3Value>,
}
impl Default for Amf0Document {
    fn default() -> Self {
        Self::new()
    }
}
impl std::ops::Deref for Amf0Document {
    type Target = crate::amf::Document<Amf0Value>;
    fn deref(&self) -> &Self::Target {
        &self.values
    }
}
impl std::ops::DerefMut for Amf0Document {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.values
    }
}
impl Amf0Document {
    /// Resolve a reference, or borrow an inline value unchanged.
    pub fn resolve<'a>(&'a self, value: &'a Amf0Value) -> Option<&'a Amf0Value> {
        match value {
            Amf0Value::Reference(id) => self.get(*id),
            inline => Some(inline),
        }
    }

    pub fn new() -> Self {
        Self {
            values: crate::amf::Document::new(),
            embedded: Vec::new(),
        }
    }
    pub fn get_amf3(&self, id: crate::amf::ObjectId) -> Option<&crate::amf3::Amf3Value> {
        self.embedded.get(id.0)
    }
    /// Import an AMF3 document with exactly one root. IDs are relocated to this arena.
    pub fn embed_amf3(
        &mut self,
        document: crate::amf3::Amf3Document,
    ) -> Result<Amf0Value, crate::amf3::Amf3Document> {
        if document.roots().len() != 1 {
            return Err(document);
        }
        let (mut roots, mut objects) = document.into_parts();
        let base = self.embedded.len();
        for value in roots.iter_mut().chain(objects.iter_mut()) {
            crate::amf3::relocate(value, base);
        }
        self.embedded.extend(objects);
        Ok(Amf0Value::AvmPlus(Box::new(roots.pop().unwrap())))
    }
    pub fn serialize_into(&self, output: &mut Vec<u8>) -> Result<(), Amf0SerializationError> {
        encode_document(self.roots(), self.objects(), &self.embedded, output)
    }
    pub fn serialize(&self) -> Result<Vec<u8>, Amf0SerializationError> {
        let mut out = Vec::new();
        self.serialize_into(&mut out)?;
        Ok(out)
    }
}
/// Decode references as IDs rather than duplicating their target objects.
pub fn deserialize_document<R: crate::amf::AmfRead>(
    input: &mut R,
) -> Result<Amf0Document, Amf0DeserializationError> {
    let mut ctx = DecodeContext {
        ..Default::default()
    };
    let mut roots = Vec::new();
    while let Some(value) = read_next_value(input, 0, &mut ctx)? {
        ensure_de_collection(roots.len() + 1)?;
        roots.push(value);
    }
    Ok(Amf0Document {
        values: crate::amf::Document::from_parts(roots, ctx.objects),
        embedded: ctx.embedded,
    })
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
    ctx: &mut EncodeContext<'_>,
) -> Result<(), Amf0SerializationError> {
    ensure_ser_depth(depth)?;
    if let Amf0Value::Reference(id) = value {
        let value = ctx
            .objects
            .get(id.0)
            .ok_or(Amf0SerializationError::InvalidReference(*id))?;
        if !is_complex(value) {
            return Err(Amf0SerializationError::InvalidReference(*id));
        }
        if let Some(index) = ctx.refs.get(id) {
            bytes.push(7);
            common::write_u16_be(bytes, *index);
            return Ok(());
        }
        if ctx.next > u16::MAX as usize {
            return Err(Amf0SerializationError::CollectionTooLarge(ctx.next));
        }
        ctx.refs.insert(*id, ctx.next as u16);
        return serialize_value(value, bytes, depth, ctx);
    }
    if is_complex(value) {
        ctx.next += 1;
    }
    match value {
        Amf0Value::Reference(_) => unreachable!(),
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
        Amf0Value::Object(val) => serialize_object(val, bytes, depth, ctx),
        Amf0Value::StrictArray(val) => serialize_strict_array(val, bytes, depth, ctx),
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
            serialize_object_body(properties, bytes, depth, ctx)
        }
        Amf0Value::AvmPlus(inner) => {
            bytes.push(markers::AVMPLUS_OBJECT_MARKER);
            crate::amf3::encode_document(std::slice::from_ref(inner.as_ref()), ctx.embedded, bytes)
                .map_err(|e| Amf0SerializationError::EmbeddedAmf3(e.to_string()))?;
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
    ctx: &mut EncodeContext<'_>,
) -> Result<(), Amf0SerializationError> {
    ensure_ser_depth(depth)?;
    ensure_ser_collection(properties.len())?;
    bytes.push(markers::OBJECT_MARKER);
    serialize_object_body(properties, bytes, depth, ctx)
}
/// Property list plus terminator, shared by `object-marker` and
/// `typed-object-marker`. Property names ride on a bare `u16` in both cases,
/// so unlike string *values* they have no long form.
fn serialize_object_body(
    properties: &Amf0Object,
    bytes: &mut Vec<u8>,
    depth: usize,
    ctx: &mut EncodeContext<'_>,
) -> Result<(), Amf0SerializationError> {
    ensure_ser_depth(depth)?;
    ensure_ser_collection(properties.len())?;
    for (name, value) in properties {
        if name.len() > common::AMF0_MAX_STRING_LEN {
            return Err(Amf0SerializationError::NormalStringTooLong);
        }
        common::write_u16_be(bytes, name.len() as u16);
        bytes.extend_from_slice(name.as_bytes());
        serialize_value(value, bytes, depth + 1, ctx)?;
    }
    common::write_u16_be(bytes, markers::UTF_8_EMPTY_MARKER);
    bytes.push(markers::OBJECT_END_MARKER);
    Ok(())
}
fn serialize_strict_array(
    array: &Vec<Amf0Value>,
    bytes: &mut Vec<u8>,
    depth: usize,
    ctx: &mut EncodeContext<'_>,
) -> Result<(), Amf0SerializationError> {
    ensure_ser_depth(depth)?;
    ensure_ser_collection(array.len())?;
    bytes.push(markers::STRICT_ARRAY_MARKER);
    common::write_u32_be(bytes, array.len() as u32);
    for value in array {
        serialize_value(value, bytes, depth + 1, ctx)?;
    }
    Ok(())
}
struct ObjectProperty {
    label: String,
    value: Amf0Value,
}
pub fn deserialize<R: crate::amf::AmfRead>(
    bytes: &mut R,
) -> Result<Vec<Amf0Value>, Amf0DeserializationError> {
    deserialize_document(bytes)?
        .to_tree(crate::amf::TreeLimits::default())
        .map_err(tree_error)
}
pub fn deserialize_single<R: crate::amf::AmfRead>(
    bytes: &mut R,
) -> Result<Amf0Value, Amf0DeserializationError> {
    let mut ctx = DecodeContext {
        ..Default::default()
    };
    let value =
        read_next_value(bytes, 0, &mut ctx)?.ok_or(Amf0DeserializationError::UnexpectedEof)?;
    let doc = Amf0Document {
        values: crate::amf::Document::from_parts(vec![value], ctx.objects),
        embedded: ctx.embedded,
    };
    Ok(doc
        .to_tree(crate::amf::TreeLimits::default())
        .map_err(tree_error)?
        .pop()
        .unwrap())
}
fn tree_error(error: crate::amf::TreeError) -> Amf0DeserializationError {
    match error {
        crate::amf::TreeError::Cycle(id) => Amf0DeserializationError::CyclicReference(id.0 as u16),
        crate::amf::TreeError::InvalidReference(id) => {
            Amf0DeserializationError::BadReference(id.0 as u16)
        }
        crate::amf::TreeError::Limit => Amf0DeserializationError::ExpansionLimit,
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
    ctx: &mut DecodeContext,
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
    if buffer[0] == 7 {
        let index = common::read_u16_be(bytes)?;
        if index as usize >= ctx.objects.len() {
            return Err(Amf0DeserializationError::BadReference(index));
        }
        return Ok(Some(Amf0Value::Reference(crate::amf::ObjectId(
            index as usize,
        ))));
    }
    let referenceable = matches!(buffer[0], 3 | 8 | 10 | 16);
    let index = ctx.objects.len();
    if referenceable {
        ensure_de_collection(index + 1)?;
        ctx.objects.push(Amf0Value::Null);
    }
    let result = match buffer[0] {
        markers::BOOLEAN_MARKER => parse_bool(bytes).map(Some),
        markers::NULL_MARKER => Ok(Some(Amf0Value::Null)),
        markers::UNDEFINED_MARKER => Ok(Some(Amf0Value::Undefined)),
        markers::NUMBER_MARKER => parse_number(bytes).map(Some),
        markers::OBJECT_MARKER => parse_object(bytes, depth, ctx).map(Some),
        markers::ECMA_ARRAY_MARKER => parse_ecma_array(bytes, depth, ctx).map(Some),
        markers::STRING_MARKER => parse_string(bytes).map(Some),
        markers::STRICT_ARRAY_MARKER => parse_strict_array(bytes, depth, ctx).map(Some),
        markers::LONG_STRING_MARKER => parse_long_string(bytes).map(Some),
        markers::DATE_MARKER => parse_date(bytes).map(Some),
        markers::XML_DOCUMENT_MARKER => parse_xml_document(bytes).map(Some),
        markers::TYPED_OBJECT_MARKER => parse_typed_object(bytes, depth, ctx).map(Some),
        markers::AVMPLUS_OBJECT_MARKER => parse_avmplus(bytes, depth, ctx).map(Some),
        other => Err(Amf0DeserializationError::UnknownMarker { marker: other }),
    }?;
    if referenceable && let Some(value) = result {
        ctx.objects[index] = value;
        return Ok(Some(Amf0Value::Reference(crate::amf::ObjectId(index))));
    }
    Ok(result)
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
    ctx: &mut DecodeContext,
) -> Result<Amf0Value, Amf0DeserializationError> {
    ensure_de_depth(depth)?;
    let name_length = common::read_u16_be(bytes)? as usize;
    let name_buffer = read_checked(bytes, name_length)?;
    let class_name = String::from_utf8(name_buffer)?;
    match parse_object(bytes, depth, ctx)? {
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
    ctx: &mut DecodeContext,
) -> Result<Amf0Value, Amf0DeserializationError> {
    ensure_de_depth(depth)?;
    let document = crate::amf3::deserialize_document_single(bytes)
        .map_err(|e| Amf0DeserializationError::EmbeddedAmf3(e.to_string()))?;
    let (mut roots, mut objects) = document.into_parts();
    let base = ctx.embedded.len();
    ensure_de_collection(base + objects.len())?;
    for value in roots.iter_mut().chain(objects.iter_mut()) {
        crate::amf3::relocate(value, base);
    }
    ctx.embedded.extend(objects);
    Ok(Amf0Value::AvmPlus(Box::new(roots.pop().unwrap())))
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
    ctx: &mut DecodeContext,
) -> Result<Amf0Value, Amf0DeserializationError> {
    ensure_de_depth(depth)?;
    let mut properties = Amf0Object::new();
    while let Some(property) = parse_object_property(bytes, depth, ctx)? {
        ensure_de_collection(properties.len() + 1)?;
        properties.insert(property.label, property.value);
    }
    Ok(Amf0Value::Object(properties))
}
fn parse_ecma_array<R: common::AmfRead>(
    bytes: &mut R,
    depth: usize,
    ctx: &mut DecodeContext,
) -> Result<Amf0Value, Amf0DeserializationError> {
    let count = common::read_u32_be(bytes)? as usize;
    ensure_de_collection(count)?;
    parse_object(bytes, depth, ctx)
}
fn parse_strict_array<R: common::AmfRead>(
    bytes: &mut R,
    depth: usize,
    ctx: &mut DecodeContext,
) -> Result<Amf0Value, Amf0DeserializationError> {
    let count = common::read_u32_be(bytes)? as usize;
    ensure_de_collection(count)?;
    let mut values = Vec::new();
    for _ in 0..count {
        match read_next_value(bytes, depth + 1, ctx)? {
            Some(value) => values.push(value),
            None => return Err(Amf0DeserializationError::UnexpectedEof),
        }
    }
    Ok(Amf0Value::StrictArray(values))
}
fn parse_object_property<R: common::AmfRead>(
    bytes: &mut R,
    depth: usize,
    ctx: &mut DecodeContext,
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
    match read_next_value(bytes, depth + 1, ctx)? {
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
            Amf0Value::Reference(id) => B::Reference(*id),
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

impl crate::amf::graph::GraphValue for Amf0Value {
    fn check_embedded(
        &self,
        objects: &[crate::amf3::Amf3Value],
        limits: &mut crate::amf::TreeLimits,
        depth: usize,
    ) -> Result<(), crate::amf::TreeError> {
        if let Self::AvmPlus(value) = self {
            crate::amf::graph::validate(
                value.as_ref(),
                objects,
                &[],
                &mut Vec::new(),
                limits,
                depth + 1,
            )?;
        }
        Ok(())
    }
    fn install_embedded(&mut self, objects: &[crate::amf3::Amf3Value]) {
        if let Self::AvmPlus(value) = self {
            crate::amf::graph::install(value.as_mut(), objects, &[]);
        }
    }

    fn reference(&self) -> Option<crate::amf::ObjectId> {
        if let Self::Reference(id) = self {
            Some(*id)
        } else {
            None
        }
    }
    fn heap_bytes(&self) -> usize {
        let map = |p: &Amf0Object| {
            p.len()
                .saturating_mul(std::mem::size_of::<(String, Self)>())
                .saturating_add(p.keys().map(|k| k.len()).sum::<usize>())
        };
        match self {
            Self::Utf8String(s) | Self::XmlDocument(s) => s.len(),
            Self::Object(p) => map(p),
            Self::TypedObject {
                class_name,
                properties,
            } => class_name.len().saturating_add(map(properties)),
            Self::StrictArray(v) => v.len() * std::mem::size_of::<Self>(),
            Self::AvmPlus(_) => std::mem::size_of::<crate::amf3::Amf3Value>(),
            _ => 0,
        }
    }
    fn children(
        &self,
        visit: &mut dyn FnMut(&Self) -> Result<(), crate::amf::TreeError>,
    ) -> Result<(), crate::amf::TreeError> {
        match self {
            Self::Object(p) | Self::TypedObject { properties: p, .. } => {
                for v in p.values() {
                    visit(v)?;
                }
            }
            Self::StrictArray(v) => {
                for v in v {
                    visit(v)?;
                }
            }
            _ => {}
        }
        Ok(())
    }
    fn children_mut(&mut self, visit: &mut dyn FnMut(&mut Self)) {
        match self {
            Self::Object(p) | Self::TypedObject { properties: p, .. } => {
                for v in p.values_mut() {
                    visit(v);
                }
            }
            Self::StrictArray(v) => {
                for v in v {
                    visit(v);
                }
            }
            _ => {}
        }
    }
}

impl Amf0Document {
    /// Expand sharing, including embedded AMF3, under one total budget. Reject cycles.
    pub fn to_tree(
        &self,
        limits: crate::amf::TreeLimits,
    ) -> Result<Vec<Amf0Value>, crate::amf::TreeError> {
        crate::amf::graph::expand_with_embedded(
            self.roots(),
            self.objects(),
            &self.embedded,
            limits,
        )
    }
}
