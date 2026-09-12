// In-house AMF3 codec for RTMP.
// See docs/enhanced-rtmp.md for wire mapping.
use std::collections::HashMap;

use crate::amf::common;
pub use crate::amf::common::{MAX_COLLECTION_LEN, MAX_DEPTH};
use crate::amf0::Amf0Object;
use thiserror::Error;
pub const MAX_STRING_LEN: usize = 4 * 1024 * 1024;
pub const MAX_BYTEARRAY_LEN: usize = 10 * 1024 * 1024;
pub const MAX_REFERENCES: usize = 100_000;
/// First byte of a type 15/16/17 payload: the values that follow are AMF0
/// encoded, with individual AMF3 values introduced by the AMF0
/// `avmplus-object-marker`.
pub const FORMAT_SELECTOR_AMF0: u8 = 0x00;
/// First byte of a type 15/16/17 payload: the values that follow are AMF3.
pub const FORMAT_SELECTOR_AMF3: u8 = 0x03;
pub const AVMPLUS_OBJECT_MARKER: u8 = 0x11;
const MARK_UNDEFINED: u8 = 0x00;
const MARK_NULL: u8 = 0x01;
const MARK_FALSE: u8 = 0x02;
const MARK_TRUE: u8 = 0x03;
const MARK_INTEGER: u8 = 0x04;
const MARK_DOUBLE: u8 = 0x05;
const MARK_STRING: u8 = 0x06;
const MARK_XML_DOC: u8 = 0x07;
const MARK_DATE: u8 = 0x08;
const MARK_ARRAY: u8 = 0x09;
const MARK_OBJECT: u8 = 0x0A;
const MARK_XML: u8 = 0x0B;
const MARK_BYTE_ARRAY: u8 = 0x0C;
const MARK_VECTOR_INT: u8 = 0x0D;
const MARK_VECTOR_UINT: u8 = 0x0E;
const MARK_VECTOR_DOUBLE: u8 = 0x0F;
const MARK_VECTOR_OBJECT: u8 = 0x10;
const MARK_DICTIONARY: u8 = 0x11;
pub const MIN_AMF3_INTEGER: i32 = -(1 << 28);
pub const MAX_AMF3_INTEGER: i32 = (1 << 28) - 1;
const U29_MASK: u32 = 0x1FFF_FFFF;
/// An AMF3 value.
///
/// Inline values form a tree. `Reference` values belong to an [`Amf3Document`].
/// The document owns each complex object once and supports shared children and
/// cycles. The convenience decoder expands acyclic graphs under a budget.
#[derive(PartialEq, Debug, Clone)]
#[non_exhaustive]
pub enum Amf3Value {
    /// A document-local object identity. Resolve through `Amf3Document::get`.
    Reference(crate::amf::ObjectId),
    Undefined,
    Null,
    Boolean(bool),
    Integer(i32),
    Double(f64),
    String(String),
    XmlDoc(String),
    Date(f64),
    Array {
        dense: Vec<Amf3Value>,
        associative: Vec<(String, Amf3Value)>,
    },
    Object {
        class_name: Option<String>,
        sealed: Vec<(String, Amf3Value)>,
        /// Dynamic members, or `None` when the traits did not set the dynamic
        /// flag. `Some(vec![])` (dynamic with no members) and `None` (not
        /// dynamic) are distinct encodings on the wire and round-trip exactly.
        dynamic: Option<Vec<(String, Amf3Value)>>,
    },
    /// An object whose class implements IExternalizable. The payload format
    /// is defined by the class rather than by AMF, so only class names in the
    /// compile-time registry ([`is_known_externalizable`]) can be decoded.
    /// Anything else is reported as
    /// [`Amf3DeserializationError::ExternalizableUnsupported`].
    Externalizable {
        class_name: String,
        value: Box<Amf3Value>,
    },
    Xml(String),
    ByteArray(Vec<u8>),
    VectorInt {
        fixed: bool,
        values: Vec<i32>,
    },
    VectorUint {
        fixed: bool,
        values: Vec<u32>,
    },
    VectorDouble {
        fixed: bool,
        values: Vec<f64>,
    },
    VectorObject {
        type_name: String,
        fixed: bool,
        values: Vec<Amf3Value>,
    },
    Dictionary {
        weak_keys: bool,
        entries: Vec<(Amf3Value, Amf3Value)>,
    },
}

/// How a registered IExternalizable class writes its payload. AMF3 does not
/// length-prefix these bytes, so a class name has to map to a fixed layout
/// or the decoder cannot continue. Add a row to
/// [`EXTERNALIZABLE_REGISTRY`] for another known class; add a variant here
/// only when the payload shape is new.
#[derive(Clone, Copy)]
enum ExternalizableLayout {
    /// Flex wrappers: exactly one nested AMF3 value (ArrayCollection,
    /// ArrayList, ObjectProxy).
    NestedAmf3,
    /// Java IDataOutput: big-endian double `clientid`, then four writeUTF
    /// strings (`code`, `description`, `details`, `level`). This is the
    /// Red5 `Status.writeExternal` layout that carries an RTMP onStatus
    /// info object.
    StatusBean,
}

/// Compile-time table of IExternalizable classes this codec can encode and
/// decode. Lookup is exact class name; unknown names are
/// [`Amf3DeserializationError::ExternalizableUnsupported`].
const EXTERNALIZABLE_REGISTRY: &[(&str, ExternalizableLayout)] = &[
    (
        "flex.messaging.io.ArrayCollection",
        ExternalizableLayout::NestedAmf3,
    ),
    (
        "flex.messaging.io.ArrayList",
        ExternalizableLayout::NestedAmf3,
    ),
    (
        "flex.messaging.io.ObjectProxy",
        ExternalizableLayout::NestedAmf3,
    ),
    (
        "org.red5.server.net.rtmp.status.Status",
        ExternalizableLayout::StatusBean,
    ),
];

fn externalizable_layout(class_name: &str) -> Option<ExternalizableLayout> {
    EXTERNALIZABLE_REGISTRY
        .iter()
        .find(|(name, _)| *name == class_name)
        .map(|(_, layout)| *layout)
}

/// True when `class_name` is in the compile-time IExternalizable registry.
pub fn is_known_externalizable(class_name: &str) -> bool {
    externalizable_layout(class_name).is_some()
}
impl Amf3Value {
    /// Borrow string storage without allocating.
    pub fn as_str(&self) -> Option<&str> {
        if let Self::String(value) = self {
            Some(value)
        } else {
            None
        }
    }

    pub fn get_number(&self) -> Option<f64> {
        match self {
            Amf3Value::Integer(v) => Some(*v as f64),
            Amf3Value::Double(v) => Some(*v),
            _ => None,
        }
    }
    pub fn get_boolean(&self) -> Option<bool> {
        match self {
            Amf3Value::Boolean(v) => Some(*v),
            _ => None,
        }
    }
    pub fn get_string(&self) -> Option<String> {
        match self {
            Amf3Value::String(v) => Some(v.clone()),
            _ => None,
        }
    }
    /// Accepts either numeric representation, mirroring [`crate::amf0::Amf0Value::get_integer`].
    ///
    /// AMF3 encoders are free to send an integral value as `Integer` or
    /// `Double`, so matching only one variant would make the same logical value
    /// readable or unreadable depending on the peer's encoder.
    pub fn get_integer(&self) -> Option<i32> {
        match self {
            Amf3Value::Integer(v) => Some(*v),
            Amf3Value::Double(n) => {
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
    /// Accepts either numeric representation; see [`Self::get_integer`].
    pub fn get_double(&self) -> Option<f64> {
        match self {
            Amf3Value::Double(v) => Some(*v),
            Amf3Value::Integer(v) => Some(*v as f64),
            _ => None,
        }
    }
    /// Sealed then dynamic members, in wire order.
    ///
    /// Order preserving for the same reason [`crate::amf0::Amf0Object`] is: an
    /// AMF3 object stores its members in sequence, so a `HashMap` here would
    /// throw away information the caller needs to re-encode faithfully.
    pub fn get_object_properties(&self) -> Option<indexmap::IndexMap<String, Amf3Value>> {
        match self {
            Amf3Value::Object {
                sealed, dynamic, ..
            } => {
                let dynamic = dynamic.as_deref().unwrap_or(&[]);
                let mut map = indexmap::IndexMap::with_capacity(sealed.len() + dynamic.len());
                for (k, v) in sealed.iter().chain(dynamic.iter()) {
                    map.insert(k.clone(), v.clone());
                }
                Some(map)
            }
            _ => None,
        }
    }
    /// Build a dynamic object with no class name, the common shape for RTMP
    /// command objects and status objects.
    pub fn dynamic_object(members: Vec<(String, Amf3Value)>) -> Amf3Value {
        Amf3Value::Object {
            class_name: None,
            sealed: Vec::new(),
            dynamic: Some(members),
        }
    }
    /// Project into the AMF0 value model.
    ///
    /// Types with a direct AMF0 counterpart convert structurally, which is what
    /// lets one set of control-plane helpers (`connect` parsing, Enhanced RTMP
    /// capability validation, metadata) read a payload regardless of the
    /// encoding it arrived in. Types with no AMF0 counterpart are wrapped in
    /// [`crate::amf0::Amf0Value::AvmPlus`] rather than degraded to `Null` or a
    /// bare number: the value stays intact, keeps its type, and re-encodes to
    /// AMF0 as the `avmplus` escape that a peer negotiating `objectEncoding` 3
    /// already understands.
    pub fn to_amf0(&self) -> crate::amf0::Amf0Value {
        use crate::amf0::Amf0Value as A;
        match self {
            Amf3Value::Reference(id) => A::Reference(*id),
            Amf3Value::Undefined => A::Undefined,
            Amf3Value::Null => A::Null,
            Amf3Value::Boolean(b) => A::Boolean(*b),
            Amf3Value::Integer(i) => A::Number(*i as f64),
            Amf3Value::Double(n) => A::Number(*n),
            Amf3Value::String(s) => A::Utf8String(s.clone()),
            Amf3Value::XmlDoc(s) => A::XmlDocument(s.clone()),
            Amf3Value::Xml(s) => A::XmlDocument(s.clone()),
            Amf3Value::Date(n) => A::Date {
                millis: *n,
                timezone: 0,
            },
            Amf3Value::Array { dense, associative } => {
                if associative.is_empty() {
                    A::StrictArray(dense.iter().map(|v| v.to_amf0()).collect())
                } else {
                    // Mixed arrays have no strict-array equivalent; AMF0 models
                    // them as an ECMA array, which decodes to an object here.
                    let mut map = Amf0Object::with_capacity(dense.len() + associative.len());
                    for (i, v) in dense.iter().enumerate() {
                        map.insert(i.to_string(), v.to_amf0());
                    }
                    for (k, v) in associative {
                        map.insert(k.clone(), v.to_amf0());
                    }
                    A::Object(map)
                }
            }
            Amf3Value::Object {
                class_name,
                sealed,
                dynamic,
            } => {
                let dynamic = dynamic.as_deref().unwrap_or(&[]);
                let mut map = Amf0Object::with_capacity(sealed.len() + dynamic.len());
                for (k, v) in sealed.iter().chain(dynamic.iter()) {
                    map.insert(k.clone(), v.to_amf0());
                }
                match class_name {
                    Some(name) => A::TypedObject {
                        class_name: name.clone(),
                        properties: map,
                    },
                    None => A::Object(map),
                }
            }
            // Registered IExternalizable wrappers are transparent: the inner
            // value is the nested AMF3 payload or the reconstructed object,
            // so project that. An AMF3-only payload inside still lands behind
            // the escape through the recursive call.
            Amf3Value::Externalizable { value, .. } => value.to_amf0(),
            // No AMF0 counterpart: preserved verbatim behind the escape.
            Amf3Value::ByteArray(_)
            | Amf3Value::VectorInt { .. }
            | Amf3Value::VectorUint { .. }
            | Amf3Value::VectorDouble { .. }
            | Amf3Value::VectorObject { .. }
            | Amf3Value::Dictionary { .. } => A::AvmPlus(Box::new(self.clone())),
        }
    }
    /// True when [`Self::to_amf0`] is structural rather than an `avmplus` escape.
    pub fn is_amf0_representable(&self) -> bool {
        match self {
            Self::Externalizable { value, .. } => value.is_amf0_representable(),
            Self::ByteArray(_)
            | Self::VectorInt { .. }
            | Self::VectorUint { .. }
            | Self::VectorDouble { .. }
            | Self::VectorObject { .. }
            | Self::Dictionary { .. } => false,
            _ => true,
        }
    }
}
impl From<crate::amf0::Amf0Value> for Amf3Value {
    fn from(value: crate::amf0::Amf0Value) -> Self {
        // Canonical conversion lives on Amf0Value so both directions have one
        // implementation each. Owned conversion clones through the borrowed
        // form to avoid divergent range checks.
        value.to_amf3()
    }
}
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Amf3SerializationError {
    #[error("AMF3 reference requires a document containing a complex object at {0:?}")]
    InvalidReference(crate::amf::ObjectId),
    #[error("AMF3 string too long: {0} bytes")]
    StringTooLong(usize),
    #[error("AMF3 collection too large: {0} items")]
    CollectionTooLarge(usize),
    #[error("AMF3 byte array too long: {0} bytes")]
    ByteArrayTooLong(usize),
    #[error("AMF3 nesting too deep")]
    DepthLimit,
    #[error("AMF3 externalizable class {0} cannot be re-encoded")]
    ExternalizableUnsupported(String),
    #[error("IO error while writing AMF3")]
    Io(#[from] std::io::Error),
}
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Amf3DeserializationError {
    #[error("AMF3 graph expansion exceeds the tree budget; use deserialize_document")]
    ExpansionLimit,
    #[error("AMF3 input ended unexpectedly")]
    UnexpectedEof,
    #[error("AMF3 unknown value marker")]
    UnknownMarker(u8),
    #[error("AMF3 invalid UTF-8")]
    InvalidUtf8,
    #[error("AMF3 string reference out of bounds")]
    BadStringReference(u32),
    #[error("AMF3 object reference out of bounds")]
    BadObjectReference(u32),
    #[error("AMF3 reference {0} points at an enclosing value; cyclic graphs are not supported")]
    CyclicReference(u32),
    #[error("AMF3 trait reference out of bounds")]
    BadTraitReference(u32),
    #[error("AMF3 string too long")]
    StringTooLong(usize),
    #[error("AMF3 collection too large")]
    CollectionTooLarge(usize),
    #[error("AMF3 byte array too long")]
    ByteArrayTooLong(usize),
    #[error("AMF3 nesting too deep")]
    DepthLimit,
    #[error("AMF3 reference table overflow")]
    TooManyReferences,
    #[error("AMF3 externalizable class {0} has a class-defined payload that cannot be skipped")]
    ExternalizableUnsupported(String),
    #[error("AMF3 expected format selector 0x00")]
    BadFormatSelector(u8),
    #[error("AMF3 message needs more values")]
    TooFewValues(usize, usize),
    #[error("AMF3 command name must be a string")]
    BadCommandName,
    #[error("AMF3 transaction id must be a number")]
    BadTransactionId,
    #[error("IO error while reading AMF3")]
    Io(#[from] std::io::Error),
}
impl From<std::string::FromUtf8Error> for Amf3DeserializationError {
    fn from(_: std::string::FromUtf8Error) -> Self {
        Amf3DeserializationError::InvalidUtf8
    }
}
fn write_u29(buf: &mut Vec<u8>, value: u32) {
    let value = value & U29_MASK;
    if value < 0x80 {
        buf.push(value as u8);
    } else if value < 0x4000 {
        buf.push((((value >> 7) & 0x7F) | 0x80) as u8);
        buf.push((value & 0x7F) as u8);
    } else if value < 0x20_0000 {
        buf.push((((value >> 14) & 0x7F) | 0x80) as u8);
        buf.push((((value >> 7) & 0x7F) | 0x80) as u8);
        buf.push((value & 0x7F) as u8);
    } else {
        buf.push((((value >> 22) & 0x7F) | 0x80) as u8);
        buf.push((((value >> 15) & 0x7F) | 0x80) as u8);
        buf.push((((value >> 8) & 0x7F) | 0x80) as u8);
        buf.push((value & 0xFF) as u8);
    }
}
fn read_u29<R: common::AmfRead>(cursor: &mut R) -> Result<u32, Amf3DeserializationError> {
    let mut value: u32 = 0;
    for i in 0..4 {
        let b = common::read_u8(cursor).map_err(|_| Amf3DeserializationError::UnexpectedEof)?;
        if i < 3 {
            value = (value << 7) | ((b & 0x7F) as u32);
            if b & 0x80 == 0 {
                return Ok(value);
            }
        } else {
            value = (value << 8) | (b as u32);
            return Ok(value);
        }
    }
    Err(Amf3DeserializationError::UnexpectedEof)
}
fn u29_to_integer(raw: u32) -> i32 {
    if raw & 0x1000_0000 != 0 {
        (raw | 0xE000_0000) as i32
    } else {
        raw as i32
    }
}
/// Serialize a sequence of values sharing one set of reference tables.
///
/// String and trait references are emitted, which is where the size wins are:
/// property names and class descriptors repeat constantly in RTMP command and
/// metadata payloads. Object references are deliberately *not* emitted.
/// [`Amf3Value`] is a tree, so two structurally equal children are still
/// distinct values; collapsing them into a reference would invent sharing that
/// the source did not express. Inline objects are always valid AMF3.
pub fn serialize(values: &[Amf3Value]) -> Result<Vec<u8>, Amf3SerializationError> {
    let mut output = Vec::new();
    serialize_into(values, &mut output)?;
    Ok(output)
}
/// Append one encoding context to reusable output. On failure, output is unchanged.
pub fn serialize_into(
    values: &[Amf3Value],
    output: &mut Vec<u8>,
) -> Result<(), Amf3SerializationError> {
    encode_document(values, &[], output)
}
pub(crate) fn encode_document(
    roots: &[Amf3Value],
    objects: &[Amf3Value],
    output: &mut Vec<u8>,
) -> Result<(), Amf3SerializationError> {
    let start = output.len();
    let mut ctx = EncodeContext {
        objects,
        ..Default::default()
    };
    for value in roots {
        if let Err(error) = write_value(output, &mut ctx, value, 0) {
            output.truncate(start);
            return Err(error);
        }
    }
    Ok(())
}
/// Decode object identity, sharing and cycles without expanding object references.
pub fn deserialize_document<R: crate::amf::AmfRead>(
    input: &mut R,
) -> Result<Amf3Document, Amf3DeserializationError> {
    let mut ctx = DecodeContext {
        ..Default::default()
    };
    let mut roots = Vec::new();
    while let Some(value) = read_optional_value(input, &mut ctx, 0)? {
        if roots.len() >= MAX_COLLECTION_LEN {
            return Err(Amf3DeserializationError::CollectionTooLarge(
                roots.len() + 1,
            ));
        }
        roots.push(value);
    }
    Ok(crate::amf::Document::from_parts(roots, ctx.objects))
}
pub(crate) fn deserialize_document_single<R: crate::amf::AmfRead>(
    input: &mut R,
) -> Result<Amf3Document, Amf3DeserializationError> {
    let mut ctx = DecodeContext {
        ..Default::default()
    };
    let value = read_value(input, &mut ctx, 0)?;
    Ok(crate::amf::Document::from_parts(vec![value], ctx.objects))
}
/// Arena-backed AMF3 roots and complex objects. IDs are local to this document.
pub type Amf3Document = crate::amf::Document<Amf3Value>;
impl Amf3Document {
    /// Resolve a reference, or borrow an inline value unchanged.
    pub fn resolve<'a>(&'a self, value: &'a Amf3Value) -> Option<&'a Amf3Value> {
        match value {
            Amf3Value::Reference(id) => self.get(*id),
            inline => Some(inline),
        }
    }

    pub fn serialize_into(&self, output: &mut Vec<u8>) -> Result<(), Amf3SerializationError> {
        encode_document(self.roots(), self.objects(), output)
    }
    pub fn serialize(&self) -> Result<Vec<u8>, Amf3SerializationError> {
        let mut out = Vec::new();
        self.serialize_into(&mut out)?;
        Ok(out)
    }
}
/// Serialize one value with fresh reference tables.
pub fn serialize_single(value: &Amf3Value) -> Result<Vec<u8>, Amf3SerializationError> {
    serialize(std::slice::from_ref(value))
}
pub fn deserialize<R: crate::amf::AmfRead>(
    cursor: &mut R,
) -> Result<Vec<Amf3Value>, Amf3DeserializationError> {
    deserialize_document(cursor)?
        .to_tree(crate::amf::TreeLimits::default())
        .map_err(tree_error)
}
pub fn deserialize_single<R: crate::amf::AmfRead>(
    cursor: &mut R,
) -> Result<Amf3Value, Amf3DeserializationError> {
    Ok(deserialize_document_single(cursor)?
        .to_tree(crate::amf::TreeLimits::default())
        .map_err(tree_error)?
        .pop()
        .unwrap())
}
fn tree_error(error: crate::amf::TreeError) -> Amf3DeserializationError {
    match error {
        crate::amf::TreeError::Cycle(id) => Amf3DeserializationError::CyclicReference(id.0 as u32),
        crate::amf::TreeError::InvalidReference(id) => {
            Amf3DeserializationError::BadObjectReference(id.0 as u32)
        }
        crate::amf::TreeError::Limit => Amf3DeserializationError::ExpansionLimit,
    }
}
/// Read one value, returning `None` at a clean end of input.
fn read_optional_value<R: common::AmfRead>(
    cursor: &mut R,
    ctx: &mut DecodeContext,
    depth: usize,
) -> Result<Option<Amf3Value>, Amf3DeserializationError> {
    let mut probe = [0u8; 1];
    match cursor.read(&mut probe) {
        Ok(0) => return Ok(None),
        Ok(_) => {}
        Err(e) => return Err(Amf3DeserializationError::Io(e)),
    }
    read_value_with_marker(cursor, ctx, depth, probe[0]).map(Some)
}
pub fn decode_avmplus_wrapped<R: crate::amf::AmfRead>(
    cursor: &mut R,
) -> Result<Amf3Value, Amf3DeserializationError> {
    let marker = common::read_u8(cursor).map_err(|_| Amf3DeserializationError::UnexpectedEof)?;
    if marker != AVMPLUS_OBJECT_MARKER {
        return Err(Amf3DeserializationError::UnknownMarker(marker));
    }
    deserialize_single(cursor)
}
#[derive(Default)]
struct DecodeContext {
    strings: Vec<String>,
    objects: Vec<Amf3Value>,
    traits: Vec<TraitInfo>,
}
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct TraitInfo {
    class_name: String,
    sealed_names: Vec<String>,
    dynamic: bool,
    externalizable: bool,
}
/// Encoder-side reference tables.
///
/// Only strings and traits are deduplicated; see [`serialize`] for why object
/// references are not emitted.
#[derive(Default)]
struct EncodeContext<'a> {
    objects: &'a [Amf3Value],
    object_refs: HashMap<crate::amf::ObjectId, u32>,
    next_object: u32,
    strings: HashMap<String, u32>,
    traits: HashMap<TraitInfo, u32>,
}
impl EncodeContext<'_> {
    fn string_ref(&self, s: &str) -> Option<u32> {
        self.strings.get(s).copied()
    }
    fn intern_string(&mut self, s: &str) {
        let next = self.strings.len() as u32;
        if next as usize >= MAX_REFERENCES {
            return;
        }
        self.strings.insert(s.to_owned(), next);
    }
    fn trait_ref(&self, info: &TraitInfo) -> Option<u32> {
        self.traits.get(info).copied()
    }
    fn intern_trait(&mut self, info: TraitInfo) {
        let next = self.traits.len() as u32;
        if next as usize >= MAX_REFERENCES {
            return;
        }
        self.traits.insert(info, next);
    }
}
impl DecodeContext {
    fn finish_object(&mut self, index: usize, value: Amf3Value) -> Amf3Value {
        self.objects[index] = value;
        Amf3Value::Reference(crate::amf::ObjectId(index))
    }
    fn store_object(&mut self, value: Amf3Value) -> Result<Amf3Value, Amf3DeserializationError> {
        let index = self.objects.len();
        self.push_object(Amf3Value::Null)?;
        Ok(self.finish_object(index, value))
    }

    // Slots are reserved before children, so references to enclosing objects work.
    fn resolve_object(&self, index: u32) -> Result<Amf3Value, Amf3DeserializationError> {
        if (index as usize) < self.objects.len() {
            Ok(Amf3Value::Reference(crate::amf::ObjectId(index as usize)))
        } else {
            Err(Amf3DeserializationError::BadObjectReference(index))
        }
    }
    fn push_string(&mut self, s: String) -> Result<(), Amf3DeserializationError> {
        if self.strings.len() >= MAX_REFERENCES {
            return Err(Amf3DeserializationError::TooManyReferences);
        }
        self.strings.push(s);
        Ok(())
    }
    fn push_object(&mut self, v: Amf3Value) -> Result<(), Amf3DeserializationError> {
        if self.objects.len() >= MAX_REFERENCES {
            return Err(Amf3DeserializationError::TooManyReferences);
        }
        self.objects.push(v);
        Ok(())
    }
    fn push_trait(&mut self, t: TraitInfo) -> Result<(), Amf3DeserializationError> {
        if self.traits.len() >= MAX_REFERENCES {
            return Err(Amf3DeserializationError::TooManyReferences);
        }
        self.traits.push(t);
        Ok(())
    }
}
fn check_depth(depth: usize) -> Result<(), Amf3DeserializationError> {
    if common::check_depth(depth) {
        Ok(())
    } else {
        Err(Amf3DeserializationError::DepthLimit)
    }
}
fn check_depth_ser(depth: usize) -> Result<(), Amf3SerializationError> {
    if common::check_depth(depth) {
        Ok(())
    } else {
        Err(Amf3SerializationError::DepthLimit)
    }
}
fn write_value(
    buf: &mut Vec<u8>,
    ctx: &mut EncodeContext,
    value: &Amf3Value,
    depth: usize,
) -> Result<(), Amf3SerializationError> {
    check_depth_ser(depth)?;
    if let Amf3Value::Reference(id) = value {
        let target = ctx
            .objects
            .get(id.0)
            .ok_or(Amf3SerializationError::InvalidReference(*id))?;
        let marker = object_marker(target).ok_or(Amf3SerializationError::InvalidReference(*id))?;
        if let Some(index) = ctx.object_refs.get(id).copied() {
            buf.push(marker);
            write_u29(buf, index << 1);
            return Ok(());
        }
        ctx.object_refs.insert(*id, ctx.next_object);
        return write_value(buf, ctx, target, depth);
    }
    if object_marker(value).is_some() {
        if ctx.next_object as usize >= MAX_REFERENCES {
            return Err(Amf3SerializationError::CollectionTooLarge(
                ctx.next_object as usize + 1,
            ));
        }
        ctx.next_object += 1;
    }
    match value {
        Amf3Value::Reference(_) => unreachable!(),
        Amf3Value::Undefined => buf.push(MARK_UNDEFINED),
        Amf3Value::Null => buf.push(MARK_NULL),
        Amf3Value::Boolean(false) => buf.push(MARK_FALSE),
        Amf3Value::Boolean(true) => buf.push(MARK_TRUE),
        Amf3Value::Integer(i) => {
            if *i < MIN_AMF3_INTEGER || *i > MAX_AMF3_INTEGER {
                buf.push(MARK_DOUBLE);
                common::write_f64_be(buf, *i as f64);
            } else {
                buf.push(MARK_INTEGER);
                write_u29(buf, (*i as u32) & U29_MASK);
            }
        }
        Amf3Value::Double(n) => {
            buf.push(MARK_DOUBLE);
            common::write_f64_be(buf, *n);
        }
        Amf3Value::String(s) => {
            buf.push(MARK_STRING);
            write_string_data(buf, ctx, s)?;
        }
        Amf3Value::XmlDoc(s) => {
            buf.push(MARK_XML_DOC);
            write_blob(buf, s.as_bytes(), MAX_STRING_LEN, true)?;
        }
        Amf3Value::Date(n) => {
            buf.push(MARK_DATE);
            write_u29(buf, 1);
            common::write_f64_be(buf, *n);
        }
        Amf3Value::Array { dense, associative } => {
            let total = dense.len() + associative.len();
            if !common::check_collection_len(total) {
                return Err(Amf3SerializationError::CollectionTooLarge(total));
            }
            buf.push(MARK_ARRAY);
            write_u29(buf, ((dense.len() as u32) << 1) | 1);
            for (k, v) in associative {
                write_string_data(buf, ctx, k)?;
                write_value(buf, ctx, v, depth + 1)?;
            }
            write_string_data(buf, ctx, "")?;
            for v in dense {
                write_value(buf, ctx, v, depth + 1)?;
            }
        }
        Amf3Value::Xml(s) => {
            buf.push(MARK_XML);
            write_blob(buf, s.as_bytes(), MAX_STRING_LEN, true)?;
        }
        Amf3Value::ByteArray(bytes) => {
            if bytes.len() > MAX_BYTEARRAY_LEN {
                return Err(Amf3SerializationError::ByteArrayTooLong(bytes.len()));
            }
            buf.push(MARK_BYTE_ARRAY);
            write_u29(buf, ((bytes.len() as u32) << 1) | 1);
            buf.extend_from_slice(bytes);
        }
        Amf3Value::VectorInt { fixed, values } => {
            if values.len() > MAX_COLLECTION_LEN {
                return Err(Amf3SerializationError::CollectionTooLarge(values.len()));
            }
            buf.push(MARK_VECTOR_INT);
            write_u29(buf, ((values.len() as u32) << 1) | 1);
            buf.push(u8::from(*fixed));
            for v in values {
                common::write_i32_be(buf, *v);
            }
        }
        Amf3Value::VectorUint { fixed, values } => {
            if values.len() > MAX_COLLECTION_LEN {
                return Err(Amf3SerializationError::CollectionTooLarge(values.len()));
            }
            buf.push(MARK_VECTOR_UINT);
            write_u29(buf, ((values.len() as u32) << 1) | 1);
            buf.push(u8::from(*fixed));
            for v in values {
                common::write_u32_be(buf, *v);
            }
        }
        Amf3Value::VectorDouble { fixed, values } => {
            if values.len() > MAX_COLLECTION_LEN {
                return Err(Amf3SerializationError::CollectionTooLarge(values.len()));
            }
            buf.push(MARK_VECTOR_DOUBLE);
            write_u29(buf, ((values.len() as u32) << 1) | 1);
            buf.push(u8::from(*fixed));
            for v in values {
                common::write_f64_be(buf, *v);
            }
        }
        Amf3Value::VectorObject {
            type_name,
            fixed,
            values,
        } => {
            if values.len() > MAX_COLLECTION_LEN {
                return Err(Amf3SerializationError::CollectionTooLarge(values.len()));
            }
            buf.push(MARK_VECTOR_OBJECT);
            write_u29(buf, ((values.len() as u32) << 1) | 1);
            buf.push(u8::from(*fixed));
            write_string_data(buf, ctx, type_name)?;
            for v in values {
                write_value(buf, ctx, v, depth + 1)?;
            }
        }
        Amf3Value::Dictionary { weak_keys, entries } => {
            if entries.len() > MAX_COLLECTION_LEN {
                return Err(Amf3SerializationError::CollectionTooLarge(entries.len()));
            }
            buf.push(MARK_DICTIONARY);
            write_u29(buf, ((entries.len() as u32) << 1) | 1);
            buf.push(u8::from(*weak_keys));
            for (k, v) in entries {
                write_value(buf, ctx, k, depth + 1)?;
                write_value(buf, ctx, v, depth + 1)?;
            }
        }
        Amf3Value::Object {
            class_name,
            sealed,
            dynamic,
        } => {
            write_object(buf, ctx, class_name, sealed, dynamic, depth)?;
        }
        Amf3Value::Externalizable { class_name, value } => {
            write_externalizable(buf, ctx, class_name, value, depth)?;
        }
    }
    Ok(())
}
fn write_object(
    buf: &mut Vec<u8>,
    ctx: &mut EncodeContext,
    class_name: &Option<String>,
    sealed: &[(String, Amf3Value)],
    dynamic: &Option<Vec<(String, Amf3Value)>>,
    depth: usize,
) -> Result<(), Amf3SerializationError> {
    check_depth_ser(depth)?;
    if sealed.len() > MAX_COLLECTION_LEN {
        return Err(Amf3SerializationError::CollectionTooLarge(sealed.len()));
    }
    let dynamic_members = dynamic.as_deref();
    if let Some(members) = dynamic_members
        && members.len() > MAX_COLLECTION_LEN
    {
        return Err(Amf3SerializationError::CollectionTooLarge(members.len()));
    }
    // The dynamic flag comes from the value's own shape, not from whether it
    // happens to carry members. A dynamic object with zero members is a
    // distinct encoding and must survive a decode/encode round trip.
    let dynamic_flag = dynamic_members.is_some();
    let sealed_count = sealed.len() as u32;
    if sealed_count > 0x0FFF_FFFF {
        return Err(Amf3SerializationError::CollectionTooLarge(sealed.len()));
    }
    buf.push(MARK_OBJECT);
    let info = TraitInfo {
        class_name: class_name.clone().unwrap_or_default(),
        sealed_names: sealed.iter().map(|(name, _)| name.clone()).collect(),
        dynamic: dynamic_flag,
        externalizable: false,
    };
    if let Some(index) = ctx.trait_ref(&info) {
        // U29O-traits-ref: bit 0 set (inline object), bit 1 clear (trait ref).
        write_u29(buf, (index << 2) | 0x01);
    } else {
        let header: u32 = (sealed_count << 4) | ((u32::from(dynamic_flag)) << 3) | 0x03;
        write_u29(buf, header);
        write_string_data(buf, ctx, info.class_name.as_str())?;
        for (name, _) in sealed {
            write_string_data(buf, ctx, name)?;
        }
        ctx.intern_trait(info);
    }
    for (_, v) in sealed {
        write_value(buf, ctx, v, depth + 1)?;
    }
    if let Some(members) = dynamic_members {
        for (k, v) in members {
            write_string_data(buf, ctx, k)?;
            write_value(buf, ctx, v, depth + 1)?;
        }
        write_string_data(buf, ctx, "")?;
    }
    Ok(())
}
/// The five RTMP status properties as carried by a Java IExternalizable bean
/// payload: one big-endian double followed by four writeUTF strings, in the
/// order clientid, code, description, details, level. This is the shared
/// reader/writer view so decode and encode agree on names and order.
struct StatusBean {
    client_id: f64,
    code: String,
    description: String,
    details: String,
    level: String,
}

impl StatusBean {
    fn from_members(members: &[(String, Amf3Value)]) -> Option<StatusBean> {
        if members.len() != 5 {
            return None;
        }
        let get = |name: &str| members.iter().find(|(k, _)| k == name).map(|(_, v)| v);
        Some(StatusBean {
            client_id: get("clientid")?.get_double()?,
            code: get("code")?.get_string()?,
            description: get("description")?.get_string()?,
            details: get("details")?.get_string()?,
            level: get("level")?.get_string()?,
        })
    }

    fn into_members(self) -> Vec<(String, Amf3Value)> {
        vec![
            ("clientid".to_string(), Amf3Value::Double(self.client_id)),
            ("code".to_string(), Amf3Value::String(self.code)),
            (
                "description".to_string(),
                Amf3Value::String(self.description),
            ),
            ("details".to_string(), Amf3Value::String(self.details)),
            ("level".to_string(), Amf3Value::String(self.level)),
        ]
    }

    fn write_payload(&self, buf: &mut Vec<u8>) -> Result<(), Amf3SerializationError> {
        common::write_f64_be(buf, self.client_id);
        write_java_utf(buf, &self.code)?;
        write_java_utf(buf, &self.description)?;
        write_java_utf(buf, &self.details)?;
        write_java_utf(buf, &self.level)?;
        Ok(())
    }
}

/// Write an IExternalizable object by looking up its class in the
/// compile-time registry and invoking that layout. Unknown classes, or a
/// registered class whose inner value does not match the layout, cannot be
/// reproduced.
fn write_externalizable(
    buf: &mut Vec<u8>,
    ctx: &mut EncodeContext,
    class_name: &str,
    value: &Amf3Value,
    depth: usize,
) -> Result<(), Amf3SerializationError> {
    check_depth_ser(depth)?;
    let Some(layout) = externalizable_layout(class_name) else {
        return Err(Amf3SerializationError::ExternalizableUnsupported(
            class_name.to_owned(),
        ));
    };
    layout.write(buf, ctx, class_name, value, depth)
}
// Object header shared by every IExternalizable encoding: the marker, then
// either a trait reference or inline externalizable traits plus the class
// name. The payload that follows is class-defined.
fn write_externalizable_header(
    buf: &mut Vec<u8>,
    ctx: &mut EncodeContext,
    class_name: &str,
) -> Result<(), Amf3SerializationError> {
    buf.push(MARK_OBJECT);
    let info = TraitInfo {
        class_name: class_name.to_owned(),
        sealed_names: Vec::new(),
        dynamic: false,
        externalizable: true,
    };
    if let Some(index) = ctx.trait_ref(&info) {
        write_u29(buf, (index << 2) | 0x01);
    } else {
        // bit 0 inline object, bit 1 inline traits, bit 2 externalizable.
        write_u29(buf, 0x07);
        write_string_data(buf, ctx, class_name)?;
        ctx.intern_trait(info);
    }
    Ok(())
}
impl ExternalizableLayout {
    fn write(
        self,
        buf: &mut Vec<u8>,
        ctx: &mut EncodeContext,
        class_name: &str,
        value: &Amf3Value,
        depth: usize,
    ) -> Result<(), Amf3SerializationError> {
        match self {
            ExternalizableLayout::NestedAmf3 => {
                write_externalizable_header(buf, ctx, class_name)?;
                write_value(buf, ctx, value, depth + 1)
            }
            ExternalizableLayout::StatusBean => write_status_bean(buf, ctx, class_name, value),
        }
    }

    fn read<R: common::AmfRead>(
        self,
        cursor: &mut R,
        ctx: &mut DecodeContext,
        class_name: String,
        depth: usize,
    ) -> Result<Amf3Value, Amf3DeserializationError> {
        match self {
            ExternalizableLayout::NestedAmf3 => {
                read_nested_externalizable(cursor, ctx, class_name, depth)
            }
            ExternalizableLayout::StatusBean => read_status_bean(cursor, ctx, class_name),
        }
    }
}

fn write_status_bean(
    buf: &mut Vec<u8>,
    ctx: &mut EncodeContext,
    class_name: &str,
    value: &Amf3Value,
) -> Result<(), Amf3SerializationError> {
    if let Amf3Value::Object {
        sealed,
        dynamic: Some(members),
        ..
    } = value
        && sealed.is_empty()
        && let Some(bean) = StatusBean::from_members(members)
    {
        write_externalizable_header(buf, ctx, class_name)?;
        return bean.write_payload(buf);
    }
    Err(Amf3SerializationError::ExternalizableUnsupported(
        class_name.to_owned(),
    ))
}

// Java IDataOutput.writeUTF as used by Red5: u16 big-endian length followed
// by UTF-8 bytes (status codes, levels and descriptions are ASCII in practice).
fn write_java_utf(buf: &mut Vec<u8>, s: &str) -> Result<(), Amf3SerializationError> {
    let bytes = s.as_bytes();
    if bytes.len() > u16::MAX as usize {
        return Err(Amf3SerializationError::StringTooLong(bytes.len()));
    }
    common::write_u16_be(buf, bytes.len() as u16);
    buf.extend_from_slice(bytes);
    Ok(())
}

/// Write a string, emitting a reference when it has been seen before.
///
/// The empty string is always inline and is never added to the table; AMF3
/// gives it a dedicated encoding and uses it as the object/array terminator, so
/// referencing it would be ambiguous.
fn write_string_data(
    buf: &mut Vec<u8>,
    ctx: &mut EncodeContext,
    s: &str,
) -> Result<(), Amf3SerializationError> {
    let bytes = s.as_bytes();
    if bytes.len() > MAX_STRING_LEN {
        return Err(Amf3SerializationError::StringTooLong(bytes.len()));
    }
    if bytes.is_empty() {
        write_u29(buf, 1);
        return Ok(());
    }
    if let Some(index) = ctx.string_ref(s) {
        write_u29(buf, index << 1);
        return Ok(());
    }
    write_u29(buf, ((bytes.len() as u32) << 1) | 1);
    buf.extend_from_slice(bytes);
    ctx.intern_string(s);
    Ok(())
}

fn write_blob(
    buf: &mut Vec<u8>,
    bytes: &[u8],
    max: usize,
    _is_string: bool,
) -> Result<(), Amf3SerializationError> {
    if bytes.len() > max {
        if max == MAX_BYTEARRAY_LEN {
            return Err(Amf3SerializationError::ByteArrayTooLong(bytes.len()));
        }
        return Err(Amf3SerializationError::StringTooLong(bytes.len()));
    }
    write_u29(buf, ((bytes.len() as u32) << 1) | 1);
    buf.extend_from_slice(bytes);
    Ok(())
}
// Shared wire reads live in amf::common so AMF0 and AMF3 use one
// implementation. Cursor maps any IO failure to UnexpectedEof to keep
// truncated payloads strictly typed.
fn read_exact<R: common::AmfRead>(
    cursor: &mut R,
    len: usize,
) -> Result<Vec<u8>, Amf3DeserializationError> {
    // Refuse a length prefix the input cannot satisfy before allocating for it.
    if !common::length_is_plausible(cursor, len) {
        return Err(Amf3DeserializationError::UnexpectedEof);
    }
    common::read_exact_vec(cursor, len).map_err(|_| Amf3DeserializationError::UnexpectedEof)
}

fn read_marker<R: common::AmfRead>(cursor: &mut R) -> Result<u8, Amf3DeserializationError> {
    common::read_u8(cursor).map_err(|_| Amf3DeserializationError::UnexpectedEof)
}

fn read_f64<R: common::AmfRead>(cursor: &mut R) -> Result<f64, Amf3DeserializationError> {
    common::read_f64_be(cursor).map_err(|_| Amf3DeserializationError::UnexpectedEof)
}

fn read_string_content<R: common::AmfRead>(
    cursor: &mut R,
    ctx: &mut DecodeContext,
) -> Result<String, Amf3DeserializationError> {
    let u29 = read_u29(cursor)?;
    if u29 & 1 == 0 {
        let idx = (u29 >> 1) as usize;
        return ctx
            .strings
            .get(idx)
            .cloned()
            .ok_or(Amf3DeserializationError::BadStringReference(u29 >> 1));
    }
    let len = (u29 >> 1) as usize;
    if len > MAX_STRING_LEN {
        return Err(Amf3DeserializationError::StringTooLong(len));
    }
    if len == 0 {
        return Ok(String::new());
    }
    let bytes = read_exact(cursor, len)?;
    let s = String::from_utf8(bytes)?;
    ctx.push_string(s.clone())?;
    Ok(s)
}
fn read_value<R: common::AmfRead>(
    cursor: &mut R,
    ctx: &mut DecodeContext,
    depth: usize,
) -> Result<Amf3Value, Amf3DeserializationError> {
    check_depth(depth)?;
    let marker = read_marker(cursor)?;
    read_value_with_marker(cursor, ctx, depth, marker)
}
fn read_value_with_marker<R: common::AmfRead>(
    cursor: &mut R,
    ctx: &mut DecodeContext,
    depth: usize,
    marker: u8,
) -> Result<Amf3Value, Amf3DeserializationError> {
    check_depth(depth)?;
    match marker {
        MARK_UNDEFINED => Ok(Amf3Value::Undefined),
        MARK_NULL => Ok(Amf3Value::Null),
        MARK_FALSE => Ok(Amf3Value::Boolean(false)),
        MARK_TRUE => Ok(Amf3Value::Boolean(true)),
        MARK_INTEGER => {
            let raw = read_u29(cursor)?;
            Ok(Amf3Value::Integer(u29_to_integer(raw)))
        }
        MARK_DOUBLE => Ok(Amf3Value::Double(read_f64(cursor)?)),
        MARK_STRING => Ok(Amf3Value::String(read_string_content(cursor, ctx)?)),
        MARK_XML_DOC => read_xml_doc(cursor, ctx),
        MARK_DATE => read_date(cursor, ctx),
        MARK_ARRAY => read_array(cursor, ctx, depth),
        MARK_OBJECT => read_object(cursor, ctx, depth),
        MARK_XML => read_xml(cursor, ctx),
        MARK_BYTE_ARRAY => read_byte_array(cursor, ctx),
        MARK_VECTOR_INT => read_vector_int(cursor, ctx),
        MARK_VECTOR_UINT => read_vector_uint(cursor, ctx),
        MARK_VECTOR_DOUBLE => read_vector_double(cursor, ctx),
        MARK_VECTOR_OBJECT => read_vector_object(cursor, ctx, depth),
        MARK_DICTIONARY => read_dictionary(cursor, ctx, depth),
        _ => Err(Amf3DeserializationError::UnknownMarker(marker)),
    }
}
fn read_xml_doc<R: common::AmfRead>(
    cursor: &mut R,
    ctx: &mut DecodeContext,
) -> Result<Amf3Value, Amf3DeserializationError> {
    let u29 = read_u29(cursor)?;
    if u29 & 1 == 0 {
        return ctx.resolve_object(u29 >> 1);
    }
    let len = (u29 >> 1) as usize;
    if len > MAX_STRING_LEN {
        return Err(Amf3DeserializationError::StringTooLong(len));
    }
    let bytes = read_exact(cursor, len)?;
    let s = String::from_utf8(bytes)?;
    let v = Amf3Value::XmlDoc(s);
    ctx.store_object(v)
}

fn read_xml<R: common::AmfRead>(
    cursor: &mut R,
    ctx: &mut DecodeContext,
) -> Result<Amf3Value, Amf3DeserializationError> {
    let u29 = read_u29(cursor)?;
    if u29 & 1 == 0 {
        return ctx.resolve_object(u29 >> 1);
    }
    let len = (u29 >> 1) as usize;
    if len > MAX_STRING_LEN {
        return Err(Amf3DeserializationError::StringTooLong(len));
    }
    let bytes = read_exact(cursor, len)?;
    let s = String::from_utf8(bytes)?;
    let v = Amf3Value::Xml(s);
    ctx.store_object(v)
}

fn read_byte_array<R: common::AmfRead>(
    cursor: &mut R,
    ctx: &mut DecodeContext,
) -> Result<Amf3Value, Amf3DeserializationError> {
    let u29 = read_u29(cursor)?;
    if u29 & 1 == 0 {
        return ctx.resolve_object(u29 >> 1);
    }
    let len = (u29 >> 1) as usize;
    if len > MAX_BYTEARRAY_LEN {
        return Err(Amf3DeserializationError::ByteArrayTooLong(len));
    }
    let bytes = read_exact(cursor, len)?;
    let v = Amf3Value::ByteArray(bytes);
    ctx.store_object(v)
}

fn read_date<R: common::AmfRead>(
    cursor: &mut R,
    ctx: &mut DecodeContext,
) -> Result<Amf3Value, Amf3DeserializationError> {
    let u29 = read_u29(cursor)?;
    if u29 & 1 == 0 {
        return ctx.resolve_object(u29 >> 1);
    }
    let millis = read_f64(cursor)?;
    let v = Amf3Value::Date(millis);
    ctx.store_object(v)
}
fn read_array<R: common::AmfRead>(
    cursor: &mut R,
    ctx: &mut DecodeContext,
    depth: usize,
) -> Result<Amf3Value, Amf3DeserializationError> {
    let u29 = read_u29(cursor)?;
    if u29 & 1 == 0 {
        return ctx.resolve_object(u29 >> 1);
    }
    let dense_len = (u29 >> 1) as usize;
    if dense_len > MAX_COLLECTION_LEN {
        return Err(Amf3DeserializationError::CollectionTooLarge(dense_len));
    }
    let placeholder = ctx.objects.len();
    ctx.push_object(Amf3Value::Null)?;
    let mut associative = Vec::new();
    loop {
        let key = read_string_content(cursor, ctx)?;
        if key.is_empty() {
            break;
        }
        if associative.len() >= MAX_COLLECTION_LEN {
            return Err(Amf3DeserializationError::CollectionTooLarge(
                associative.len() + 1,
            ));
        }
        let val = read_value(cursor, ctx, depth + 1)?;
        associative.push((key, val));
        if associative.len() + dense_len > MAX_COLLECTION_LEN {
            return Err(Amf3DeserializationError::CollectionTooLarge(
                associative.len() + dense_len,
            ));
        }
    }
    let mut dense = Vec::with_capacity(dense_len.min(1024));
    for _ in 0..dense_len {
        dense.push(read_value(cursor, ctx, depth + 1)?);
    }
    let v = Amf3Value::Array { dense, associative };
    Ok(ctx.finish_object(placeholder, v))
}
fn install_externalizable(
    ctx: &mut DecodeContext,
    placeholder: usize,
    class_name: String,
    inner: Amf3Value,
) -> Amf3Value {
    let v = Amf3Value::Externalizable {
        class_name,
        value: Box::new(inner),
    };
    ctx.finish_object(placeholder, v)
}

fn read_nested_externalizable<R: common::AmfRead>(
    cursor: &mut R,
    ctx: &mut DecodeContext,
    class_name: String,
    depth: usize,
) -> Result<Amf3Value, Amf3DeserializationError> {
    let placeholder = ctx.objects.len();
    ctx.push_object(Amf3Value::Null)?;
    let inner = read_value(cursor, ctx, depth + 1)?;
    Ok(install_externalizable(ctx, placeholder, class_name, inner))
}

// Decode a status-bean payload into the generic Externalizable shape: the
// bean properties surface as an ordinary dynamic object so control-plane
// code keeps matching on Object plus a code string. Registered in the
// object table like any other object so later references resolve to it. A
// truncated or mistyped payload means this class is not the bean layout
// after all, so report it as unsupported rather than as EOF.
fn read_status_bean<R: common::AmfRead>(
    cursor: &mut R,
    ctx: &mut DecodeContext,
    class_name: String,
) -> Result<Amf3Value, Amf3DeserializationError> {
    let placeholder = ctx.objects.len();
    ctx.push_object(Amf3Value::Null)?;
    let bean = read_status_bean_payload(cursor)
        .map_err(|_| Amf3DeserializationError::ExternalizableUnsupported(class_name.clone()))?;
    Ok(install_externalizable(
        ctx,
        placeholder,
        class_name,
        Amf3Value::Object {
            class_name: None,
            sealed: Vec::new(),
            dynamic: Some(bean.into_members()),
        },
    ))
}

// Bean payload: big-endian double clientid, then Java-UTF code, description,
// details, level.
fn read_status_bean_payload<R: common::AmfRead>(
    cursor: &mut R,
) -> Result<StatusBean, Amf3DeserializationError> {
    let client_id =
        common::read_f64_be(cursor).map_err(|_| Amf3DeserializationError::UnexpectedEof)?;
    let code = read_java_utf(cursor)?;
    let description = read_java_utf(cursor)?;
    let details = read_java_utf(cursor)?;
    let level = read_java_utf(cursor)?;
    Ok(StatusBean {
        client_id,
        code,
        description,
        details,
        level,
    })
}
// Java DataInput.readUTF: u16 big-endian length followed by that many bytes.
fn read_java_utf<R: common::AmfRead>(cursor: &mut R) -> Result<String, Amf3DeserializationError> {
    let len =
        common::read_u16_be(cursor).map_err(|_| Amf3DeserializationError::UnexpectedEof)? as usize;
    if !common::length_is_plausible(cursor, len) {
        return Err(Amf3DeserializationError::UnexpectedEof);
    }
    let bytes =
        common::read_exact_vec(cursor, len).map_err(|_| Amf3DeserializationError::UnexpectedEof)?;
    String::from_utf8(bytes).map_err(|_| Amf3DeserializationError::InvalidUtf8)
}
fn read_object<R: common::AmfRead>(
    cursor: &mut R,
    ctx: &mut DecodeContext,
    depth: usize,
) -> Result<Amf3Value, Amf3DeserializationError> {
    let header = read_u29(cursor)?;
    if header & 1 == 0 {
        return ctx.resolve_object(header >> 1);
    }
    let (class_name, sealed_names, dynamic, externalizable): (String, Vec<String>, bool, bool);
    if header & 2 == 0 {
        let trait_idx = (header >> 2) as usize;
        let t = ctx.traits.get(trait_idx).cloned().ok_or(
            Amf3DeserializationError::BadTraitReference(trait_idx as u32),
        )?;
        class_name = t.class_name;
        sealed_names = t.sealed_names;
        dynamic = t.dynamic;
        externalizable = t.externalizable;
    } else {
        let ext_flag = (header >> 2) & 1 != 0;
        let dyn_flag = (header >> 3) & 1 != 0;
        let sealed_count = if ext_flag { 0 } else { (header >> 4) as usize };
        if sealed_count > MAX_COLLECTION_LEN {
            return Err(Amf3DeserializationError::CollectionTooLarge(sealed_count));
        }
        let cn = read_string_content(cursor, ctx)?;
        let mut names = Vec::with_capacity(sealed_count.min(256));
        for _ in 0..sealed_count {
            names.push(read_string_content(cursor, ctx)?);
        }
        ctx.push_trait(TraitInfo {
            class_name: cn.clone(),
            sealed_names: names.clone(),
            dynamic: dyn_flag,
            externalizable: ext_flag,
        })?;
        class_name = cn;
        sealed_names = names;
        dynamic = dyn_flag;
        externalizable = ext_flag;
    }
    if externalizable {
        // An IExternalizable payload is written by the class's own
        // writeExternal, so AMF carries no length and a decoder cannot skip
        // what it does not understand. The compile-time registry maps a class
        // name to a layout and invokes it; unknown names stop here so the
        // caller can fall back to relaying the original bytes untouched.
        let Some(layout) = externalizable_layout(&class_name) else {
            return Err(Amf3DeserializationError::ExternalizableUnsupported(
                class_name,
            ));
        };
        return layout.read(cursor, ctx, class_name, depth);
    }
    let placeholder = ctx.objects.len();
    ctx.push_object(Amf3Value::Null)?;
    let mut sealed = Vec::with_capacity(sealed_names.len().min(256));
    for name in &sealed_names {
        let val = read_value(cursor, ctx, depth + 1)?;
        sealed.push((name.clone(), val));
    }
    // `Some(vec![])` (dynamic traits, no members) and `None` (non-dynamic
    // traits) are different bytes on the wire, so the distinction is kept
    // rather than collapsed into an empty vector.
    let dyn_members = if dynamic {
        let mut members = Vec::new();
        loop {
            let key = read_string_content(cursor, ctx)?;
            if key.is_empty() {
                break;
            }
            if members.len() >= MAX_COLLECTION_LEN {
                return Err(Amf3DeserializationError::CollectionTooLarge(
                    members.len() + 1,
                ));
            }
            let val = read_value(cursor, ctx, depth + 1)?;
            members.push((key, val));
        }
        Some(members)
    } else {
        None
    };
    let class_opt = if class_name.is_empty() {
        None
    } else {
        Some(class_name)
    };
    let v = Amf3Value::Object {
        class_name: class_opt,
        sealed,
        dynamic: dyn_members,
    };
    Ok(ctx.finish_object(placeholder, v))
}
fn read_vector_int<R: common::AmfRead>(
    cursor: &mut R,
    ctx: &mut DecodeContext,
) -> Result<Amf3Value, Amf3DeserializationError> {
    let u29 = read_u29(cursor)?;
    if u29 & 1 == 0 {
        return ctx.resolve_object(u29 >> 1);
    }
    let len = (u29 >> 1) as usize;
    if len > MAX_COLLECTION_LEN {
        return Err(Amf3DeserializationError::CollectionTooLarge(len));
    }
    let fixed = read_marker(cursor)? != 0;
    let placeholder = ctx.objects.len();
    ctx.push_object(Amf3Value::Null)?;
    let mut values = Vec::with_capacity(len.min(1024));
    for _ in 0..len {
        let v = common::read_i32_be(cursor).map_err(|_| Amf3DeserializationError::UnexpectedEof)?;
        values.push(v);
    }
    let v = Amf3Value::VectorInt { fixed, values };
    Ok(ctx.finish_object(placeholder, v))
}
fn read_vector_uint<R: common::AmfRead>(
    cursor: &mut R,
    ctx: &mut DecodeContext,
) -> Result<Amf3Value, Amf3DeserializationError> {
    let u29 = read_u29(cursor)?;
    if u29 & 1 == 0 {
        return ctx.resolve_object(u29 >> 1);
    }
    let len = (u29 >> 1) as usize;
    if len > MAX_COLLECTION_LEN {
        return Err(Amf3DeserializationError::CollectionTooLarge(len));
    }
    let fixed = read_marker(cursor)? != 0;
    let placeholder = ctx.objects.len();
    ctx.push_object(Amf3Value::Null)?;
    let mut values = Vec::with_capacity(len.min(1024));
    for _ in 0..len {
        let v = common::read_u32_be(cursor).map_err(|_| Amf3DeserializationError::UnexpectedEof)?;
        values.push(v);
    }
    let v = Amf3Value::VectorUint { fixed, values };
    Ok(ctx.finish_object(placeholder, v))
}

fn read_vector_double<R: common::AmfRead>(
    cursor: &mut R,
    ctx: &mut DecodeContext,
) -> Result<Amf3Value, Amf3DeserializationError> {
    let u29 = read_u29(cursor)?;
    if u29 & 1 == 0 {
        return ctx.resolve_object(u29 >> 1);
    }
    let len = (u29 >> 1) as usize;
    if len > MAX_COLLECTION_LEN {
        return Err(Amf3DeserializationError::CollectionTooLarge(len));
    }
    let fixed = read_marker(cursor)? != 0;
    let placeholder = ctx.objects.len();
    ctx.push_object(Amf3Value::Null)?;
    let mut values = Vec::with_capacity(len.min(1024));
    for _ in 0..len {
        values.push(read_f64(cursor)?);
    }
    let v = Amf3Value::VectorDouble { fixed, values };
    Ok(ctx.finish_object(placeholder, v))
}
fn read_vector_object<R: common::AmfRead>(
    cursor: &mut R,
    ctx: &mut DecodeContext,
    depth: usize,
) -> Result<Amf3Value, Amf3DeserializationError> {
    let u29 = read_u29(cursor)?;
    if u29 & 1 == 0 {
        return ctx.resolve_object(u29 >> 1);
    }
    let len = (u29 >> 1) as usize;
    if len > MAX_COLLECTION_LEN {
        return Err(Amf3DeserializationError::CollectionTooLarge(len));
    }
    let fixed = read_marker(cursor)? != 0;
    let type_name = read_string_content(cursor, ctx)?;
    let placeholder = ctx.objects.len();
    ctx.push_object(Amf3Value::Null)?;
    let mut values = Vec::with_capacity(len.min(1024));
    for _ in 0..len {
        values.push(read_value(cursor, ctx, depth + 1)?);
    }
    let v = Amf3Value::VectorObject {
        type_name,
        fixed,
        values,
    };
    Ok(ctx.finish_object(placeholder, v))
}

fn read_dictionary<R: common::AmfRead>(
    cursor: &mut R,
    ctx: &mut DecodeContext,
    depth: usize,
) -> Result<Amf3Value, Amf3DeserializationError> {
    let u29 = read_u29(cursor)?;
    if u29 & 1 == 0 {
        return ctx.resolve_object(u29 >> 1);
    }
    let len = (u29 >> 1) as usize;
    if len > MAX_COLLECTION_LEN {
        return Err(Amf3DeserializationError::CollectionTooLarge(len));
    }
    let weak_keys = read_marker(cursor)? != 0;
    let placeholder = ctx.objects.len();
    ctx.push_object(Amf3Value::Null)?;
    let mut entries = Vec::with_capacity(len.min(1024));
    for _ in 0..len {
        let k = read_value(cursor, ctx, depth + 1)?;
        let v = read_value(cursor, ctx, depth + 1)?;
        entries.push((k, v));
    }
    let v = Amf3Value::Dictionary { weak_keys, entries };
    Ok(ctx.finish_object(placeholder, v))
}

fn object_marker(value: &Amf3Value) -> Option<u8> {
    Some(match value {
        Amf3Value::XmlDoc(_) => MARK_XML_DOC,
        Amf3Value::Date(_) => MARK_DATE,
        Amf3Value::Array { .. } => MARK_ARRAY,
        Amf3Value::Object { .. } | Amf3Value::Externalizable { .. } => MARK_OBJECT,
        Amf3Value::Xml(_) => MARK_XML,
        Amf3Value::ByteArray(_) => MARK_BYTE_ARRAY,
        Amf3Value::VectorInt { .. } => MARK_VECTOR_INT,
        Amf3Value::VectorUint { .. } => MARK_VECTOR_UINT,
        Amf3Value::VectorDouble { .. } => MARK_VECTOR_DOUBLE,
        Amf3Value::VectorObject { .. } => MARK_VECTOR_OBJECT,
        Amf3Value::Dictionary { .. } => MARK_DICTIONARY,
        _ => return None,
    })
}

// Relocate document-local IDs when importing an embedded AMF3 arena.
pub(crate) fn relocate(value: &mut Amf3Value, base: usize) {
    match value {
        Amf3Value::Reference(id) => id.0 += base,
        Amf3Value::Array { dense, associative } => {
            for v in dense {
                relocate(v, base);
            }
            for (_, v) in associative {
                relocate(v, base);
            }
        }
        Amf3Value::Object {
            sealed, dynamic, ..
        } => {
            for (_, v) in sealed {
                relocate(v, base);
            }
            if let Some(members) = dynamic {
                for (_, v) in members {
                    relocate(v, base);
                }
            }
        }
        Amf3Value::Externalizable { value, .. } => relocate(value, base),
        Amf3Value::VectorObject { values, .. } => {
            for v in values {
                relocate(v, base);
            }
        }
        Amf3Value::Dictionary { entries, .. } => {
            for (k, v) in entries {
                relocate(k, base);
                relocate(v, base);
            }
        }
        _ => {}
    }
}

impl crate::amf::graph::GraphValue for Amf3Value {
    fn reference(&self) -> Option<crate::amf::ObjectId> {
        if let Self::Reference(id) = self {
            Some(*id)
        } else {
            None
        }
    }
    fn heap_bytes(&self) -> usize {
        let pairs = |p: &[(String, Self)]| {
            p.len()
                .saturating_mul(std::mem::size_of::<(String, Self)>())
                .saturating_add(p.iter().map(|(k, _)| k.len()).sum::<usize>())
        };
        match self {
            Self::String(s) | Self::Xml(s) | Self::XmlDoc(s) => s.len(),
            Self::ByteArray(b) => b.len(),
            Self::Array { dense, associative } => dense
                .len()
                .saturating_mul(std::mem::size_of::<Self>())
                .saturating_add(pairs(associative)),
            Self::Object {
                class_name,
                sealed,
                dynamic,
            } => class_name
                .as_ref()
                .map_or(0, |s| s.len())
                .saturating_add(pairs(sealed))
                .saturating_add(dynamic.as_ref().map_or(0, |p| pairs(p))),
            Self::Externalizable { class_name, .. } => {
                class_name.len() + std::mem::size_of::<Self>()
            }
            Self::VectorInt { values, .. } => values.len() * 4,
            Self::VectorUint { values, .. } => values.len() * 4,
            Self::VectorDouble { values, .. } => values.len() * 8,
            Self::VectorObject {
                type_name, values, ..
            } => type_name.len() + values.len() * std::mem::size_of::<Self>(),
            Self::Dictionary { entries, .. } => entries.len() * std::mem::size_of::<(Self, Self)>(),
            _ => 0,
        }
    }
    fn children(
        &self,
        visit: &mut dyn FnMut(&Self) -> Result<(), crate::amf::TreeError>,
    ) -> Result<(), crate::amf::TreeError> {
        match self {
            Self::Array { dense, associative } => {
                for v in dense {
                    visit(v)?;
                }
                for (_, v) in associative {
                    visit(v)?;
                }
            }
            Self::Object {
                sealed, dynamic, ..
            } => {
                for (_, v) in sealed {
                    visit(v)?;
                }
                if let Some(p) = dynamic {
                    for (_, v) in p {
                        visit(v)?;
                    }
                }
            }
            Self::Externalizable { value, .. } => visit(value)?,
            Self::VectorObject { values, .. } => {
                for v in values {
                    visit(v)?;
                }
            }
            Self::Dictionary { entries, .. } => {
                for (k, v) in entries {
                    visit(k)?;
                    visit(v)?;
                }
            }
            _ => {}
        }
        Ok(())
    }
    fn children_mut(&mut self, visit: &mut dyn FnMut(&mut Self)) {
        match self {
            Self::Array { dense, associative } => {
                for v in dense {
                    visit(v);
                }
                for (_, v) in associative {
                    visit(v);
                }
            }
            Self::Object {
                sealed, dynamic, ..
            } => {
                for (_, v) in sealed {
                    visit(v);
                }
                if let Some(p) = dynamic {
                    for (_, v) in p {
                        visit(v);
                    }
                }
            }
            Self::Externalizable { value, .. } => visit(value),
            Self::VectorObject { values, .. } => {
                for v in values {
                    visit(v);
                }
            }
            Self::Dictionary { entries, .. } => {
                for (k, v) in entries {
                    visit(k);
                    visit(v);
                }
            }
            _ => {}
        }
    }
}
impl Amf3Document {
    /// Expand sharing into an owned tree only after validating the complete budget.
    pub fn to_tree(
        &self,
        limits: crate::amf::TreeLimits,
    ) -> Result<Vec<Amf3Value>, crate::amf::TreeError> {
        crate::amf::graph::expand(self.roots(), self.objects(), limits)
    }
}
