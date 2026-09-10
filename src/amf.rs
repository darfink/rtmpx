//! Encoding-neutral view over the two AMF value models.
//!
//! RTMP negotiates `objectEncoding` at `connect` time, and an
//! `objectEncoding` 3 peer is permitted to use *either* AMF0 or AMF3 on any
//! subsequent message. The encoding is therefore a runtime property of a
//! connection, not something a caller can pick when constructing a session, so
//! it is modelled as [`AmfEncoding`] rather than as a type parameter.
//!
//! What generics are good for here is the leaf helpers. Building a status
//! object or reading a stream key out of a command argument is the same logic
//! in both encodings and differs only in which constructors it calls, so those
//! are written once against [`AmfValue`].

use indexmap::IndexMap;

use crate::amf0::Amf0Value;
use crate::amf3::Amf3Value;

/// Property view over an object-like AMF value.
///
/// Order preserving: both value models store properties in wire order, so a
/// `HashMap` here would discard information the caller may need to re-encode
/// faithfully.
pub type AmfProperties<V> = IndexMap<String, V>;

/// Which AMF version a peer negotiated, and therefore which encoding this side
/// uses for the values it originates.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Hash)]
#[non_exhaustive]
pub enum AmfEncoding {
    #[default]
    Amf0,
    Amf3,
}

impl AmfEncoding {
    /// The highest encoding this crate can encode and decode.
    ///
    /// This is a statement about the codec, not a policy. What a session
    /// actually agrees to is set by
    /// [`crate::sessions::ServerSessionConfig::max_object_encoding`] and
    /// [`crate::sessions::ClientSessionConfig::object_encoding`].
    pub const MAX_SUPPORTED: AmfEncoding = AmfEncoding::Amf3;

    /// Interpret a raw `objectEncoding` property.
    ///
    /// Only 0 and 3 are defined. Anything else - including a non-numeric value,
    /// or AMF3's 1 and 2 which were never used for RTMP - is treated as AMF0,
    /// which is the encoding every peer must support.
    pub fn from_object_encoding(value: f64) -> AmfEncoding {
        if value == 3.0 {
            AmfEncoding::Amf3
        } else {
            AmfEncoding::Amf0
        }
    }

    /// The numeric value to echo back in the `connect` response.
    pub fn as_object_encoding(self) -> f64 {
        match self {
            AmfEncoding::Amf0 => 0.0,
            AmfEncoding::Amf3 => 3.0,
        }
    }

    /// Resolve what a client asked for against what this side supports.
    ///
    /// The response must never advertise more than we can honour: echoing a
    /// client's request unchanged is what lets it switch to an encoding the
    /// server cannot actually decode.
    pub fn negotiate(requested: AmfEncoding, supported: AmfEncoding) -> AmfEncoding {
        match (requested, supported) {
            (AmfEncoding::Amf3, AmfEncoding::Amf3) => AmfEncoding::Amf3,
            _ => AmfEncoding::Amf0,
        }
    }

    pub fn is_amf3(self) -> bool {
        matches!(self, AmfEncoding::Amf3)
    }
}

/// The operations the session layer needs from an AMF value, in both
/// directions.
///
/// Inspection alone is not enough to remove the duplicated session code: the
/// server *builds* status objects and responses as well as reading them, so the
/// constructors are part of the trait.
pub trait AmfValue: Sized + Clone + PartialEq + std::fmt::Debug {
    fn as_str(&self) -> Option<&str>;
    /// Accepts any numeric representation the encoding allows.
    fn as_number(&self) -> Option<f64>;
    fn as_bool(&self) -> Option<bool>;
    /// Flattened property map for object-like values, in wire order.
    fn as_properties(&self) -> Option<AmfProperties<Self>>;
    fn is_null(&self) -> bool;

    fn string(value: &str) -> Self;
    fn number(value: f64) -> Self;
    fn boolean(value: bool) -> Self;
    fn null() -> Self;
    /// An anonymous object with the given members.
    fn object(members: Vec<(String, Self)>) -> Self;

    /// The encoding this value belongs to.
    fn encoding() -> AmfEncoding;

    /// Interpret as a message stream id.
    ///
    /// Some encoders send stream ids as strings, so the string form is accepted
    /// in both encodings rather than only in the one where it was first seen.
    fn as_stream_id(&self) -> Option<u32> {
        if let Some(number) = self.as_number() {
            if number.is_finite() && number >= 0.0 && number <= u32::MAX as f64 {
                return Some(number as u32);
            }
            return None;
        }
        self.as_str().and_then(|s| s.parse::<u32>().ok())
    }
}

impl AmfValue for Amf0Value {
    fn as_str(&self) -> Option<&str> {
        match self {
            Amf0Value::Utf8String(v) => Some(v.as_str()),
            _ => None,
        }
    }
    fn as_number(&self) -> Option<f64> {
        match self {
            Amf0Value::Number(v) => Some(*v),
            _ => None,
        }
    }
    fn as_bool(&self) -> Option<bool> {
        match self {
            Amf0Value::Boolean(v) => Some(*v),
            _ => None,
        }
    }
    fn as_properties(&self) -> Option<AmfProperties<Self>> {
        match self {
            Amf0Value::Object(map) => Some(map.clone()),
            Amf0Value::TypedObject { properties, .. } => Some(properties.clone()),
            _ => None,
        }
    }
    fn is_null(&self) -> bool {
        matches!(self, Amf0Value::Null | Amf0Value::Undefined)
    }
    fn string(value: &str) -> Self {
        Amf0Value::Utf8String(value.to_owned())
    }
    fn number(value: f64) -> Self {
        Amf0Value::Number(value)
    }
    fn boolean(value: bool) -> Self {
        Amf0Value::Boolean(value)
    }
    fn null() -> Self {
        Amf0Value::Null
    }
    fn object(members: Vec<(String, Self)>) -> Self {
        Amf0Value::Object(members.into_iter().collect())
    }
    fn encoding() -> AmfEncoding {
        AmfEncoding::Amf0
    }
}

impl AmfValue for Amf3Value {
    fn as_str(&self) -> Option<&str> {
        match self {
            Amf3Value::String(v) => Some(v.as_str()),
            _ => None,
        }
    }
    fn as_number(&self) -> Option<f64> {
        match self {
            Amf3Value::Integer(v) => Some(*v as f64),
            Amf3Value::Double(v) => Some(*v),
            _ => None,
        }
    }
    fn as_bool(&self) -> Option<bool> {
        match self {
            Amf3Value::Boolean(v) => Some(*v),
            _ => None,
        }
    }
    fn as_properties(&self) -> Option<AmfProperties<Self>> {
        self.get_object_properties()
    }
    fn is_null(&self) -> bool {
        matches!(self, Amf3Value::Null | Amf3Value::Undefined)
    }
    fn string(value: &str) -> Self {
        Amf3Value::String(value.to_owned())
    }
    fn number(value: f64) -> Self {
        Amf0Value::Number(value).to_amf3()
    }
    fn boolean(value: bool) -> Self {
        Amf3Value::Boolean(value)
    }
    fn null() -> Self {
        Amf3Value::Null
    }
    fn object(members: Vec<(String, Self)>) -> Self {
        Amf3Value::dynamic_object(members)
    }
    fn encoding() -> AmfEncoding {
        AmfEncoding::Amf3
    }
}

/// A NetConnection/NetStream status object, built once for either encoding.
pub fn status_object<V: AmfValue>(level: &str, code: &str, description: &str) -> V {
    V::object(vec![
        ("level".to_string(), V::string(level)),
        ("code".to_string(), V::string(code)),
        ("description".to_string(), V::string(description)),
    ])
}

/// Reader extension point for AMF decoders.
pub use crate::amf_common::AmfRead;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn negotiate_never_advertises_more_than_supported() {
        assert_eq!(
            AmfEncoding::negotiate(AmfEncoding::Amf3, AmfEncoding::Amf0),
            AmfEncoding::Amf0
        );
        assert_eq!(
            AmfEncoding::negotiate(AmfEncoding::Amf3, AmfEncoding::Amf3),
            AmfEncoding::Amf3
        );
        assert_eq!(
            AmfEncoding::negotiate(AmfEncoding::Amf0, AmfEncoding::Amf3),
            AmfEncoding::Amf0
        );
    }

    #[test]
    fn undefined_object_encoding_values_fall_back_to_amf0() {
        for raw in [0.0, 1.0, 2.0, 4.0, -1.0, f64::NAN] {
            assert_eq!(AmfEncoding::from_object_encoding(raw), AmfEncoding::Amf0);
        }
        assert_eq!(AmfEncoding::from_object_encoding(3.0), AmfEncoding::Amf3);
    }

    #[test]
    fn stream_ids_parse_from_both_numbers_and_strings() {
        assert_eq!(Amf0Value::Number(7.0).as_stream_id(), Some(7));
        assert_eq!(Amf3Value::Integer(7).as_stream_id(), Some(7));
        assert_eq!(Amf3Value::Double(7.0).as_stream_id(), Some(7));
        assert_eq!(Amf0Value::Utf8String("7".into()).as_stream_id(), Some(7));
        assert_eq!(Amf0Value::Number(-1.0).as_stream_id(), None);
    }

    #[test]
    fn status_object_has_the_same_shape_in_both_encodings() {
        let a: Amf0Value = status_object("status", "NetStream.Publish.Start", "ok");
        let b: Amf3Value = status_object("status", "NetStream.Publish.Start", "ok");
        let a_props = a.as_properties().unwrap();
        let b_props = b.as_properties().unwrap();
        assert_eq!(a_props.len(), b_props.len());
        for (key, value) in &a_props {
            assert_eq!(value.as_str(), b_props[key].as_str());
        }
    }
}
