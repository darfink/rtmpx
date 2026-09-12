#[path = "support/api.rs"]
mod api;
use crate::api::amf0::Amf0Object;
use crate::api::amf0::{self, Amf0DeserializationError, Amf0SerializationError, Amf0Value};
use crate::api::amf3::Amf3Value;
use std::io::Cursor;

fn round_trip(value: &Amf0Value) -> Amf0Value {
    let bytes = amf0::serialize(std::slice::from_ref(value)).expect("must encode");
    let mut cursor = Cursor::new(bytes.as_slice());
    let out = amf0::deserialize(&mut cursor).expect("must decode");
    assert_eq!(out.len(), 1);
    out.into_iter().next().unwrap()
}

#[test]
fn every_variant_round_trips() {
    let mut props = Amf0Object::new();
    props.insert("n".to_string(), Amf0Value::Number(1.5));
    let values = vec![
        Amf0Value::Number(42.5),
        Amf0Value::Boolean(true),
        Amf0Value::Boolean(false),
        Amf0Value::Utf8String("hello".to_string()),
        Amf0Value::Object(props),
        Amf0Value::StrictArray(vec![Amf0Value::Null, Amf0Value::Number(2.0)]),
        Amf0Value::Null,
        Amf0Value::Undefined,
    ];
    let bytes = amf0::serialize(&values).expect("must encode");
    let mut cursor = Cursor::new(bytes.as_slice());
    let out = amf0::deserialize(&mut cursor).expect("must decode");
    assert_eq!(out, values);
}

#[test]
fn serialize_accepts_slice_and_vec() {
    let values = vec![Amf0Value::Null, Amf0Value::Undefined];
    let from_slice = amf0::serialize(&values[..]).expect("slice must encode");
    let from_vec_ref = amf0::serialize(&values).expect("vec ref must encode");
    assert_eq!(from_slice, from_vec_ref);
    assert_eq!(from_slice, vec![5u8, 6u8]);
}

#[test]
fn getters_borrow_and_support_numbers() {
    let n = Amf0Value::Number(3.0);
    assert_eq!(n.get_number(), Some(3.0));
    assert_eq!(n.get_number(), Some(3.0));
    assert_eq!(n.get_integer(), Some(3));
    assert_eq!(n.get_double(), Some(3.0));
    assert_eq!(Amf0Value::Number(1.5).get_integer(), None);
    assert_eq!(Amf0Value::Number(f64::NAN).get_integer(), None);
    let s = Amf0Value::Utf8String("x".to_string());
    assert_eq!(s.get_string(), Some("x".to_string()));
    assert_eq!(s.get_string(), Some("x".to_string()));
    let b = Amf0Value::Boolean(true);
    assert_eq!(b.get_boolean(), Some(true));
    let mut map = Amf0Object::new();
    map.insert("k".to_string(), Amf0Value::Null);
    let o = Amf0Value::Object(map.clone());
    assert_eq!(o.get_object_properties(), Some(map));
}

#[test]
fn deserialize_single_reads_one_value() {
    let bytes = amf0::serialize(&[Amf0Value::Number(1.0), Amf0Value::Number(2.0)]).unwrap();
    let mut cursor = Cursor::new(bytes.as_slice());
    let first = amf0::deserialize_single(&mut cursor).expect("must decode one");
    assert_eq!(first, Amf0Value::Number(1.0));
    let second = amf0::deserialize_single(&mut cursor).expect("must decode two");
    assert_eq!(second, Amf0Value::Number(2.0));
}

#[test]
fn ecma_array_decodes_as_object() {
    let mut bytes = Vec::new();
    bytes.push(8u8);
    bytes.extend_from_slice(&1u32.to_be_bytes());
    bytes.extend_from_slice(&3u16.to_be_bytes());
    bytes.extend_from_slice(b"foo");
    bytes.push(0u8);
    bytes.extend_from_slice(&5f64.to_be_bytes());
    bytes.extend_from_slice(&0u16.to_be_bytes());
    bytes.push(9u8);
    let mut cursor = Cursor::new(bytes.as_slice());
    let out = amf0::deserialize(&mut cursor).expect("ecma must decode");
    assert_eq!(out.len(), 1);
    let mut expected = Amf0Object::new();
    expected.insert("foo".to_string(), Amf0Value::Number(5.0));
    assert_eq!(out[0], Amf0Value::Object(expected));
}

#[test]
fn truncated_strict_array_is_strict_eof() {
    let mut bytes = Vec::new();
    bytes.push(10u8);
    bytes.extend_from_slice(&2u32.to_be_bytes());
    bytes.push(0u8);
    bytes.extend_from_slice(&1f64.to_be_bytes());
    let mut cursor = Cursor::new(bytes.as_slice());
    let err = amf0::deserialize(&mut cursor).expect_err("truncated array must fail");
    assert!(matches!(err, Amf0DeserializationError::UnexpectedEof));
}

#[test]
fn unknown_marker_is_typed() {
    let bytes = vec![0xFFu8];
    let mut cursor = Cursor::new(bytes.as_slice());
    let err = amf0::deserialize(&mut cursor).expect_err("unknown marker must fail");
    assert!(matches!(
        err,
        Amf0DeserializationError::UnknownMarker { marker: 0xFF }
    ));
}

#[test]
fn oversize_collection_is_rejected_on_decode() {
    let mut bytes = Vec::new();
    bytes.push(10u8);
    bytes.extend_from_slice(&100_001u32.to_be_bytes());
    let mut cursor = Cursor::new(bytes.as_slice());
    let err = amf0::deserialize(&mut cursor).expect_err("oversize must fail");
    assert!(matches!(
        err,
        Amf0DeserializationError::CollectionTooLarge(100_001)
    ));
}

#[test]
fn oversize_collection_is_rejected_on_encode() {
    let values = vec![Amf0Value::Null; 100_001];
    let err = amf0::serialize(&[Amf0Value::StrictArray(values)]).expect_err("oversize must fail");
    assert!(matches!(
        err,
        Amf0SerializationError::CollectionTooLarge(100_001)
    ));
}

#[test]
fn deep_nesting_hits_depth_limit() {
    let mut value = Amf0Value::Null;
    for _ in 0..80 {
        let mut map = Amf0Object::new();
        map.insert("n".to_string(), value);
        value = Amf0Value::Object(map);
    }
    let err = amf0::serialize(std::slice::from_ref(&value)).expect_err("deep must fail");
    assert!(matches!(err, Amf0SerializationError::DepthLimit));
}

#[test]
fn long_string_uses_the_long_string_marker() {
    // A string past the u16 ceiling has a defined AMF0 encoding
    // (`long-string-marker`), so it round-trips rather than failing.
    let s = "a".repeat(70_000);
    let bytes = amf0::serialize(std::slice::from_ref(&Amf0Value::Utf8String(s.clone())))
        .expect("long strings encode");
    assert_eq!(bytes[0], 12, "expected long-string-marker");
    let mut cursor = Cursor::new(bytes.as_slice());
    assert_eq!(
        amf0::deserialize(&mut cursor).unwrap(),
        vec![Amf0Value::Utf8String(s)]
    );
}

#[test]
fn long_property_names_are_still_rejected() {
    // Property names ride on a bare u16 with no long form.
    let mut map = Amf0Object::new();
    map.insert("a".repeat(70_000), Amf0Value::Null);
    let err = amf0::serialize(std::slice::from_ref(&Amf0Value::Object(map)))
        .expect_err("long property names must fail");
    assert!(matches!(err, Amf0SerializationError::NormalStringTooLong));
}

#[test]
fn amf0_amf3_conversions_are_symmetric() {
    let cases = vec![
        Amf0Value::Undefined,
        Amf0Value::Null,
        Amf0Value::Boolean(true),
        Amf0Value::Number(42.0),
        Amf0Value::Utf8String("hi".to_string()),
    ];
    for v in cases {
        let as3: Amf3Value = v.clone().into();
        assert_eq!(as3, v.to_amf3());
        let back: Amf0Value = as3.clone().into();
        assert_eq!(back, as3.to_amf0());
    }
    let n = Amf0Value::Number(42.0);
    assert_eq!(n.to_amf3(), Amf3Value::Integer(42));
    let arr = Amf0Value::StrictArray(vec![Amf0Value::Number(1.0)]);
    let as3 = arr.to_amf3();
    match as3 {
        Amf3Value::Array { dense, associative } => {
            assert_eq!(dense.len(), 1);
            assert!(associative.is_empty());
        }
        other => panic!("expected array, got {:?}", other),
    }
}

#[test]
fn individual_round_trip_helper() {
    assert_eq!(round_trip(&Amf0Value::Null), Amf0Value::Null);
    assert_eq!(round_trip(&Amf0Value::Number(7.0)), Amf0Value::Number(7.0));
}

// ---------------------------------------------------------------------------
// Markers a real client can send that the vendored decoder did not know about.
// ---------------------------------------------------------------------------

/// The `avmplus-object-marker` carries exactly one AMF3 value and then returns
/// to AMF0. This is how an `objectEncoding` 3 peer mixes AMF3 values into an
/// AMF0-framed type 15/17 payload.
#[test]
fn avmplus_escape_round_trips_and_is_not_sticky() {
    let escaped = Amf0Value::AvmPlus(Box::new(Amf3Value::ByteArray(vec![1, 2, 3])));
    let values = vec![
        Amf0Value::Utf8String("before".to_string()),
        escaped.clone(),
        // An ordinary AMF0 value directly after the escape proves the switch
        // applied to one value only.
        Amf0Value::Number(42.0),
    ];
    let bytes = amf0::serialize(&values).expect("must encode");
    assert!(bytes.contains(&0x11), "expected an avmplus marker");

    let mut cursor = Cursor::new(bytes.as_slice());
    assert_eq!(amf0::deserialize(&mut cursor).expect("must decode"), values);
}

/// AMF3-only values have no AMF0 form, so `to_amf0` parks them behind the
/// escape instead of degrading them to `Null` or a bare number.
#[test]
fn amf3_only_values_survive_the_amf0_projection() {
    for value in [
        Amf3Value::ByteArray(vec![9, 9]),
        Amf3Value::VectorInt {
            fixed: true,
            values: vec![-1, 2],
        },
        Amf3Value::Dictionary {
            weak_keys: false,
            entries: vec![],
        },
    ] {
        let projected = value.to_amf0();
        assert!(!value.is_amf0_representable());
        assert_eq!(projected, Amf0Value::AvmPlus(Box::new(value.clone())));
        // ...and comes back unchanged.
        assert_eq!(projected.to_amf3(), value);
    }

    // Values that do have an AMF0 form convert structurally.
    assert!(Amf3Value::String("x".to_string()).is_amf0_representable());
    assert!(Amf3Value::Integer(1).is_amf0_representable());
}

#[test]
fn date_xml_and_typed_objects_round_trip() {
    let date = Amf0Value::Date {
        millis: 1_700_000_000_000.0,
        timezone: 0,
    };
    assert_eq!(round_trip(&date), date);

    let xml = Amf0Value::XmlDocument("<a/>".to_string());
    assert_eq!(round_trip(&xml), xml);

    let mut properties = Amf0Object::new();
    properties.insert("x".to_string(), Amf0Value::Number(1.0));
    let typed = Amf0Value::TypedObject {
        class_name: "com.example.T".to_string(),
        properties,
    };
    assert_eq!(round_trip(&typed), typed);
}

/// A command message with fewer than three values used to hit `drain(..3)` and
/// panic the session; it must be a typed error.
#[test]
fn short_amf0_command_is_an_error_not_a_panic() {
    use crate::api::messages::{RawMessage, RtmpMessage};
    use crate::api::time::RtmpTimestamp;

    for values in [
        vec![],
        vec![Amf0Value::Utf8String("connect".to_string())],
        vec![
            Amf0Value::Utf8String("connect".to_string()),
            Amf0Value::Number(1.0),
        ],
    ] {
        let data = amf0::serialize(&values).expect("must encode");
        let payload = RawMessage {
            timestamp: RtmpTimestamp::new(0),
            type_id: 20,
            message_stream_id: 0,
            data: bytes::Bytes::from(data),
        };
        assert!(
            payload.to_rtmp_message().is_err(),
            "a {}-value command must be rejected, not panic",
            values.len()
        );
    }

    // Three values still decode.
    let values = vec![
        Amf0Value::Utf8String("connect".to_string()),
        Amf0Value::Number(1.0),
        Amf0Value::Null,
    ];
    let data = amf0::serialize(&values).unwrap();
    let payload = RawMessage {
        timestamp: RtmpTimestamp::new(0),
        type_id: 20,
        message_stream_id: 0,
        data: bytes::Bytes::from(data),
    };
    assert!(matches!(
        payload.to_rtmp_message(),
        Ok(RtmpMessage::Amf0Command { .. })
    ));
}

#[test]
fn oversized_string_length_does_not_allocate() {
    // string-marker claiming 65535 bytes with none behind it.
    let bytes = [0x02u8, 0xFF, 0xFF];
    let mut cursor = Cursor::new(bytes.as_slice());
    assert!(matches!(
        amf0::deserialize(&mut cursor).unwrap_err(),
        Amf0DeserializationError::UnexpectedEof
    ));
}
