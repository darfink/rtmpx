//! AMF3 codec and Enhanced RTMP mapping coverage.
//!
//! Mirrors the AMF0 API shape: `amf3::serialize` / `amf3::deserialize` over
//! value vecs, `RtmpMessage::Amf3Command` (type 17) / `RtmpMessage::Amf3Data`
//! (type 15), and strict typed errors instead of AMF0 fallback.

use bytes::Bytes;
use rtmpx::amf::AmfEncoding;
use rtmpx::amf0::Amf0Object;
use rtmpx::amf0::Amf0Value;
use rtmpx::amf3::{self, Amf3Value};
use rtmpx::chunk_io::{ChunkDeserializer, ChunkSerializer};
use rtmpx::messages::{MessagePayload, RtmpMessage};
use rtmpx::sessions::{
    ClientSession, ClientSessionConfig, ClientSessionResult, PublishRequestType, ServerSession,
    ServerSessionConfig, ServerSessionEvent, ServerSessionResult,
};
use rtmpx::time::RtmpTimestamp;
use std::io::Cursor;

fn round_trip(value: &Amf3Value) -> Amf3Value {
    let bytes = amf3::serialize(std::slice::from_ref(value)).expect("must encode");
    let mut cursor = Cursor::new(bytes.as_slice());
    let out = amf3::deserialize(&mut cursor).expect("must decode");
    assert_eq!(out.len(), 1);
    out.into_iter().next().unwrap()
}

fn decode_all(bytes: &[u8]) -> Vec<Amf3Value> {
    let mut cursor = Cursor::new(bytes);
    amf3::deserialize(&mut cursor).expect("must decode")
}

fn amf3_obj(pairs: Vec<(&str, Amf3Value)>) -> Amf3Value {
    Amf3Value::dynamic_object(pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect())
}

#[test]
fn u29_integer_boundaries_round_trip() {
    for v in [
        0,
        0x7f,
        0x80,
        0x3fff,
        0x4000,
        0x1f_ffff,
        0x20_0000,
        0x0fff_ffff,
        -1,
        -268_435_456,
        268_435_455,
    ] {
        assert_eq!(
            round_trip(&Amf3Value::Integer(v)),
            Amf3Value::Integer(v),
            "u29 {v}"
        );
    }
}

#[test]
fn out_of_range_integer_encodes_as_double() {
    let bytes = amf3::serialize(std::slice::from_ref(&Amf3Value::Integer(1 << 28))).unwrap();
    assert_eq!(bytes[0], 0x05, "past U29 range must widen to double");
    assert_eq!(decode_all(&bytes)[0].get_number(), Some((1 << 28) as f64));
}

#[test]
fn every_marker_round_trips() {
    let values = vec![
        Amf3Value::Undefined,
        Amf3Value::Null,
        Amf3Value::Boolean(false),
        Amf3Value::Boolean(true),
        Amf3Value::Integer(-42),
        Amf3Value::Double(1.5),
        Amf3Value::String("hello".to_string()),
        Amf3Value::XmlDoc("<a/>".to_string()),
        Amf3Value::Date(1_700_000_000_000.0),
        Amf3Value::Array {
            dense: vec![Amf3Value::Integer(1)],
            associative: Vec::new(),
        },
        amf3_obj(vec![("k", Amf3Value::Boolean(true))]),
        Amf3Value::Xml("<b/>".to_string()),
        Amf3Value::ByteArray(vec![0, 1, 2, 255]),
        Amf3Value::VectorInt {
            fixed: false,
            values: vec![-1, 2],
        },
        Amf3Value::VectorUint {
            fixed: true,
            values: vec![1, 2],
        },
        Amf3Value::VectorDouble {
            fixed: false,
            values: vec![0.5],
        },
        Amf3Value::VectorObject {
            type_name: "T".to_string(),
            fixed: false,
            values: vec![Amf3Value::Null],
        },
        Amf3Value::Dictionary {
            weak_keys: false,
            entries: vec![(Amf3Value::String("k".to_string()), Amf3Value::Integer(7))],
        },
    ];
    let bytes = amf3::serialize(&values).expect("must encode");
    assert_eq!(decode_all(&bytes), values);
}

#[test]
fn empty_dense_and_associative_arrays() {
    let empty = Amf3Value::Array {
        dense: Vec::new(),
        associative: Vec::new(),
    };
    assert_eq!(round_trip(&empty), empty);
    let assoc = Amf3Value::Array {
        dense: vec![Amf3Value::Integer(9)],
        associative: vec![("name".to_string(), Amf3Value::String("v".to_string()))],
    };
    assert_eq!(round_trip(&assoc), assoc);
}

#[test]
fn string_reference_resolves() {
    let bytes = [0x06, 0x05, b'h', b'i', 0x06, 0x00];
    assert_eq!(
        decode_all(&bytes),
        vec![
            Amf3Value::String("hi".to_string()),
            Amf3Value::String("hi".to_string())
        ]
    );
}

#[test]
fn trait_reference_reuses_sealed_shape() {
    let bytes = [
        0x0A, 0x13, 0x01, 0x03, b'x', 0x04, 0x01, 0x0A, 0x01, 0x04, 0x02,
    ];
    let out = decode_all(&bytes);
    assert_eq!(out.len(), 2);
    for (value, n) in out.iter().zip([1, 2]) {
        match value {
            Amf3Value::Object {
                sealed, dynamic, ..
            } => {
                // Traits with the dynamic bit clear decode as `None`, not as an
                // empty member list.
                assert!(dynamic.is_none());
                assert_eq!(*sealed, vec![("x".to_string(), Amf3Value::Integer(n))]);
            }
            other => panic!("expected object, got {other:?}"),
        }
    }
}

#[test]
fn avmplus_wrapped_single_value_decodes() {
    let mut cursor = Cursor::new([0x11, 0x04, 0x05].as_slice());
    assert_eq!(
        amf3::decode_avmplus_wrapped(&mut cursor).unwrap(),
        Amf3Value::Integer(5)
    );
}

#[test]
fn avmplus_references_are_not_sticky() {
    let first = [0x11, 0x06, 0x05, b'h', b'i'];
    let mut cursor = Cursor::new(first.as_slice());
    assert_eq!(
        amf3::decode_avmplus_wrapped(&mut cursor).unwrap(),
        Amf3Value::String("hi".to_string())
    );
    let second = [0x11, 0x06, 0x00];
    let mut cursor = Cursor::new(second.as_slice());
    let err = amf3::decode_avmplus_wrapped(&mut cursor).unwrap_err();
    assert!(
        matches!(err, rtmpx::Amf3DeserializationError::BadStringReference(0)),
        "{err:?}"
    );
}

#[test]
fn malformed_inputs_are_typed_errors() {
    let mut cursor = Cursor::new([0x04].as_slice());
    assert!(matches!(
        amf3::deserialize_single(&mut cursor).unwrap_err(),
        rtmpx::Amf3DeserializationError::UnexpectedEof
    ));
    let mut cursor = Cursor::new([0x12].as_slice());
    assert!(matches!(
        amf3::deserialize_single(&mut cursor).unwrap_err(),
        rtmpx::Amf3DeserializationError::UnknownMarker(0x12)
    ));
    let mut cursor = Cursor::new([0x06, 0x00].as_slice());
    assert!(matches!(
        amf3::deserialize_single(&mut cursor).unwrap_err(),
        rtmpx::Amf3DeserializationError::BadStringReference(_)
    ));
    let mut cursor = Cursor::new([0x09, 0x00].as_slice());
    assert!(matches!(
        amf3::deserialize_single(&mut cursor).unwrap_err(),
        rtmpx::Amf3DeserializationError::BadObjectReference(_)
    ));
    let mut cursor = Cursor::new([0x0A, 0x05].as_slice());
    assert!(matches!(
        amf3::deserialize_single(&mut cursor).unwrap_err(),
        rtmpx::Amf3DeserializationError::BadTraitReference(_)
    ));
    // An externalizable class this codec has no reader for stops with the class
    // name attached so the caller can fall back to opaque relay.
    let mut cursor = Cursor::new([0x0A, 0x07, 0x03, b'X'].as_slice());
    match amf3::deserialize_single(&mut cursor).unwrap_err() {
        rtmpx::Amf3DeserializationError::ExternalizableUnsupported(name) => {
            assert_eq!(name, "X");
        }
        other => panic!("expected ExternalizableUnsupported, got {other:?}"),
    }
}

#[test]
fn depth_and_size_limits_hold() {
    let mut deep = Amf3Value::Null;
    for _ in 0..80 {
        deep = Amf3Value::Array {
            dense: vec![deep],
            associative: Vec::new(),
        };
    }
    assert!(matches!(
        amf3::serialize(std::slice::from_ref(&deep)).unwrap_err(),
        rtmpx::Amf3SerializationError::DepthLimit
    ));
    let mut bytes = Vec::new();
    for _ in 0..70 {
        bytes.extend_from_slice(&[0x09, 0x03, 0x01]);
    }
    bytes.push(0x01);
    let mut cursor = Cursor::new(bytes.as_slice());
    assert!(matches!(
        amf3::deserialize_single(&mut cursor).unwrap_err(),
        rtmpx::Amf3DeserializationError::DepthLimit
    ));
    let big = Amf3Value::String("x".repeat(5 * 1024 * 1024));
    assert!(matches!(
        amf3::serialize(std::slice::from_ref(&big)).unwrap_err(),
        rtmpx::Amf3SerializationError::StringTooLong(_)
    ));
}

#[test]
fn message_type_ids_follow_enhanced_mapping() {
    let cmd0 = RtmpMessage::Amf0Command {
        command_name: "connect".to_string(),
        transaction_id: 1.0,
        command_object: Amf0Value::Null,
        additional_arguments: Vec::new(),
    };
    assert_eq!(cmd0.get_message_type_id(), 20);
    let cmd3 = RtmpMessage::Amf3Command {
        command_name: "connect".to_string(),
        transaction_id: 1.0,
        command_object: Amf3Value::Null,
        additional_arguments: Vec::new(),
        format: AmfEncoding::Amf0,
    };
    assert_eq!(cmd3.get_message_type_id(), 17);
    let data0 = RtmpMessage::Amf0Data { values: Vec::new() };
    assert_eq!(data0.get_message_type_id(), 18);
    let data3 = RtmpMessage::Amf3Data {
        values: Vec::new(),
        format: AmfEncoding::Amf0,
    };
    assert_eq!(data3.get_message_type_id(), 15);
    assert_eq!(
        RtmpMessage::Amf0SharedObject { data: Bytes::new() }.get_message_type_id(),
        19
    );
    assert_eq!(
        RtmpMessage::Amf3SharedObject { data: Bytes::new() }.get_message_type_id(),
        16
    );
}

#[test]
fn amf3_command_round_trips_over_type_17() {
    for (format, selector) in [(AmfEncoding::Amf0, 0x00u8), (AmfEncoding::Amf3, 0x03u8)] {
        // One property, so the assertion does not depend on map ordering; the
        // AMF0-framed path stores objects in a `HashMap` and cannot preserve
        // property order. `Integer` rather than `Double` for the same reason:
        // AMF0 has a single numeric type, so an integral `Double` comes back as
        // `Integer`. See `amf0_framing_normalises_numbers_and_drops_order`.
        let msg = RtmpMessage::Amf3Command {
            command_name: "connect".to_string(),
            transaction_id: 1.0,
            command_object: amf3_obj(vec![("app", Amf3Value::String("live".to_string()))]),
            additional_arguments: vec![Amf3Value::Boolean(true), Amf3Value::Integer(4)],
            format,
        };
        let payload = msg
            .clone()
            .into_message_payload(RtmpTimestamp::new(0), 0)
            .unwrap();
        assert_eq!(payload.type_id, 17);
        assert_eq!(payload.data[0], selector);
        assert_eq!(payload.to_rtmp_message().unwrap(), msg);
        let create = RtmpMessage::Amf3Command {
            command_name: "createStream".to_string(),
            transaction_id: 4.0,
            command_object: Amf3Value::Null,
            additional_arguments: Vec::new(),
            format,
        };
        let payload = create
            .clone()
            .into_message_payload(RtmpTimestamp::new(0), 0)
            .unwrap();
        assert_eq!(payload.to_rtmp_message().unwrap(), create);
    }
}

/// The `0x00` selector genuinely encodes an AMF0 body, so AMF3 distinctions
/// AMF0 does not have are lost. This is a property of the wire format, not of
/// this implementation, and it is why the numeric accessors on both value types
/// accept either representation.
#[test]
fn amf0_framing_normalises_numbers_and_drops_order() {
    let msg = RtmpMessage::Amf3Data {
        values: vec![amf3_obj(vec![
            ("b", Amf3Value::Double(3.0)),
            ("a", Amf3Value::Double(1.5)),
        ])],
        format: AmfEncoding::Amf0,
    };
    let payload = msg.into_message_payload(RtmpTimestamp::new(0), 0).unwrap();
    match payload.to_rtmp_message().unwrap() {
        RtmpMessage::Amf3Data { values, .. } => {
            let props = values[0].get_object_properties().unwrap();
            // Integral doubles come back as integers...
            assert_eq!(props.get("b"), Some(&Amf3Value::Integer(3)));
            // ...non-integral ones keep their type...
            assert_eq!(props.get("a"), Some(&Amf3Value::Double(1.5)));
            // ...and both read back identically through the accessors.
            assert_eq!(props["b"].get_double(), Some(3.0));
            assert_eq!(props["b"].get_integer(), Some(3));
        }
        other => panic!("expected AMF3 data, got {other:?}"),
    }

    // The AMF3-framed path preserves both exactly.
    let exact = RtmpMessage::Amf3Data {
        values: vec![amf3_obj(vec![
            ("b", Amf3Value::Double(3.0)),
            ("a", Amf3Value::Double(1.5)),
        ])],
        format: AmfEncoding::Amf3,
    };
    let payload = exact
        .clone()
        .into_message_payload(RtmpTimestamp::new(0), 0)
        .unwrap();
    assert_eq!(payload.to_rtmp_message().unwrap(), exact);
}

#[test]
fn amf3_data_round_trips_over_type_15() {
    for (format, selector) in [(AmfEncoding::Amf0, 0x00u8), (AmfEncoding::Amf3, 0x03u8)] {
        let msg = RtmpMessage::Amf3Data {
            values: vec![
                Amf3Value::String("@setDataFrame".to_string()),
                Amf3Value::String("onMetaData".to_string()),
                amf3_obj(vec![("width", Amf3Value::Integer(1280))]),
            ],
            format,
        };
        let payload = msg
            .clone()
            .into_message_payload(RtmpTimestamp::new(0), 1)
            .unwrap();
        assert_eq!(payload.type_id, 15);
        assert_eq!(payload.data[0], selector);
        assert_eq!(payload.to_rtmp_message().unwrap(), msg);
    }
}

#[test]
fn undefined_format_selectors_are_strict_errors() {
    // Only 0x00 (AMF0 body) and 0x03 (AMF3 body) are defined.
    for bad in [0x01u8, 0x02, 0x04, 0xFF] {
        let msg = RtmpMessage::Amf3Command {
            command_name: "connect".to_string(),
            transaction_id: 1.0,
            command_object: Amf3Value::Null,
            additional_arguments: Vec::new(),
            format: AmfEncoding::Amf0,
        };
        let payload = msg.into_message_payload(RtmpTimestamp::new(0), 0).unwrap();
        let mut raw = payload.data.to_vec();
        raw[0] = bad;
        let payload = MessagePayload {
            data: Bytes::from(raw),
            ..payload
        };
        let err = payload.to_rtmp_message().unwrap_err();
        assert!(
            format!("{err:?}").contains("BadFormatSelector"),
            "got {err:?}"
        );
    }
}

/// A type 17/15 payload whose selector is 0x00 carries an **AMF0** body. This
/// is what Flash, librtmp and FFmpeg actually send, and it used to be fed to
/// the AMF3 decoder and rejected.
#[test]
fn amf0_bodies_behind_a_zero_selector_decode_on_types_15_and_17() {
    let legacy = RtmpMessage::Amf0Command {
        command_name: "connect".to_string(),
        transaction_id: 15.0,
        command_object: Amf0Value::Object(
            [("app".to_string(), Amf0Value::Utf8String("live".to_string()))]
                .into_iter()
                .collect(),
        ),
        additional_arguments: Vec::new(),
    };
    let payload = legacy
        .into_message_payload(RtmpTimestamp::new(0), 0)
        .unwrap();
    let mut raw = vec![0x00u8];
    raw.extend_from_slice(&payload.data);
    let payload = MessagePayload {
        data: Bytes::from(raw),
        type_id: 17,
        ..payload
    };
    match payload
        .to_rtmp_message()
        .expect("AMF0 body on type 17 must decode")
    {
        RtmpMessage::Amf3Command {
            command_name,
            transaction_id,
            command_object,
            format,
            ..
        } => {
            assert_eq!(command_name, "connect");
            assert_eq!(transaction_id, 15.0);
            assert_eq!(
                command_object,
                amf3_obj(vec![("app", Amf3Value::String("live".to_string()))])
            );
            assert_eq!(format, AmfEncoding::Amf0);
        }
        other => panic!("expected an AMF3 command, got {other:?}"),
    }

    let legacy_data = RtmpMessage::Amf0Data {
        values: vec![Amf0Value::Boolean(true)],
    };
    let payload = legacy_data
        .into_message_payload(RtmpTimestamp::new(0), 0)
        .unwrap();
    let mut raw = vec![0x00u8];
    raw.extend_from_slice(&payload.data);
    let payload = MessagePayload {
        data: Bytes::from(raw),
        type_id: 15,
        ..payload
    };
    match payload
        .to_rtmp_message()
        .expect("AMF0 body on type 15 must decode")
    {
        RtmpMessage::Amf3Data { values, format } => {
            assert_eq!(values, vec![Amf3Value::Boolean(true)]);
            assert_eq!(format, AmfEncoding::Amf0);
        }
        other => panic!("expected AMF3 data, got {other:?}"),
    }
}

#[test]
fn shared_objects_stay_opaque() {
    for (type_id, make) in [
        (
            19u8,
            RtmpMessage::Amf0SharedObject {
                data: Bytes::from_static(b"s0"),
            },
        ),
        (
            16u8,
            RtmpMessage::Amf3SharedObject {
                data: Bytes::from_static(b"s3"),
            },
        ),
    ] {
        let payload = make
            .clone()
            .into_message_payload(RtmpTimestamp::new(0), 0)
            .unwrap();
        assert_eq!(payload.type_id, type_id);
        assert_eq!(payload.to_rtmp_message().unwrap(), make);
    }
}

struct XorShift(u64);

impl XorShift {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
}

fn fuzz_value(rng: &mut XorShift, depth: usize) -> Amf3Value {
    match rng.next() % if depth > 2 { 8 } else { 13 } {
        0 => Amf3Value::Undefined,
        1 => Amf3Value::Null,
        2 => Amf3Value::Boolean(rng.next().is_multiple_of(2)),
        3 => Amf3Value::Integer((rng.next() % 500_000) as i32 - 250_000),
        4 => Amf3Value::Double((rng.next() % 1000) as f64 / 4.0),
        5 => Amf3Value::String(format!("s{}", rng.next() % 16)),
        6 => Amf3Value::ByteArray(vec![(rng.next() % 251) as u8; (rng.next() % 8) as usize]),
        7 => Amf3Value::Date((rng.next() % 1_000_000) as f64),
        8 => {
            let n = (rng.next() % 4) as usize;
            Amf3Value::Array {
                dense: (0..n).map(|_| fuzz_value(rng, depth + 1)).collect(),
                associative: Vec::new(),
            }
        }
        9 => amf3_obj(vec![(
            if rng.next().is_multiple_of(2) {
                "a"
            } else {
                "b"
            },
            fuzz_value(rng, depth + 1),
        )]),
        10 => Amf3Value::VectorInt {
            fixed: false,
            values: vec![rng.next() as i32],
        },
        11 => Amf3Value::Dictionary {
            weak_keys: false,
            entries: vec![(
                Amf3Value::String("k".to_string()),
                fuzz_value(rng, depth + 1),
            )],
        },
        _ => Amf3Value::Boolean(true),
    }
}

#[test]
fn fuzz_round_trip_equivalence() {
    let mut rng = XorShift(0x1234_5678_9abc_def1);
    for _ in 0..300 {
        let value = fuzz_value(&mut rng, 0);
        assert_eq!(round_trip(&value), value);
    }
}

fn drain_server_outbound(deserializer: &mut ChunkDeserializer, results: Vec<ServerSessionResult>) {
    for result in results {
        if let ServerSessionResult::OutboundResponse(packet) = result {
            let payload = deserializer
                .get_next_message(&packet.bytes)
                .expect("response must decode")
                .expect("response must be complete");
            if let RtmpMessage::SetChunkSize { size } =
                payload.to_rtmp_message().expect("response must parse")
            {
                deserializer
                    .set_max_chunk_size(size as usize)
                    .expect("chunk size applies");
            }
        }
    }
}

fn send_to_server(
    session: &mut ServerSession,
    serializer: &mut ChunkSerializer,
    deserializer: &mut ChunkDeserializer,
    message: RtmpMessage,
    stream_id: u32,
    _first_on_stream: bool,
) -> (Vec<RtmpMessage>, Vec<ServerSessionEvent>, Bytes) {
    let payload = message
        .into_message_payload(RtmpTimestamp::new(0), stream_id)
        .unwrap();
    let raw = payload.data.clone();
    let packet = serializer.serialize(&payload, true, false).unwrap();
    let mut responses = Vec::new();
    let mut events = Vec::new();
    for result in session.handle_input(&packet.bytes).unwrap() {
        match result {
            ServerSessionResult::OutboundResponse(packet) => {
                let payload = deserializer
                    .get_next_message(&packet.bytes)
                    .unwrap()
                    .unwrap();
                let message = payload.to_rtmp_message().unwrap();
                if let RtmpMessage::SetChunkSize { size } = &message {
                    deserializer.set_max_chunk_size(*size as usize).unwrap();
                }
                responses.push(message);
            }
            ServerSessionResult::RaisedEvent(event) => events.push(event),
            ServerSessionResult::UnhandleableMessageReceived(_) => {}
        }
    }
    (responses, events, raw)
}

fn amf3_connect(app: &str) -> RtmpMessage {
    RtmpMessage::Amf3Command {
        command_name: "connect".to_string(),
        transaction_id: 1.0,
        command_object: amf3_obj(vec![
            ("app", Amf3Value::String(app.to_string())),
            ("objectEncoding", Amf3Value::Double(3.0)),
            (
                "tcUrl",
                Amf3Value::String(format!("rtmp://localhost/{app}")),
            ),
        ]),
        additional_arguments: Vec::new(),
        format: AmfEncoding::Amf0,
    }
}

#[test]
fn amf3_end_to_end_publish_flow_preserves_bytes() {
    let (mut session, initial) = ServerSession::new(ServerSessionConfig::new()).unwrap();
    let mut deserializer = ChunkDeserializer::new();
    let mut serializer = ChunkSerializer::new();
    drain_server_outbound(&mut deserializer, initial);

    let (_, events, _) = send_to_server(
        &mut session,
        &mut serializer,
        &mut deserializer,
        amf3_connect("live"),
        0,
        true,
    );
    // An AMF3 connect raises the same event as an AMF0 one: the session
    // normalises the value model so callers - and Enhanced RTMP capability
    // validation - see one shape regardless of encoding.
    let request_id = events
        .iter()
        .find_map(|e| match e {
            ServerSessionEvent::ConnectionRequested { request_id, .. } => Some(*request_id),
            _ => None,
        })
        .expect("AMF3 connect must raise ConnectionRequested");
    let event = events
        .into_iter()
        .find(|e| matches!(e, ServerSessionEvent::ConnectionRequested { .. }))
        .unwrap();
    match event {
        ServerSessionEvent::ConnectionRequested {
            app_name,
            additional_properties,
            ..
        } => {
            assert_eq!(app_name.as_ref(), "live");
            assert!(
                additional_properties.contains_key("tcUrl"),
                "remainder forwards verbatim"
            );
            assert!(!additional_properties.contains_key("app"));
            assert!(!additional_properties.contains_key("objectEncoding"));
        }
        _ => unreachable!(),
    }
    let results = session
        .accept_request(request_id)
        .expect("accept must work");
    let mut saw_result = false;
    for result in results {
        if let ServerSessionResult::OutboundResponse(packet) = result {
            let payload = deserializer
                .get_next_message(&packet.bytes)
                .unwrap_or_else(|e| panic!("chunk parse failed: {e:?}"))
                .expect("accept response must be complete");
            let message = payload.to_rtmp_message().unwrap();
            if let RtmpMessage::SetChunkSize { size } = &message {
                deserializer.set_max_chunk_size(*size as usize).unwrap();
                continue;
            }
            if let RtmpMessage::Amf3Command {
                command_name,
                additional_arguments,
                ..
            } = &message
                && command_name == "_result"
            {
                saw_result = true;
                let status = &additional_arguments[0];
                let props = status.get_object_properties().unwrap();
                // AMF0-framed response body, so the number arrives integral.
                assert_eq!(
                    props.get("objectEncoding").and_then(|v| v.get_double()),
                    Some(3.0)
                );
            }
        }
    }
    assert!(saw_result, "AMF3 accept must answer with an AMF3 _result");

    let create = RtmpMessage::Amf3Command {
        command_name: "createStream".to_string(),
        transaction_id: 4.0,
        command_object: Amf3Value::Null,
        additional_arguments: Vec::new(),
        format: AmfEncoding::Amf0,
    };
    let (responses, _, _) = send_to_server(
        &mut session,
        &mut serializer,
        &mut deserializer,
        create,
        0,
        false,
    );
    let stream_id = responses
        .iter()
        .find_map(|m| match m {
            RtmpMessage::Amf3Command {
                command_name,
                additional_arguments,
                ..
            } if command_name == "_result" => additional_arguments
                .first()
                .and_then(|v| v.get_number())
                .map(|n| n as u32),
            _ => None,
        })
        .expect("createStream needs an AMF3 _result with a stream id");

    let publish = RtmpMessage::Amf3Command {
        command_name: "publish".to_string(),
        transaction_id: 5.0,
        command_object: Amf3Value::Null,
        additional_arguments: vec![
            Amf3Value::String("key".to_string()),
            Amf3Value::String("live".to_string()),
        ],
        format: AmfEncoding::Amf0,
    };
    let (_, events, _) = send_to_server(
        &mut session,
        &mut serializer,
        &mut deserializer,
        publish,
        stream_id,
        false,
    );
    let request_id = events
        .iter()
        .find_map(|e| match e {
            ServerSessionEvent::PublishStreamRequested { request_id, .. } => Some(*request_id),
            _ => None,
        })
        .expect("publish must raise PublishStreamRequested");
    drain_server_outbound(
        &mut deserializer,
        session.accept_request(request_id).unwrap(),
    );

    let meta = RtmpMessage::Amf3Data {
        values: vec![
            Amf3Value::String("@setDataFrame".to_string()),
            Amf3Value::String("onMetaData".to_string()),
            amf3_obj(vec![("width", Amf3Value::Double(1280.0))]),
        ],
        format: AmfEncoding::Amf0,
    };
    let (_, events, raw) = send_to_server(
        &mut session,
        &mut serializer,
        &mut deserializer,
        meta,
        stream_id,
        false,
    );
    assert_eq!(events.len(), 1);
    match &events[0] {
        ServerSessionEvent::StreamMetadataChanged {
            is_amf3,
            raw_payload,
            raw_metadata,
            ..
        } => {
            assert!(is_amf3);
            assert_eq!(raw_payload, &raw, "relay keeps the original bytes");
            assert!(raw_metadata.iter().any(|(k, _)| k == "width"));
        }
        other => panic!("expected metadata, got {other:?}"),
    }

    let caption = RtmpMessage::Amf3Data {
        values: vec![
            Amf3Value::String("onCaption".to_string()),
            amf3_obj(vec![("text", Amf3Value::String("hi".to_string()))]),
        ],
        format: AmfEncoding::Amf0,
    };
    let (_, events, raw) = send_to_server(
        &mut session,
        &mut serializer,
        &mut deserializer,
        caption,
        stream_id,
        false,
    );
    assert_eq!(events.len(), 1);
    match &events[0] {
        ServerSessionEvent::StreamDataReceived {
            is_amf3,
            raw_payload,
            ..
        } => {
            assert!(is_amf3);
            assert_eq!(raw_payload, &raw);
        }
        other => panic!("expected script data, got {other:?}"),
    }
}

fn client_outbound_messages(
    deserializer: &mut ChunkDeserializer,
    results: Vec<ClientSessionResult>,
) -> Vec<(MessagePayload, RtmpMessage)> {
    let mut out = Vec::new();
    for result in results {
        if let ClientSessionResult::OutboundResponse(packet) = result {
            let payload = deserializer
                .get_next_message(&packet.bytes)
                .unwrap()
                .unwrap();
            let message = payload.to_rtmp_message().unwrap_or_else(|e| {
                panic!(
                    "type {} first bytes {:02x?}: {e:?}",
                    payload.type_id,
                    &payload.data[..payload.data.len().min(8)]
                )
            });
            if let RtmpMessage::SetChunkSize { size } = &message {
                deserializer.set_max_chunk_size(*size as usize).unwrap();
            }
            out.push((payload, message));
        }
    }
    out
}

fn server_packet(
    serializer: &mut ChunkSerializer,
    message: RtmpMessage,
    stream_id: u32,
) -> Vec<u8> {
    let payload = message
        .into_message_payload(RtmpTimestamp::new(0), stream_id)
        .unwrap();
    serializer
        .serialize(&payload, true, false)
        .unwrap()
        .bytes
        .to_vec()
}

fn amf0_connect_success() -> RtmpMessage {
    let mut cmd = Amf0Object::new();
    cmd.insert(
        "fmsVer".to_string(),
        Amf0Value::Utf8String("fms".to_string()),
    );
    cmd.insert("capabilities".to_string(), Amf0Value::Number(31.0));
    let mut status = Amf0Object::new();
    status.insert(
        "level".to_string(),
        Amf0Value::Utf8String("status".to_string()),
    );
    status.insert(
        "code".to_string(),
        Amf0Value::Utf8String("NetConnection.Connect.Success".to_string()),
    );
    status.insert("objectEncoding".to_string(), Amf0Value::Number(0.0));
    RtmpMessage::Amf0Command {
        command_name: "_result".to_string(),
        transaction_id: 1.0,
        command_object: Amf0Value::Object(cmd),
        additional_arguments: vec![Amf0Value::Object(status)],
    }
}

#[test]
fn client_raw_amf3_relay_keeps_type_15_bytes() {
    let config = ClientSessionConfig::new();
    let mut deserializer = ChunkDeserializer::new();
    let mut serializer = ChunkSerializer::new();
    let (mut session, initial) = ClientSession::new(config).unwrap();
    client_outbound_messages(&mut deserializer, initial);
    let connect = session.request_connection("live".to_string()).unwrap();
    let outbound = client_outbound_messages(&mut deserializer, vec![connect]);
    let txn = outbound
        .iter()
        .find_map(|(_, m)| match m {
            RtmpMessage::Amf0Command { transaction_id, .. } => Some(*transaction_id),
            _ => None,
        })
        .unwrap();
    assert_eq!(txn, 1.0);
    let bytes = server_packet(&mut serializer, amf0_connect_success(), 0);
    client_outbound_messages(&mut deserializer, session.handle_input(&bytes).unwrap());
    let publish_req = session
        .request_publishing("key".to_string(), PublishRequestType::Live)
        .unwrap();
    let outbound = client_outbound_messages(&mut deserializer, vec![publish_req]);
    let create_txn = outbound
        .iter()
        .find_map(|(_, m)| match m {
            RtmpMessage::Amf0Command {
                command_name,
                transaction_id,
                ..
            } if command_name == "createStream" => Some(*transaction_id),
            _ => None,
        })
        .unwrap();
    let create_ok = RtmpMessage::Amf0Command {
        command_name: "_result".to_string(),
        transaction_id: create_txn,
        command_object: Amf0Value::Null,
        additional_arguments: vec![Amf0Value::Number(7.0)],
    };
    let bytes = server_packet(&mut serializer, create_ok, 0);
    client_outbound_messages(&mut deserializer, session.handle_input(&bytes).unwrap());
    let mut status = Amf0Object::new();
    status.insert(
        "level".to_string(),
        Amf0Value::Utf8String("status".to_string()),
    );
    status.insert(
        "code".to_string(),
        Amf0Value::Utf8String("NetStream.Publish.Start".to_string()),
    );
    let publish_ok = RtmpMessage::Amf0Command {
        command_name: "onStatus".to_string(),
        transaction_id: 0.0,
        command_object: Amf0Value::Null,
        additional_arguments: vec![Amf0Value::Object(status)],
    };
    let bytes = server_packet(&mut serializer, publish_ok, 7);
    client_outbound_messages(&mut deserializer, session.handle_input(&bytes).unwrap());

    let body = Bytes::from_static(b"\x00\x06\x05hi\x06\x00");
    let result = session
        .publish_raw_amf3_data_payload(body.clone(), RtmpTimestamp::new(10))
        .unwrap();
    let packet = match result {
        ClientSessionResult::OutboundResponse(packet) => packet,
        other => panic!("expected outbound packet, got {other:?}"),
    };
    let payload = deserializer
        .get_next_message(&packet.bytes)
        .unwrap()
        .unwrap();
    assert_eq!(payload.type_id, 15, "AMF3 relay must stay type 15");
    assert_eq!(payload.data, body);
    let body0 = Bytes::from_static(b"amf0-bytes");
    let result = session
        .publish_raw_data_payload(body0.clone(), RtmpTimestamp::new(11))
        .unwrap();
    let packet = match result {
        ClientSessionResult::OutboundResponse(packet) => packet,
        other => panic!("expected outbound packet, got {other:?}"),
    };
    let payload = deserializer
        .get_next_message(&packet.bytes)
        .unwrap()
        .unwrap();
    assert_eq!(payload.type_id, 18, "AMF0 relay must stay type 18");
    assert_eq!(payload.data, body0);
}

// ---------------------------------------------------------------------------
// Regression coverage for the AMF3 codec fixes.
// ---------------------------------------------------------------------------

/// `Some(vec![])` (dynamic traits, no members) and `None` (non-dynamic traits)
/// are different bytes and must not collapse into each other.
#[test]
fn dynamic_flag_survives_a_round_trip_when_there_are_no_members() {
    let dynamic_empty = Amf3Value::Object {
        class_name: None,
        sealed: Vec::new(),
        dynamic: Some(Vec::new()),
    };
    let not_dynamic = Amf3Value::Object {
        class_name: None,
        sealed: Vec::new(),
        dynamic: None,
    };
    assert_eq!(round_trip(&dynamic_empty), dynamic_empty);
    assert_eq!(round_trip(&not_dynamic), not_dynamic);

    let dynamic_bytes = amf3::serialize(std::slice::from_ref(&dynamic_empty)).unwrap();
    let plain_bytes = amf3::serialize(std::slice::from_ref(&not_dynamic)).unwrap();
    assert_ne!(
        dynamic_bytes, plain_bytes,
        "the two shapes must encode differently"
    );
}

/// The Flex wrapper classes are registered as NestedAmf3: they serialize as
/// exactly one nested AMF3 value, so they decode instead of killing the
/// connection.
#[test]
fn known_externalizable_classes_round_trip() {
    let collection = Amf3Value::Externalizable {
        class_name: "flex.messaging.io.ArrayCollection".to_string(),
        value: Box::new(Amf3Value::Array {
            dense: vec![Amf3Value::Integer(1), Amf3Value::String("two".to_string())],
            associative: Vec::new(),
        }),
    };
    assert_eq!(round_trip(&collection), collection);
}

/// An externalizable class with no known reader cannot be skipped - AMF carries
/// no length for it - so it stops with the class name attached rather than
/// silently mis-parsing the rest of the stream.
#[test]
fn unknown_externalizable_reports_its_class_name() {
    let mut bytes = vec![0x0A, 0x07];
    bytes.push(0x0B); // inline string, 5 bytes
    bytes.extend_from_slice(b"DSA\0\0"[..5].as_ref());
    let mut cursor = Cursor::new(bytes.as_slice());
    match amf3::deserialize_single(&mut cursor).unwrap_err() {
        rtmpx::Amf3DeserializationError::ExternalizableUnsupported(name) => {
            assert_eq!(name.len(), 5);
        }
        other => panic!("expected ExternalizableUnsupported, got {other:?}"),
    }
}

/// Repeated strings and repeated object shapes are emitted as references, so a
/// decode/re-encode does not inflate the payload.
#[test]
fn encoder_emits_string_and_trait_references() {
    let object = || Amf3Value::Object {
        class_name: Some("com.example.Track".to_string()),
        sealed: vec![("codec".to_string(), Amf3Value::String("avc1".to_string()))],
        dynamic: None,
    };
    let one = amf3::serialize(&[object()]).unwrap();
    let four = amf3::serialize(&[object(), object(), object(), object()]).unwrap();

    // Four copies must cost far less than four times one, because the class
    // name, the sealed property name and the traits are all referenced.
    assert!(
        four.len() < one.len() * 2,
        "expected references to be emitted: one={} four={}",
        one.len(),
        four.len()
    );
    assert_eq!(
        decode_all(&four),
        vec![object(), object(), object(), object()]
    );
}

/// A reference that points at a value still being decoded means the graph is
/// cyclic. `Amf3Value` is a tree and cannot hold one, so it is reported as
/// such rather than as an out-of-bounds index.
#[test]
fn cyclic_reference_is_reported_as_a_cycle() {
    // An array of length 1 whose single element is a reference to the array.
    let bytes = [0x09, 0x03, 0x01, 0x09, 0x00];
    let mut cursor = Cursor::new(bytes.as_slice());
    match amf3::deserialize_single(&mut cursor).unwrap_err() {
        rtmpx::Amf3DeserializationError::CyclicReference(0) => {}
        other => panic!("expected CyclicReference(0), got {other:?}"),
    }

    // A genuinely out-of-range index still reports as out of bounds.
    let bytes = [0x09, 0x40];
    let mut cursor = Cursor::new(bytes.as_slice());
    assert!(matches!(
        amf3::deserialize_single(&mut cursor).unwrap_err(),
        rtmpx::Amf3DeserializationError::BadObjectReference(_)
    ));
}

/// A length prefix larger than the input must fail before it is used to
/// allocate.
#[test]
fn oversized_length_prefixes_do_not_allocate() {
    // A 1 MB length prefix with no payload behind it. Under the size caps, so
    // only the remaining-bytes check can reject it.
    let one_megabyte_prefix = [0x80u8, 0xC0, 0x80, 0x01];

    for marker in [0x0Cu8 /* ByteArray */, 0x06 /* String */] {
        let mut bytes = vec![marker];
        bytes.extend_from_slice(&one_megabyte_prefix);
        let mut cursor = Cursor::new(bytes.as_slice());
        assert!(
            matches!(
                amf3::deserialize_single(&mut cursor).unwrap_err(),
                rtmpx::Amf3DeserializationError::UnexpectedEof
            ),
            "marker {marker:#x} must fail before allocating"
        );
    }

    // Past the caps, the caps still fire first.
    let bytes = [0x0Cu8, 0xC0, 0x80, 0x80, 0x01];
    let mut cursor = Cursor::new(bytes.as_slice());
    assert!(matches!(
        amf3::deserialize_single(&mut cursor).unwrap_err(),
        rtmpx::Amf3DeserializationError::ByteArrayTooLong(_)
    ));
}

/// Both value types must agree on integral numbers regardless of which variant
/// the encoder chose.
#[test]
fn numeric_accessors_are_symmetric_across_encodings() {
    assert_eq!(Amf3Value::Integer(3).get_double(), Some(3.0));
    assert_eq!(Amf3Value::Double(3.0).get_integer(), Some(3));
    assert_eq!(Amf3Value::Double(3.5).get_integer(), None);
    assert_eq!(Amf0Value::Number(3.0).get_integer(), Some(3));
    assert_eq!(Amf0Value::Number(3.0).get_double(), Some(3.0));
}

// ---------------------------------------------------------------------------
// objectEncoding negotiation and Enhanced RTMP reachability.
// ---------------------------------------------------------------------------

fn amf0_connect(app: &str, extra: Vec<(&str, Amf0Value)>) -> RtmpMessage {
    let mut properties = Amf0Object::new();
    properties.insert("app".to_string(), Amf0Value::Utf8String(app.to_string()));
    for (key, value) in extra {
        properties.insert(key.to_string(), value);
    }
    RtmpMessage::Amf0Command {
        command_name: "connect".to_string(),
        transaction_id: 1.0,
        command_object: Amf0Value::Object(properties),
        additional_arguments: Vec::new(),
    }
}

fn connect_and_accept(connect: RtmpMessage) -> (Vec<RtmpMessage>, Vec<ServerSessionEvent>) {
    connect_and_accept_with(connect, ServerSessionConfig::new())
}

fn connect_and_accept_with(
    connect: RtmpMessage,
    config: ServerSessionConfig,
) -> (Vec<RtmpMessage>, Vec<ServerSessionEvent>) {
    let (mut session, initial) = ServerSession::new(config).unwrap();
    let mut deserializer = ChunkDeserializer::new();
    let mut serializer = ChunkSerializer::new();
    drain_server_outbound(&mut deserializer, initial);

    let (_, events, _) = send_to_server(
        &mut session,
        &mut serializer,
        &mut deserializer,
        connect,
        0,
        true,
    );
    let request_id = events
        .iter()
        .find_map(|e| match e {
            ServerSessionEvent::ConnectionRequested { request_id, .. } => Some(*request_id),
            _ => None,
        })
        .expect("connect must raise ConnectionRequested");

    let mut responses = Vec::new();
    for result in session
        .accept_request(request_id)
        .expect("accept must work")
    {
        if let ServerSessionResult::OutboundResponse(packet) = result {
            let payload = deserializer
                .get_next_message(&packet.bytes)
                .unwrap()
                .unwrap();
            let message = payload.to_rtmp_message().unwrap();
            if let RtmpMessage::SetChunkSize { size } = &message {
                deserializer.set_max_chunk_size(*size as usize).unwrap();
            }
            responses.push(message);
        }
    }
    (responses, events)
}

fn echoed_object_encoding(responses: &[RtmpMessage]) -> Option<f64> {
    responses.iter().find_map(|message| match message {
        RtmpMessage::Amf0Command {
            command_name,
            additional_arguments,
            ..
        } if command_name == "_result" => additional_arguments
            .first()
            .and_then(|v| v.get_object_properties())
            .and_then(|p| p.get("objectEncoding").and_then(|v| v.get_number())),
        _ => None,
    })
}

/// The response must state the encoding that will actually be used, not repeat
/// whatever the client asked for. Undefined values fall back to AMF0.
#[test]
fn object_encoding_is_negotiated_rather_than_echoed() {
    for (requested, expected) in [
        (Amf0Value::Number(3.0), 3.0),
        (Amf0Value::Number(0.0), 0.0),
        // Never defined for RTMP; must not be repeated back.
        (Amf0Value::Number(7.0), 0.0),
        (Amf0Value::Number(1.0), 0.0),
        // Not even a number.
        (Amf0Value::Utf8String("3".to_string()), 0.0),
    ] {
        let (responses, _) = connect_and_accept(amf0_connect(
            "live",
            vec![("objectEncoding", requested.clone())],
        ));
        assert_eq!(
            echoed_object_encoding(&responses),
            Some(expected),
            "objectEncoding {requested:?} should negotiate to {expected}"
        );
    }

    // Absent entirely means AMF0.
    let (responses, _) = connect_and_accept(amf0_connect("live", Vec::new()));
    assert_eq!(echoed_object_encoding(&responses), Some(0.0));
}

/// The negotiation ceiling is configurable, so an operator can keep a server
/// on AMF0 without it becoming brittle against a peer that sends AMF3 anyway.
#[test]
fn max_object_encoding_config_caps_what_the_server_advertises() {
    let mut amf0_only = ServerSessionConfig::new();
    amf0_only.max_object_encoding = AmfEncoding::Amf0;

    let (responses, _) = connect_and_accept_with(
        amf0_connect("live", vec![("objectEncoding", Amf0Value::Number(3.0))]),
        amf0_only,
    );
    assert_eq!(
        echoed_object_encoding(&responses),
        Some(0.0),
        "a server capped at AMF0 must not advertise 3 even when asked"
    );

    // The default still agrees to 3.
    let (responses, _) = connect_and_accept(amf0_connect(
        "live",
        vec![("objectEncoding", Amf0Value::Number(3.0))],
    ));
    assert_eq!(echoed_object_encoding(&responses), Some(3.0));
}

/// Capping the server at AMF0 changes what it advertises and emits, not what it
/// accepts: a client that sends type 17 regardless is still understood.
#[test]
fn an_amf0_capped_server_still_decodes_inbound_amf3() {
    let mut amf0_only = ServerSessionConfig::new();
    amf0_only.max_object_encoding = AmfEncoding::Amf0;

    let (responses, events) = connect_and_accept_with(amf3_connect("live"), amf0_only);
    assert!(
        events
            .iter()
            .any(|e| matches!(e, ServerSessionEvent::ConnectionRequested { .. })),
        "an AMF3 connect must still be understood"
    );
    assert_eq!(echoed_object_encoding(&responses), Some(0.0));
    assert!(
        responses.iter().any(|m| matches!(
            m,
            RtmpMessage::Amf0Command { command_name, .. } if command_name == "_result"
        )),
        "a capped server answers in AMF0, got {responses:?}"
    );
}

/// The client asks for whatever its config says.
#[test]
fn client_object_encoding_config_drives_the_connect_request() {
    for encoding in [AmfEncoding::Amf0, AmfEncoding::Amf3] {
        let mut config = ClientSessionConfig::new();
        config.object_encoding = encoding;
        config.tc_url = Some("rtmp://localhost/live".to_string());

        let (mut session, _) = ClientSession::new(config).unwrap();
        let result = session.request_connection("live".to_string()).unwrap();
        let ClientSessionResult::OutboundResponse(packet) = result else {
            panic!("expected an outbound connect");
        };

        let mut deserializer = ChunkDeserializer::new();
        let payload = deserializer
            .get_next_message(&packet.bytes)
            .unwrap()
            .expect("connect must be complete");
        match payload.to_rtmp_message().unwrap() {
            RtmpMessage::Amf0Command {
                command_name,
                command_object,
                ..
            } => {
                assert_eq!(command_name, "connect");
                let properties = command_object.get_object_properties().unwrap();
                assert_eq!(
                    properties
                        .get("objectEncoding")
                        .and_then(|v| v.get_number()),
                    Some(encoding.as_object_encoding())
                );
            }
            other => panic!("expected an AMF0 connect, got {other:?}"),
        }
    }
}

/// Property order is part of the AMF0 encoding, so the same logical value must
/// produce the same bytes every time. A `std` `HashMap` randomises iteration
/// per process, which made this false.
#[test]
fn amf0_object_encoding_is_deterministic_and_order_preserving() {
    let keys = ["zebra", "alpha", "middle", "beta", "yankee", "delta"];
    let object = Amf0Value::Object(
        keys.iter()
            .map(|k| (k.to_string(), Amf0Value::Utf8String(k.to_string())))
            .collect(),
    );

    // Insertion order is what comes back, not hash order.
    let decoded = rtmpx::amf0::deserialize(&mut Cursor::new(
        rtmpx::amf0::serialize(std::slice::from_ref(&object))
            .unwrap()
            .as_slice(),
    ))
    .unwrap();
    let Amf0Value::Object(properties) = &decoded[0] else {
        panic!("expected an object");
    };
    assert_eq!(properties.keys().cloned().collect::<Vec<_>>(), keys);

    // And the bytes are stable across repeated encodes.
    let first = rtmpx::amf0::serialize(std::slice::from_ref(&object)).unwrap();
    for _ in 0..16 {
        assert_eq!(
            rtmpx::amf0::serialize(std::slice::from_ref(&object)).unwrap(),
            first
        );
    }
}

/// The same guarantee has to survive the AMF3 <-> AMF0 projection, which is
/// what the type 15/17 AMF0-framed path runs through.
#[test]
fn property_order_survives_the_amf3_projection() {
    let ordered = amf3_obj(vec![
        ("zebra", Amf3Value::Integer(1)),
        ("alpha", Amf3Value::Integer(2)),
        ("middle", Amf3Value::Integer(3)),
    ]);
    let round_tripped = ordered.to_amf0().to_amf3();
    assert_eq!(round_tripped, ordered);

    let msg = RtmpMessage::Amf3Data {
        values: vec![ordered.clone()],
        format: AmfEncoding::Amf0,
    };
    let payload = msg.into_message_payload(RtmpTimestamp::new(0), 0).unwrap();
    match payload.to_rtmp_message().unwrap() {
        RtmpMessage::Amf3Data { values, .. } => assert_eq!(values[0], ordered),
        other => panic!("expected AMF3 data, got {other:?}"),
    }
}

/// An AMF0 `connect` is answered as AMF0 even when `objectEncoding` 3 was
/// negotiated: RTMP peers mirror each other rather than switching unilaterally.
#[test]
fn amf0_connect_is_answered_as_amf0_even_at_object_encoding_three() {
    let (responses, _) = connect_and_accept(amf0_connect(
        "live",
        vec![("objectEncoding", Amf0Value::Number(3.0))],
    ));
    assert!(
        responses
            .iter()
            .any(|m| matches!(m, RtmpMessage::Amf0Command { command_name, .. } if command_name == "_result")),
        "expected an AMF0 _result, got {responses:?}"
    );
}

/// Enhanced RTMP capability validation used to be unreachable from an AMF3
/// connect because it only accepted AMF0 properties. The session now hands
/// callers one shape regardless of encoding.
#[test]
fn enhanced_capabilities_are_validated_on_an_amf3_connect() {
    use rtmpx::{EnhancedCapabilities, EnhancedValidationMode};

    let connect_with = |caps_ex: Amf3Value| RtmpMessage::Amf3Command {
        command_name: "connect".to_string(),
        transaction_id: 1.0,
        command_object: amf3_obj(vec![
            ("app", Amf3Value::String("live".to_string())),
            ("objectEncoding", Amf3Value::Integer(3)),
            ("capsEx", caps_ex),
        ]),
        additional_arguments: Vec::new(),
        format: AmfEncoding::Amf0,
    };

    let properties_from = |connect: RtmpMessage| {
        let (_, events) = connect_and_accept(connect);
        events
            .into_iter()
            .find_map(|e| match e {
                ServerSessionEvent::ConnectionRequested {
                    additional_properties,
                    ..
                } => Some(additional_properties),
                _ => None,
            })
            .expect("connect must raise ConnectionRequested")
    };

    // A valid capsEx mask passes strict validation.
    let good = properties_from(connect_with(Amf3Value::Integer(3)));
    assert!(EnhancedCapabilities::parse(&good, EnhancedValidationMode::Strict).is_ok());

    // An out-of-range mask is rejected, which is only possible because the
    // AMF3 connect reaches the same validator as an AMF0 one.
    let bad = properties_from(connect_with(Amf3Value::Integer(255)));
    assert!(
        EnhancedCapabilities::parse(&bad, EnhancedValidationMode::Strict).is_err(),
        "an invalid capsEx must be caught on the AMF3 path too"
    );
}

/// Wire bytes of a Java IExternalizable status bean: object marker, inline
/// externalizable traits, inline class name, then the bean payload of one
/// big-endian double plus four writeUTF (u16 length + bytes) strings in
/// code, description, details, level order.
const RED5_STATUS: &str = "org.red5.server.net.rtmp.status.Status";

fn status_bean_bytes(class: &str, client_id: f64, parts: [&str; 4]) -> Vec<u8> {
    let name = class.as_bytes();
    assert!(name.len() < 64, "test class names stay single-byte U29");
    let mut bytes = vec![0x0A, 0x07, ((name.len() << 1) | 1) as u8];
    bytes.extend_from_slice(name);
    bytes.extend_from_slice(&client_id.to_be_bytes());
    for part in parts {
        let part = part.as_bytes();
        bytes.extend_from_slice(&(part.len() as u16).to_be_bytes());
        bytes.extend_from_slice(part);
    }
    bytes
}

fn play_start_bean_value(class: &str) -> Amf3Value {
    Amf3Value::Externalizable {
        class_name: class.to_string(),
        value: Box::new(Amf3Value::dynamic_object(vec![
            ("clientid".to_string(), Amf3Value::Double(0.0)),
            (
                "code".to_string(),
                Amf3Value::String("NetStream.Play.Start".to_string()),
            ),
            (
                "description".to_string(),
                Amf3Value::String("Playing live.".to_string()),
            ),
            ("details".to_string(), Amf3Value::String(String::new())),
            ("level".to_string(), Amf3Value::String("status".to_string())),
        ])),
    }
}

/// Registered status beans decode to the generic Externalizable shape with
/// the bean properties as an ordinary dynamic object.
#[test]
fn status_bean_externalizable_decodes_to_generic_object() {
    let bytes = status_bean_bytes(
        RED5_STATUS,
        0.0,
        ["NetStream.Play.Start", "Playing live.", "", "status"],
    );
    let mut cursor = Cursor::new(bytes.as_slice());
    let value = amf3::deserialize_single(&mut cursor).expect("bean must decode");
    assert_eq!(value, play_start_bean_value(RED5_STATUS));
    // The wrapper is transparent for the AMF0 projection, so the shared
    // onStatus handler keeps matching on Object plus a code string.
    match value.to_amf0() {
        Amf0Value::Object(map) => {
            assert_eq!(
                map.get("code"),
                Some(&Amf0Value::Utf8String("NetStream.Play.Start".to_string()))
            );
            assert_eq!(
                map.get("level"),
                Some(&Amf0Value::Utf8String("status".to_string()))
            );
        }
        other => panic!("bean must project to a plain AMF0 object, got {other:?}"),
    }
    // And re-encodes to the exact wire bytes.
    let re = amf3::serialize(std::slice::from_ref(&value)).expect("bean must encode");
    assert_eq!(re, bytes);
}

/// Captured wire shape for the onStatus info object one Java RTMP server
/// emits (class org.red5.server.net.rtmp.status.Status): it must decode as
/// a status bean and survive inside a multi-value onStatus command, which is
/// the sequence that used to fail the session with ExternalizableUnsupported.
#[test]
fn live_server_status_wire_shape_decodes_in_on_status_command() {
    let bean = status_bean_bytes(
        RED5_STATUS,
        1.0,
        [
            "NetStream.Publish.Start",
            "Publishing live.",
            "live",
            "status",
        ],
    );
    let mut head = amf3::serialize(&[
        Amf3Value::String("onStatus".to_string()),
        Amf3Value::Double(0.0),
        Amf3Value::Null,
    ])
    .expect("command head must encode");
    head.extend_from_slice(&bean);
    let values = decode_all(&head);
    assert_eq!(values.len(), 4);
    let Amf3Value::Externalizable { class_name, value } = &values[3] else {
        panic!(
            "info object must be a bean Externalizable, got {:?}",
            values[3]
        );
    };
    assert_eq!(class_name, RED5_STATUS);
    let props = value.get_object_properties().expect("bean holds an object");
    assert_eq!(
        props.get("code"),
        Some(&Amf3Value::String("NetStream.Publish.Start".to_string()))
    );
    match value.to_amf0() {
        Amf0Value::Object(map) => assert_eq!(
            map.get("code"),
            Some(&Amf0Value::Utf8String(
                "NetStream.Publish.Start".to_string()
            ))
        ),
        other => panic!("bean must project to a plain AMF0 object, got {other:?}"),
    }
}

/// The registry matches exact class names only: a bean-looking payload under
/// an unregistered name, including a Status-suffixed name, reports the class
/// rather than being sniffed into an object.
#[test]
fn unregistered_externalizable_with_bean_payload_still_errors() {
    for class in ["com.example.Widget", "com.example.Status"] {
        let bytes = status_bean_bytes(
            class,
            0.0,
            ["NetStream.Play.Start", "Playing live.", "", "status"],
        );
        let mut cursor = Cursor::new(bytes.as_slice());
        match amf3::deserialize_single(&mut cursor).unwrap_err() {
            rtmpx::Amf3DeserializationError::ExternalizableUnsupported(name) => {
                assert_eq!(name, class);
            }
            other => panic!("expected ExternalizableUnsupported, got {other:?}"),
        }
    }
}

/// A truncated bean payload under a registered Status class reports the class
/// as unsupported rather than surfacing a confusing EOF or partial value.
#[test]
fn truncated_status_bean_reports_unsupported() {
    let mut bytes = status_bean_bytes(
        RED5_STATUS,
        0.0,
        ["NetStream.Play.Start", "Playing live.", "", "status"],
    );
    bytes.truncate(bytes.len() - 3);
    let mut cursor = Cursor::new(bytes.as_slice());
    assert!(matches!(
        amf3::deserialize_single(&mut cursor).unwrap_err(),
        rtmpx::Amf3DeserializationError::ExternalizableUnsupported(_)
    ));
}

/// A registered Status Externalizable whose inner value is not the five
/// status properties cannot be encoded as a bean.
#[test]
fn status_bean_writer_rejects_wrong_shape() {
    let odd = Amf3Value::Externalizable {
        class_name: RED5_STATUS.to_string(),
        value: Box::new(Amf3Value::dynamic_object(vec![(
            "code".to_string(),
            Amf3Value::String("NetStream.Play.Start".to_string()),
        )])),
    };
    assert!(matches!(
        amf3::serialize(std::slice::from_ref(&odd)).unwrap_err(),
        rtmpx::Amf3SerializationError::ExternalizableUnsupported(_)
    ));
}

/// The compile-time table is the only gate: Flex wrappers and Red5 Status
/// are known, a Status-suffixed name that is not in the table is not.
#[test]
fn externalizable_registry_is_exact_class_names() {
    assert!(amf3::is_known_externalizable(
        "flex.messaging.io.ArrayCollection"
    ));
    assert!(amf3::is_known_externalizable("flex.messaging.io.ArrayList"));
    assert!(amf3::is_known_externalizable(
        "flex.messaging.io.ObjectProxy"
    ));
    assert!(amf3::is_known_externalizable(RED5_STATUS));
    assert!(!amf3::is_known_externalizable("com.example.Status"));
    assert!(!amf3::is_known_externalizable(
        "org.red5.server.net.rtmp.status.StatusObject"
    ));
}
