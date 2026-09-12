//! Data-event preservation coverage: script data must survive ingest and relay.
//!
//! The listener must forward data events, not just audio and video. Captions
//! arrive as AMF0 script-data messages such as onCaption, and metadata
//! arrives as @setDataFrame / onMetaData. The server session surfaces both
//! without decoding them (StreamDataReceived and StreamDataReceived carry
//! the raw encoded payload), and the GStreamer layer relays those bytes
//! verbatim inside FLV script-data tags (tag type 18). These tests prove the
//! protocol half: what a publisher sends is exactly what the session raises.

#[path = "support/api.rs"]
mod api;
use crate::api::amf0::Amf0Object;
use crate::api::{
    amf0::Amf0Value,
    messages::{RawMessage, RtmpMessage},
    sessions::{ClientSessionResult, ServerSessionEvent, ServerSessionResult},
    time::RtmpTimestamp,
};
use bytes::Bytes;

#[path = "support/sessions.rs"]
mod session_support;
use session_support::*;

fn script_data_payload(
    values: Vec<Amf0Value>,
    timestamp: RtmpTimestamp,
    stream_id: u32,
) -> RawMessage {
    RtmpMessage::Amf0Data { values }
        .into_raw_message(timestamp, stream_id)
        .expect("script data must encode")
}

/// An onCaption-style data message must reach the session with its encoded
/// bytes untouched, so captions survive the relay instead of being dropped
/// as an unknown script message.
#[test]
fn oncaption_style_data_message_is_preserved_verbatim() {
    let (mut deserializer, mut serializer, mut session, stream_id) =
        publishing_server(APP, "captions");
    let timestamp = RtmpTimestamp::new(1234);
    let payload = script_data_payload(
        vec![
            Amf0Value::Utf8String("onCaption".to_string()),
            Amf0Value::Utf8String("hello captions".to_string()),
        ],
        timestamp,
        stream_id,
    );
    let expected: Bytes = payload.data.clone();
    let packet = serializer
        .serialize(&payload, false, false)
        .expect("caption message must serialize");

    let (_, events) = feed_server(&mut session, &mut deserializer, &packet.to_vec());
    assert_eq!(
        events.len(),
        1,
        "onCaption chunk must raise exactly one event"
    );
    match &events[0] {
        ServerSessionEvent::StreamDataReceived { message, .. } => {
            let raw_payload = message.payload().clone();
            let event_timestamp = message.timestamp();
            assert_eq!(event_timestamp, timestamp, "caption timestamp must survive");
            assert_eq!(
                raw_payload, &expected,
                "onCaption bytes must survive verbatim"
            );
        }
        other => panic!("onCaption must surface as StreamDataReceived, got {other:?}"),
    }
}

/// An onMetaData frame must keep its exact encoded bytes, including fields
/// the typed metadata view does not model.
#[test]
fn metadata_raw_payload_is_preserved_verbatim() {
    let (mut deserializer, mut serializer, mut session, stream_id) = publishing_server(APP, "meta");
    let timestamp = RtmpTimestamp::new(0);
    let mut properties = Amf0Object::new();
    properties.insert("width".to_string(), Amf0Value::Number(1920.0));
    properties.insert(
        "unknownVendorKey".to_string(),
        Amf0Value::Utf8String("keep-me".to_string()),
    );
    let payload = script_data_payload(
        vec![
            Amf0Value::Utf8String("@setDataFrame".to_string()),
            Amf0Value::Utf8String("onMetaData".to_string()),
            Amf0Value::Object(properties),
        ],
        timestamp,
        stream_id,
    );
    let expected: Bytes = payload.data.clone();
    let packet = serializer
        .serialize(&payload, false, false)
        .expect("metadata message must serialize");

    let (_, events) = feed_server(&mut session, &mut deserializer, &packet.to_vec());
    assert_eq!(
        events.len(),
        1,
        "onMetaData chunk must raise exactly one event"
    );
    match &events[0] {
        ServerSessionEvent::StreamDataReceived { message, .. } => {
            let raw_metadata = message
                .metadata()
                .unwrap()
                .unwrap()
                .into_iter()
                .collect::<Vec<_>>();
            let raw_payload = message.payload().clone();
            assert_eq!(
                raw_payload, &expected,
                "onMetaData bytes must survive verbatim"
            );
            let raw: Amf0Object = raw_metadata.iter().cloned().collect();
            assert_eq!(
                raw.get("unknownVendorKey"),
                Some(&Amf0Value::Utf8String("keep-me".to_string())),
                "unmodelled metadata keys must survive alongside the typed view"
            );
        }
        other => panic!("onMetaData must surface as StreamDataReceived, got {other:?}"),
    }
}

// --- client-side helpers: drive a ClientSession to Publishing -------------

/// End to end: bytes handed to the client publisher must come out of the
/// server session bit-identical. This is the caption path through real
/// session machinery on both sides.
#[test]
fn client_raw_data_payload_round_trips_to_server_verbatim() {
    let (mut server_deserializer, _, mut server, stream_id) = publishing_server(APP, "interop");
    let mut client = publishing_client(stream_id, "interop");

    let timestamp = RtmpTimestamp::new(777);
    let wire: Bytes = script_data_payload(
        vec![
            Amf0Value::Utf8String("onCaption".to_string()),
            Amf0Value::Utf8String("interop caption".to_string()),
        ],
        timestamp,
        stream_id,
    )
    .data
    .clone();

    let result = client
        .publish_data(crate::api::sessions::DataMessage::new(
            crate::api::sessions::DataMessageType::Amf0,
            timestamp,
            wire.clone(),
        ))
        .expect("raw data publish must build");
    let packet_bytes = match result {
        ClientSessionResult::Packet(packet) => packet.to_vec(),
        other => panic!("raw data publish must emit a packet, got {other:?}"),
    };

    let (_, events) = feed_server(&mut server, &mut server_deserializer, &packet_bytes);
    assert_eq!(
        events.len(),
        1,
        "caption packet must raise exactly one event"
    );
    match &events[0] {
        ServerSessionEvent::StreamDataReceived { message, .. } => {
            let raw_payload = message.payload().clone();
            let event_timestamp = message.timestamp();
            assert_eq!(event_timestamp, timestamp, "caption timestamp must survive");
            assert_eq!(
                raw_payload, &wire,
                "caption bytes must survive client and server verbatim"
            );
        }
        other => panic!("caption must surface as StreamDataReceived, got {other:?}"),
    }
}

#[test]
fn segmented_session_ingest_and_relay_retain_payload_storage() {
    use crate::api::chunk_io::MessageDecoder;
    use crate::api::{Payload, Segments};
    let (_, mut serializer, mut server, stream_id) = publishing_server(APP, "segments");
    let original = Bytes::from(vec![0x27; 4097]);
    let payload = RawMessage {
        timestamp: RtmpTimestamp::new(77),
        type_id: 9,
        message_stream_id: stream_id,
        data: original.clone(),
    };
    let wire = Bytes::from(
        serializer
            .serialize(&payload, false, false)
            .unwrap()
            .to_vec(),
    );
    let mut received = None;
    // Split inside both RTMP headers and payloads.
    for offset in (0..wire.len()).step_by(113) {
        let part = wire.slice(offset..(offset + 113).min(wire.len()));
        server
            .handle_bytes(part, |result| {
                if let ServerSessionResult::Event(ServerSessionEvent::VideoDataReceived {
                    data,
                    ..
                }) = result
                {
                    received = Some(data);
                }
            })
            .unwrap();
    }
    let data: Payload = received.unwrap();
    assert!(data.segment_count() > 1);
    for part in data.segments() {
        assert!((part.as_ptr() as usize) >= wire.as_ptr() as usize);
        assert!((part.as_ptr() as usize) + part.len() <= wire.as_ptr() as usize + wire.len());
    }
    assert_eq!(data.to_bytes(), original);
    let mut encoder = rtmpx::chunk_io::ChunkEncoder::new();
    let plan = encoder
        .encode(
            RawMessage {
                data,
                timestamp: RtmpTimestamp::new(77),
                type_id: 9,
                message_stream_id: stream_id,
            },
            rtmpx::EncodeOptions::default(),
        )
        .unwrap();
    let mut decoder = MessageDecoder::new();
    let out = decoder
        .decode(&mut Bytes::from(plan.to_vec()))
        .unwrap()
        .unwrap();
    assert_eq!(out.data.into_bytes(), original);
}
