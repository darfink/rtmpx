//! Data-event preservation coverage: script data must survive ingest and relay.
//!
//! The listener must forward data events, not just audio and video. Captions
//! arrive as AMF0 script-data messages such as onCaption, and metadata
//! arrives as @setDataFrame / onMetaData. The server session surfaces both
//! without decoding them (StreamDataReceived and StreamMetadataChanged carry
//! the raw encoded payload), and the GStreamer layer relays those bytes
//! verbatim inside FLV script-data tags (tag type 18). These tests prove the
//! protocol half: what a publisher sends is exactly what the session raises.

use bytes::Bytes;
use rtmpx::amf0::Amf0Object;
use rtmpx::{
    amf0::Amf0Value,
    chunk_io::{ChunkDeserializer, ChunkSerializer},
    messages::{MessagePayload, RtmpMessage},
    sessions::{
        ClientSession, ClientSessionConfig, ClientSessionEvent, ClientSessionResult,
        PublishRequestType, ServerSession, ServerSessionConfig, ServerSessionEvent,
        ServerSessionResult,
    },
    time::RtmpTimestamp,
};

const APP: &str = "live";

fn consume_server_outbound(
    deserializer: &mut ChunkDeserializer,
    results: Vec<ServerSessionResult>,
) {
    for result in results {
        if let ServerSessionResult::OutboundResponse(packet) = result {
            let payload = deserializer
                .get_next_message(&packet.bytes)
                .expect("server response must decode")
                .expect("server response must be complete");
            if let RtmpMessage::SetChunkSize { size } =
                payload.to_rtmp_message().expect("response must parse")
            {
                deserializer
                    .set_max_chunk_size(size as usize)
                    .expect("chunk size must apply");
            }
        }
    }
}

fn feed_server(
    session: &mut ServerSession,
    deserializer: &mut ChunkDeserializer,
    bytes: &[u8],
) -> (Vec<RtmpMessage>, Vec<ServerSessionEvent>) {
    let mut responses = Vec::new();
    let mut events = Vec::new();
    let results = session
        .handle_input(bytes)
        .expect("server must accept well-formed chunks");
    for result in results {
        match result {
            ServerSessionResult::OutboundResponse(packet) => {
                let payload = deserializer
                    .get_next_message(&packet.bytes)
                    .expect("server response must decode")
                    .expect("server response must be complete");
                let message = payload.to_rtmp_message().expect("response must parse");
                if let RtmpMessage::SetChunkSize { size } = &message {
                    deserializer
                        .set_max_chunk_size(*size as usize)
                        .expect("chunk size must apply");
                }
                responses.push(message);
            }
            ServerSessionResult::RaisedEvent(event) => events.push(event),
            ServerSessionResult::UnhandleableMessageReceived(_) => {}
            #[allow(unreachable_patterns)]
            _ => panic!("unexpected future protocol variant"),
        }
    }
    (responses, events)
}

fn send_to_server(
    session: &mut ServerSession,
    serializer: &mut ChunkSerializer,
    deserializer: &mut ChunkDeserializer,
    message: RtmpMessage,
    stream_id: u32,
    first_on_stream: bool,
) -> (Vec<RtmpMessage>, Vec<ServerSessionEvent>) {
    let payload = message
        .into_message_payload(RtmpTimestamp::new(0), stream_id)
        .expect("message must encode");
    let packet = serializer
        .serialize(&payload, first_on_stream, false)
        .expect("message must serialize");
    feed_server(session, deserializer, &packet.bytes)
}

fn connect_message(app: &str) -> RtmpMessage {
    let mut properties = Amf0Object::new();
    properties.insert("app".to_string(), Amf0Value::Utf8String(app.to_string()));
    properties.insert("objectEncoding".to_string(), Amf0Value::Number(0.0));
    RtmpMessage::Amf0Command {
        command_name: "connect".to_string(),
        transaction_id: 1.0,
        command_object: Amf0Value::Object(properties),
        additional_arguments: Vec::new(),
    }
}

/// Drive a server session to a live publish; returns the negotiated stream id.
fn publishing_server(
    app: &str,
    stream_key: &str,
) -> (ChunkDeserializer, ChunkSerializer, ServerSession, u32) {
    let (mut session, initial) =
        ServerSession::new(ServerSessionConfig::new()).expect("server session must start");
    let mut deserializer = ChunkDeserializer::new();
    let mut serializer = ChunkSerializer::new();
    consume_server_outbound(&mut deserializer, initial);

    let (_, events) = send_to_server(
        &mut session,
        &mut serializer,
        &mut deserializer,
        connect_message(app),
        0,
        true,
    );
    let request_id = events
        .iter()
        .find_map(|event| match event {
            ServerSessionEvent::ConnectionRequested { request_id, .. } => Some(*request_id),
            _ => None,
        })
        .expect("connect must raise ConnectionRequested");
    consume_server_outbound(
        &mut deserializer,
        session
            .accept_request(request_id)
            .expect("accept must work"),
    );

    let create = RtmpMessage::Amf0Command {
        command_name: "createStream".to_string(),
        transaction_id: 4.0,
        command_object: Amf0Value::Null,
        additional_arguments: Vec::new(),
    };
    let (responses, _) = send_to_server(
        &mut session,
        &mut serializer,
        &mut deserializer,
        create,
        0,
        true,
    );
    let stream_id = match responses.first().expect("createStream needs a reply") {
        RtmpMessage::Amf0Command {
            command_name,
            additional_arguments,
            ..
        } if command_name == "_result" => match additional_arguments.first() {
            Some(Amf0Value::Number(id)) => *id as u32,
            other => panic!("createStream reply must carry a stream id, got {other:?}"),
        },
        other => panic!("createStream reply must be _result, got {other:?}"),
    };

    let publish = RtmpMessage::Amf0Command {
        command_name: "publish".to_string(),
        transaction_id: 5.0,
        command_object: Amf0Value::Null,
        additional_arguments: vec![
            Amf0Value::Utf8String(stream_key.to_string()),
            Amf0Value::Utf8String("live".to_string()),
        ],
    };
    let (_, events) = send_to_server(
        &mut session,
        &mut serializer,
        &mut deserializer,
        publish,
        stream_id,
        false,
    );
    let request_id = events
        .iter()
        .find_map(|event| match event {
            ServerSessionEvent::PublishStreamRequested { request_id, .. } => Some(*request_id),
            _ => None,
        })
        .expect("publish must raise PublishStreamRequested");
    consume_server_outbound(
        &mut deserializer,
        session
            .accept_request(request_id)
            .expect("accept must work"),
    );

    (deserializer, serializer, session, stream_id)
}

fn script_data_payload(
    values: Vec<Amf0Value>,
    timestamp: RtmpTimestamp,
    stream_id: u32,
) -> MessagePayload {
    RtmpMessage::Amf0Data { values }
        .into_message_payload(timestamp, stream_id)
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

    let (_, events) = feed_server(&mut session, &mut deserializer, &packet.bytes);
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

    let (_, events) = feed_server(&mut session, &mut deserializer, &packet.bytes);
    assert_eq!(
        events.len(),
        1,
        "onMetaData chunk must raise exactly one event"
    );
    match &events[0] {
        ServerSessionEvent::StreamMetadataChanged { message, .. } => {
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
        other => panic!("onMetaData must surface as StreamMetadataChanged, got {other:?}"),
    }
}

// --- client-side helpers: drive a ClientSession to Publishing -------------

fn consume_client_outbound(
    deserializer: &mut ChunkDeserializer,
    results: Vec<ClientSessionResult>,
) {
    for result in results {
        if let ClientSessionResult::OutboundResponse(packet) = result {
            let payload = deserializer
                .get_next_message(&packet.bytes)
                .expect("client request must decode")
                .expect("client request must be complete");
            if let RtmpMessage::SetChunkSize { size } =
                payload.to_rtmp_message().expect("request must parse")
            {
                deserializer
                    .set_max_chunk_size(size as usize)
                    .expect("chunk size must apply");
            }
        }
    }
}

fn feed_client(
    session: &mut ClientSession,
    deserializer: &mut ChunkDeserializer,
    bytes: &[u8],
) -> (Vec<RtmpMessage>, Vec<ClientSessionEvent>) {
    let mut responses = Vec::new();
    let mut events = Vec::new();
    let results = session
        .handle_input(bytes)
        .expect("client must accept well-formed chunks");
    for result in results {
        match result {
            ClientSessionResult::OutboundResponse(packet) => {
                let payload = deserializer
                    .get_next_message(&packet.bytes)
                    .expect("client request must decode")
                    .expect("client request must be complete");
                responses.push(payload.to_rtmp_message().expect("request must parse"));
            }
            ClientSessionResult::RaisedEvent(event) => events.push(event),
            ClientSessionResult::UnhandleableMessageReceived(_) => {}
            #[allow(unreachable_patterns)]
            _ => panic!("unexpected future protocol variant"),
        }
    }
    (responses, events)
}

fn fake_connect_success(serializer: &mut ChunkSerializer) -> Vec<u8> {
    let mut command_properties = Amf0Object::new();
    command_properties.insert(
        "fmsVer".to_string(),
        Amf0Value::Utf8String("fms".to_string()),
    );
    command_properties.insert("capabilities".to_string(), Amf0Value::Number(31.0));
    let mut status = Amf0Object::new();
    status.insert(
        "level".to_string(),
        Amf0Value::Utf8String("status".to_string()),
    );
    status.insert(
        "code".to_string(),
        Amf0Value::Utf8String("NetConnection.Connect.Success".to_string()),
    );
    status.insert(
        "description".to_string(),
        Amf0Value::Utf8String("hi".to_string()),
    );
    status.insert("objectEncoding".to_string(), Amf0Value::Number(0.0));
    let message = RtmpMessage::Amf0Command {
        command_name: "_result".to_string(),
        transaction_id: 1.0,
        command_object: Amf0Value::Object(command_properties),
        additional_arguments: vec![Amf0Value::Object(status)],
    };
    let payload = message
        .into_message_payload(RtmpTimestamp::new(0), 0)
        .expect("connect reply must encode");
    serializer
        .serialize(&payload, false, false)
        .expect("connect reply must serialize")
        .bytes
}

fn fake_create_stream_success(
    serializer: &mut ChunkSerializer,
    transaction_id: f64,
    stream_id: u32,
) -> Vec<u8> {
    let message = RtmpMessage::Amf0Command {
        command_name: "_result".to_string(),
        transaction_id,
        command_object: Amf0Value::Null,
        additional_arguments: vec![Amf0Value::Number(stream_id as f64)],
    };
    let payload = message
        .into_message_payload(RtmpTimestamp::new(0), 0)
        .expect("createStream reply must encode");
    serializer
        .serialize(&payload, false, false)
        .expect("createStream reply must serialize")
        .bytes
}

fn fake_publish_success(serializer: &mut ChunkSerializer, stream_id: u32) -> Vec<u8> {
    let mut status = Amf0Object::new();
    status.insert(
        "level".to_string(),
        Amf0Value::Utf8String("status".to_string()),
    );
    status.insert(
        "code".to_string(),
        Amf0Value::Utf8String("NetStream.Publish.Start".to_string()),
    );
    status.insert(
        "description".to_string(),
        Amf0Value::Utf8String("hi".to_string()),
    );
    let message = RtmpMessage::Amf0Command {
        command_name: "onStatus".to_string(),
        transaction_id: 0.0,
        command_object: Amf0Value::Null,
        additional_arguments: vec![Amf0Value::Object(status)],
    };
    let payload = message
        .into_message_payload(RtmpTimestamp::new(0), stream_id)
        .expect("publish reply must encode");
    serializer
        .serialize(&payload, false, false)
        .expect("publish reply must serialize")
        .bytes
}

/// Drive a client to Publishing on the given stream id (chosen to match the
/// server under test so chunk stream ids line up end to end).
fn publishing_client(stream_id: u32, stream_key: &str) -> ClientSession {
    let (mut session, initial) =
        ClientSession::new(ClientSessionConfig::new()).expect("client session must start");
    let mut deserializer = ChunkDeserializer::new();
    let mut fake_server = ChunkSerializer::new();
    consume_client_outbound(&mut deserializer, initial);

    let request = session
        .request_connection(APP.to_string())
        .expect("connect request must build");
    consume_client_outbound(&mut deserializer, vec![request]);
    let (_, events) = feed_client(
        &mut session,
        &mut deserializer,
        &fake_connect_success(&mut fake_server),
    );
    assert!(
        matches!(
            events.first(),
            Some(ClientSessionEvent::ConnectionRequestAccepted { .. })
        ),
        "connect must be accepted, got {events:?}"
    );

    let request = session
        .request_publishing(stream_key.to_string(), PublishRequestType::Live)
        .expect("publish request must build");
    let mut responses = Vec::new();
    if let ClientSessionResult::OutboundResponse(packet) = request {
        let payload = deserializer
            .get_next_message(&packet.bytes)
            .expect("createStream must decode")
            .expect("createStream must be complete");
        responses.push(payload.to_rtmp_message().expect("createStream must parse"));
    }
    let transaction_id = match responses.first().expect("publish starts with createStream") {
        RtmpMessage::Amf0Command {
            command_name,
            transaction_id,
            ..
        } if command_name == "createStream" => *transaction_id,
        other => panic!("expected createStream, got {other:?}"),
    };
    let reply = fake_create_stream_success(&mut fake_server, transaction_id, stream_id);
    feed_client(&mut session, &mut deserializer, &reply);
    let reply = fake_publish_success(&mut fake_server, stream_id);
    let (_, events) = feed_client(&mut session, &mut deserializer, &reply);
    assert!(
        matches!(
            events.first(),
            Some(ClientSessionEvent::PublishRequestAccepted { .. })
        ),
        "publish must be accepted, got {events:?}"
    );
    session
}

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
        .publish_data(rtmpx::sessions::DataMessage::new(
            rtmpx::sessions::DataMessageType::Amf0,
            timestamp,
            wire.clone(),
        ))
        .expect("raw data publish must build");
    let packet_bytes = match result {
        ClientSessionResult::OutboundResponse(packet) => packet.bytes,
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
