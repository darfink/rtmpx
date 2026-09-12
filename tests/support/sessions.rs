#![allow(dead_code)]
//! Shared RTMP session setup for integration tests.
use crate::api::amf0::Amf0Object;
use crate::api::{
    amf0::Amf0Value,
    chunk_io::{ChunkEncoder, ContiguousDecoder},
    messages::RtmpMessage,
    sessions::{
        ClientSession, ClientSessionConfig, ClientSessionEvent, ClientSessionResult, PublishMode,
        ServerSession, ServerSessionConfig, ServerSessionEvent, ServerSessionResult,
    },
    time::RtmpTimestamp,
};

pub const APP: &str = "live";

pub fn consume_server_outbound(
    deserializer: &mut ContiguousDecoder,
    results: Vec<ServerSessionResult>,
) {
    for result in results {
        if let ServerSessionResult::Packet(packet) = result {
            let payload = deserializer
                .get_next_message(&packet.to_vec())
                .expect("server response must decode")
                .expect("server response must be complete");
            if let RtmpMessage::SetChunkSize { size } =
                payload.to_rtmp_message().expect("response must parse")
            {
                deserializer
                    .set_chunk_size(size as usize)
                    .expect("chunk size must apply");
            }
        }
    }
}

pub fn feed_server(
    session: &mut ServerSession,
    deserializer: &mut ContiguousDecoder,
    bytes: &[u8],
) -> (Vec<RtmpMessage>, Vec<ServerSessionEvent>) {
    let mut responses = Vec::new();
    let mut events = Vec::new();
    let results = session
        .handle_input(bytes)
        .expect("server must accept well-formed chunks");
    for result in results {
        match result {
            ServerSessionResult::Packet(packet) => {
                let payload = deserializer
                    .get_next_message(&packet.to_vec())
                    .expect("server response must decode")
                    .expect("server response must be complete");
                let message = payload.to_rtmp_message().expect("response must parse");
                if let RtmpMessage::SetChunkSize { size } = &message {
                    deserializer
                        .set_chunk_size(*size as usize)
                        .expect("chunk size must apply");
                }
                responses.push(message);
            }
            ServerSessionResult::Event(event) => events.push(event),
            ServerSessionResult::UnhandledMessage(_) => {}
            #[allow(unreachable_patterns)]
            _ => panic!("unexpected future protocol variant"),
        }
    }
    (responses, events)
}

pub fn send_to_server(
    session: &mut ServerSession,
    serializer: &mut ChunkEncoder,
    deserializer: &mut ContiguousDecoder,
    message: RtmpMessage,
    stream_id: u32,
    first_on_stream: bool,
) -> (Vec<RtmpMessage>, Vec<ServerSessionEvent>) {
    let payload = message
        .into_raw_message(RtmpTimestamp::new(0), stream_id)
        .expect("message must encode");
    let packet = serializer
        .serialize(&payload, first_on_stream, false)
        .expect("message must serialize");
    feed_server(session, deserializer, &packet.to_vec())
}

pub fn connect_message(app: &str) -> RtmpMessage {
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
pub fn publishing_server(
    app: &str,
    stream_key: &str,
) -> (ContiguousDecoder, ChunkEncoder, ServerSession, u32) {
    let (mut session, initial) =
        ServerSession::new(ServerSessionConfig::new()).expect("server session must start");
    let mut deserializer = ContiguousDecoder::new();
    let mut serializer = ChunkEncoder::new();
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

pub fn consume_client_outbound(
    deserializer: &mut ContiguousDecoder,
    results: Vec<ClientSessionResult>,
) {
    for result in results {
        if let ClientSessionResult::Packet(packet) = result {
            let payload = deserializer
                .get_next_message(&packet.to_vec())
                .expect("client request must decode")
                .expect("client request must be complete");
            if let RtmpMessage::SetChunkSize { size } =
                payload.to_rtmp_message().expect("request must parse")
            {
                deserializer
                    .set_chunk_size(size as usize)
                    .expect("chunk size must apply");
            }
        }
    }
}

pub fn feed_client(
    session: &mut ClientSession,
    deserializer: &mut ContiguousDecoder,
    bytes: &[u8],
) -> (Vec<RtmpMessage>, Vec<ClientSessionEvent>) {
    let mut responses = Vec::new();
    let mut events = Vec::new();
    let results = session
        .handle_input(bytes)
        .expect("client must accept well-formed chunks");
    for result in results {
        match result {
            ClientSessionResult::Packet(packet) => {
                let payload = deserializer
                    .get_next_message(&packet.to_vec())
                    .expect("client request must decode")
                    .expect("client request must be complete");
                responses.push(payload.to_rtmp_message().expect("request must parse"));
            }
            ClientSessionResult::Event(event) => events.push(event),
            ClientSessionResult::UnhandledMessage(_) => {}
            #[allow(unreachable_patterns)]
            _ => panic!("unexpected future protocol variant"),
        }
    }
    (responses, events)
}

pub fn fake_connect_success(serializer: &mut ChunkEncoder) -> Vec<u8> {
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
        .into_raw_message(RtmpTimestamp::new(0), 0)
        .expect("connect reply must encode");
    serializer
        .serialize(&payload, false, false)
        .expect("connect reply must serialize")
        .to_vec()
}

pub fn fake_create_stream_success(
    serializer: &mut ChunkEncoder,
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
        .into_raw_message(RtmpTimestamp::new(0), 0)
        .expect("createStream reply must encode");
    serializer
        .serialize(&payload, false, false)
        .expect("createStream reply must serialize")
        .to_vec()
}

pub fn fake_publish_success(serializer: &mut ChunkEncoder, stream_id: u32) -> Vec<u8> {
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
        .into_raw_message(RtmpTimestamp::new(0), stream_id)
        .expect("publish reply must encode");
    serializer
        .serialize(&payload, false, false)
        .expect("publish reply must serialize")
        .to_vec()
}

/// Drive a client to Publishing on the given stream id (chosen to match the
/// server under test so chunk stream ids line up end to end).
pub fn publishing_client(stream_id: u32, stream_key: &str) -> ClientSession {
    let (mut session, initial) =
        ClientSession::new(ClientSessionConfig::new()).expect("client session must start");
    let mut deserializer = ContiguousDecoder::new();
    let mut fake_server = ChunkEncoder::new();
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
        .request_publishing(stream_key.to_string(), PublishMode::Live)
        .expect("publish request must build");
    let mut responses = Vec::new();
    if let ClientSessionResult::Packet(packet) = request {
        let payload = deserializer
            .get_next_message(&packet.to_vec())
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
