//! AMF3 publish path into our own ServerSession (default suite).
//!
//! The Red5 legs prove our AMF3 client against an independent server, and
//! the ffmpeg leg proves our server against an independent (AMF0-only)
//! encoder. No third-party encoder publishes AMF3, so the only way to exercise
//! our server AMF3 decode path - a type-17 connect, type-15 script data,
//! AMF3-framed answers - is to drive it with our own AMF3 client. That is what
//! this file does: two sans-I/O sessions shuttling bytes in-process, no
//! network, no binaries.

use bytes::Bytes;
use rtmpx::amf::AmfEncoding;
use rtmpx::amf0::Amf0Value;
use rtmpx::amf3::{self, Amf3Value};
use rtmpx::chunk_io::ChunkDeserializer;
use rtmpx::messages::RtmpMessage;
use rtmpx::sessions::{
    ClientSession, ClientSessionConfig, ClientSessionEvent, ClientSessionResult,
    PublishRequestType, ServerSession, ServerSessionConfig, ServerSessionEvent,
    ServerSessionResult, StreamMetadata,
};
use rtmpx::time::RtmpTimestamp;

/// Two sessions wired back to back. Every outbound packet from one side is fed
/// straight into the other until neither side has anything left to say, so
/// each push leaves the pair quiescent. Raw bytes are kept per direction
/// for wire-framing assertions.
struct Pump {
    client: ClientSession,
    server: ServerSession,
    client_events: Vec<ClientSessionEvent>,
    server_events: Vec<ServerSessionEvent>,
    client_to_server: Vec<u8>,
    server_to_client: Vec<u8>,
}

impl Pump {
    fn new(client_config: ClientSessionConfig, server_config: ServerSessionConfig) -> Self {
        let (client, client_initial) =
            ClientSession::new(client_config).expect("client session must start");
        let (server, server_initial) =
            ServerSession::new(server_config).expect("server session must start");
        let mut pump = Self {
            client,
            server,
            client_events: Vec::new(),
            server_events: Vec::new(),
            client_to_server: Vec::new(),
            server_to_client: Vec::new(),
        };
        pump.push_client(client_initial);
        pump.push_server(server_initial);
        pump
    }

    fn push_client(&mut self, results: Vec<ClientSessionResult>) {
        let mut bytes = Vec::new();
        for result in results {
            match result {
                ClientSessionResult::OutboundResponse(packet) => {
                    bytes.extend_from_slice(&packet.bytes);
                }
                ClientSessionResult::RaisedEvent(event) => self.client_events.push(event),
                ClientSessionResult::UnhandleableMessageReceived(_) => {}
                #[allow(unreachable_patterns)]
                _ => panic!("unexpected future protocol variant"),
            }
        }
        if bytes.is_empty() {
            return;
        }
        self.client_to_server.extend_from_slice(&bytes);
        let out = self
            .server
            .handle_input(&bytes)
            .expect("server must accept well-formed client bytes");
        self.push_server(out);
    }

    fn push_server(&mut self, results: Vec<ServerSessionResult>) {
        let mut bytes = Vec::new();
        for result in results {
            match result {
                ServerSessionResult::OutboundResponse(packet) => {
                    bytes.extend_from_slice(&packet.bytes);
                }
                ServerSessionResult::RaisedEvent(event) => self.server_events.push(event),
                ServerSessionResult::UnhandleableMessageReceived(_) => {}
                #[allow(unreachable_patterns)]
                _ => panic!("unexpected future protocol variant"),
            }
        }
        if bytes.is_empty() {
            return;
        }
        self.server_to_client.extend_from_slice(&bytes);
        let out = self
            .client
            .handle_input(&bytes)
            .expect("client must accept well-formed server bytes");
        self.push_client(out);
    }

    fn take_server_events(&mut self) -> Vec<ServerSessionEvent> {
        std::mem::take(&mut self.server_events)
    }

    fn take_client_events(&mut self) -> Vec<ClientSessionEvent> {
        std::mem::take(&mut self.client_events)
    }
}

/// Decode every message in a captured byte stream, honouring in-band chunk
/// size changes the way a real peer would.
fn decode_all(raw: &[u8]) -> Vec<RtmpMessage> {
    let mut deserializer = ChunkDeserializer::new();
    let mut messages = Vec::new();
    let mut first = true;
    loop {
        let next = if first {
            first = false;
            deserializer
                .get_next_message(raw)
                .expect("captured bytes must decode")
        } else {
            deserializer
                .get_next_message(&[])
                .expect("buffered bytes must decode")
        };
        match next {
            None => break,
            Some(payload) => {
                let message = payload
                    .to_rtmp_message()
                    .expect("captured payload must parse");
                if let RtmpMessage::SetChunkSize { size } = &message {
                    deserializer
                        .set_max_chunk_size(*size as usize)
                        .expect("chunk size must apply");
                }
                messages.push(message);
            }
        }
    }
    messages
}

fn amf3_client_config() -> ClientSessionConfig {
    let mut config = ClientSessionConfig::new();
    config.object_encoding = AmfEncoding::Amf3;
    config.tc_url = Some("rtmp://127.0.0.1/live".to_string());
    config
}

fn connect_and_publish(pump: &mut Pump, stream_key: &str) {
    let out = pump
        .client
        .request_connection("live".to_string())
        .expect("connect must build");
    pump.push_client(vec![out]);

    let request_id = pump
        .take_server_events()
        .into_iter()
        .find_map(|event| match event {
            ServerSessionEvent::ConnectionRequested { request_id, .. } => Some(request_id),
            _ => None,
        })
        .expect("connect must raise ConnectionRequested");
    let out = pump
        .server
        .accept_request(request_id)
        .expect("accept must work");
    pump.push_server(out);
    pump.take_client_events();

    let out = pump
        .client
        .request_publishing(stream_key.to_string(), PublishRequestType::Live)
        .expect("publish must build");
    pump.push_client(vec![out]);
    let request_id = pump
        .take_server_events()
        .into_iter()
        .find_map(|event| match event {
            ServerSessionEvent::PublishStreamRequested { request_id, .. } => Some(request_id),
            _ => None,
        })
        .expect("publish must raise PublishStreamRequested");
    let out = pump
        .server
        .accept_request(request_id)
        .expect("accept must work");
    pump.push_server(out);
    assert!(
        pump.take_client_events()
            .iter()
            .any(|event| matches!(event, ClientSessionEvent::PublishRequestAccepted { .. })),
        "client must see Publish.Start"
    );
}

/// A type-15 body the publisher sends and the server must surface with its
/// marker intact. Mirrors the Red5 harness probe (tests/red5/fixtures.rs).
fn amf3_probe_body() -> Bytes {
    let values = vec![
        Amf3Value::String("@setDataFrame".to_string()),
        Amf3Value::String("onMetaData".to_string()),
        Amf3Value::dynamic_object(vec![("ccLoopbackProbe".to_string(), Amf3Value::Integer(7))]),
    ];
    let mut body = vec![0x03];
    let mut encoded = amf3::serialize(&values).expect("probe AMF3 must encode");
    body.append(&mut encoded);
    Bytes::from(body)
}

#[test]
fn amf3_client_publishes_into_our_server() {
    let mut pump = Pump::new(amf3_client_config(), ServerSessionConfig::new());
    connect_and_publish(&mut pump, "amf3-loopback");

    assert_eq!(
        pump.client.negotiated_encoding(),
        AmfEncoding::Amf3,
        "client must negotiate AMF3"
    );
    assert_eq!(
        pump.server.negotiated_encoding(),
        AmfEncoding::Amf3,
        "server must negotiate AMF3"
    );

    // RTMP peers mirror framing: `connect` goes out as type-20 AMF0 even
    // when `objectEncoding` 3 is negotiated (same as Red5, see
    // tests/red5/main.rs). AMF3 framing only appears once a side actually
    // sends type-15/17. So both directions must show an AMF0 `connect` /
    // `_result` here; the type-15 probe below is what exercises the AMF3 path.
    let server_wire = decode_all(&pump.server_to_client);
    assert!(
        server_wire.iter().any(|message| matches!(
            message,
            RtmpMessage::Amf0Command { command_name, .. } if command_name == "_result"
        )),
        "server must answer connect as Amf0Command _result, saw {server_wire:?}"
    );
    assert!(
        !server_wire
            .iter()
            .any(|message| matches!(message, RtmpMessage::Amf3Command { .. })),
        "no AMF3 framing is expected before either side sends type-15/17, saw {server_wire:?}"
    );
    let client_wire = decode_all(&pump.client_to_server);
    assert!(
        client_wire.iter().any(|message| matches!(
            message,
            RtmpMessage::Amf0Command { command_name, .. } if command_name == "connect"
        )),
        "client must send connect as Amf0Command, saw {client_wire:?}"
    );
    assert!(
        !client_wire
            .iter()
            .any(|message| matches!(message, RtmpMessage::Amf3Command { .. })),
        "client must not use AMF3 framing before the type-15 probe, saw {client_wire:?}"
    );

    // Legacy metadata still flows over the AMF3 connection as AMF0 script data.
    let mut metadata = StreamMetadata::new();
    metadata.video_width = Some(1280);
    metadata.video_height = Some(720);
    metadata.video_codec_id = Some(7);
    metadata.encoder = Some("rtmpx-amf3-loopback".to_string());
    let out = pump
        .client
        .publish_metadata(&metadata)
        .expect("metadata must build");
    pump.push_client(vec![out]);
    let events = pump.take_server_events();
    let meta = events
        .iter()
        .find_map(|event| match event {
            ServerSessionEvent::StreamMetadataChanged {
                metadata, message, ..
            } => Some((
                metadata.clone(),
                message.wire_type() == rtmpx::sessions::DataMessageType::Amf3,
            )),
            _ => None,
        })
        .expect("metadata must raise StreamMetadataChanged");
    assert!(!meta.1, "typed metadata must arrive as AMF0 even on AMF3");
    assert_eq!(meta.0.video_width, Some(1280));
    assert_eq!(meta.0.video_codec_id, Some(7));

    // Media is encoding-agnostic: byte-exact both ways.
    let video = Bytes::from(vec![0x17, 0x01, 0x00, 0x00, 0x05, 0x65, 0x88]);
    let audio = Bytes::from(vec![0xAF, 0x01, 0x21, 0x22]);
    let out = pump
        .client
        .publish_video_data(video.clone(), RtmpTimestamp::new(40), false)
        .expect("video must build");
    pump.push_client(vec![out]);
    let out = pump
        .client
        .publish_audio_data(audio.clone(), RtmpTimestamp::new(23), false)
        .expect("audio must build");
    pump.push_client(vec![out]);
    let events = pump.take_server_events();
    assert!(
        events.iter().any(|event| matches!(
            event,
            ServerSessionEvent::VideoDataReceived { data, .. } if data == &video
        )),
        "video must arrive byte-exact, saw {events:?}"
    );
    assert!(
        events.iter().any(|event| matches!(
            event,
            ServerSessionEvent::AudioDataReceived { data, .. } if data == &audio
        )),
        "audio must arrive byte-exact, saw {events:?}"
    );

    // Type-15 script data decodes through the AMF3 path with bytes intact.
    let probe = amf3_probe_body();
    let out = pump
        .client
        .publish_data(rtmpx::sessions::DataMessage::new(
            rtmpx::sessions::DataMessageType::Amf3,
            RtmpTimestamp::new(0),
            probe.clone(),
        ))
        .expect("amf3 data must build");
    pump.push_client(vec![out]);
    let events = pump.take_server_events();
    let probe_event = events
        .iter()
        .find_map(|event| match event {
            ServerSessionEvent::StreamMetadataChanged { message, .. } => Some((
                message.payload().clone(),
                message.wire_type() == rtmpx::sessions::DataMessageType::Amf3,
            )),
            _ => None,
        })
        .expect("amf3 setDataFrame must raise StreamMetadataChanged");
    assert!(probe_event.1, "type-15 data must be flagged AMF3");
    assert_eq!(
        probe_event.0, probe,
        "amf3 script bytes must survive verbatim"
    );
}

#[test]
fn amf3_connect_confirms_object_encoding_3() {
    // Pins the exact confirmation the Red5 legs also assert, so a server
    // change that stops confirming AMF3 fails here instead of in CI logs.
    let mut pump = Pump::new(amf3_client_config(), ServerSessionConfig::new());
    let out = pump
        .client
        .request_connection("live".to_string())
        .expect("connect must build");
    pump.push_client(vec![out]);
    let request_id = pump
        .take_server_events()
        .into_iter()
        .find_map(|event| match event {
            ServerSessionEvent::ConnectionRequested { request_id, .. } => Some(request_id),
            _ => None,
        })
        .expect("connect must raise ConnectionRequested");
    let out = pump
        .server
        .accept_request(request_id)
        .expect("accept must work");
    pump.push_server(out);
    let info = pump
        .take_client_events()
        .into_iter()
        .find_map(|event| match event {
            ClientSessionEvent::ConnectionRequestAccepted {
                additional_properties,
                ..
            } => Some(additional_properties),
            _ => None,
        })
        .expect("client must see ConnectionRequestAccepted");
    assert_eq!(
        info.get("objectEncoding").and_then(Amf0Value::get_number),
        Some(3.0),
        "server must confirm objectEncoding 3, got {info:?}"
    );
    assert_eq!(
        info.get("code").and_then(Amf0Value::get_string).as_deref(),
        Some("NetConnection.Connect.Success"),
        "connect must succeed, got {info:?}"
    );
}

#[test]
fn amf0_ceiling_clamps_amf3_client_to_amf0() {
    // A server that only advertises AMF0 must talk the client back down to
    // AMF0 instead of echoing an encoding it will not honour.
    let mut server_config = ServerSessionConfig::new();
    server_config.max_object_encoding = AmfEncoding::Amf0;
    let mut pump = Pump::new(amf3_client_config(), server_config);
    connect_and_publish(&mut pump, "clamped");

    assert_eq!(
        pump.client.negotiated_encoding(),
        AmfEncoding::Amf0,
        "client must fall back to AMF0"
    );
    assert_eq!(
        pump.server.negotiated_encoding(),
        AmfEncoding::Amf0,
        "server must fall back to AMF0"
    );
    let wire = decode_all(&pump.server_to_client);
    assert!(
        wire.iter().any(|message| matches!(
            message,
            RtmpMessage::Amf0Command { command_name, .. } if command_name == "_result"
        )),
        "clamped server must answer connect as Amf0Command, saw {wire:?}"
    );
}
