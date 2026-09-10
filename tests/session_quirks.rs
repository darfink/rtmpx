// Session-level quirks the live legs can never hit (default suite).
//
// Three gaps the Red5/ffmpeg/OBS legs leave open, all cheap crafted-bytes
// tests in the obs_ingest style:
//
// 1. Extended timestamps: chunk_io unit tests prove the framing, but no
//    session test ever pushed RtmpTimestamp past 0xFFFFFF through ingest and
//    relay. Long streams cross that boundary after about 4.6 hours, so the
//    pump below publishes past it and relays past it, asserting the exact
//    value on both ends.
// 2. GStreamer deleteStream: its RTMP sink sends the stream id as an AMF
//    string instead of a number (see the note on
//    handle_command_delete_stream). Only a code comment pinned it; this
//    locks the wire shape.
// 3. Rejection shapes: connect refusals are _error (a transaction reply),
//    while publish/play refusals are onStatus at level error (stream
//    events). Upstream once sent _error for all three and encoders missed
//    the rejection. rtmpx never asserted the two shapes over the wire;
//    this does, including what our own client raises for each and that the
//    framing follows the negotiated exchange (AMF0 mirror vs type 17).

use bytes::Bytes;
use rtmpx::amf::AmfEncoding;
use rtmpx::amf0::{Amf0Object, Amf0Value};
use rtmpx::amf3::Amf3Value;
use rtmpx::chunk_io::{ChunkDeserializer, ChunkSerializer};
use rtmpx::messages::RtmpMessage;
use rtmpx::sessions::{
    ClientSession, ClientSessionConfig, ClientSessionEvent, ClientSessionResult,
    PublishRequestType, ServerSession, ServerSessionConfig, ServerSessionEvent,
    ServerSessionResult,
};
use rtmpx::time::RtmpTimestamp;

// ---------------------------------------------------------------------------
// Shared pump: two sessions wired back to back, byte-exact in-process.
// ---------------------------------------------------------------------------

struct Pump {
    client: ClientSession,
    server: ServerSession,
    client_events: Vec<ClientSessionEvent>,
    server_events: Vec<ServerSessionEvent>,
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

fn connect(pump: &mut Pump, app: &str) {
    let out = pump
        .client
        .request_connection(app.to_string())
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
    assert!(
        pump.take_client_events()
            .iter()
            .any(|event| matches!(event, ClientSessionEvent::ConnectionRequestAccepted { .. })),
        "client must see ConnectionRequestAccepted"
    );
}

fn publish(pump: &mut Pump, stream_key: &str) {
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

// Drive a second pump to Playing so relayed media can be pushed through
// ServerSession::send_video_data and observed on the player. Returns the
// server-side stream id to send on.
fn play(pump: &mut Pump, stream_key: &str) -> rtmpx::sessions::StreamId {
    let out = pump
        .client
        .request_playback(stream_key.to_string())
        .expect("play must build");
    pump.push_client(vec![out]);
    let (request_id, stream_id) = pump
        .take_server_events()
        .into_iter()
        .find_map(|event| match event {
            ServerSessionEvent::PlayStreamRequested {
                request_id,
                stream_id,
                ..
            } => Some((request_id, stream_id)),
            _ => None,
        })
        .expect("play must raise PlayStreamRequested");
    let out = pump
        .server
        .accept_request(request_id)
        .expect("accept play must work");
    pump.push_server(out);
    assert!(
        pump.take_client_events()
            .iter()
            .any(|event| matches!(event, ClientSessionEvent::PlaybackRequestAccepted { .. })),
        "client must see Play.Start"
    );
    stream_id
}

// Decode every message in a captured server-to-client byte stream, honouring
// in-band chunk size changes the way a real peer would. A rejection reuses
// the session's existing chunk stream, so it can only be decoded with the
// full history, not a fresh deserializer.
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

fn status_object_of(args: &[Amf0Value]) -> &Amf0Object {
    match args.first() {
        Some(Amf0Value::Object(props)) => props,
        other => panic!("rejection must carry a status object, got {other:?}"),
    }
}

fn amf0_commands<'a>(messages: &'a [RtmpMessage], name: &str) -> Vec<&'a RtmpMessage> {
    messages
        .iter()
        .filter(
            |m| matches!(m, RtmpMessage::Amf0Command { command_name, .. } if command_name == name),
        )
        .collect()
}

// ---------------------------------------------------------------------------
// 1. Extended timestamps survive ingest and relay.
// ---------------------------------------------------------------------------

// 0xFFFFFF is about 4.66 hours of milliseconds; long streams cross it live,
// and neither Red5 nor short ffmpeg runs ever get there. The chunk framing
// is unit-tested, but this is the only test that carries such timestamps
// through both session directions end to end.
#[test]
fn extended_timestamps_survive_ingest_and_relay() {
    const JUST_OVER: u32 = 16_777_215 + 100;
    const LARGE: u32 = 100_000_000;

    // Ingest leg: publisher -> server 1.
    let mut ingest = Pump::new(ClientSessionConfig::new(), ServerSessionConfig::new());
    connect(&mut ingest, "live");
    publish(&mut ingest, "long-stream");
    let frame = Bytes::from(vec![0x17, 0x01, 0x02, 0x03]);
    for ts in [JUST_OVER, LARGE] {
        let out = ingest
            .client
            .publish_video_data(frame.clone(), RtmpTimestamp::new(ts), false)
            .expect("video past 0xFFFFFF must build");
        ingest.push_client(vec![out]);
    }
    let ingested: Vec<(Bytes, RtmpTimestamp)> = ingest
        .take_server_events()
        .into_iter()
        .filter_map(|event| match event {
            ServerSessionEvent::VideoDataReceived {
                data, timestamp, ..
            } => Some((data, timestamp)),
            _ => None,
        })
        .collect();
    assert_eq!(
        ingested,
        vec![
            (frame.clone(), RtmpTimestamp::new(JUST_OVER)),
            (frame.clone(), RtmpTimestamp::new(LARGE)),
        ],
        "server must see exact extended timestamps, not truncated 24-bit values"
    );

    // Relay leg: server 2 -> player, forwarded with the same timestamps, the
    // way a proxy forwards without touching payloads.
    let mut playout = Pump::new(ClientSessionConfig::new(), ServerSessionConfig::new());
    connect(&mut playout, "live");
    let play_stream_id = play(&mut playout, "long-stream");
    for (data, timestamp) in &ingested {
        let packet = playout
            .server
            .send_video_data(play_stream_id, data.clone(), *timestamp, false)
            .expect("relay send with extended timestamp must build");
        playout.push_server(vec![ServerSessionResult::OutboundResponse(packet)]);
    }
    let played: Vec<(Bytes, RtmpTimestamp)> = playout
        .take_client_events()
        .into_iter()
        .filter_map(|event| match event {
            ClientSessionEvent::VideoDataReceived {
                data, timestamp, ..
            } => Some((data, timestamp)),
            _ => None,
        })
        .collect();
    assert_eq!(
        played,
        vec![
            (frame.clone(), RtmpTimestamp::new(JUST_OVER)),
            (frame, RtmpTimestamp::new(LARGE)),
        ],
        "player must see exact extended timestamps after relay"
    );
}

// ---------------------------------------------------------------------------
// 2. GStreamer string deleteStream.
// ---------------------------------------------------------------------------

fn collect_outbound(
    deserializer: &mut ChunkDeserializer,
    results: Vec<ServerSessionResult>,
) -> Vec<RtmpMessage> {
    let mut messages = Vec::new();
    for result in results {
        if let ServerSessionResult::OutboundResponse(packet) = result {
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
            messages.push(message);
        }
    }
    messages
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
        .expect("server must accept crafted bytes");
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
    timestamp: u32,
    first_on_stream: bool,
) -> (Vec<RtmpMessage>, Vec<ServerSessionEvent>) {
    let payload = message
        .into_message_payload(RtmpTimestamp::new(timestamp), stream_id)
        .expect("message must encode");
    let packet = serializer
        .serialize(&payload, first_on_stream, false)
        .expect("must serialize");
    feed_server(session, deserializer, &packet.bytes)
}

fn amf0_command(name: &str, tid: f64, args: Vec<Amf0Value>) -> RtmpMessage {
    RtmpMessage::Amf0Command {
        command_name: name.to_string(),
        transaction_id: tid,
        command_object: Amf0Value::Null,
        additional_arguments: args,
    }
}

fn amf3_play_command(key: &str) -> RtmpMessage {
    // A genuine type-17 play: 0x03 selector with a real AMF3 body, the shape
    // a Flash-style AMF3 player sends. Our own client only ever sends type
    // 20, so the pump tests cannot produce this framing.
    RtmpMessage::Amf3Command {
        command_name: "play".to_string(),
        transaction_id: 0.0,
        command_object: Amf3Value::Null,
        additional_arguments: vec![Amf3Value::String(key.to_string())],
        format: AmfEncoding::Amf3,
    }
}

fn minimal_connect_message(app: &str, object_encoding: f64) -> RtmpMessage {
    let mut properties = Amf0Object::new();
    properties.insert("app".to_string(), Amf0Value::Utf8String(app.to_string()));
    properties.insert(
        "objectEncoding".to_string(),
        Amf0Value::Number(object_encoding),
    );
    RtmpMessage::Amf0Command {
        command_name: "connect".to_string(),
        transaction_id: 1.0,
        command_object: Amf0Value::Object(properties),
        additional_arguments: Vec::new(),
    }
}

fn connected_server(
    deserializer: &mut ChunkDeserializer,
    object_encoding: f64,
) -> (ServerSession, ChunkSerializer) {
    let (mut session, initial) =
        ServerSession::new(ServerSessionConfig::new()).expect("server must start");
    let mut serializer = ChunkSerializer::new();
    collect_outbound(deserializer, initial);
    let (_, events) = send_to_server(
        &mut session,
        &mut serializer,
        deserializer,
        minimal_connect_message("live", object_encoding),
        0,
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
    let results = session
        .accept_request(request_id)
        .expect("accept connect must work");
    collect_outbound(deserializer, results);
    (session, serializer)
}

fn create_stream(
    session: &mut ServerSession,
    serializer: &mut ChunkSerializer,
    deserializer: &mut ChunkDeserializer,
    tid: f64,
) -> u32 {
    let (responses, _) = send_to_server(
        session,
        serializer,
        deserializer,
        amf0_command("createStream", tid, vec![]),
        0,
        0,
        false,
    );
    match responses.first().expect("createStream needs a reply") {
        RtmpMessage::Amf0Command {
            command_name,
            additional_arguments,
            ..
        } if command_name == "_result" => match additional_arguments.first() {
            Some(Amf0Value::Number(id)) => *id as u32,
            other => panic!("createStream reply must carry stream id, got {other:?}"),
        },
        other => panic!("createStream reply must be _result, got {other:?}"),
    }
}

fn create_stream_and_publish(
    session: &mut ServerSession,
    serializer: &mut ChunkSerializer,
    deserializer: &mut ChunkDeserializer,
    key: &str,
) -> u32 {
    let stream_id = create_stream(session, serializer, deserializer, 2.0);
    let (_, events) = send_to_server(
        session,
        serializer,
        deserializer,
        amf0_command(
            "publish",
            3.0,
            vec![
                Amf0Value::Utf8String(key.to_string()),
                Amf0Value::Utf8String("live".to_string()),
            ],
        ),
        stream_id,
        0,
        false,
    );
    let request_id = events
        .iter()
        .find_map(|e| match e {
            ServerSessionEvent::PublishStreamRequested {
                request_id,
                stream_key,
                ..
            } if stream_key.as_ref() == key => Some(*request_id),
            _ => None,
        })
        .expect("publish must raise PublishStreamRequested");
    let results = session
        .accept_request(request_id)
        .expect("accept publish must work");
    collect_outbound(deserializer, results);
    stream_id
}

// GStreamer's RTMP sink sends deleteStream with the stream id as a decimal
// string, not a number. The server must accept that spelling and finish the
// publish exactly as for the numeric form.
#[test]
fn string_delete_stream_finishes_publish() {
    let mut deserializer = ChunkDeserializer::new();
    let (mut session, mut serializer) = connected_server(&mut deserializer, 0.0);
    let key = "gstreamer-quirk-key";
    let stream_id =
        create_stream_and_publish(&mut session, &mut serializer, &mut deserializer, key);
    let (_, events) = send_to_server(
        &mut session,
        &mut serializer,
        &mut deserializer,
        amf0_command(
            "deleteStream",
            0.0,
            vec![Amf0Value::Utf8String(stream_id.to_string())],
        ),
        0,
        0,
        false,
    );
    assert!(
        events.iter().any(
            |e| matches!(e, ServerSessionEvent::PublishStreamFinished { stream_key, .. } if stream_key.as_ref() == key)
        ),
        "string deleteStream must finish the publish, saw {events:?}"
    );
}

// A non-numeric deleteStream argument must be ignored harmlessly: no event,
// no error, and the session must still be usable afterwards.
#[test]
fn garbage_delete_stream_is_ignored() {
    let mut deserializer = ChunkDeserializer::new();
    let (mut session, mut serializer) = connected_server(&mut deserializer, 0.0);
    let (_, events) = send_to_server(
        &mut session,
        &mut serializer,
        &mut deserializer,
        amf0_command(
            "deleteStream",
            0.0,
            vec![Amf0Value::Utf8String("not-a-stream".to_string())],
        ),
        0,
        0,
        false,
    );
    assert!(
        events.is_empty(),
        "garbage deleteStream must raise no events, saw {events:?}"
    );
    let (responses, _) = send_to_server(
        &mut session,
        &mut serializer,
        &mut deserializer,
        amf0_command("createStream", 2.0, vec![]),
        0,
        0,
        false,
    );
    assert!(
        matches!(responses.first(), Some(RtmpMessage::Amf0Command { command_name, .. }) if command_name == "_result"),
        "session must still answer createStream, saw {responses:?}"
    );
}

// ---------------------------------------------------------------------------
// 3. NetConnection vs NetStream rejection shapes.
// ---------------------------------------------------------------------------

// connect is a request/response transaction, so refusing it must be an
// _error carrying the connect's transaction id -- and our own client must
// surface it as ConnectionRequestRejected.
#[test]
fn connect_rejection_is_error_reply() {
    let mut pump = Pump::new(ClientSessionConfig::new(), ServerSessionConfig::new());
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
    let results = pump
        .server
        .reject_request(request_id, "NetConnection.Connect.Rejected", "no entry")
        .expect("reject must work");
    pump.push_server(results);
    let messages = decode_all(&pump.server_to_client);
    let errors = amf0_commands(&messages, "_error");
    assert_eq!(
        errors.len(),
        1,
        "connect rejection must be exactly one _error, saw {messages:?}"
    );
    match errors[0] {
        RtmpMessage::Amf0Command {
            transaction_id,
            additional_arguments,
            ..
        } => {
            assert_eq!(*transaction_id, 1.0, "_error must echo the connect tid");
            let status = status_object_of(additional_arguments);
            assert_eq!(
                status.get("code"),
                Some(&Amf0Value::Utf8String(
                    "NetConnection.Connect.Rejected".to_string()
                )),
                "status code must survive, saw {status:?}"
            );
        }
        other => panic!("connect refusal must be an AMF0 command, got {other:?}"),
    }
    assert!(
        pump.take_client_events().iter().any(|event| matches!(
            event,
            ClientSessionEvent::ConnectionRequestRejected { description, .. } if description == "no entry"
        )),
        "client must surface the connect rejection"
    );
}

// publish is answered with stream events, so refusing it must be an onStatus
// at level error -- never _error, which encoders waiting for
// NetStream.Publish.* would miss.
#[test]
fn publish_rejection_is_onstatus_error() {
    let mut pump = Pump::new(ClientSessionConfig::new(), ServerSessionConfig::new());
    connect(&mut pump, "live");
    let out = pump
        .client
        .request_publishing("denied-key".to_string(), PublishRequestType::Live)
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
    let results = pump
        .server
        .reject_request(request_id, "NetStream.Publish.Denied", "taken")
        .expect("reject must work");
    pump.push_server(results);
    let messages = decode_all(&pump.server_to_client);
    assert!(
        amf0_commands(&messages, "_error").is_empty(),
        "publish refusal must not be _error, saw {messages:?}"
    );
    let refusals = amf0_commands(&messages, "onStatus");
    assert_eq!(
        refusals.len(),
        1,
        "publish rejection must be exactly one onStatus, saw {messages:?}"
    );
    match refusals[0] {
        RtmpMessage::Amf0Command {
            transaction_id,
            additional_arguments,
            ..
        } => {
            assert_eq!(*transaction_id, 0.0, "onStatus carries tid 0");
            let status = status_object_of(additional_arguments);
            assert_eq!(
                status.get("level"),
                Some(&Amf0Value::Utf8String("error".to_string())),
                "publish refusal level must be error, saw {status:?}"
            );
            assert_eq!(
                status.get("code"),
                Some(&Amf0Value::Utf8String(
                    "NetStream.Publish.Denied".to_string()
                )),
                "publish refusal code must survive, saw {status:?}"
            );
        }
        other => panic!("publish refusal must be an AMF0 command, got {other:?}"),
    }
    assert!(
        pump.take_client_events().iter().any(|event| matches!(
            event,
            ClientSessionEvent::PublishRequestRejected { status, .. } if status.code() == Some("NetStream.Publish.Denied")
        )),
        "client must surface the publish refusal code"
    );
}

// Same contract as publish: play refusals are onStatus errors, and the
// client surfaces the code.
#[test]
fn play_rejection_is_onstatus_error() {
    let mut pump = Pump::new(ClientSessionConfig::new(), ServerSessionConfig::new());
    connect(&mut pump, "live");
    let out = pump
        .client
        .request_playback("missing-key".to_string())
        .expect("play must build");
    pump.push_client(vec![out]);
    let request_id = pump
        .take_server_events()
        .into_iter()
        .find_map(|event| match event {
            ServerSessionEvent::PlayStreamRequested { request_id, .. } => Some(request_id),
            _ => None,
        })
        .expect("play must raise PlayStreamRequested");
    let results = pump
        .server
        .reject_request(request_id, "NetStream.Play.Failed", "no such stream")
        .expect("reject must work");
    pump.push_server(results);
    let messages = decode_all(&pump.server_to_client);
    assert!(
        amf0_commands(&messages, "_error").is_empty(),
        "play refusal must not be _error, saw {messages:?}"
    );
    let refusals = amf0_commands(&messages, "onStatus");
    assert_eq!(
        refusals.len(),
        1,
        "play rejection must be exactly one onStatus, saw {messages:?}"
    );
    match refusals[0] {
        RtmpMessage::Amf0Command {
            transaction_id,
            additional_arguments,
            ..
        } => {
            assert_eq!(*transaction_id, 0.0, "onStatus carries tid 0");
            let status = status_object_of(additional_arguments);
            assert_eq!(
                status.get("level"),
                Some(&Amf0Value::Utf8String("error".to_string())),
                "play refusal level must be error, saw {status:?}"
            );
            assert_eq!(
                status.get("code"),
                Some(&Amf0Value::Utf8String("NetStream.Play.Failed".to_string())),
                "play refusal code must survive, saw {status:?}"
            );
        }
        other => panic!("play refusal must be an AMF0 command, got {other:?}"),
    }
    assert!(
        pump.take_client_events().iter().any(|event| matches!(
            event,
            ClientSessionEvent::PlaybackRequestRejected { status, .. } if status.code() == Some("NetStream.Play.Failed")
        )),
        "client must surface the play refusal code"
    );
}

fn amf3_status_code(args: &[Amf3Value]) -> (String, String) {
    let converted: Vec<Amf0Value> = args.iter().map(|v| v.to_amf0()).collect();
    let status = status_object_of(&converted);
    let level = match status.get("level") {
        Some(Amf0Value::Utf8String(level)) => level.clone(),
        other => panic!("AMF3 status must carry a level, saw {other:?}"),
    };
    let code = match status.get("code") {
        Some(Amf0Value::Utf8String(code)) => code.clone(),
        other => panic!("AMF3 status must carry a code, saw {other:?}"),
    };
    (level, code)
}

// The refusal rides the framing of the exchange it answers: a type-17 play
// gets a type-17 onStatus, exactly like the accept path's Play.Start. Our
// own client only ever sends type 20, so this needs a crafted type-17 play
// (the pump tests above cover the mirrored-AMF0 case).
#[test]
fn amf3_framed_play_rejection_is_amf3_command() {
    let mut deserializer = ChunkDeserializer::new();
    // objectEncoding 3 so the session negotiates AMF3.
    let (mut session, mut serializer) = connected_server(&mut deserializer, 3.0);
    assert_eq!(
        session.negotiated_encoding(),
        AmfEncoding::Amf3,
        "precondition: session must have negotiated AMF3"
    );
    // Accept leg: type-17 play answered with Play.Start. Its framing is the
    // reference the refusal must match.
    let accepted_stream = create_stream(&mut session, &mut serializer, &mut deserializer, 2.0);
    let (_, events) = send_to_server(
        &mut session,
        &mut serializer,
        &mut deserializer,
        amf3_play_command("present-key"),
        accepted_stream,
        0,
        false,
    );
    let request_id = events
        .iter()
        .find_map(|e| match e {
            ServerSessionEvent::PlayStreamRequested { request_id, .. } => Some(*request_id),
            _ => None,
        })
        .expect("type-17 play must raise PlayStreamRequested");
    let results = session
        .accept_request(request_id)
        .expect("accept play must work");
    let accepted_messages = collect_outbound(&mut deserializer, results);
    let accepted_start = accepted_messages
        .iter()
        .filter_map(|m| match m {
            RtmpMessage::Amf3Command {
                command_name,
                additional_arguments,
                ..
            } if command_name == "onStatus" => Some(additional_arguments),
            _ => None,
        })
        .find(|args| amf3_status_code(args).1 == "NetStream.Play.Start")
        .expect("accept leg must send Play.Start as an AMF3 command");
    let _ = accepted_start;
    // Reject leg: same type-17 framing, refused.
    let refused_stream = create_stream(&mut session, &mut serializer, &mut deserializer, 4.0);
    let (_, events) = send_to_server(
        &mut session,
        &mut serializer,
        &mut deserializer,
        amf3_play_command("missing-key"),
        refused_stream,
        0,
        false,
    );
    let request_id = events
        .iter()
        .find_map(|e| match e {
            ServerSessionEvent::PlayStreamRequested { request_id, .. } => Some(*request_id),
            _ => None,
        })
        .expect("type-17 play must raise PlayStreamRequested");
    let results = session
        .reject_request(request_id, "NetStream.Play.Failed", "no such stream")
        .expect("reject must work");
    let refused_messages = collect_outbound(&mut deserializer, results);
    assert_eq!(
        refused_messages.len(),
        1,
        "play rejection must be exactly one message, saw {refused_messages:?}"
    );
    match &refused_messages[0] {
        RtmpMessage::Amf3Command {
            command_name,
            transaction_id,
            additional_arguments,
            ..
        } => {
            assert_eq!(
                command_name, "onStatus",
                "play refusal must be onStatus, not _error"
            );
            assert_eq!(*transaction_id, 0.0, "onStatus carries tid 0");
            let (level, code) = amf3_status_code(additional_arguments);
            assert_eq!(level, "error", "play refusal level must be error");
            assert_eq!(
                code, "NetStream.Play.Failed",
                "play refusal code must survive"
            );
        }
        other => panic!("type-17 play refusal must be an AMF3 command, saw {other:?}"),
    }
}

#[test]
fn requests_are_exclusive_while_pending_and_cancellation_releases_created_streams() {
    use rtmpx::sessions::ClientState;
    for publishing in [false, true] {
        let mut pump = Pump::new(
            ClientSessionConfig::default(),
            ServerSessionConfig::default(),
        );
        let connect_result = pump.client.request_connection("live".into()).unwrap();
        assert_eq!(pump.client.state(), &ClientState::ConnectionRequested);
        assert!(pump.client.request_connection("again".into()).is_err());
        assert!(!pump.client.is_failed());
        pump.push_client(vec![connect_result]);
        let id = pump
            .take_server_events()
            .into_iter()
            .find_map(|e| match e {
                ServerSessionEvent::ConnectionRequested { request_id, .. } => Some(request_id),
                _ => None,
            })
            .unwrap();
        let accepted = pump.server.accept_request(id).unwrap();
        assert!(pump.server.accept_request(id).is_err());
        assert!(!pump.server.is_failed());
        pump.push_server(accepted);
        pump.take_client_events();

        let request = if publishing {
            pump.client
                .request_publishing("demo".into(), PublishRequestType::Live)
                .unwrap()
        } else {
            pump.client.request_playback("demo".into()).unwrap()
        };
        assert_eq!(
            pump.client.state(),
            if publishing {
                &ClientState::CreatingPublishStream
            } else {
                &ClientState::CreatingPlayStream
            }
        );
        assert!(pump.client.request_playback("overlap".into()).is_err());
        assert!(
            pump.client
                .request_publishing("overlap".into(), PublishRequestType::Live)
                .is_err()
        );
        let cancel = if publishing {
            pump.client.stop_publishing()
        } else {
            pump.client.stop_playback()
        }
        .unwrap();
        assert!(cancel.is_empty());
        assert_eq!(
            pump.client.state(),
            if publishing {
                &ClientState::CancellingPublish
            } else {
                &ClientState::CancellingPlay
            }
        );
        assert!(pump.client.request_playback("too-soon".into()).is_err());
        // Deliver the request after cancellation. Its response must delete the newly
        // allocated stream and must never issue a play/publish command.
        pump.push_client(vec![request]);
        assert_eq!(pump.client.state(), &ClientState::Connected);
        assert_eq!(pump.client.active_stream_id(), None);
        assert!(pump.take_server_events().is_empty());
        assert!(pump.take_client_events().is_empty());
        publish(&mut pump, "next");
        assert_eq!(pump.client.state(), &ClientState::Publishing);
    }
}

#[test]
fn script_messages_relay_both_directions_without_changing_wire_type_or_bytes() {
    use rtmpx::sessions::{DataMessage, DataMessageType};
    let mut ingest = Pump::new(
        ClientSessionConfig::default(),
        ServerSessionConfig::default(),
    );
    connect(&mut ingest, "live");
    publish(&mut ingest, "demo");
    let source_id = ingest.client.active_stream_id().unwrap();
    let mut playback = Pump::new(
        ClientSessionConfig::default(),
        ServerSessionConfig::default(),
    );
    connect(&mut playback, "live");
    let destination_id = play(&mut playback, "demo");
    for metadata in [false, true] {
        let values = vec![
            Amf0Value::Utf8String(if metadata { "onMetaData" } else { "onCaption" }.into()),
            Amf0Value::Object(Amf0Object::from([(
                "vendor".into(),
                Amf0Value::Utf8String("retained".into()),
            )])),
        ];
        let amf0 = rtmpx::amf0::serialize(&values).unwrap();
        let amf3 =
            rtmpx::amf3::serialize(&values.iter().map(Amf0Value::to_amf3).collect::<Vec<_>>())
                .unwrap();
        let mut wrapped0 = vec![0];
        wrapped0.extend_from_slice(&amf0);
        let mut wrapped3 = vec![3];
        wrapped3.extend_from_slice(&amf3);
        for (wire_type, bytes) in [
            (DataMessageType::Amf0, amf0),
            (DataMessageType::Amf3, wrapped0),
            (DataMessageType::Amf3, wrapped3),
            // Deliberately mistyped bare AMF3: interpretation must not change type 18.
            (DataMessageType::Amf0, amf3),
            (DataMessageType::Amf0, vec![0xff, 0xfe]),
        ] {
            let original = DataMessage::new(
                wire_type,
                RtmpTimestamp::new(0xffff_fffe),
                Bytes::from(bytes),
            );
            let sent = ingest.client.publish_data(original.clone()).unwrap();
            ingest.push_client(vec![sent]);
            let received = ingest
                .take_server_events()
                .into_iter()
                .find_map(|e| match e {
                    ServerSessionEvent::StreamMetadataChanged {
                        stream_id, message, ..
                    }
                    | ServerSessionEvent::StreamDataReceived {
                        stream_id, message, ..
                    } => {
                        assert_eq!(stream_id, source_id);
                        Some(message)
                    }
                    _ => None,
                })
                .unwrap();
            assert_eq!(received, original);
            let packet = playback.server.send_data(destination_id, received).unwrap();
            playback.push_server(vec![ServerSessionResult::OutboundResponse(packet)]);
            let received = playback
                .take_client_events()
                .into_iter()
                .find_map(|e| match e {
                    ClientSessionEvent::StreamMetadataReceived { message, .. }
                    | ClientSessionEvent::StreamDataReceived { message, .. } => Some(message),
                    _ => None,
                })
                .unwrap();
            assert_eq!(received, original);
            // Republish the client-observed message back into a server session.
            let sent = ingest.client.publish_data(received).unwrap();
            ingest.push_client(vec![sent]);
            assert!(ingest.take_server_events().into_iter().any(|e| match e {
                ServerSessionEvent::StreamMetadataChanged { message, .. }
                | ServerSessionEvent::StreamDataReceived { message, .. } => message == original,
                _ => false,
            }));
        }
    }
}

#[test]
fn rejection_and_completion_preserve_status_and_allow_another_request() {
    use rtmpx::sessions::ClientState;
    let mut pump = Pump::new(
        ClientSessionConfig::default(),
        ServerSessionConfig::default(),
    );
    connect(&mut pump, "live");
    for publishing in [false, true] {
        let request = if publishing {
            pump.client
                .request_publishing("denied".into(), PublishRequestType::Live)
                .unwrap()
        } else {
            pump.client.request_playback("denied".into()).unwrap()
        };
        pump.push_client(vec![request]);
        let (id, stream_id) = pump
            .take_server_events()
            .into_iter()
            .find_map(|e| match e {
                ServerSessionEvent::PlayStreamRequested {
                    request_id,
                    stream_id,
                    ..
                }
                | ServerSessionEvent::PublishStreamRequested {
                    request_id,
                    stream_id,
                    ..
                } => Some((request_id, stream_id)),
                _ => None,
            })
            .unwrap();
        // A vendor code is still a rejection when level=error.
        let rejected = pump
            .server
            .reject_request(id, "Vendor.PermissionDenied", "Access requires a token")
            .unwrap();
        pump.push_server(rejected);
        assert!(pump.take_client_events().into_iter().any(|e| match e {
            ClientSessionEvent::PlaybackRequestRejected { status, .. }
            | ClientSessionEvent::PublishRequestRejected { status, .. } => {
                status.code() == Some("Vendor.PermissionDenied")
                    && status.description() == Some("Access requires a token")
                    && status.stream_id() == Some(stream_id)
                    && status.properties().get("level")
                        == Some(&Amf0Value::Utf8String("error".into()))
            }
            _ => false,
        }));
        assert_eq!(pump.client.state(), &ClientState::Connected);
        assert_eq!(pump.client.active_stream_id(), None);
    }
    let stream_id = play(&mut pump, "working");
    let finished = pump.server.finish_playing(stream_id).unwrap();
    pump.push_server(vec![ServerSessionResult::OutboundResponse(finished)]);
    assert!(pump.take_client_events().into_iter().any(|e| matches!(e,
        ClientSessionEvent::PlaybackFinished { status, .. } if status.code() == Some("NetStream.Play.Complete"))));
    assert_eq!(pump.client.state(), &ClientState::Connected);
    publish(&mut pump, "after-completion");
}
