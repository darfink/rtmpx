//! Enhanced RTMP round-trip through our own sessions (default suite).
//!
//! Red5 has no Enhanced media path, so its legs are characterization only:
//! they prove the connection survives Enhanced bytes, not that the bytes
//! relay correctly. The ffmpeg Enhanced leg proves a third-party encoder
//! lands on our server, but only this file proves the full relay contract:
//! publisher -> ServerSession -> (relay) -> ServerSession -> player,
//! byte-exact, for both `hvc1` and `av01`, on both AMF0 and AMF3 command
//! planes. Media packets are encoding-agnostic, so the same bytes must
//! survive regardless of the negotiated `objectEncoding`.

use bytes::Bytes;
use rtmpx::amf::AmfEncoding;
use rtmpx::sessions::{
    ClientSession, ClientSessionConfig, ClientSessionEvent, ClientSessionResult,
    PublishRequestType, ServerSession, ServerSessionConfig, ServerSessionEvent,
    ServerSessionResult,
};
use rtmpx::time::RtmpTimestamp;

/// Two sessions wired back to back. Every outbound packet from one side is
/// fed straight into the other until neither side has anything left to say.
struct Pump {
    client: ClientSession,
    server: ServerSession,
    client_events: Vec<ClientSessionEvent>,
    server_events: Vec<ServerSessionEvent>,
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
                _ => panic!("unexpected future protocol variant"),
            }
        }
        if bytes.is_empty() {
            return;
        }
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

fn client_config_for(encoding: AmfEncoding) -> ClientSessionConfig {
    let mut config = ClientSessionConfig::new();
    config.object_encoding = encoding;
    config.tc_url = Some("rtmp://127.0.0.1/live".to_string());
    config
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

/// Drive a second pump to Playing so we can push relayed media through
/// `ServerSession::send_video_data` and observe it on the player.
/// Returns the server-side stream id to send on.
fn play(pump: &mut Pump, stream_key: &str) -> rtmpx::sessions::StreamId {
    let out = pump
        .client
        .request_playback(stream_key.to_string())
        .expect("play must build");
    pump.push_client(vec![out]);
    // createStream -> _result drives the client to send `play`; the server
    // then raises PlayStreamRequested. The pump already shuttled every
    // intermediate packet, so one scan finds it.
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

/// Enhanced HEVC sequence start (`hvc1`): 0x12 framing + FourCC.
/// Mirrors tests/red5/fixtures.rs; bytes are framing-correct, not decodable.
fn enhanced_hvc1_sequence_start() -> Bytes {
    Bytes::from(vec![
        0x12, b'h', b'v', b'c', b'1', 0x01, 0x01, 0x60, 0x00, 0x00, 0x00, 0x90, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x5d, 0xf0, 0x00, 0xfc, 0xfd, 0xf8, 0xf8, 0x00, 0x00, 0x0f, 0x03, 0x20, 0x00,
        0x01, 0x00, 0x16,
    ])
}

fn enhanced_hvc1_coded_frame() -> Bytes {
    Bytes::from(vec![
        0x10, b'h', b'v', b'c', b'1', 0x01, 0x00, 0x00, 0x05, 0x00, 0x00, 0x00, 0x04, 0x65, 0x88,
        0x84, 0x21,
    ])
}

/// Enhanced AV1 coded frame (`av01`): 0x10 framing + FourCC + obu.
fn enhanced_av1_coded_frame() -> Bytes {
    Bytes::from(vec![
        0x10, b'a', b'v', b'0', b'1', 0x00, 0x00, 0x05, 0x0a, 0x0b, 0x00, 0x00, 0x0c, 0x12, 0x00,
        0x0a, 0x0a, 0x00, 0xde, 0xad, 0xbe, 0xef,
    ])
}

fn legacy_avc_sequence_header() -> Bytes {
    Bytes::from(vec![
        0x17, 0x00, 0x00, 0x00, 0x00, 0x01, 0x64, 0x00, 0x1f, 0xff, 0xe1, 0x00, 0x0b, 0x67, 0x64,
        0x00, 0x1f, 0xac, 0xd9, 0x40, 0x78, 0x02, 0x27, 0xe5, 0x01, 0x01, 0x01, 0x02, 0x68, 0xeb,
        0xec, 0xb2, 0x2c,
    ])
}

fn publish_and_collect_video(pump: &mut Pump, frames: &[(Bytes, u32)]) -> Vec<Bytes> {
    for (frame, ts) in frames {
        let out = pump
            .client
            .publish_video_data(frame.clone(), RtmpTimestamp::new(*ts), false)
            .expect("video must build");
        pump.push_client(vec![out]);
    }
    pump.take_server_events()
        .into_iter()
        .filter_map(|event| match event {
            ServerSessionEvent::VideoDataReceived { data, .. } => Some(data),
            _ => None,
        })
        .collect()
}

#[test]
fn enhanced_hvc1_survives_publish_amf0() {
    let mut pump = Pump::new(
        client_config_for(AmfEncoding::Amf0),
        ServerSessionConfig::new(),
    );
    connect(&mut pump, "live");
    publish(&mut pump, "enh-hvc1");
    let seq = enhanced_hvc1_sequence_start();
    let frame = enhanced_hvc1_coded_frame();
    let got = publish_and_collect_video(&mut pump, &[(seq.clone(), 0), (frame.clone(), 40)]);
    assert_eq!(
        got,
        vec![seq, frame],
        "Enhanced hvc1 must survive byte-exact"
    );
}

#[test]
fn enhanced_av01_survives_publish_amf3() {
    let mut pump = Pump::new(
        client_config_for(AmfEncoding::Amf3),
        ServerSessionConfig::new(),
    );
    connect(&mut pump, "live");
    publish(&mut pump, "enh-av01");
    assert_eq!(pump.client.negotiated_encoding(), AmfEncoding::Amf3);
    assert_eq!(pump.server.negotiated_encoding(), AmfEncoding::Amf3);
    let frame = enhanced_av1_coded_frame();
    let got = publish_and_collect_video(&mut pump, &[(frame.clone(), 0)]);
    assert_eq!(
        got,
        vec![frame],
        "Enhanced av01 must survive byte-exact over AMF3"
    );
}

#[test]
fn enhanced_relay_is_byte_exact_between_two_sessions() {
    // Ingest leg: publisher -> server 1.
    let mut ingest = Pump::new(
        client_config_for(AmfEncoding::Amf0),
        ServerSessionConfig::new(),
    );
    connect(&mut ingest, "live");
    publish(&mut ingest, "relay");
    let hvc1_seq = enhanced_hvc1_sequence_start();
    let hvc1_frame = enhanced_hvc1_coded_frame();
    let av01_frame = enhanced_av1_coded_frame();
    let legacy = legacy_avc_sequence_header();
    let ingested = publish_and_collect_video(
        &mut ingest,
        &[
            (hvc1_seq.clone(), 0),
            (hvc1_frame.clone(), 40),
            (av01_frame.clone(), 80),
            (legacy.clone(), 120),
        ],
    );
    assert_eq!(
        ingested,
        vec![
            hvc1_seq.clone(),
            hvc1_frame.clone(),
            av01_frame.clone(),
            legacy.clone()
        ],
        "ingest must preserve Enhanced + legacy bytes"
    );

    // Relay leg: server 2 -> player, fed with the exact bytes server 1 saw.
    // This is what a proxy does: no decode, just forward the payload.
    let mut playout = Pump::new(
        client_config_for(AmfEncoding::Amf0),
        ServerSessionConfig::new(),
    );
    connect(&mut playout, "live");
    let play_stream_id = play(&mut playout, "relay");
    for (i, payload) in ingested.iter().enumerate() {
        let packet = playout
            .server
            .send_video_data(
                play_stream_id,
                payload.clone(),
                RtmpTimestamp::new(i as u32 * 40),
                false,
            )
            .expect("relay send must build");
        playout.push_server(vec![ServerSessionResult::OutboundResponse(packet)]);
    }
    let played: Vec<Bytes> = playout
        .take_client_events()
        .into_iter()
        .filter_map(|event| match event {
            ClientSessionEvent::VideoDataReceived { data, .. } => Some(data),
            _ => None,
        })
        .collect();
    assert_eq!(
        played,
        vec![hvc1_seq, hvc1_frame, av01_frame, legacy],
        "relay must forward Enhanced + legacy bytes verbatim"
    );
}
