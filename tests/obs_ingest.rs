//! OBS ingest compatibility (default suite).
//!
//! ffmpeg proves spec compliance; OBS proves quirk tolerance. OBS is
//! AMF0-only, so it tells nothing about AMF3 — what it proves is that our
//! `ServerSession` survives the exact command sequence OBS sends:
//! `connect` (FMLE-style `flashVer`, `tcUrl`, capabilities) ->
//! `releaseStream` -> `FCPublish` -> `createStream` -> `publish` ->
//! `@setDataFrame onMetaData` -> audio/video -> `FCUnpublish` ->
//! `deleteStream`. `releaseStream`/`FCPublish`/`FCUnpublish` have no RTMP
//! semantics on our side; they must surface as `UnhandleableAmf0Command`
//! and leave the session usable, exactly as the live ffmpeg leg already
//! proves for ffmpeg's own `releaseStream`/`FCPublish`.
//!
//! No network, no binaries: raw chunk bytes in-process, mirroring
//! `tests/data_events.rs`.

use bytes::Bytes;
use rtmpx::amf0::{Amf0Object, Amf0Value};
use rtmpx::chunk_io::{ChunkDeserializer, ChunkSerializer};
use rtmpx::messages::RtmpMessage;
use rtmpx::sessions::{
    ServerSession, ServerSessionConfig, ServerSessionEvent, ServerSessionResult,
};
use rtmpx::time::RtmpTimestamp;

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
        .expect("server must accept OBS bytes");
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

/// OBS-style `connect`: FMLE `flashVer`, `tcUrl`, capabilities, AMF0.
fn obs_connect_message(app: &str) -> RtmpMessage {
    let mut properties = Amf0Object::new();
    properties.insert("app".to_string(), Amf0Value::Utf8String(app.to_string()));
    properties.insert(
        "flashVer".to_string(),
        Amf0Value::Utf8String("FMLE/3.0 (compatible; OBS-Studio/30.2.3)".to_string()),
    );
    properties.insert(
        "tcUrl".to_string(),
        Amf0Value::Utf8String(format!("rtmp://127.0.0.1/live/{app}")),
    );
    properties.insert("objectEncoding".to_string(), Amf0Value::Number(0.0));
    properties.insert("capabilities".to_string(), Amf0Value::Number(15.0));
    properties.insert("audioCodecs".to_string(), Amf0Value::Number(3575.0));
    properties.insert("videoCodecs".to_string(), Amf0Value::Number(252.0));
    RtmpMessage::Amf0Command {
        command_name: "connect".to_string(),
        transaction_id: 1.0,
        command_object: Amf0Value::Object(properties),
        additional_arguments: Vec::new(),
    }
}

/// OBS-shaped `@setDataFrame onMetaData`: wide object, stringly video codec
/// hints, stereo flag, encoder string. Only a subset is modelled in
/// `StreamMetadata`; the rest must not break parsing.
fn obs_metadata_message() -> RtmpMessage {
    let mut props = Amf0Object::new();
    props.insert("width".to_string(), Amf0Value::Number(1920.0));
    props.insert("height".to_string(), Amf0Value::Number(1080.0));
    props.insert("videocodecid".to_string(), Amf0Value::Number(7.0));
    props.insert("framerate".to_string(), Amf0Value::Number(60.0));
    props.insert("videodatarate".to_string(), Amf0Value::Number(6000.0));
    props.insert("audiocodecid".to_string(), Amf0Value::Number(10.0));
    props.insert("audiodatarate".to_string(), Amf0Value::Number(128.0));
    props.insert("audiosamplerate".to_string(), Amf0Value::Number(48000.0));
    props.insert("audiochannels".to_string(), Amf0Value::Number(2.0));
    props.insert("stereo".to_string(), Amf0Value::Boolean(true));
    props.insert(
        "encoder".to_string(),
        Amf0Value::Utf8String("OBS-Studio/30.2.3".to_string()),
    );
    RtmpMessage::Amf0Data {
        values: vec![
            Amf0Value::Utf8String("@setDataFrame".to_string()),
            Amf0Value::Utf8String("onMetaData".to_string()),
            Amf0Value::Object(props),
        ],
    }
}

fn avc_sequence_header() -> Bytes {
    Bytes::from(vec![
        0x17, 0x00, 0x00, 0x00, 0x00, 0x01, 0x64, 0x00, 0x1f, 0xff, 0xe1, 0x00, 0x0b, 0x67, 0x64,
        0x00, 0x1f, 0xac, 0xd9, 0x40, 0x78, 0x02, 0x27, 0xe5, 0x01, 0x01, 0x01, 0x02, 0x68, 0xeb,
        0xec, 0xb2, 0x2c,
    ])
}

fn aac_sequence_header() -> Bytes {
    Bytes::from(vec![0xAF, 0x00, 0x12, 0x10])
}

#[test]
fn obs_connect_sequence_ingests_media() {
    let (mut session, initial) =
        ServerSession::new(ServerSessionConfig::new()).expect("server must start");
    let mut deserializer = ChunkDeserializer::new();
    let mut serializer = ChunkSerializer::new();
    consume_server_outbound(&mut deserializer, initial);

    // connect
    let (_, events) = send_to_server(
        &mut session,
        &mut serializer,
        &mut deserializer,
        obs_connect_message("live"),
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
    consume_server_outbound(
        &mut deserializer,
        session.accept_request(request_id).expect("accept connect"),
    );

    // releaseStream + FCPublish: OBS sends both before createStream.
    // They carry no server-side semantics; the session must surface them
    // as unhandleable and stay usable — never error, never drop the peer.
    let key = "obs-quirk-key";
    for (name, tid) in [("releaseStream", 2.0), ("FCPublish", 3.0)] {
        let (_, events) = send_to_server(
            &mut session,
            &mut serializer,
            &mut deserializer,
            amf0_command(name, tid, vec![Amf0Value::Utf8String(key.to_string())]),
            0,
            0,
            false,
        );
        assert!(
            events.iter().any(|e| matches!(e, ServerSessionEvent::UnhandleableAmf0Command { command_name, .. } if command_name == name)),
            "{name} must surface as UnhandleableAmf0Command, saw {events:?}"
        );
    }

    // createStream -> publish
    let (responses, _) = send_to_server(
        &mut session,
        &mut serializer,
        &mut deserializer,
        amf0_command("createStream", 4.0, vec![]),
        0,
        0,
        false,
    );
    let stream_id = match responses.first().expect("createStream needs a reply") {
        RtmpMessage::Amf0Command {
            command_name,
            additional_arguments,
            ..
        } if command_name == "_result" => match additional_arguments.first() {
            Some(Amf0Value::Number(id)) => *id as u32,
            other => panic!("createStream reply must carry stream id, got {other:?}"),
        },
        other => panic!("createStream reply must be _result, got {other:?}"),
    };
    let (_, events) = send_to_server(
        &mut session,
        &mut serializer,
        &mut deserializer,
        amf0_command(
            "publish",
            5.0,
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
    consume_server_outbound(
        &mut deserializer,
        session.accept_request(request_id).expect("accept publish"),
    );

    // OBS metadata shape
    let (_, events) = send_to_server(
        &mut session,
        &mut serializer,
        &mut deserializer,
        obs_metadata_message(),
        stream_id,
        0,
        false,
    );
    let meta = events
        .iter()
        .find_map(|e| match e {
            ServerSessionEvent::StreamMetadataChanged {
                metadata,
                stream_key,
                ..
            } if stream_key.as_ref() == key => Some(metadata.clone()),
            _ => None,
        })
        .expect("metadata must raise StreamMetadataChanged");
    assert_eq!(meta.video_width, Some(1920));
    assert_eq!(meta.video_height, Some(1080));
    assert_eq!(meta.video_codec_id, Some(7));
    assert_eq!(meta.audio_codec_id, Some(10));
    assert_eq!(meta.encoder.as_deref(), Some("OBS-Studio/30.2.3"));

    // Media still flows after the quirk commands.
    let video = avc_sequence_header();
    let audio = aac_sequence_header();
    let (_, events) = send_to_server(
        &mut session,
        &mut serializer,
        &mut deserializer,
        RtmpMessage::VideoData {
            data: video.clone(),
        },
        stream_id,
        40,
        false,
    );
    assert!(
        events.iter().any(
            |e| matches!(e, ServerSessionEvent::VideoDataReceived { data, .. } if data == &video)
        ),
        "video must arrive, saw {events:?}"
    );
    let (_, events) = send_to_server(
        &mut session,
        &mut serializer,
        &mut deserializer,
        RtmpMessage::AudioData {
            data: audio.clone(),
        },
        stream_id,
        23,
        false,
    );
    assert!(
        events.iter().any(
            |e| matches!(e, ServerSessionEvent::AudioDataReceived { data, .. } if data == &audio)
        ),
        "audio must arrive, saw {events:?}"
    );

    // Teardown quirks must not error either.
    let (_, events) = send_to_server(
        &mut session,
        &mut serializer,
        &mut deserializer,
        amf0_command(
            "FCUnpublish",
            6.0,
            vec![Amf0Value::Utf8String(key.to_string())],
        ),
        0,
        0,
        false,
    );
    assert!(events.iter().any(|e| matches!(e, ServerSessionEvent::UnhandleableAmf0Command { command_name, .. } if command_name == "FCUnpublish")), "FCUnpublish must be unhandleable-but-harmless, saw {events:?}");
    let (_, events) = send_to_server(
        &mut session,
        &mut serializer,
        &mut deserializer,
        amf0_command(
            "deleteStream",
            0.0,
            vec![Amf0Value::Number(stream_id as f64)],
        ),
        0,
        0,
        false,
    );
    assert!(events.iter().any(|e| matches!(e, ServerSessionEvent::PublishStreamFinished { stream_key, .. } if stream_key.as_ref() == key)), "deleteStream must finish the publish, saw {events:?}");
}
