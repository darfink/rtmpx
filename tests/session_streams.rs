//! Stream lifecycle tests use only the public API.
use bytes::Bytes;
use rtmpx::{DropPolicy, Packet, sessions::*, time::RtmpTimestamp};

struct Pair {
    client: ClientSession,
    server: ServerSession,
    client_events: Vec<ClientEvent>,
    server_events: Vec<ServerEvent>,
    to_client: Bytes,
    to_server: Bytes,
}
fn append(target: &mut Bytes, packet: Packet) {
    let mut bytes = target.to_vec();
    packet.copy_to(&mut bytes);
    *target = bytes.into();
}
impl Pair {
    fn new() -> Self {
        Self::with_server_limits(SessionLimits::default())
    }
    fn with_server_limits(limits: SessionLimits) -> Self {
        let mut pair = Self {
            client: ClientSession::new(ClientSessionConfig::default()).unwrap(),
            server: ServerSession::new(ServerSessionConfig {
                session_limits: limits,
                ..Default::default()
            })
            .unwrap(),
            client_events: Vec::new(),
            server_events: Vec::new(),
            to_client: Bytes::new(),
            to_server: Bytes::new(),
        };
        pair.client.connect("live").unwrap();
        pair.drive();
        pair.client_events.clear();
        pair.server_events.clear();
        pair
    }
    fn drive(&mut self) {
        self.drive_with_acceptance(true);
    }
    fn drive_with_acceptance(&mut self, accept: bool) {
        for _ in 0..100 {
            let mut progress = false;
            while let Some(output) = self.client.receive(&mut self.to_client).unwrap() {
                progress = true;
                match output {
                    ClientOutput::Packet(packet) => append(&mut self.to_server, packet),
                    ClientOutput::Event(event) => self.client_events.push(event),
                    _ => {}
                }
            }
            while let Some(output) = self.server.receive(&mut self.to_server).unwrap() {
                progress = true;
                match output {
                    ServerOutput::Packet(packet) => append(&mut self.to_client, packet),
                    ServerOutput::Event(event) => {
                        match &event {
                            ServerEvent::ConnectionRequested { request_id, .. }
                            | ServerEvent::PlayStreamRequested { request_id, .. }
                            | ServerEvent::PublishStreamRequested { request_id, .. }
                                if accept =>
                            {
                                self.server.accept_request(*request_id).unwrap()
                            }
                            _ => {}
                        }
                        self.server_events.push(event);
                    }
                    _ => {}
                }
            }
            if !progress {
                return;
            }
        }
        panic!("protocol did not settle");
    }
    fn peer_handle(&self, stream: StreamHandle) -> StreamHandle {
        let id = self.client.stream_id(stream).unwrap();
        self.server
            .streams()
            .find(|(_, wire)| *wire == id)
            .unwrap()
            .0
    }
}

#[test]
fn concurrent_play_and_publish_route_media_and_preserve_independent_states() {
    let mut pair = Pair::new();
    let play = pair.client.play("watch").unwrap();
    let pub_one = pair.client.publish("one", PublishMode::Live).unwrap();
    let pub_two = pair.client.publish("two", PublishMode::Record).unwrap();
    assert_eq!(pair.client.state(), ConnectionState::Connected);
    assert_eq!(
        pair.client.stream_state(play),
        Some(ClientStreamState::Creating)
    );
    assert_eq!(pair.client.stream_id(play), None);
    pair.drive();
    assert_eq!(
        pair.client.stream_state(play),
        Some(ClientStreamState::Playing)
    );
    assert_eq!(
        pair.client.stream_state(pub_one),
        Some(ClientStreamState::Publishing)
    );
    assert_eq!(
        pair.client.stream_state(pub_two),
        Some(ClientStreamState::Publishing)
    );
    for h in [pub_one, pub_two] {
        assert!(pair.client_events.iter().any(
            |e| matches!(e, ClientEvent::PublishRequestAccepted { stream, .. } if *stream==h)
        ));
    }
    let peer_play = pair.peer_handle(play);
    let peer_one = pair.peer_handle(pub_one);
    let peer_two = pair.peer_handle(pub_two);
    let video = Bytes::from_static(b"\x27\x01\0\0\0video");
    let audio = Bytes::from_static(b"\xaf\x01audio");
    let packet = pair
        .server
        .send_video(
            peer_play,
            video.clone(),
            RtmpTimestamp::new(13),
            DropPolicy::Never,
        )
        .unwrap();
    append(&mut pair.to_client, packet);
    let packet = pair
        .client
        .send_audio(
            pub_one,
            audio.clone(),
            RtmpTimestamp::new(17),
            DropPolicy::Never,
        )
        .unwrap();
    append(&mut pair.to_server, packet);
    let packet = pair
        .client
        .send_video(
            pub_two,
            video.clone(),
            RtmpTimestamp::new(19),
            DropPolicy::Never,
        )
        .unwrap();
    append(&mut pair.to_server, packet);
    pair.drive();
    assert!(pair.client_events.iter().any(|e| matches!(e, ClientEvent::VideoDataReceived { stream, data, .. } if *stream==play && data.to_bytes()==video)));
    assert!(pair.server_events.iter().any(|e| matches!(e, ServerEvent::AudioDataReceived { stream, data, .. } if *stream==peer_one && data.to_bytes()==audio)));
    assert!(pair.server_events.iter().any(|e| matches!(e, ServerEvent::VideoDataReceived { stream, data, .. } if *stream==peer_two && data.to_bytes()==video)));

    // Delete playback and immediately publish on a new local handle.
    pair.client.delete_stream(play).unwrap();
    let replacement = pair
        .client
        .publish("replacement", PublishMode::Live)
        .unwrap();
    assert_ne!(play, replacement);
    assert_eq!(pair.client.stream_state(play), None);
    pair.drive();
    assert_eq!(
        pair.client.stream_state(replacement),
        Some(ClientStreamState::Publishing)
    );
    assert_eq!(
        pair.client.stream_state(pub_one),
        Some(ClientStreamState::Publishing)
    );
    assert_eq!(pair.server.stream_id(peer_play), None);
    assert!(matches!(
        pair.server.send_video(
            peer_play,
            video.clone(),
            RtmpTimestamp::new(1),
            DropPolicy::Never
        ),
        Err(ServerSessionError::InvalidStreamHandle)
    ));
    assert!(matches!(
        pair.client.delete_stream(play),
        Err(ClientSessionError::InvalidStreamHandle)
    ));
    assert!(matches!(
        pair.client
            .send_video(peer_one, video, RtmpTimestamp::new(1), DropPolicy::Never),
        Err(ClientSessionError::InvalidStreamHandle)
    ));
}

#[test]
fn cancel_creation_then_publish_immediately_does_not_start_cancelled_operation() {
    let mut pair = Pair::new();
    let cancelled = pair.client.play("cancelled").unwrap();
    pair.client.delete_stream(cancelled).unwrap();
    let publish = pair
        .client
        .publish("replacement", PublishMode::Live)
        .unwrap();
    assert_ne!(cancelled, publish);
    pair.drive();
    assert_eq!(pair.client.stream_state(cancelled), None);
    assert_eq!(
        pair.client.stream_state(publish),
        Some(ClientStreamState::Publishing)
    );
    assert_eq!(pair.server.streams().count(), 1);
    assert!(
        !pair
            .server_events
            .iter()
            .any(|e| matches!(e, ServerEvent::PlayStreamRequested { .. }))
    );
    assert!(
        !pair
            .client_events
            .iter()
            .any(|e| matches!(e, ClientEvent::PlaybackRequestAccepted { .. }))
    );
}

#[test]
fn completing_one_playback_does_not_finish_another() {
    let mut pair = Pair::new();
    let one = pair.client.play("one").unwrap();
    let two = pair.client.play("two").unwrap();
    pair.drive();
    let packet = pair
        .server
        .complete_playback(pair.peer_handle(one))
        .unwrap();
    append(&mut pair.to_client, packet);
    pair.drive();
    assert_eq!(pair.client.stream_state(one), None);
    assert_eq!(
        pair.client.stream_state(two),
        Some(ClientStreamState::Playing)
    );
    assert!(
        pair.client_events
            .iter()
            .any(|e| matches!(e, ClientEvent::PlaybackFinished { stream, .. } if *stream==one))
    );
    assert_eq!(pair.server.streams().count(), 1);
}

#[test]
fn handles_cannot_cross_sessions_even_when_wire_ids_match() {
    let mut one = Pair::new();
    let mut two = Pair::new();
    let a = one.client.publish("same", PublishMode::Live).unwrap();
    let b = two.client.publish("same", PublishMode::Live).unwrap();
    one.drive();
    two.drive();
    assert_eq!(one.client.stream_id(a), two.client.stream_id(b));
    assert_ne!(a, b);
    assert!(matches!(
        two.client.delete_stream(a),
        Err(ClientSessionError::InvalidStreamHandle)
    ));
    let foreign = one.peer_handle(a);
    assert!(matches!(
        two.server.send_audio(
            foreign,
            Bytes::new(),
            RtmpTimestamp::new(0),
            DropPolicy::Never
        ),
        Err(ServerSessionError::InvalidStreamHandle)
    ));
}

struct MockPeer {
    encoder: rtmpx::chunk_io::ChunkEncoder,
    decoder: rtmpx::chunk_io::MessageDecoder,
    commands: Vec<(u32, String, f64, Vec<rtmpx::Amf0Value>)>,
    events: Vec<ClientEvent>,
}
impl MockPeer {
    fn new() -> (Self, ClientSession) {
        Self::with_limits(SessionLimits::default())
    }
    fn with_limits(limits: SessionLimits) -> (Self, ClientSession) {
        let mut peer = Self {
            encoder: rtmpx::chunk_io::ChunkEncoder::new(),
            decoder: rtmpx::chunk_io::MessageDecoder::new(),
            commands: Vec::new(),
            events: Vec::new(),
        };
        let mut client = ClientSession::new(ClientSessionConfig {
            session_limits: limits,
            ..Default::default()
        })
        .unwrap();
        client.connect("live").unwrap();
        peer.collect(&mut client, Bytes::new());
        let txn = peer.commands.pop().unwrap().2;
        peer.reply(
            &mut client,
            "_result",
            txn,
            vec![rtmpx::Amf0Value::Object(Default::default())],
        );
        peer.commands.clear();
        peer.events.clear();
        (peer, client)
    }
    fn collect(&mut self, client: &mut ClientSession, mut input: Bytes) {
        while let Some(output) = client.receive(&mut input).unwrap() {
            match output {
                ClientOutput::Packet(packet) => {
                    let mut bytes = Bytes::from(packet.to_vec());
                    while let Some(raw) = self.decoder.decode(&mut bytes).unwrap() {
                        let id = raw.message_stream_id;
                        let message = raw
                            .map_data(rtmpx::Payload::into_bytes)
                            .to_rtmp_message()
                            .unwrap();
                        match message {
                            rtmpx::messages::RtmpMessage::SetChunkSize { size } => {
                                self.decoder.set_chunk_size(size as usize).unwrap()
                            }
                            rtmpx::messages::RtmpMessage::Amf0Command {
                                command_name,
                                transaction_id,
                                additional_arguments,
                                ..
                            } => self.commands.push((
                                id,
                                command_name,
                                transaction_id,
                                additional_arguments,
                            )),
                            _ => {}
                        }
                    }
                }
                ClientOutput::Event(event) => self.events.push(event),
                _ => {}
            }
        }
    }
    fn reply(
        &mut self,
        client: &mut ClientSession,
        name: &str,
        transaction: f64,
        args: Vec<rtmpx::Amf0Value>,
    ) {
        let message = rtmpx::messages::RtmpMessage::Amf0Command {
            command_name: name.into(),
            transaction_id: transaction,
            command_object: rtmpx::Amf0Value::Null,
            additional_arguments: args,
        };
        let packet = self
            .encoder
            .encode(
                message.into_raw_message(RtmpTimestamp::new(0), 0).unwrap(),
                Default::default(),
            )
            .unwrap();
        self.collect(client, Bytes::from(packet.to_vec()));
    }
}

#[test]
fn reversed_creation_responses_keep_request_identity_and_reused_slots_distinct() {
    use rtmpx::Amf0Value as V;
    let (mut peer, mut client) = MockPeer::new();
    let first = client.play("first").unwrap();
    let second = client.publish("second", PublishMode::Append).unwrap();
    peer.collect(&mut client, Bytes::new());
    let first_txn = peer.commands[0].2;
    let second_txn = peer.commands[1].2;
    peer.commands.clear();
    // Wire identifiers need not match local order or local handle storage.
    peer.reply(&mut client, "_result", second_txn, vec![V::Number(99.0)]);
    peer.reply(&mut client, "_result", first_txn, vec![V::Number(7.0)]);
    assert_eq!(client.stream_id(first).unwrap().get(), 7);
    assert_eq!(client.stream_id(second).unwrap().get(), 99);
    assert!(peer.commands.iter().any(|(id, name, _, args)| *id == 99
        && name == "publish"
        && args
            == &vec![
                V::Utf8String("second".into()),
                V::Utf8String("append".into())
            ]));
    assert!(peer.commands.iter().any(|(id, name, _, args)| *id == 7
        && name == "play"
        && args[0] == V::Utf8String("first".into())));
    client.delete_stream(first).unwrap();
    let replacement = client.play("replacement").unwrap();
    peer.commands.clear();
    peer.collect(&mut client, Bytes::new());
    let replacement_txn = peer
        .commands
        .iter()
        .find(|(_, name, _, _)| name == "createStream")
        .unwrap()
        .2;
    assert!(peer.commands.iter().any(|(id, name, _, args)| *id == 0
        && name == "deleteStream"
        && args == &vec![V::Number(7.0)]));
    peer.reply(
        &mut client,
        "_result",
        replacement_txn,
        vec![V::Number(7.0)],
    );
    assert_eq!(client.stream_id(replacement).unwrap().get(), 7);
    assert_eq!(client.stream_id(first), None);
    assert!(matches!(
        client.delete_stream(first),
        Err(ClientSessionError::InvalidStreamHandle)
    ));
    assert_eq!(
        client.stream_state(second),
        Some(ClientStreamState::StartingPublish)
    );
}

#[test]
fn rejecting_one_creation_preserves_other_streams_and_reports_its_handle() {
    use rtmpx::Amf0Value as V;
    let (mut peer, mut client) = MockPeer::new();
    let bad = client.play("missing").unwrap();
    let good = client.publish("good", PublishMode::Live).unwrap();
    peer.collect(&mut client, Bytes::new());
    let bad_txn = peer.commands[0].2;
    let good_txn = peer.commands[1].2;
    peer.reply(
        &mut client,
        "_error",
        bad_txn,
        vec![V::Object(Default::default())],
    );
    peer.reply(&mut client, "_result", good_txn, vec![V::Number(1.0)]);
    assert!(
        peer.events.iter().any(
            |e| matches!(e,ClientEvent::PlaybackRequestRejected { stream,.. } if *stream==bad)
        )
    );
    assert_eq!(client.stream_state(bad), None);
    assert_eq!(
        client.stream_state(good),
        Some(ClientStreamState::StartingPublish)
    );
    assert_eq!(client.state(), ConnectionState::Connected);
}

#[test]
fn deleting_a_stream_invalidates_its_pending_server_decision() {
    let mut pair = Pair::new();
    let handle = pair.client.play("pending").unwrap();
    pair.drive_with_acceptance(false);
    let (request, server_handle) = pair
        .server_events
        .iter()
        .find_map(|event| match event {
            ServerEvent::PlayStreamRequested {
                request_id, stream, ..
            } => Some((*request_id, *stream)),
            _ => None,
        })
        .unwrap();
    pair.client.delete_stream(handle).unwrap();
    pair.drive_with_acceptance(false);
    assert_eq!(pair.server.stream_id(server_handle), None);
    assert!(matches!(
        pair.server.accept_request(request),
        Err(ServerSessionError::InvalidRequestId)
    ));
    assert!(!pair.server.is_failed());
}

#[test]
fn client_limits_include_cancelled_but_unanswered_creations() {
    let (mut peer, mut client) = MockPeer::with_limits(SessionLimits {
        max_streams: 1,
        max_pending_requests: 1,
    });
    let first = client.play("one").unwrap();
    assert!(matches!(
        client.play("too-many"),
        Err(ClientSessionError::LimitExceeded(
            SessionLimitError::Streams { limit: 1 }
        ))
    ));
    client.delete_stream(first).unwrap();
    assert!(matches!(
        client.play("still-pending"),
        Err(ClientSessionError::LimitExceeded(
            SessionLimitError::PendingRequests { limit: 1 }
        ))
    ));
    assert!(!client.is_failed());
    peer.collect(&mut client, Bytes::new());
    let transaction = peer
        .commands
        .iter()
        .find(|(_, name, _, _)| name == "createStream")
        .unwrap()
        .2;
    peer.reply(
        &mut client,
        "_result",
        transaction,
        vec![rtmpx::Amf0Value::Number(1.0)],
    );
    assert!(client.play("released").is_ok());
}

#[test]
fn server_stream_limit_releases_capacity_on_delete_and_rejects_overflow() {
    let mut pair = Pair::with_server_limits(SessionLimits {
        max_streams: 1,
        ..Default::default()
    });
    let first = pair.client.play("one").unwrap();
    pair.drive();
    pair.client.delete_stream(first).unwrap();
    let replacement = pair.client.play("replacement").unwrap();
    pair.drive();
    assert_eq!(
        pair.client.stream_state(replacement),
        Some(ClientStreamState::Playing)
    );
    pair.client.play("overflow").unwrap();
    while let Some(output) = pair.client.receive(&mut pair.to_client).unwrap() {
        if let ClientOutput::Packet(packet) = output {
            append(&mut pair.to_server, packet);
        }
    }
    assert!(matches!(
        pair.server.receive(&mut pair.to_server),
        Err(ServerSessionError::LimitExceeded(
            SessionLimitError::Streams { limit: 1 }
        ))
    ));
    assert!(pair.server.is_failed());
}

#[test]
fn server_bounds_requests_awaiting_application_decisions() {
    let mut pair = Pair::with_server_limits(SessionLimits {
        max_pending_requests: 1,
        ..Default::default()
    });
    let first = pair.client.play("one").unwrap();
    pair.drive_with_acceptance(false);
    let wire_id = pair.client.stream_id(first).unwrap();
    // A peer can send repeated requests without waiting for our application decision.
    let message = rtmpx::messages::RtmpMessage::Amf0Command {
        command_name: "play".into(),
        transaction_id: 0.0,
        command_object: rtmpx::Amf0Value::Null,
        additional_arguments: vec![rtmpx::Amf0Value::Utf8String("second".into())],
    };
    let packet = rtmpx::chunk_io::ChunkEncoder::new()
        .encode(
            message
                .into_raw_message(RtmpTimestamp::new(0), wire_id.get())
                .unwrap(),
            Default::default(),
        )
        .unwrap();
    let mut bytes = Bytes::from(packet.to_vec());
    assert!(matches!(
        pair.server.receive(&mut bytes),
        Err(ServerSessionError::LimitExceeded(
            SessionLimitError::PendingRequests { limit: 1 }
        ))
    ));
    assert!(pair.server.is_failed());
}
