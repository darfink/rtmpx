//! Public API behavior, independent of the historical batch fixture adapters.
use bytes::Bytes;
use rtmpx::{
    DropPolicy, EncodeOptions, HeaderMode, Packet, Payload, PayloadPool, PayloadPoolConfig,
    Segments,
    chunk_io::ChunkEncoder,
    messages::RawMessage,
    sessions::{
        ClientEvent, ClientOutput, ClientSession, ClientSessionConfig, PublishMode, ServerEvent,
        ServerOutput, ServerSession, ServerSessionConfig, ServerSessionError,
    },
    time::RtmpTimestamp,
};
use std::io::IoSlice;

fn publishing_pair() -> (ClientSession, ServerSession) {
    let mut client = ClientSession::new(ClientSessionConfig::default()).unwrap();
    let config = ServerSessionConfig {
        payload_pool: Some(PayloadPool::new(PayloadPoolConfig {
            max_cached_payloads: 4,
            max_descriptors_per_payload: 4096,
        })),
        ..Default::default()
    };
    let mut server = ServerSession::new(config).unwrap();
    let (mut to_client, mut to_server) = (Bytes::new(), Bytes::new());
    let mut publishing = false;
    client.connect("live").unwrap();
    for _ in 0..20 {
        while let Some(output) = client.receive(&mut to_client).unwrap() {
            match output {
                ClientOutput::Packet(packet) => {
                    assert!(to_server.is_empty());
                    to_server = Bytes::from(packet.to_vec());
                    while let Some(output) = server.receive(&mut to_server).unwrap() {
                        match output {
                            ServerOutput::Packet(packet) => {
                                let mut combined = to_client.to_vec();
                                packet.copy_to(&mut combined);
                                to_client = combined.into();
                            }
                            ServerOutput::Event(
                                ServerEvent::ConnectionRequested { request_id, .. }
                                | ServerEvent::PublishStreamRequested { request_id, .. },
                            ) => server.accept_request(request_id).unwrap(),
                            _ => {}
                        }
                    }
                }
                ClientOutput::Event(ClientEvent::ConnectionRequestAccepted { .. }) => {
                    client.publish("demo", PublishMode::Live).unwrap();
                }
                ClientOutput::Event(ClientEvent::PublishRequestAccepted { .. }) => {
                    publishing = true
                }
                _ => {}
            }
        }
        if publishing {
            return (client, server);
        }
    }
    panic!("publish did not complete");
}

#[test]
fn receive_yields_one_output_and_retains_the_unread_tail() {
    let (mut client, mut server) = publishing_pair();
    let stream = client.streams().next().unwrap().0;
    let one = Bytes::from_static(b"\x27\x01\0\0\0one");
    let two = Bytes::from_static(b"\x27\x01\0\0\0two");
    let mut wire = Vec::new();
    client
        .send_video(
            stream,
            one.clone(),
            RtmpTimestamp::new(10),
            DropPolicy::Never,
        )
        .unwrap()
        .copy_to(&mut wire);
    let boundary = wire.len();
    client
        .send_video(
            stream,
            two.clone(),
            RtmpTimestamp::new(20),
            DropPolicy::Never,
        )
        .unwrap()
        .copy_to(&mut wire);
    let backing = Bytes::from(wire);
    let mut input = backing.clone();
    let first = server.receive(&mut input).unwrap().unwrap();
    assert_eq!(input.len(), backing.len() - boundary);
    // The owned output can outlive this call or be sent to another task.
    match first {
        ServerOutput::Event(ServerEvent::VideoDataReceived { data, .. }) => {
            assert_eq!(data.to_bytes(), one);
            assert!(data.segment(0).as_ptr() as usize >= backing.as_ptr() as usize);
            assert!(
                data.segment(0).as_ptr() as usize + data.len()
                    <= backing.as_ptr() as usize + backing.len()
            );
        }
        _ => panic!(),
    }
    match server.receive(&mut input).unwrap().unwrap() {
        ServerOutput::Event(ServerEvent::VideoDataReceived { data, .. }) => {
            assert_eq!(data.to_bytes(), two)
        }
        _ => panic!(),
    }
    assert!(input.is_empty());
    assert!(server.receive(&mut input).unwrap().is_none());
}

#[test]
fn control_actions_drain_through_receive_and_guard_packet_order() {
    let mut client = ClientSession::new(ClientSessionConfig::default()).unwrap();
    let mut server = ServerSession::new(ServerSessionConfig::default()).unwrap();
    let mut empty = Bytes::new();
    assert!(server.receive(&mut empty).unwrap().is_none());
    assert!(matches!(
        server.send_ping_request(),
        Err(ServerSessionError::NotConnected)
    ));
    client.connect("live").unwrap();
    let ClientOutput::Packet(packet) = client.receive(&mut empty).unwrap().unwrap() else {
        panic!()
    };
    let mut input = Bytes::from(packet.to_vec());
    let ServerOutput::Event(ServerEvent::ConnectionRequested { request_id, .. }) =
        server.receive(&mut input).unwrap().unwrap()
    else {
        panic!()
    };
    server.accept_request(request_id).unwrap();
    assert!(matches!(
        server.send_ping_request(),
        Err(ServerSessionError::PendingOutput)
    ));
    let mut packets = 0;
    while let Some(output) = server.receive(&mut input).unwrap() {
        if let ServerOutput::Packet(packet) = output {
            packets += 1;
            let mut bytes = Bytes::from(packet.to_vec());
            while client.receive(&mut bytes).unwrap().is_some() {}
        }
    }
    assert!(packets >= 4);
    assert!(server.send_ping_request().is_ok());
}

#[test]
fn packet_owns_write_progress_across_moves_and_only_unsent_packets_can_drop() {
    let data: Payload = [Bytes::from_static(b"abc"), Bytes::from(vec![42; 400])]
        .into_iter()
        .collect();
    let mut encoder = ChunkEncoder::new();
    let packet: Packet = encoder
        .encode(
            RawMessage {
                data,
                timestamp: RtmpTimestamp::new(0),
                message_stream_id: 1,
                type_id: 9,
            },
            EncodeOptions {
                drop_policy: DropPolicy::Allowed,
                headers: HeaderMode::Full,
            },
        )
        .unwrap();
    let expected = packet.to_vec();
    assert!(packet.can_drop());
    let mut queue = std::collections::VecDeque::from([packet]);
    let mut output = Vec::new();
    while let Some(mut packet) = queue.pop_front() {
        let mut slices = [IoSlice::new(&[]); 4];
        let n = packet.io_slices(&mut slices);
        assert!(n > 0);
        let written = slices[0].len().min(3);
        output.extend_from_slice(&slices[0][..written]);
        packet.advance(written);
        assert!(!packet.can_drop());
        assert_eq!(packet.to_vec(), expected[output.len()..]);
        if !packet.is_complete() {
            queue.push_back(packet);
        }
    }
    assert_eq!(output, expected);
}

#[test]
fn malformed_tail_is_terminal_after_prior_output_was_observed() {
    let (mut client, mut server) = publishing_pair();
    let stream = client.streams().next().unwrap().0;
    let mut wire = client
        .send_audio(
            stream,
            Bytes::from_static(b"\xaf\x01sample"),
            RtmpTimestamp::new(0),
            DropPolicy::Never,
        )
        .unwrap()
        .to_vec();
    // Type-3 header referencing a CSID that has never appeared.
    wire.push(0xff);
    let mut input = Bytes::from(wire);
    assert!(matches!(
        server.receive(&mut input).unwrap(),
        Some(ServerOutput::Event(ServerEvent::AudioDataReceived { .. }))
    ));
    assert!(server.receive(&mut input).is_err());
    assert!(matches!(
        server.receive(&mut input),
        Err(ServerSessionError::SessionFailed)
    ));
    assert!(matches!(
        server.send_ping_request(),
        Err(ServerSessionError::SessionFailed)
    ));
}
