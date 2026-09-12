//! Steady-state allocation contracts. Count only allocations on the measured thread.
use bytes::Bytes;
use rtmpx::{
    chunk_io::{ChunkEncoder, ChunkParser, MessageDecoder},
    messages::RawMessage,
    time::RtmpTimestamp,
};
use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
    hint::black_box,
    io::IoSlice,
};
struct Allocator;
thread_local! { static COUNTS:Cell<(bool,usize,usize)>=const{Cell::new((false,0,0))}; }
fn count(realloc: bool) {
    let _ = COUNTS.try_with(|c| {
        let (enabled, a, r) = c.get();
        if enabled {
            c.set((true, a + usize::from(!realloc), r + usize::from(realloc)));
        }
    });
}
unsafe impl GlobalAlloc for Allocator {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        count(false);
        unsafe { System.alloc(l) }
    }
    unsafe fn alloc_zeroed(&self, l: Layout) -> *mut u8 {
        count(false);
        unsafe { System.alloc_zeroed(l) }
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, n: usize) -> *mut u8 {
        count(true);
        unsafe { System.realloc(p, l, n) }
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        unsafe { System.dealloc(p, l) }
    }
}
#[global_allocator]
static ALLOCATOR: Allocator = Allocator;
fn measure<T>(f: impl FnOnce() -> T) -> (T, (usize, usize)) {
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            COUNTS.with(|c| {
                let (_, a, r) = c.get();
                c.set((false, a, r));
            });
        }
    }
    COUNTS.with(|c| c.set((true, 0, 0)));
    let reset = Reset;
    let value = f();
    drop(reset);
    let counts = COUNTS.with(|c| {
        let (_, a, r) = c.get();
        (a, r)
    });
    (value, counts)
}
#[test]
fn outbound_and_borrowed_inbound_are_allocation_free_after_warmup() {
    for chunk in [128, 4096] {
        let message = RawMessage {
            timestamp: RtmpTimestamp::new(10),
            type_id: 9,
            message_stream_id: 1,
            data: Bytes::from(vec![0; 256 * 1024]),
        };
        let mut encoder = ChunkEncoder::new();
        encoder
            .set_chunk_size(chunk, RtmpTimestamp::new(0))
            .unwrap();
        let (mut plan, alloc) = measure(|| {
            encoder
                .encode(
                    (black_box(&message)).as_ref(),
                    rtmpx::EncodeOptions {
                        headers: if false {
                            rtmpx::HeaderMode::Full
                        } else {
                            rtmpx::HeaderMode::Compressed
                        },
                        drop_policy: if false {
                            rtmpx::DropPolicy::Allowed
                        } else {
                            rtmpx::DropPolicy::Never
                        },
                    },
                )
                .unwrap()
        });
        assert_eq!(alloc, (0, 0));
        let wire = plan.to_vec();
        let (_, alloc) = measure(|| {
            let cursor = &mut plan;
            while !cursor.is_complete() {
                let mut slices = [IoSlice::new(&[]); 32];
                let count = cursor.io_slices(&mut slices);
                let n = slices[..count].iter().map(|b| b.len()).sum();
                black_box(&slices[..count]);
                cursor.advance(n);
            }
        });
        assert_eq!(alloc, (0, 0));
        let mut parser = ChunkParser::new();
        parser.set_chunk_size(chunk as usize).unwrap();
        let mut consume = || {
            let mut pos = 0;
            while pos < wire.len() {
                let step = parser.consume(black_box(&wire[pos..])).unwrap();
                pos += step.consumed;
                black_box(step.fragment);
            }
        };
        consume();
        let (_, alloc) = measure(consume);
        assert_eq!(alloc, (0, 0));
        let mut out = Vec::with_capacity(plan.wire_len());
        let (_, alloc) = measure(|| {
            encoder
                .encode(message.as_ref(), Default::default())
                .unwrap()
                .copy_to(&mut out)
        });
        assert_eq!(alloc, (0, 0));
        let (_, alloc) = measure(|| {
            encoder
                .encode(message.as_ref(), Default::default())
                .unwrap()
                .to_vec()
        });
        assert_eq!(alloc, (1, 0));
    }
}
#[test]
fn owned_single_chunk_needs_no_message_allocation() {
    let mut encoder = ChunkEncoder::new();
    let message = RawMessage {
        timestamp: RtmpTimestamp::new(0),
        type_id: 8,
        message_stream_id: 1,
        data: Bytes::from_static(b"abc"),
    };
    let wire = Bytes::from(
        encoder
            .encode(
                (message).clone(),
                rtmpx::EncodeOptions {
                    headers: if true {
                        rtmpx::HeaderMode::Full
                    } else {
                        rtmpx::HeaderMode::Compressed
                    },
                    drop_policy: if false {
                        rtmpx::DropPolicy::Allowed
                    } else {
                        rtmpx::DropPolicy::Never
                    },
                },
            )
            .unwrap()
            .to_vec(),
    );
    let mut decoder = MessageDecoder::new();
    decoder.decode(&mut wire.clone()).unwrap();
    let mut input = wire.clone();
    let (message, alloc) = measure(|| decoder.decode(&mut input).unwrap().unwrap());
    assert_eq!(alloc, (0, 0));
    assert_eq!(message.data.len(), 3);
}

#[path = "support/api.rs"]
mod api;
#[path = "support/sessions.rs"]
mod session_support;

#[test]
fn validated_session_relay_is_allocation_free_with_recycled_descriptors() {
    use rtmpx::{
        EnhancedValidationMode, PayloadPool, ValidatedMedia,
        sessions::{ServerEvent, ServerOutput},
    };
    for chunk in [128, 4096] {
        for (size, type_id) in [(256, 8), (16 * 1024, 9), (256 * 1024, 9)] {
            let (_, mut source, mut server, stream_id) =
                session_support::publishing_server("live", "relay");
            let client = session_support::publishing_client(stream_id, "relay");
            server.set_payload_pool(PayloadPool::new(rtmpx::PayloadPoolConfig {
                max_cached_payloads: 4,
                max_descriptors_per_payload: 8193,
            }));
            let control = source.set_chunk_size(chunk, RtmpTimestamp::new(0)).unwrap();
            server.handle_input(&control.to_vec()).unwrap();
            let mut raw = vec![0; size];
            if type_id == 9 {
                raw[..5].copy_from_slice(b"\x27\x01\0\0\0");
            } else {
                raw[..2].copy_from_slice(b"\xaf\x01");
            }
            let message = RawMessage {
                data: Bytes::from(raw),
                type_id,
                message_stream_id: stream_id,
                timestamp: RtmpTimestamp::new(10),
            };
            // Full headers make repeated input valid without resetting either session.
            let wire = Bytes::from(
                source
                    .encode(
                        (message).clone(),
                        rtmpx::EncodeOptions {
                            headers: if true {
                                rtmpx::HeaderMode::Full
                            } else {
                                rtmpx::HeaderMode::Compressed
                            },
                            drop_policy: if false {
                                rtmpx::DropPolicy::Allowed
                            } else {
                                rtmpx::DropPolicy::Never
                            },
                        },
                    )
                    .unwrap()
                    .to_vec(),
            );
            let parts: Vec<_> = (0..wire.len())
                .step_by(16384)
                .map(|start| wire.slice(start..(start + 16384).min(wire.len())))
                .collect();
            let mut server = server.into_inner();
            let mut client = client.into_inner();
            let stream = client.streams().next().unwrap().0;
            let mut sink = Vec::with_capacity(wire.len() + 1024);
            let mut relay = |sink: &mut Vec<u8>| {
                sink.clear();
                let mut forwarded = 0;
                for part in &parts {
                    let mut input = part.clone();
                    while let Some(event) = server.receive(&mut input).unwrap() {
                        let mut packet = match event {
                            ServerOutput::Event(ServerEvent::VideoDataReceived {
                                data,
                                timestamp,
                                ..
                            }) => {
                                let media = ValidatedMedia::parse_video(
                                    data.view(),
                                    EnhancedValidationMode::Strict,
                                )
                                .unwrap();
                                assert!(media.classification().coded);
                                client
                                    .send_video(stream, data, timestamp, rtmpx::DropPolicy::Never)
                                    .unwrap()
                            }
                            ServerOutput::Event(ServerEvent::AudioDataReceived {
                                data,
                                timestamp,
                                ..
                            }) => {
                                let media = ValidatedMedia::parse_audio(
                                    data.view(),
                                    EnhancedValidationMode::Strict,
                                )
                                .unwrap();
                                assert!(media.classification().coded);
                                client
                                    .send_audio(stream, data, timestamp, rtmpx::DropPolicy::Never)
                                    .unwrap()
                            }
                            _ => panic!("unexpected control response"),
                        };
                        let cursor = &mut packet;
                        while !cursor.is_complete() {
                            let mut slices = [IoSlice::new(&[]); 32];
                            let count = cursor.io_slices(&mut slices);
                            // Simulate short writes into preallocated transport storage.
                            let mut written = 0;
                            for bytes in &slices[..count] {
                                let n = bytes.len().min(137 - written);
                                sink.extend_from_slice(&bytes[..n]);
                                written += n;
                                if written == 137 {
                                    break;
                                }
                            }
                            cursor.advance(written);
                        }
                        forwarded += 1;
                    }
                }
                assert_eq!(forwarded, 1);
            };
            let (_, mut peer_encoder, mut peer, _) =
                session_support::publishing_server("live", "relay");
            let control = peer_encoder
                .set_chunk_size(4096, RtmpTimestamp::new(0))
                .unwrap();
            peer.handle_input(&control.to_vec()).unwrap();
            let mut verify = |wire: &[u8]| {
                use crate::api::sessions::{
                    ServerSessionEvent as ServerEvent, ServerSessionResult as ServerOutput,
                };
                let mut got = 0;
                for result in peer.handle_input(wire).unwrap() {
                    match result {
                        ServerOutput::Event(ServerEvent::VideoDataReceived {
                            data,
                            timestamp,
                            ..
                        })
                        | ServerOutput::Event(ServerEvent::AudioDataReceived {
                            data,
                            timestamp,
                            ..
                        }) => {
                            assert_eq!(data, message.data);
                            assert_eq!(timestamp, message.timestamp);
                            got += 1;
                        }
                        _ => panic!("unexpected peer result"),
                    }
                }
                assert_eq!(got, 1);
            };
            for _ in 0..3 {
                relay(&mut sink);
                verify(&sink);
            }
            let (_, counts) = measure(|| relay(&mut sink));
            assert_eq!(counts, (0, 0), "size={size}, chunk={chunk}");
            verify(&sink);
        }
    }
}

#[test]
fn explicit_contiguous_decoder_allocations_do_not_scale_with_read_or_chunk_count() {
    for chunk in [128, 4096] {
        let mut encoder = ChunkEncoder::new();
        encoder
            .set_chunk_size(chunk, RtmpTimestamp::new(0))
            .unwrap();
        let message = RawMessage {
            data: Bytes::from(vec![0; 256 * 1024]),
            timestamp: RtmpTimestamp::new(0),
            type_id: 9,
            message_stream_id: 1,
        };
        let wire = encoder
            .encode(
                message.as_ref(),
                rtmpx::EncodeOptions {
                    headers: rtmpx::HeaderMode::Full,
                    ..Default::default()
                },
            )
            .unwrap()
            .to_vec();
        let mut decoder = MessageDecoder::new();
        decoder.set_chunk_size(chunk as usize).unwrap();
        let mut receive = || {
            let mut got = 0;
            for mut part in wire.chunks(16384) {
                while let Some(out) = decoder.decode_slice(&mut part).unwrap() {
                    assert_eq!(out.data.into_bytes(), message.data);
                    got += 1;
                }
            }
            assert_eq!(got, 1);
        };
        receive();
        let (_, counts) = measure(receive);
        assert_eq!(counts, (1, 0));
    }
}

#[test]
fn segmented_single_track_validation_needs_no_allocations() {
    use rtmpx::{EnhancedValidationMode, Payload, ValidatedMedia};
    for (audio, wire) in [
        (true, b"\x91Opusframe".as_slice()),
        (true, b"\x97\x02\0\0\x01\x01Opusframe".as_slice()),
        (false, b"\x91hvc1\xff\xff\xffframe".as_slice()),
        (false, b"\x93av01frame".as_slice()),
        (false, b"\x90av01\x80\0\0\0".as_slice()),
    ] {
        let payload: Payload = wire.chunks(1).map(Bytes::copy_from_slice).collect();
        let (_, counts) = measure(|| {
            if audio {
                black_box(
                    ValidatedMedia::parse_audio(payload.view(), EnhancedValidationMode::Strict)
                        .unwrap(),
                );
            } else {
                black_box(
                    ValidatedMedia::parse_video(payload.view(), EnhancedValidationMode::Strict)
                        .unwrap(),
                );
            }
        });
        assert_eq!(counts, (0, 0));
    }
}

#[test]
fn concurrent_client_playback_routing_is_allocation_free_after_warmup() {
    use rtmpx::sessions::{ClientEvent, ClientOutput, ClientSession, ClientSessionConfig};
    let mut peer = crate::api::chunk_io::ChunkEncoder::new();
    let mut client = ClientSession::new(ClientSessionConfig {
        payload_pool: Some(rtmpx::PayloadPool::default()),
        ..Default::default()
    })
    .unwrap();
    fn drain(client: &mut ClientSession, bytes: Bytes) {
        let mut input = bytes;
        while client.receive(&mut input).unwrap().is_some() {}
    }
    client.connect("live").unwrap();
    drain(&mut client, Bytes::new());
    drain(
        &mut client,
        session_support::fake_connect_success(&mut peer).into(),
    );
    let one = client.play("one").unwrap();
    let two = client.play("two").unwrap();
    drain(&mut client, Bytes::new());
    drain(
        &mut client,
        session_support::fake_create_stream_success(&mut peer, 2.0, 7).into(),
    );
    drain(
        &mut client,
        session_support::fake_create_stream_success(&mut peer, 3.0, 99).into(),
    );
    let control = peer.set_chunk_size(4096, RtmpTimestamp::new(0)).unwrap();
    drain(&mut client, control.to_vec().into());
    let mut video = vec![42; 32 * 1024];
    video[..5].copy_from_slice(b"\x27\x01\0\0\0");
    let data = Bytes::from(video);
    let mut wire = Vec::new();
    for stream_id in [7, 99] {
        peer.encode(
            RawMessage {
                data: data.clone(),
                timestamp: RtmpTimestamp::new(0),
                type_id: 9,
                message_stream_id: stream_id,
            },
            rtmpx::EncodeOptions {
                headers: rtmpx::HeaderMode::Full,
                ..Default::default()
            },
        )
        .unwrap()
        .copy_to(&mut wire);
    }
    let wire = Bytes::from(wire);
    let mut receive = || {
        let mut seen = 0;
        for part in (0..wire.len()).step_by(997) {
            let mut input = wire.slice(part..(part + 997).min(wire.len()));
            while let Some(output) = client.receive(&mut input).unwrap() {
                let ClientOutput::Event(ClientEvent::VideoDataReceived {
                    stream, data: body, ..
                }) = output
                else {
                    panic!("unexpected output")
                };
                assert_eq!(stream, if seen == 0 { one } else { two });
                let media = rtmpx::ValidatedMedia::parse_video(
                    body.view(),
                    rtmpx::EnhancedValidationMode::Strict,
                )
                .unwrap();
                assert!(media.classification().coded);
                seen += 1;
            }
        }
        assert_eq!(seen, 2);
    };
    receive();
    let (_, counts) = measure(|| {
        for _ in 0..10 {
            receive();
        }
    });
    assert_eq!(counts, (0, 0));
}
