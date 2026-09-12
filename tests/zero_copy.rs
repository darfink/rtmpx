use bytes::Bytes;
use rtmpx::{
    Payload, Segments,
    chunk_io::{ChunkEncoder, ChunkParser, DecoderLimits, MessageDecoder},
    messages::RawMessage,
    time::RtmpTimestamp,
};
use std::io::{IoSlice, Read};
fn message(data: Bytes, time: u32) -> RawMessage {
    RawMessage {
        timestamp: RtmpTimestamp::new(time),
        type_id: 9,
        message_stream_id: 1,
        data,
    }
}

#[test]
fn cursor_survives_every_short_write_and_segment_boundary() {
    for len in [0, 1, 127, 128, 129, 8193] {
        let data = Bytes::from((0..len).map(|i| i as u8).collect::<Vec<_>>());
        let segmented: Payload = data.chunks(113).map(Bytes::copy_from_slice).collect();
        for chunk in [1, 128, 4096] {
            for force in [false, true] {
                let mut encoder = ChunkEncoder::new();
                encoder
                    .set_chunk_size(chunk, RtmpTimestamp::new(0))
                    .unwrap();
                let msg = RawMessage {
                    timestamp: RtmpTimestamp::new(0x1000000),
                    type_id: 9,
                    message_stream_id: 1,
                    data: &segmented,
                };
                let plan = encoder
                    .encode(
                        msg,
                        rtmpx::EncodeOptions {
                            headers: if force {
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
                    .unwrap();
                let expected = plan.to_vec();
                assert!(!expected.is_empty());
                for max_write in [1, 2, 7, 4096] {
                    let mut actual = Vec::new();
                    let mut fresh_encoder = ChunkEncoder::new();
                    fresh_encoder
                        .set_chunk_size(chunk, RtmpTimestamp::new(0))
                        .unwrap();
                    let mut cursor = fresh_encoder
                        .encode(
                            RawMessage {
                                timestamp: RtmpTimestamp::new(0x1000000),
                                type_id: 9,
                                message_stream_id: 1,
                                data: &segmented,
                            },
                            rtmpx::EncodeOptions {
                                headers: if force {
                                    rtmpx::HeaderMode::Full
                                } else {
                                    rtmpx::HeaderMode::Compressed
                                },
                                ..Default::default()
                            },
                        )
                        .unwrap();
                    while !cursor.is_complete() {
                        let mut slices = [IoSlice::new(&[]); 7];
                        let count = cursor.io_slices(&mut slices);
                        assert!(count > 0);
                        let mut written = 0;
                        for slice in &slices[..count] {
                            let n = slice.len().min(max_write - written);
                            actual.extend_from_slice(&slice[..n]);
                            written += n;
                            if written == max_write {
                                break;
                            }
                        }
                        cursor.advance(written);
                    }
                    assert_eq!(actual, expected);
                }
                let mut decoder = MessageDecoder::new();
                decoder.set_chunk_size(chunk as usize).unwrap();
                let received = decoder.decode(&mut Bytes::from(expected)).unwrap().unwrap();
                assert_eq!(received.data.into_bytes(), data);
            }
        }
    }
}

#[test]
fn every_transport_split_preserves_timestamps_and_payload_ownership() {
    for chunk in [128, 4096] {
        for force in [false, true] {
            let body = Bytes::from(vec![17; 300]);
            let mut encoder = ChunkEncoder::new();
            encoder
                .set_chunk_size(chunk, RtmpTimestamp::new(0))
                .unwrap();
            let mut wire = Vec::new();
            for time in [u32::MAX - 10, 20, 0x1000020, 0x2000020] {
                encoder
                    .encode(
                        message(body.clone(), time),
                        rtmpx::EncodeOptions {
                            headers: if force {
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
                    .copy_to(&mut wire);
            }
            let wire = Bytes::from(wire);
            for split in 0..=wire.len() {
                let mut decoder = MessageDecoder::new();
                decoder.set_chunk_size(chunk as usize).unwrap();
                let mut outputs = Vec::new();
                for mut part in [wire.slice(..split), wire.slice(split..)] {
                    while let Some(payload) = decoder.decode(&mut part).unwrap() {
                        for segment in payload.data.segments() {
                            let p = segment.as_ptr() as usize;
                            assert!(
                                p >= wire.as_ptr() as usize
                                    && p + segment.len() <= wire.as_ptr() as usize + wire.len()
                            );
                        }
                        outputs.push((payload.timestamp.value, payload.data.into_bytes()));
                    }
                    assert!(part.is_empty());
                }
                assert!(decoder.is_idle());
                assert_eq!(
                    outputs.iter().map(|x| x.0).collect::<Vec<_>>(),
                    [u32::MAX - 10, 20, 0x1000020, 0x2000020]
                );
                assert!(outputs.iter().all(|x| x.1 == body));
            }
        }
    }
}

#[test]
fn borrowed_parser_emits_partial_chunk_data_without_buffering() {
    let mut encoder = ChunkEncoder::new();
    let wire = encoder
        .encode(
            (message(Bytes::from(vec![42; 300]), 10)).clone(),
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
        .to_vec();
    let mut parser = ChunkParser::new();
    let mut recovered = Vec::new();
    for byte in wire.chunks(1) {
        let mut position = 0;
        while position < byte.len() {
            let step = parser.consume(&byte[position..]).unwrap();
            assert!(step.consumed > 0);
            if let Some(fragment) = step.fragment {
                assert_eq!(fragment.offset, recovered.len());
                assert_eq!(
                    fragment.data.as_ptr(),
                    byte[position + step.consumed - fragment.data.len()..].as_ptr()
                );
                recovered.extend_from_slice(fragment.data);
            }
            position += step.consumed;
        }
    }
    assert_eq!(recovered, vec![42; 300]);
    assert!(parser.is_idle());
}

#[test]
fn interleaving_abort_and_fragment_limits() {
    let mut encoder = ChunkEncoder::new();
    let first = encoder
        .encode(
            (message(Bytes::from(vec![1; 300]), 0)).clone(),
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
        .to_vec();
    let control = encoder
        .encode(
            (RawMessage {
                timestamp: RtmpTimestamp::new(0),
                type_id: 2,
                message_stream_id: 0,
                data: Bytes::from_static(&[0, 0, 0, 4]),
            })
            .clone(),
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
        .to_vec();
    let mut wire = Bytes::from([&first[..140], &control].concat());
    let mut decoder = MessageDecoder::new();
    let abort = decoder.decode(&mut wire).unwrap().unwrap();
    assert_eq!(abort.type_id, 2);
    decoder.abort_chunk_stream(4);
    let second = encoder
        .encode(
            (message(Bytes::from_static(b"next"), 1)).clone(),
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
        .to_vec();
    assert_eq!(
        decoder
            .decode(&mut Bytes::from(second))
            .unwrap()
            .unwrap()
            .data
            .into_bytes(),
        Bytes::from_static(b"next")
    );
    let mut decoder =
        MessageDecoder::with_limits(DecoderLimits::default().with_maximum_fragments_per_message(2));
    assert!(decoder.decode(&mut Bytes::from(first)).is_err());
}

#[test]
fn payload_reader_crosses_segments_and_ignores_empty_parts() {
    let p: Payload = [
        Bytes::new(),
        Bytes::from_static(b"abc"),
        Bytes::new(),
        Bytes::from_static(b"def"),
    ]
    .into_iter()
    .collect();
    assert_eq!(p.segment_count(), 2);
    let mut output = [0; 6];
    p.reader().read_exact(&mut output).unwrap();
    assert_eq!(&output, b"abcdef");
}

#[test]
fn size_change_is_sent_with_old_chunk_size_and_dropping_does_not_break_history() {
    let mut serializer = ChunkEncoder::new();
    let mut decoder = MessageDecoder::new();
    let change = serializer.set_chunk_size(1, RtmpTimestamp::new(0)).unwrap();
    let control = decoder
        .decode(&mut Bytes::from(change.to_vec()))
        .unwrap()
        .unwrap();
    assert_eq!(control.data.to_bytes().as_ref(), &[0, 0, 0, 1]);
    decoder.set_chunk_size(1).unwrap();
    let change = serializer
        .set_chunk_size(4096, RtmpTimestamp::new(0))
        .unwrap();
    let control = decoder
        .decode(&mut Bytes::from(change.to_vec()))
        .unwrap()
        .unwrap();
    assert_eq!(control.data.to_bytes().as_ref(), &[0, 0, 16, 0]);
    decoder.set_chunk_size(4096).unwrap();
    let original = message(Bytes::from_static(b"aaa"), 10);
    let first = serializer
        .encode(
            (original).as_ref(),
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
        .unwrap();
    decoder
        .decode(&mut Bytes::from(first.to_vec()))
        .unwrap()
        .unwrap();
    let discarded = serializer
        .encode(
            message(Bytes::from_static(b"bbb"), 20),
            rtmpx::EncodeOptions {
                headers: if false {
                    rtmpx::HeaderMode::Full
                } else {
                    rtmpx::HeaderMode::Compressed
                },
                drop_policy: if true {
                    rtmpx::DropPolicy::Allowed
                } else {
                    rtmpx::DropPolicy::Never
                },
            },
        )
        .unwrap();
    assert!(discarded.can_drop());
    drop(discarded);
    let next = serializer
        .encode(
            message(Bytes::from_static(b"ccc"), 30),
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
        .unwrap();
    let result = decoder
        .decode(&mut Bytes::from(next.to_vec()))
        .unwrap()
        .unwrap();
    assert_eq!(result.timestamp.value, 30);
    assert_eq!(result.data.to_bytes().as_ref(), b"ccc");
}

#[test]
fn truncated_headers_and_payloads_remain_non_idle_and_limits_apply_to_owned_parts() {
    let mut serializer = ChunkEncoder::new();
    let wire = Bytes::from(
        serializer
            .encode(
                (message(Bytes::from_static(b"abc"), 0)).clone(),
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
    for end in 1..wire.len() {
        let mut decoder = MessageDecoder::new();
        assert!(decoder.decode(&mut wire.slice(..end)).unwrap().is_none());
        assert!(!decoder.is_idle());
    }
    let mut decoder =
        MessageDecoder::with_limits(DecoderLimits::default().with_maximum_buffered_bytes(2));
    assert!(decoder.decode(&mut wire.clone()).is_err());
    let mut decoder =
        MessageDecoder::with_limits(DecoderLimits::default().with_maximum_partial_messages(0));
    assert!(decoder.decode(&mut wire.slice(..13)).is_err());
}

#[test]
fn mixed_receive_ownership_preserves_partial_messages_and_limits() {
    use rtmpx::chunk_io::DecoderLimits;
    let message = RawMessage {
        timestamp: RtmpTimestamp::new(10),
        type_id: 9,
        message_stream_id: 1,
        data: Bytes::from(vec![0x27; 1025]),
    };
    let mut serializer = ChunkEncoder::new();
    let wire = Bytes::from(
        serializer
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
    for owned_first in [false, true] {
        let mut decoder = MessageDecoder::new();
        let mut result = None;
        for (index, start) in (0..wire.len()).step_by(37).enumerate() {
            let mut part = wire.slice(start..(start + 37).min(wire.len()));
            let got = if (index % 2 == 0) == owned_first {
                decoder.decode(&mut part).unwrap()
            } else {
                decoder.decode_slice(&mut part.as_ref()).unwrap()
            };
            if got.is_some() {
                assert!(result.is_none());
                result = got;
            }
        }
        assert_eq!(result.unwrap().data.into_bytes(), message.data);
        assert!(decoder.is_idle());
    }
    let mut config = DecoderLimits::default();
    config.maximum_fragments_per_message = 0;
    let mut decoder = MessageDecoder::with_limits(config);
    let tiny = RawMessage {
        data: Bytes::from_static(b"x"),
        timestamp: message.timestamp,
        type_id: message.type_id,
        message_stream_id: message.message_stream_id,
    };
    let mut tiny_wire = Bytes::from(
        serializer
            .encode(
                (tiny).clone(),
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
    assert!(decoder.decode(&mut tiny_wire).is_err());
    // Borrowed assembly reserves the full body under the connection budget.
    config.maximum_fragments_per_message = 1000;
    config.maximum_buffered_bytes = 1024;
    let mut decoder = MessageDecoder::with_limits(config);
    assert!(decoder.decode_slice(&mut wire.as_ref()).is_err());
}
