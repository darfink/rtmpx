use bytes::Bytes;
use rtmpx::{EnhancedValidationMode, MediaInterpretation, Payload, Segments, ValidatedMedia};

#[test]
fn every_split_and_truncated_prefix_matches_contiguous_validation() {
    let corpus: &[&[u8]] = &[
        b"\xaf\x00\x11\x88",
        b"\xaf\x01sample",
        b"\x2f\xff\xfb",
        b"\x17\x01\xff\xff\xffsample",
        b"\x17\x00\0\0\0\x01\0\0\0\0\0\0",
        b"\x90Opusconfig",
        b"\x91Opusframe",
        b"\x92Opus",
        b"\x94mp4a\x00\x02",
        b"\x94mp4a\x02\x02\x00\x01",
        b"\x95\x02Opus\x00",
        b"\x95\x11Opus\x00\0\0\x03one\x01\0\0\x03two",
        b"\x97\x02\0\0\x01\x02Opus",
        b"\x90vp08config",
        b"\x91av01frame",
        b"\x91hvc1\xff\xff\xffframe",
        b"\x92hvc1",
        b"\x93av01frame",
        b"\x94av01",
        b"\x95vp08descriptor",
        b"\x96\x02hvc1\x00",
        b"\x96\x23av01\x00\0\0\x03onehvc1\x01\0\0\x03two",
        b"\x97\x02\0\0\x01\x02hvc1",
        b"\x92zzzz",
        b"\x93Opus",
        b"\x97\x00\x2a\x12Opus",
        b"\x98hvc1",
        b"\x92vvc1",
        b"\xd1\x01",
        b"\x90av01\x80\0\0\0",
        b"\x90avc1\x01\0\0\0\0\0\0",
        b"\x90hvc1\x01\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0",
    ];
    for fixture in corpus {
        for len in 0..=fixture.len() {
            let raw = Bytes::copy_from_slice(&fixture[..len]);
            for split in 0..=len {
                let payload: Payload = [raw.slice(..split), Bytes::new(), raw.slice(split..)]
                    .into_iter()
                    .collect();
                for mode in [
                    EnhancedValidationMode::Strict,
                    EnhancedValidationMode::Passthrough,
                ] {
                    macro_rules! compare {
                        ($parse:ident) => {
                            let contiguous = ValidatedMedia::$parse(raw.clone(), mode);
                            let segmented = ValidatedMedia::$parse(payload.view(), mode);
                            match (contiguous, segmented) {
                                (Ok(a), Ok(b)) => {
                                    assert_eq!(a.classification(), b.classification());
                                    match (a.interpretation(), b.interpretation()) {
                                        (
                                            MediaInterpretation::Parsed(a),
                                            MediaInterpretation::Parsed(b),
                                        ) => assert_eq!(a.header, b.header),
                                        (
                                            MediaInterpretation::Opaque { reason: a, .. },
                                            MediaInterpretation::Opaque { reason: b, .. },
                                        ) => assert_eq!(a, b),
                                        _ => panic!("interpretations disagree"),
                                    }
                                }
                                (Err(a), Err(b)) => assert_eq!(a.to_string(), b.to_string()),
                                _ => panic!("validation disagrees at len={len} split={split}"),
                            }
                        };
                    }
                    compare!(parse_audio);
                    compare!(parse_video);
                }
            }
        }
    }
}

#[test]
fn parsed_body_borrows_original_segments() {
    use rtmpx::flv::{EnhancedVideoBody, VideoPacket, VideoTagBody};
    let payload: Payload = [
        Bytes::from_static(b"\x91hv"),
        Bytes::from_static(b"c1\xff"),
        Bytes::from_static(b"\xff\xffone"),
        Bytes::from_static(b"two"),
    ]
    .into_iter()
    .collect();
    let media =
        ValidatedMedia::parse_video(payload.view(), EnhancedValidationMode::Strict).unwrap();
    let MediaInterpretation::Parsed(parsed) = media.interpretation() else {
        panic!()
    };
    let VideoTagBody::Enhanced(EnhancedVideoBody::NoMultitrack {
        packet:
            VideoPacket::CodedFrames {
                composition_time_offset,
                data,
            },
        ..
    }) = &parsed.body
    else {
        panic!()
    };
    assert_eq!(*composition_time_offset, -1);
    assert_eq!(data.len(), 6);
    assert_eq!(data.segment_count(), 2);
    assert_eq!(data.segment(0), b"one");
    assert_eq!(data.segment(0).as_ptr(), payload.segment(2)[2..].as_ptr());
    assert_eq!(data.segment(1).as_ptr(), payload.segment(3).as_ptr());
}
