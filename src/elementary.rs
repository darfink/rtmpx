//! Elementary media and control events from validated RTMP messages.
//!
//! Codec configurations and coded payloads retain slices of the original input.
//! RTMPX removes FLV framing but does not validate codec decodability or split
//! messages into individual codec frames. Event visitors also expose sequence
//! ends and typed reasons for skipped messages.

use bytes::Bytes;

use crate::{
    MediaInterpretation, ParsedAudio, ParsedVideo, ValidatedMedia,
    flv::{
        AudioFourCc, AudioPacket, AudioTagBody, EnhancedAudioBody, EnhancedVideoBody,
        LegacyAudioBody, LegacyAvcPacket, LegacyVideoBody, LegacyVideoHeader,
        VIDEO_FRAME_GENERATED_KEY, VIDEO_FRAME_KEY, VideoFourCc, VideoPacket, VideoTagBody,
        VideoTagHeaderData,
    },
    media::MediaValidationError,
};

// Codecs this ingest path can present as elementary access units.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ElementaryCodec {
    Avc,
    Hevc,
    Av1,
    Aac,
    Opus,
    Flac,
    Ac3,
    Eac3,
    Mp3,
    Vp8,
    Vp9,
    Vvc,
}

impl ElementaryCodec {
    pub fn is_video(self) -> bool {
        matches!(
            self,
            Self::Avc | Self::Hevc | Self::Av1 | Self::Vp8 | Self::Vp9 | Self::Vvc
        )
    }

    pub fn is_audio(self) -> bool {
        matches!(
            self,
            Self::Aac | Self::Opus | Self::Flac | Self::Ac3 | Self::Eac3 | Self::Mp3
        )
    }
}

// One validated RTMP message, reduced to decoder config or a coded sample.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum ElementaryUnit {
    #[non_exhaustive]
    Configuration {
        codec: ElementaryCodec,
        extradata: Bytes,
        // Enhanced RTMP track id when the message names one; legacy is None.
        track_id: Option<u8>,
    },
    /// FLAC coded payloads can contain multiple frames. Consumers must parse
    /// their frame boundaries and sample counts; RTMPX preserves the message.
    #[non_exhaustive]
    Sample {
        codec: ElementaryCodec,
        payload: Bytes,
        keyframe: bool,
        // Composition offset in milliseconds on the RTMP clock. Audio is 0.
        composition_time_offset: i32,
        track_id: Option<u8>,
    },
}

impl ElementaryUnit {
    pub fn codec(&self) -> ElementaryCodec {
        match self {
            Self::Configuration { codec, .. } | Self::Sample { codec, .. } => *codec,
        }
    }

    // Enhanced RTMP track id when the message names one; legacy is None.
    pub fn track_id(&self) -> Option<u8> {
        match self {
            Self::Configuration { track_id, .. } | Self::Sample { track_id, .. } => *track_id,
        }
    }
}

/// Wire codec identity for messages without an elementary mapping.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ElementaryCodecId {
    AudioFourCc([u8; 4]),
    VideoFourCc([u8; 4]),
    LegacyAudio(u8),
    LegacyVideo(u8),
}

/// Why a parsed message does not yield an elementary unit.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ElementarySkipReason {
    UnsupportedCodec,
    UnknownPacketType(u8),
    VideoCommand,
    Metadata,
    MultichannelConfig,
    Mpeg2TsConfiguration,
}

/// Ordered media and control observations from one validated RTMP message.
/// Opaque or malformed input remains an error, not a skipped event.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum ElementaryEvent {
    Unit(ElementaryUnit),
    #[non_exhaustive]
    SequenceEnd {
        codec: ElementaryCodec,
        track_id: Option<u8>,
    },
    #[non_exhaustive]
    Skipped {
        reason: ElementarySkipReason,
        codec_id: Option<ElementaryCodecId>,
        track_id: Option<u8>,
    },
}

fn skipped(
    reason: ElementarySkipReason,
    codec_id: Option<ElementaryCodecId>,
    track_id: Option<u8>,
) -> ElementaryEvent {
    ElementaryEvent::Skipped {
        reason,
        codec_id,
        track_id,
    }
}

impl ValidatedMedia<ParsedAudio> {
    /// Collect configurations and samples. Use `elementary_events` to observe
    /// sequence ends and messages intentionally omitted from this media-only API.
    pub fn elementary_units(&self) -> Result<Vec<ElementaryUnit>, MediaValidationError> {
        let mut output = Vec::new();
        self.visit_elementary_units(|unit| output.push(unit))?;
        Ok(output)
    }

    /// Emit mapped units without allocating a result collection.
    pub fn visit_elementary_units(
        &self,
        mut emit: impl FnMut(ElementaryUnit),
    ) -> Result<(), MediaValidationError> {
        self.visit_elementary_events(|event| {
            if let ElementaryEvent::Unit(unit) = event {
                emit(unit);
            }
        })
    }

    /// Collect media units, sequence ends, and typed skipped-message observations.
    pub fn elementary_events(&self) -> Result<Vec<ElementaryEvent>, MediaValidationError> {
        let mut events = Vec::new();
        self.visit_elementary_events(|event| events.push(event))?;
        Ok(events)
    }

    /// Visit events in wire track order without allocating a result collection.
    pub fn visit_elementary_events(
        &self,
        mut emit: impl FnMut(ElementaryEvent),
    ) -> Result<(), MediaValidationError> {
        match self.interpretation() {
            MediaInterpretation::Opaque { reason } => Err(MediaValidationError::Malformed {
                kind: "audio",
                reason: reason.clone(),
            }),
            MediaInterpretation::Parsed(parsed) => match &parsed.body {
                AudioTagBody::Legacy(LegacyAudioBody::AacSequenceHeader(_)) => {
                    emit(ElementaryEvent::Unit(ElementaryUnit::Configuration {
                        codec: ElementaryCodec::Aac,
                        extradata: slice_after(self.raw(), LEGACY_AAC_HEADER_BYTES, "audio")?,
                        track_id: None,
                    }));
                    Ok(())
                }
                AudioTagBody::Legacy(LegacyAudioBody::AacRaw(_)) => {
                    emit(ElementaryEvent::Unit(ElementaryUnit::Sample {
                        codec: ElementaryCodec::Aac,
                        payload: slice_after(self.raw(), LEGACY_AAC_HEADER_BYTES, "audio")?,
                        keyframe: true,
                        composition_time_offset: 0,
                        track_id: None,
                    }));
                    Ok(())
                }
                AudioTagBody::Legacy(body) => {
                    let crate::flv::AudioTagHeader::Legacy(header) = &parsed.header else {
                        unreachable!("legacy body has legacy header")
                    };
                    if matches!(header.sound_format, 2 | 14) {
                        emit(ElementaryEvent::Unit(ElementaryUnit::Sample {
                            codec: ElementaryCodec::Mp3,
                            payload: slice_after(self.raw(), 1, "audio")?,
                            keyframe: true,
                            composition_time_offset: 0,
                            track_id: None,
                        }));
                    } else {
                        let reason = match body {
                            LegacyAudioBody::AacUnknown { packet_type, .. } => {
                                ElementarySkipReason::UnknownPacketType(*packet_type)
                            }
                            _ => ElementarySkipReason::UnsupportedCodec,
                        };
                        emit(skipped(
                            reason,
                            Some(ElementaryCodecId::LegacyAudio(header.sound_format)),
                            None,
                        ));
                    }
                    Ok(())
                }
                AudioTagBody::Enhanced(body) => enhanced_audio(body, &mut emit),
            },
        }
    }

    /// Return zero or one mapped unit. Multiple units produce an error.
    pub fn elementary_unit(&self) -> Result<Option<ElementaryUnit>, MediaValidationError> {
        let mut first = None;
        let mut count = 0;
        self.visit_elementary_units(|unit| {
            count += 1;
            if first.is_none() {
                first = Some(unit);
            }
        })?;
        if count > 1 {
            return Err(MediaValidationError::MultipleUnits { count });
        }
        Ok(first)
    }
}

impl ValidatedMedia<ParsedVideo> {
    // Maps a validated video message onto elementary units.
    //
    // A packed Enhanced ManyTracks tag yields one unit per mapped track.
    pub fn elementary_units(&self) -> Result<Vec<ElementaryUnit>, MediaValidationError> {
        let mut output = Vec::new();
        self.visit_elementary_units(|unit| output.push(unit))?;
        Ok(output)
    }

    /// Emit mapped units without allocating a result collection.
    pub fn visit_elementary_units(
        &self,
        mut emit: impl FnMut(ElementaryUnit),
    ) -> Result<(), MediaValidationError> {
        self.visit_elementary_events(|event| {
            if let ElementaryEvent::Unit(unit) = event {
                emit(unit);
            }
        })
    }

    /// Collect media units, sequence ends, and typed skipped-message observations.
    pub fn elementary_events(&self) -> Result<Vec<ElementaryEvent>, MediaValidationError> {
        let mut events = Vec::new();
        self.visit_elementary_events(|event| events.push(event))?;
        Ok(events)
    }

    /// Visit events in wire track order without allocating a result collection.
    pub fn visit_elementary_events(
        &self,
        mut emit: impl FnMut(ElementaryEvent),
    ) -> Result<(), MediaValidationError> {
        match self.interpretation() {
            MediaInterpretation::Opaque { reason } => Err(MediaValidationError::Malformed {
                kind: "video",
                reason: reason.clone(),
            }),
            MediaInterpretation::Parsed(parsed) => {
                let keyframe = parsed.header.frame_type == VIDEO_FRAME_KEY
                    || parsed.header.frame_type == VIDEO_FRAME_GENERATED_KEY;
                match (&parsed.header.data, &parsed.body) {
                    (
                        VideoTagHeaderData::Legacy(LegacyVideoHeader::AvcPacket(
                            LegacyAvcPacket::SequenceHeader,
                        )),
                        _,
                    ) => {
                        emit(ElementaryEvent::Unit(ElementaryUnit::Configuration {
                            codec: ElementaryCodec::Avc,
                            extradata: slice_after(self.raw(), LEGACY_AVC_HEADER_BYTES, "video")?,
                            track_id: None,
                        }));
                        Ok(())
                    }
                    (
                        VideoTagHeaderData::Legacy(LegacyVideoHeader::AvcPacket(
                            LegacyAvcPacket::Nalu {
                                composition_time_offset,
                            },
                        )),
                        VideoTagBody::Legacy(LegacyVideoBody::Other(_)),
                    ) => {
                        emit(ElementaryEvent::Unit(ElementaryUnit::Sample {
                            codec: ElementaryCodec::Avc,
                            payload: slice_after(self.raw(), LEGACY_AVC_HEADER_BYTES, "video")?,
                            keyframe,
                            composition_time_offset: *composition_time_offset,
                            track_id: None,
                        }));
                        Ok(())
                    }
                    (_, VideoTagBody::Enhanced(body)) => enhanced_video(body, keyframe, &mut emit),
                    (VideoTagHeaderData::Legacy(header), _) => {
                        let event = match header {
                            LegacyVideoHeader::AvcPacket(LegacyAvcPacket::EndOfSequence) => {
                                ElementaryEvent::SequenceEnd {
                                    codec: ElementaryCodec::Avc,
                                    track_id: None,
                                }
                            }
                            LegacyVideoHeader::AvcPacket(LegacyAvcPacket::Unknown {
                                packet_type,
                                ..
                            }) => skipped(
                                ElementarySkipReason::UnknownPacketType(*packet_type),
                                Some(ElementaryCodecId::LegacyVideo(7)),
                                None,
                            ),
                            LegacyVideoHeader::VideoCommand(_) => {
                                skipped(ElementarySkipReason::VideoCommand, None, None)
                            }
                            LegacyVideoHeader::Other { codec_id } => skipped(
                                ElementarySkipReason::UnsupportedCodec,
                                Some(ElementaryCodecId::LegacyVideo(*codec_id)),
                                None,
                            ),
                            _ => unreachable!("parsed AVC packet and body agree"),
                        };
                        emit(event);
                        Ok(())
                    }
                    _ => unreachable!("parsed video header and body agree"),
                }
            }
        }
    }

    /// Return zero or one mapped unit. Multiple units produce an error.
    pub fn elementary_unit(&self) -> Result<Option<ElementaryUnit>, MediaValidationError> {
        let mut first = None;
        let mut count = 0;
        self.visit_elementary_units(|unit| {
            count += 1;
            if first.is_none() {
                first = Some(unit);
            }
        })?;
        if count > 1 {
            return Err(MediaValidationError::MultipleUnits { count });
        }
        Ok(first)
    }
}

const LEGACY_AAC_HEADER_BYTES: usize = 2;
const LEGACY_AVC_HEADER_BYTES: usize = 5;

fn slice_after(
    raw: &Bytes,
    header_bytes: usize,
    kind: &'static str,
) -> Result<Bytes, MediaValidationError> {
    if raw.len() < header_bytes {
        return Err(MediaValidationError::Malformed {
            kind,
            reason: "tag is shorter than its FLV header".into(),
        });
    }
    Ok(raw.slice(header_bytes..))
}

fn enhanced_audio(
    body: &EnhancedAudioBody,
    emit: &mut impl FnMut(ElementaryEvent),
) -> Result<(), MediaValidationError> {
    match body {
        EnhancedAudioBody::NoMultitrack { four_cc, packet } => {
            emit(audio_packet(*four_cc, packet, None));
            Ok(())
        }
        EnhancedAudioBody::ManyTracks(tracks) => {
            for track in tracks {
                emit(audio_packet(
                    track.four_cc,
                    &track.packet,
                    Some(track.track_id),
                ));
            }
            Ok(())
        }
    }
}

fn enhanced_video(
    body: &EnhancedVideoBody,
    keyframe: bool,
    emit: &mut impl FnMut(ElementaryEvent),
) -> Result<(), MediaValidationError> {
    match body {
        EnhancedVideoBody::Command => {
            emit(skipped(ElementarySkipReason::VideoCommand, None, None));
            Ok(())
        }
        EnhancedVideoBody::NoMultitrack { four_cc, packet } => {
            emit(video_packet(*four_cc, packet, keyframe, None));
            Ok(())
        }
        EnhancedVideoBody::ManyTracks(tracks) => {
            for track in tracks {
                emit(video_packet(
                    track.four_cc,
                    &track.packet,
                    keyframe,
                    Some(track.track_id),
                ));
            }
            Ok(())
        }
    }
}

fn audio_packet(
    four_cc: AudioFourCc,
    packet: &AudioPacket,
    track_id: Option<u8>,
) -> ElementaryEvent {
    let codec = match four_cc.0 {
        v if v == *b"mp4a" => ElementaryCodec::Aac,
        v if v == *b"Opus" => ElementaryCodec::Opus,
        v if v == *b"fLaC" => ElementaryCodec::Flac,
        v if v == *b"ac-3" => ElementaryCodec::Ac3,
        v if v == *b"ec-3" => ElementaryCodec::Eac3,
        v if v == *b".mp3" => ElementaryCodec::Mp3,
        _ => {
            return skipped(
                ElementarySkipReason::UnsupportedCodec,
                Some(ElementaryCodecId::AudioFourCc(four_cc.0)),
                track_id,
            );
        }
    };
    match packet {
        AudioPacket::SequenceStart(data) => ElementaryEvent::Unit(ElementaryUnit::Configuration {
            codec,
            extradata: data.clone(),
            track_id,
        }),
        AudioPacket::CodedFrames(data) => ElementaryEvent::Unit(ElementaryUnit::Sample {
            codec,
            payload: data.clone(),
            keyframe: true,
            composition_time_offset: 0,
            track_id,
        }),
        AudioPacket::SequenceEnd => ElementaryEvent::SequenceEnd { codec, track_id },
        AudioPacket::MultichannelConfig { .. } => skipped(
            ElementarySkipReason::MultichannelConfig,
            Some(ElementaryCodecId::AudioFourCc(four_cc.0)),
            track_id,
        ),
        AudioPacket::Unknown { packet_type, .. } => skipped(
            ElementarySkipReason::UnknownPacketType(*packet_type),
            Some(ElementaryCodecId::AudioFourCc(four_cc.0)),
            track_id,
        ),
    }
}

fn video_packet(
    four_cc: VideoFourCc,
    packet: &VideoPacket,
    keyframe: bool,
    track_id: Option<u8>,
) -> ElementaryEvent {
    let codec = match four_cc.0 {
        v if v == *b"avc1" => ElementaryCodec::Avc,
        v if v == *b"hvc1" => ElementaryCodec::Hevc,
        v if v == *b"av01" => ElementaryCodec::Av1,
        v if v == *b"vp08" => ElementaryCodec::Vp8,
        v if v == *b"vp09" => ElementaryCodec::Vp9,
        v if v == *b"vvc1" => ElementaryCodec::Vvc,
        _ => {
            return skipped(
                ElementarySkipReason::UnsupportedCodec,
                Some(ElementaryCodecId::VideoFourCc(four_cc.0)),
                track_id,
            );
        }
    };
    match packet {
        VideoPacket::SequenceStart(data) => ElementaryEvent::Unit(ElementaryUnit::Configuration {
            codec,
            extradata: data.clone(),
            track_id,
        }),
        VideoPacket::CodedFrames {
            composition_time_offset,
            data,
        } => ElementaryEvent::Unit(ElementaryUnit::Sample {
            codec,
            payload: data.clone(),
            keyframe,
            composition_time_offset: *composition_time_offset,
            track_id,
        }),
        VideoPacket::CodedFramesX(data) => ElementaryEvent::Unit(ElementaryUnit::Sample {
            codec,
            payload: data.clone(),
            keyframe,
            composition_time_offset: 0,
            track_id,
        }),
        VideoPacket::SequenceEnd => ElementaryEvent::SequenceEnd { codec, track_id },
        VideoPacket::Metadata(_) => skipped(
            ElementarySkipReason::Metadata,
            Some(ElementaryCodecId::VideoFourCc(four_cc.0)),
            track_id,
        ),
        VideoPacket::Mpeg2TsSequenceStart(_) => skipped(
            ElementarySkipReason::Mpeg2TsConfiguration,
            Some(ElementaryCodecId::VideoFourCc(four_cc.0)),
            track_id,
        ),
        VideoPacket::Unknown { packet_type, .. } => skipped(
            ElementarySkipReason::UnknownPacketType(*packet_type),
            Some(ElementaryCodecId::VideoFourCc(four_cc.0)),
            track_id,
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::EnhancedValidationMode;

    #[test]
    fn legacy_aac_sequence_header_is_the_audio_specific_config() {
        let raw = Bytes::from_static(&[0xaf, 0x00, 0x11, 0x88]);
        let media =
            ValidatedMedia::parse_audio(raw, EnhancedValidationMode::Strict).expect("legacy AAC");
        match media.elementary_unit().expect("maps") {
            Some(ElementaryUnit::Configuration {
                codec: ElementaryCodec::Aac,
                extradata,
                track_id: None,
            }) => assert_eq!(extradata.as_ref(), &[0x11, 0x88]),
            other => panic!("expected AAC config, got {other:?}"),
        }
    }

    #[test]
    fn legacy_aac_raw_drops_the_flv_packet_type() {
        let raw = Bytes::from_static(&[0xaf, 0x01, 0xde, 0x02, 0x00]);
        let media =
            ValidatedMedia::parse_audio(raw, EnhancedValidationMode::Strict).expect("legacy AAC");
        match media.elementary_unit().expect("maps") {
            Some(ElementaryUnit::Sample {
                codec: ElementaryCodec::Aac,
                payload,
                keyframe: true,
                composition_time_offset: 0,
                track_id: None,
            }) => assert_eq!(payload.as_ref(), &[0xde, 0x02, 0x00]),
            other => panic!("expected AAC sample, got {other:?}"),
        }
    }

    #[test]
    fn legacy_avc_nalu_is_length_prefixed_and_keeps_signed_cts() {
        let mut raw = vec![0x17, 0x01, 0xff, 0xff, 0xff];
        raw.extend_from_slice(&[0x00, 0x00, 0x00, 0x04, 0x65, 0x88, 0x84, 0x05]);
        let media = ValidatedMedia::parse_video(Bytes::from(raw), EnhancedValidationMode::Strict)
            .expect("legacy AVC");
        match media.elementary_unit().expect("maps") {
            Some(ElementaryUnit::Sample {
                codec: ElementaryCodec::Avc,
                payload,
                keyframe: true,
                composition_time_offset,
                track_id: None,
            }) => {
                assert_eq!(
                    payload.as_ref(),
                    &[0x00, 0x00, 0x00, 0x04, 0x65, 0x88, 0x84, 0x05]
                );
                assert_eq!(composition_time_offset, -1);
            }
            other => panic!("expected AVC sample, got {other:?}"),
        }
    }

    #[test]
    fn legacy_mp3_is_mapped() {
        // Legacy MP3: sound format 2, 44.1 kHz, 16-bit, stereo.
        let mp3 = Bytes::from_static(&[0x2f, 0xff, 0xfb]);
        let media =
            ValidatedMedia::parse_audio(mp3, EnhancedValidationMode::Strict).expect("legacy MP3");
        assert!(matches!(
            media.elementary_unit().expect("maps"),
            Some(ElementaryUnit::Sample {
                codec: ElementaryCodec::Mp3,
                ..
            })
        ));
    }

    #[test]
    fn enhanced_aac_sequence_start_is_header_data() {
        let mut raw = b"\x90mp4a".to_vec();
        raw.extend_from_slice(&[0x11, 0x88]);
        let media = ValidatedMedia::parse_audio(Bytes::from(raw), EnhancedValidationMode::Strict)
            .expect("enhanced AAC");
        match media.elementary_unit().expect("maps") {
            Some(ElementaryUnit::Configuration {
                codec: ElementaryCodec::Aac,
                extradata,
                ..
            }) => assert_eq!(extradata.as_ref(), &[0x11, 0x88]),
            other => panic!("expected AAC config, got {other:?}"),
        }
    }

    #[test]
    fn one_track_aac_names_the_enhanced_track_id() {
        let mut raw = vec![0x95, 0x00, b'm', b'p', b'4', b'a', 2];
        raw.extend_from_slice(&[0x11, 0x88]);
        let media = ValidatedMedia::parse_audio(Bytes::from(raw), EnhancedValidationMode::Strict)
            .expect("OneTrack AAC");
        match media.elementary_unit().expect("maps") {
            Some(ElementaryUnit::Configuration {
                codec: ElementaryCodec::Aac,
                track_id: Some(2),
                extradata,
            }) => assert_eq!(extradata.as_ref(), &[0x11, 0x88]),
            other => panic!("expected track 2 AAC config, got {other:?}"),
        }
    }

    #[test]
    fn packed_many_tracks_yields_one_unit_per_mapped_track() {
        let mut raw = vec![0x95, 0x10, b'm', b'p', b'4', b'a'];
        for (id, payload) in [(1_u8, [0x11, 0x88]), (3, [0x12, 0x10])] {
            raw.push(id);
            raw.extend_from_slice(&[0x00, 0x00, payload.len() as u8]);
            raw.extend_from_slice(&payload);
        }
        let media = ValidatedMedia::parse_audio(Bytes::from(raw), EnhancedValidationMode::Strict)
            .expect("ManyTracks AAC");
        let units = media.elementary_units().expect("maps");
        assert_eq!(units.len(), 2);
        assert_eq!(units[0].track_id(), Some(1));
        assert_eq!(units[1].track_id(), Some(3));
        assert!(
            units
                .iter()
                .all(|unit| unit.codec() == ElementaryCodec::Aac)
        );
    }

    #[test]
    fn flac_sequence_and_frames_preserve_bytes_without_copying() -> Result<(), MediaValidationError>
    {
        assert!(ElementaryCodec::Flac.is_audio());
        assert!(!ElementaryCodec::Flac.is_video());
        // Codec payloads are opaque at this layer. Sequence metadata and any
        // number of coded frames must reach the consumer unchanged.
        for (packet_type, payload) in [
            (0, b"fLaC\x80\x00\x00\x22streaminfo".as_slice()),
            (1, b"\xff\xf8first\xff\xf8second".as_slice()),
        ] {
            let mut raw = vec![0x90 | packet_type];
            raw.extend_from_slice(b"fLaC");
            raw.extend_from_slice(payload);
            let raw = Bytes::from(raw);
            let media = ValidatedMedia::parse_audio(raw.clone(), EnhancedValidationMode::Strict)?;
            let unit = media.elementary_unit()?.expect("FLAC is mapped");
            assert_eq!(unit.codec(), ElementaryCodec::Flac);
            assert_eq!(unit.track_id(), None);
            let bytes = match unit {
                ElementaryUnit::Configuration { extradata, .. } if packet_type == 0 => extradata,
                ElementaryUnit::Sample {
                    payload,
                    keyframe,
                    composition_time_offset,
                    ..
                } if packet_type == 1 => {
                    assert!(keyframe);
                    assert_eq!(composition_time_offset, 0);
                    payload
                }
                other => panic!("unexpected FLAC unit: {other:?}"),
            };
            assert_eq!(bytes.as_ref(), payload);
            assert_eq!(bytes.as_ptr(), raw[5..].as_ptr());
        }
        Ok(())
    }

    #[test]
    fn flac_one_track_preserves_identity_and_sequence_end_is_not_media()
    -> Result<(), MediaValidationError> {
        for packet_type in [0, 1, 2] {
            let mut raw = vec![0x95, packet_type];
            raw.extend_from_slice(b"fLaC");
            raw.push(7);
            if packet_type != 2 {
                raw.extend_from_slice(b"payload");
            }
            let media =
                ValidatedMedia::parse_audio(Bytes::from(raw), EnhancedValidationMode::Strict)?;
            let unit = media.elementary_unit()?;
            if packet_type == 2 {
                assert!(unit.is_none());
            } else {
                let unit = unit.expect("FLAC track");
                assert_eq!(unit.codec(), ElementaryCodec::Flac);
                assert_eq!(unit.track_id(), Some(7));
            }
        }
        Ok(())
    }

    #[test]
    fn flac_many_tracks_preserves_all_units_and_visitor_order() -> Result<(), MediaValidationError>
    {
        // ManyTracks uses a shared FourCC; ManyTracksManyCodecs names it per track.
        for mixed in [false, true] {
            for packet_type in [0, 1] {
                let mut raw = vec![0x95, (if mixed { 0x20 } else { 0x10 }) | packet_type];
                if !mixed {
                    raw.extend_from_slice(b"fLaC");
                }
                for (id, fourcc, payload) in [
                    (3, b"fLaC", b"first".as_slice()),
                    (
                        9,
                        if mixed { b"Opus" } else { b"fLaC" },
                        b"second".as_slice(),
                    ),
                ] {
                    if mixed {
                        raw.extend_from_slice(fourcc);
                    }
                    raw.push(id);
                    raw.extend_from_slice(&[
                        0,
                        0,
                        u8::try_from(payload.len()).expect("short fixture"),
                    ]);
                    raw.extend_from_slice(payload);
                }
                let media =
                    ValidatedMedia::parse_audio(Bytes::from(raw), EnhancedValidationMode::Strict)?;
                let units = media.elementary_units()?;
                assert_eq!(units.len(), 2);
                assert_eq!(units[0].track_id(), Some(3));
                assert_eq!(units[1].track_id(), Some(9));
                assert_eq!(units[0].codec(), ElementaryCodec::Flac);
                assert_eq!(
                    units[1].codec(),
                    if mixed {
                        ElementaryCodec::Opus
                    } else {
                        ElementaryCodec::Flac
                    }
                );
                for (unit, expected) in units
                    .iter()
                    .zip([b"first".as_slice(), b"second".as_slice()])
                {
                    let bytes = match unit {
                        ElementaryUnit::Configuration { extradata, .. } => extradata,
                        ElementaryUnit::Sample { payload, .. } => payload,
                    };
                    assert_eq!(bytes.as_ref(), expected);
                }
                let mut visited = Vec::new();
                media.visit_elementary_units(|unit| visited.push(unit))?;
                assert_eq!(visited, units);
                assert!(matches!(
                    media.elementary_unit(),
                    Err(MediaValidationError::MultipleUnits { count: 2 })
                ));
            }
        }
        Ok(())
    }
    #[test]
    fn controls_have_explicit_reasons_and_identity() -> Result<(), MediaValidationError> {
        let cases = [
            (
                AudioPacket::MultichannelConfig { channel_count: 6 },
                ElementarySkipReason::MultichannelConfig,
            ),
            (
                AudioPacket::Unknown {
                    packet_type: 8,
                    data: Bytes::new(),
                },
                ElementarySkipReason::UnknownPacketType(8),
            ),
        ];
        for (packet, reason) in cases {
            assert_eq!(
                audio_packet(AudioFourCc(*b"ac-3"), &packet, Some(9)),
                skipped(
                    reason,
                    Some(ElementaryCodecId::AudioFourCc(*b"ac-3")),
                    Some(9)
                )
            );
        }
        assert_eq!(
            audio_packet(
                AudioFourCc(*b"xxxx"),
                &AudioPacket::CodedFrames(Bytes::new()),
                Some(2)
            ),
            skipped(
                ElementarySkipReason::UnsupportedCodec,
                Some(ElementaryCodecId::AudioFourCc(*b"xxxx")),
                Some(2)
            )
        );
        for (packet, reason) in [
            (
                VideoPacket::Metadata(Bytes::new()),
                ElementarySkipReason::Metadata,
            ),
            (
                VideoPacket::Mpeg2TsSequenceStart(Bytes::new()),
                ElementarySkipReason::Mpeg2TsConfiguration,
            ),
            (
                VideoPacket::Unknown {
                    packet_type: 8,
                    data: Bytes::new(),
                },
                ElementarySkipReason::UnknownPacketType(8),
            ),
        ] {
            assert_eq!(
                video_packet(VideoFourCc(*b"vp09"), &packet, false, Some(4)),
                skipped(
                    reason,
                    Some(ElementaryCodecId::VideoFourCc(*b"vp09")),
                    Some(4)
                )
            );
        }
        let legacy = ValidatedMedia::parse_video(
            Bytes::from_static(&[0x17, 2, 0, 0, 0]),
            EnhancedValidationMode::Strict,
        )?;
        assert_eq!(
            legacy.elementary_events()?,
            vec![ElementaryEvent::SequenceEnd {
                codec: ElementaryCodec::Avc,
                track_id: None
            }]
        );
        assert!(legacy.elementary_units()?.is_empty());
        let unsupported = ValidatedMedia::parse_audio(
            Bytes::from_static(&[0x3f, 1]),
            EnhancedValidationMode::Strict,
        )?;
        assert_eq!(
            unsupported.elementary_events()?,
            vec![skipped(
                ElementarySkipReason::UnsupportedCodec,
                Some(ElementaryCodecId::LegacyAudio(3)),
                None
            )]
        );
        for format in [2, 14] {
            let raw = Bytes::from(vec![(format << 4) | 15, 0xff, 0xfb]);
            let media = ValidatedMedia::parse_audio(raw.clone(), EnhancedValidationMode::Strict)?;
            let Some(ElementaryUnit::Sample { codec, payload, .. }) = media.elementary_unit()?
            else {
                panic!("sample")
            };
            assert_eq!(codec, ElementaryCodec::Mp3);
            assert_eq!(payload.as_ptr(), raw[1..].as_ptr());
        }
        Ok(())
    }

    #[test]
    fn multitrack_sequence_ends_preserve_order() -> Result<(), MediaValidationError> {
        for video in [false, true] {
            let mut raw = vec![if video { 0x96 } else { 0x95 }, 0x22];
            for (four_cc, id) in [
                (if video { *b"vp09" } else { *b"ac-3" }, 3),
                (if video { *b"vvc1" } else { *b"fLaC" }, 7),
            ] {
                raw.extend_from_slice(&four_cc);
                raw.push(id);
                raw.extend_from_slice(&[0, 0, 0]);
            }
            let events = if video {
                ValidatedMedia::parse_video(Bytes::from(raw), EnhancedValidationMode::Strict)?
                    .elementary_events()?
            } else {
                ValidatedMedia::parse_audio(Bytes::from(raw), EnhancedValidationMode::Strict)?
                    .elementary_events()?
            };
            assert_eq!(
                events,
                vec![
                    ElementaryEvent::SequenceEnd {
                        codec: if video {
                            ElementaryCodec::Vp9
                        } else {
                            ElementaryCodec::Ac3
                        },
                        track_id: Some(3)
                    },
                    ElementaryEvent::SequenceEnd {
                        codec: if video {
                            ElementaryCodec::Vvc
                        } else {
                            ElementaryCodec::Flac
                        },
                        track_id: Some(7)
                    }
                ]
            );
        }
        Ok(())
    }

    #[test]
    fn vvc_offsets_and_sized_payload_boundaries() -> Result<(), MediaValidationError> {
        for (offset, encoded) in [
            (-8388608, [0x80, 0, 0]),
            (-1, [255, 255, 255]),
            (0, [0, 0, 0]),
            (8388607, [0x7f, 255, 255]),
        ] {
            let mut raw = b"\x96\x11vvc1".to_vec();
            for id in [1, 2] {
                raw.push(id);
                raw.extend_from_slice(&[0, 0, 4]);
                raw.extend_from_slice(&encoded);
                raw.push(42);
            }
            let raw = Bytes::from(raw);
            let media = ValidatedMedia::parse_video(raw.clone(), EnhancedValidationMode::Strict)?;
            let units = media.elementary_units()?;
            assert_eq!(units.len(), 2);
            for (index, unit) in units.iter().enumerate() {
                let ElementaryUnit::Sample {
                    codec,
                    payload,
                    composition_time_offset,
                    track_id,
                    ..
                } = unit
                else {
                    panic!("sample")
                };
                assert_eq!(*codec, ElementaryCodec::Vvc);
                assert_eq!(*composition_time_offset, offset);
                assert_eq!(*track_id, Some(index as u8 + 1));
                assert_eq!(payload.as_ref(), &[42]);
                assert_eq!(payload.as_ptr(), raw[13 + index * 8..].as_ptr());
            }
        }
        let malformed = Bytes::from_static(b"\x96\x11vvc1\x01\x00\x00\x02\x00\x00\x00");
        assert!(ValidatedMedia::parse_video(malformed, EnhancedValidationMode::Strict).is_err());
        let short = Bytes::from_static(b"\x91vvc1\x00\x00");
        assert!(ValidatedMedia::parse_video(short, EnhancedValidationMode::Strict).is_err());
        Ok(())
    }
    #[test]
    fn added_codecs_preserve_payload_slices_and_track_ids() -> Result<(), MediaValidationError> {
        for (four_cc, codec) in [
            (*b"ac-3", ElementaryCodec::Ac3),
            (*b"ec-3", ElementaryCodec::Eac3),
            (*b".mp3", ElementaryCodec::Mp3),
            (*b"vp08", ElementaryCodec::Vp8),
            (*b"vp09", ElementaryCodec::Vp9),
            (*b"vvc1", ElementaryCodec::Vvc),
        ] {
            for tracked in [false, true] {
                for packet_type in [0, 1] {
                    let mut raw = vec![if tracked {
                        if codec.is_audio() { 0x95 } else { 0x96 }
                    } else {
                        0x90 | packet_type
                    }];
                    if tracked {
                        raw.push(packet_type);
                    }
                    raw.extend_from_slice(&four_cc);
                    if tracked {
                        raw.push(9);
                    }
                    if codec == ElementaryCodec::Vvc && packet_type == 1 {
                        raw.extend_from_slice(&[0, 0, 5]);
                    }
                    let offset = raw.len();
                    raw.extend_from_slice(b"payload");
                    let raw = Bytes::from(raw);
                    let events = if codec.is_audio() {
                        ValidatedMedia::parse_audio(raw.clone(), EnhancedValidationMode::Strict)?
                            .elementary_events()?
                    } else {
                        ValidatedMedia::parse_video(raw.clone(), EnhancedValidationMode::Strict)?
                            .elementary_events()?
                    };
                    let [ElementaryEvent::Unit(unit)] = events.as_slice() else {
                        panic!("one unit")
                    };
                    assert_eq!(unit.codec(), codec);
                    assert_eq!(unit.track_id(), if tracked { Some(9) } else { None });
                    let payload = match unit {
                        ElementaryUnit::Sample { payload, .. } => payload,
                        ElementaryUnit::Configuration { extradata, .. } => extradata,
                    };
                    assert_eq!(payload.as_ptr(), raw[offset..].as_ptr());
                    assert_eq!(payload.as_ref(), b"payload");
                }
            }
        }
        Ok(())
    }

    #[test]
    fn event_visitors_report_commands_and_reject_opaque_input() -> Result<(), MediaValidationError>
    {
        for bytes in [&b"\x52\x00"[..], &b"\xd1\x00"[..]] {
            let media = ValidatedMedia::parse_video(
                Bytes::copy_from_slice(bytes),
                EnhancedValidationMode::Strict,
            )?;
            let mut events = Vec::new();
            media.visit_elementary_events(|event| events.push(event))?;
            assert_eq!(
                events,
                vec![skipped(ElementarySkipReason::VideoCommand, None, None)]
            );
            assert!(media.elementary_units()?.is_empty());
        }
        let opaque = ValidatedMedia::parse_audio(
            Bytes::from_static(b"\x91xxxxpayload"),
            EnhancedValidationMode::Passthrough,
        )?;
        assert!(opaque.elementary_events().is_err());
        Ok(())
    }
}
