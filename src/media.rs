//! Typed validation over original legacy and Enhanced RTMP media bodies.
//!
//! Owned contiguous bytes and borrowed segmented views share the parser.
//! Parsed ranges retain the source representation. Classification produces small
//! owned facts that can outlive a borrow. Release borrowed interpretations before
//! moving the payload into a relay queue.
//!
//! Strict mode rejects invalid or unsupported structures. Passthrough retains
//! opaque bodies with a reason. Neither mode decodes codec samples.
//! Valid single-track borrowed inspection avoids allocation; multitrack vectors
//! and diagnostic strings can allocate.

use bytes::Bytes;
use thiserror::Error;

use crate::{
    EnhancedValidationMode,
    flv::{
        AudioHeaderContent, AudioPacket, AudioTagBody, AudioTagHeader, EnhancedAudioBody,
        EnhancedVideoBody, LegacyAudioBody, LegacyAvcPacket, LegacyVideoBody, LegacyVideoHeader,
        VIDEO_FRAME_KEY, VideoHeaderContent, VideoPacket, VideoTagBody, VideoTagHeaderData,
    },
};

pub use crate::flv::{ParsedAudio, ParsedVideo};

/// A media payload and its immutable interpretation of the same bytes.
///
/// `raw` stays authoritative for republishing. The interpretation is a typed
/// view of those exact bytes, so the two cannot drift apart.
///
/// # Example
///
/// Parse one Enhanced RTMP Opus frame and confirm the typed view:
///
/// ```
/// use bytes::Bytes;
/// use rtmpx::{EnhancedValidationMode, MediaInterpretation, ValidatedMedia};
///
/// let raw = Bytes::from_static(b"\x91Opusframe");
/// let media =
///     ValidatedMedia::parse_audio(raw.clone(), EnhancedValidationMode::Strict).unwrap();
/// assert!(media.classification().coded);
/// assert!(matches!(
///     media.interpretation(),
///     MediaInterpretation::Parsed(_)
/// ));
/// assert_eq!(media.raw(), &raw);
/// ```
#[derive(Clone, Debug, PartialEq)]
pub struct ValidatedMedia<T, D = Bytes> {
    // Original RTMP message body. This is authoritative for republishing.
    raw: D,
    // Parsed interpretation, or an opaque reason in passthrough mode.
    interpretation: MediaInterpretation<T>,
}

#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum MediaInterpretation<T> {
    Parsed(T),
    #[non_exhaustive]
    Opaque {
        reason: String,
    },
}

// Media facts needed by relays without exposing FLV parser internals.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub struct MediaClassification {
    // The message contains one or more coded media frames.
    pub coded: bool,
    // The message carries a codec configuration/sequence header.
    pub configuration: bool,
    // A coded video message is marked as a keyframe.
    pub keyframe: bool,
}

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum MediaValidationError {
    #[error("expected at most one elementary unit, found {count}")]
    #[non_exhaustive]
    MultipleUnits { count: usize },
    #[error("malformed Enhanced/legacy FLV {kind} payload: {reason}")]
    #[non_exhaustive]
    Malformed { kind: &'static str, reason: String },
}

impl<P: crate::flv::MediaData> ValidatedMedia<ParsedAudio<P>, P> {
    pub fn parse_audio(raw: P, mode: EnhancedValidationMode) -> Result<Self, MediaValidationError> {
        match ParsedAudio::demux(&raw) {
            Ok(parsed) => finish_audio(raw, parsed, mode),
            Err(error) if mode == EnhancedValidationMode::Passthrough => Ok(Self {
                raw,
                interpretation: MediaInterpretation::Opaque {
                    reason: error.to_string(),
                },
            }),
            Err(error) => Err(MediaValidationError::Malformed {
                kind: "audio",
                reason: error.to_string(),
            }),
        }
    }

    // Classify a validated audio message. Opaque passthrough messages are
    // deliberately left unclassified rather than guessed from raw bytes.
    pub fn classification(&self) -> MediaClassification {
        let MediaInterpretation::Parsed(parsed) = &self.interpretation else {
            return MediaClassification::default();
        };
        match &parsed.body {
            AudioTagBody::Legacy(LegacyAudioBody::AacSequenceHeader(_)) => MediaClassification {
                configuration: true,
                ..Default::default()
            },
            AudioTagBody::Legacy(LegacyAudioBody::AacRaw(_) | LegacyAudioBody::Other(_)) => {
                MediaClassification {
                    coded: true,
                    ..Default::default()
                }
            }
            AudioTagBody::Enhanced(body) => classify_enhanced_audio(body),
            AudioTagBody::Legacy(LegacyAudioBody::AacUnknown { .. }) => {
                MediaClassification::default()
            }
        }
    }
}

impl<P: crate::flv::MediaData> ValidatedMedia<ParsedVideo<P>, P> {
    pub fn parse_video(raw: P, mode: EnhancedValidationMode) -> Result<Self, MediaValidationError> {
        match ParsedVideo::demux(&raw) {
            Ok(parsed) => finish_video(raw, parsed, mode),
            Err(error) if mode == EnhancedValidationMode::Passthrough => Ok(Self {
                raw,
                interpretation: MediaInterpretation::Opaque {
                    reason: error.to_string(),
                },
            }),
            Err(error) => Err(MediaValidationError::Malformed {
                kind: "video",
                reason: error.to_string(),
            }),
        }
    }

    // Classify a validated video message across legacy and Enhanced RTMP.
    pub fn classification(&self) -> MediaClassification {
        let MediaInterpretation::Parsed(parsed) = &self.interpretation else {
            return MediaClassification::default();
        };
        let keyframe = parsed.header.frame_type == VIDEO_FRAME_KEY;
        match (&parsed.header.data, &parsed.body) {
            (
                VideoTagHeaderData::Legacy(LegacyVideoHeader::AvcPacket(
                    LegacyAvcPacket::SequenceHeader,
                )),
                _,
            ) => MediaClassification {
                configuration: true,
                ..Default::default()
            },
            (
                VideoTagHeaderData::Legacy(LegacyVideoHeader::AvcPacket(LegacyAvcPacket::Nalu {
                    ..
                })),
                _,
            ) => MediaClassification {
                coded: true,
                keyframe,
                configuration: false,
            },
            (VideoTagHeaderData::Legacy(LegacyVideoHeader::AvcPacket(_)), _) => {
                MediaClassification::default()
            }
            (VideoTagHeaderData::Legacy(LegacyVideoHeader::VideoCommand(_)), _)
            | (_, VideoTagBody::Legacy(LegacyVideoBody::Command)) => MediaClassification::default(),
            (_, VideoTagBody::Legacy(LegacyVideoBody::AvcSequenceHeader(_))) => {
                MediaClassification {
                    configuration: true,
                    ..Default::default()
                }
            }
            (_, VideoTagBody::Legacy(LegacyVideoBody::Other(_))) => MediaClassification {
                coded: true,
                keyframe,
                configuration: false,
            },
            (_, VideoTagBody::Enhanced(body)) => classify_enhanced_video(body, keyframe),
        }
    }
}

fn classify_enhanced_audio<P>(body: &EnhancedAudioBody<P>) -> MediaClassification {
    let mut classification = MediaClassification::default();
    let mut classify = |packet: &AudioPacket<P>| match packet {
        AudioPacket::SequenceStart(_) => classification.configuration = true,
        AudioPacket::CodedFrames(_) => classification.coded = true,
        _ => {}
    };
    match body {
        EnhancedAudioBody::NoMultitrack { packet, .. } => classify(packet),
        EnhancedAudioBody::ManyTracks(tracks) => {
            for track in tracks {
                classify(&track.packet);
            }
        }
    }
    classification
}

fn classify_enhanced_video<P>(body: &EnhancedVideoBody<P>, keyframe: bool) -> MediaClassification {
    let mut classification = MediaClassification::default();
    let mut classify = |packet: &VideoPacket<P>| match packet {
        VideoPacket::SequenceStart(_) | VideoPacket::Mpeg2TsSequenceStart(_) => {
            classification.configuration = true;
        }
        VideoPacket::CodedFrames { .. } | VideoPacket::CodedFramesX(_) => {
            classification.coded = true;
            classification.keyframe |= keyframe;
        }
        _ => {}
    };
    match body {
        EnhancedVideoBody::NoMultitrack { packet, .. } => classify(packet),
        EnhancedVideoBody::ManyTracks(tracks) => {
            for track in tracks {
                classify(&track.packet);
            }
        }
        EnhancedVideoBody::Command => {}
    }
    classification
}

fn finish_audio<P>(
    raw: P,
    parsed: ParsedAudio<P>,
    mode: EnhancedValidationMode,
) -> Result<ValidatedMedia<ParsedAudio<P>, P>, MediaValidationError> {
    match validate_audio_unknowns(&parsed) {
        Ok(()) => Ok(ValidatedMedia {
            raw,
            interpretation: MediaInterpretation::Parsed(parsed),
        }),
        Err(reason) if mode == EnhancedValidationMode::Passthrough => Ok(ValidatedMedia {
            raw,
            interpretation: MediaInterpretation::Opaque { reason },
        }),
        Err(reason) => Err(MediaValidationError::Malformed {
            kind: "audio",
            reason,
        }),
    }
}

fn finish_video<P>(
    raw: P,
    parsed: ParsedVideo<P>,
    mode: EnhancedValidationMode,
) -> Result<ValidatedMedia<ParsedVideo<P>, P>, MediaValidationError> {
    match validate_video_unknowns(&parsed) {
        Ok(()) => Ok(ValidatedMedia {
            raw,
            interpretation: MediaInterpretation::Parsed(parsed),
        }),
        Err(reason) if mode == EnhancedValidationMode::Passthrough => Ok(ValidatedMedia {
            raw,
            interpretation: MediaInterpretation::Opaque { reason },
        }),
        Err(reason) => Err(MediaValidationError::Malformed {
            kind: "video",
            reason,
        }),
    }
}

fn validate_audio_unknowns<P>(parsed: &ParsedAudio<P>) -> Result<(), String> {
    let AudioTagHeader::Enhanced(header) = &parsed.header else {
        return Ok(());
    };
    if header.has_unknown_modex {
        return Err("unknown audio ModEx type".to_owned());
    }
    if matches!(header.content, AudioHeaderContent::Unknown { .. }) {
        return Err("unknown audio multitrack type".to_owned());
    }
    let AudioTagBody::Enhanced(body) = &parsed.body else {
        return Ok(());
    };
    match body {
        EnhancedAudioBody::NoMultitrack { four_cc, packet } => {
            validate_audio_track(four_cc.0, packet)
        }
        EnhancedAudioBody::ManyTracks(tracks) => tracks
            .iter()
            .try_for_each(|track| validate_audio_track(track.four_cc.0, &track.packet)),
    }
}

fn validate_audio_track<P>(four_cc: [u8; 4], packet: &AudioPacket<P>) -> Result<(), String> {
    const KNOWN: [[u8; 4]; 6] = [*b"ac-3", *b"ec-3", *b"Opus", *b".mp3", *b"fLaC", *b"mp4a"];
    if !KNOWN.contains(&four_cc) {
        return Err(format!(
            "unknown audio FourCC {:?}",
            String::from_utf8_lossy(&four_cc)
        ));
    }
    if matches!(packet, AudioPacket::Unknown { .. }) {
        return Err("unknown audio packet type".to_owned());
    }
    Ok(())
}

fn validate_video_unknowns<P>(parsed: &ParsedVideo<P>) -> Result<(), String> {
    let VideoTagHeaderData::Enhanced(header) = &parsed.header.data else {
        return Ok(());
    };
    if header.has_unknown_modex {
        return Err("unknown video ModEx type".to_owned());
    }
    if matches!(header.content, VideoHeaderContent::Unknown { .. }) {
        return Err("unknown video multitrack type".to_owned());
    }
    let VideoTagBody::Enhanced(body) = &parsed.body else {
        return Ok(());
    };
    match body {
        EnhancedVideoBody::Command => Ok(()),
        EnhancedVideoBody::NoMultitrack { four_cc, packet } => {
            validate_video_track(four_cc.0, packet)
        }
        EnhancedVideoBody::ManyTracks(tracks) => tracks
            .iter()
            .try_for_each(|track| validate_video_track(track.four_cc.0, &track.packet)),
    }
}

fn validate_video_track<P>(four_cc: [u8; 4], packet: &VideoPacket<P>) -> Result<(), String> {
    const KNOWN: [[u8; 4]; 6] = [*b"vp08", *b"vp09", *b"av01", *b"avc1", *b"hvc1", *b"vvc1"];
    if !KNOWN.contains(&four_cc) {
        return Err(format!(
            "unknown video FourCC {:?}",
            String::from_utf8_lossy(&four_cc)
        ));
    }
    if matches!(packet, VideoPacket::Unknown { .. }) {
        return Err("unknown video packet type".to_owned());
    }
    Ok(())
}

impl<T, D> ValidatedMedia<T, D> {
    /// Original message body, authoritative for forwarding.
    pub fn raw(&self) -> &D {
        &self.raw
    }
    /// Immutable interpretation of the original body.
    pub fn interpretation(&self) -> &MediaInterpretation<T> {
        &self.interpretation
    }
    /// Consume this value without copying its bytes or interpretation.
    pub fn into_parts(self) -> (D, MediaInterpretation<T>) {
        (self.raw, self.interpretation)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn malformed_enhanced_video_is_strict_by_default_and_opaque_in_passthrough() {
        // Enhanced flag + SequenceStart, but no required FourCC.
        let raw = Bytes::from_static(&[0x90]);
        assert!(ValidatedMedia::parse_video(raw.clone(), EnhancedValidationMode::Strict).is_err());
        let media =
            ValidatedMedia::parse_video(raw.clone(), EnhancedValidationMode::Passthrough).unwrap();
        assert_eq!(media.raw, raw);
        assert!(matches!(
            media.interpretation,
            MediaInterpretation::Opaque { .. }
        ));
    }

    #[test]
    fn parses_vvc_as_an_owned_unknown_fourcc_model() {
        // Key frame + enhanced SequenceEnd + VVC FourCC. SequenceEnd has no body.
        let raw = Bytes::from_static(b"\x92vvc1");
        let media =
            ValidatedMedia::parse_video(raw.clone(), EnhancedValidationMode::Strict).unwrap();
        assert_eq!(media.raw, raw);
        assert!(matches!(
            media.interpretation,
            MediaInterpretation::Parsed(_)
        ));
    }

    #[test]
    fn unknown_fourcc_is_rejected_or_retained_opaque() {
        let raw = Bytes::from_static(b"\x92zzzz");
        assert!(ValidatedMedia::parse_video(raw.clone(), EnhancedValidationMode::Strict).is_err());
        let media =
            ValidatedMedia::parse_video(raw.clone(), EnhancedValidationMode::Passthrough).unwrap();
        assert_eq!(media.raw, raw);
        assert!(matches!(
            media.interpretation,
            MediaInterpretation::Opaque { .. }
        ));
    }

    #[test]
    fn unknown_modex_is_rejected_or_retained_opaque() {
        let raw = Bytes::from_static(b"\x97\x00\x2a\x12hvc1");
        assert!(ValidatedMedia::parse_video(raw.clone(), EnhancedValidationMode::Strict).is_err());
        assert!(matches!(
            ValidatedMedia::parse_video(raw, EnhancedValidationMode::Passthrough)
                .unwrap()
                .interpretation,
            MediaInterpretation::Opaque { .. }
        ));
    }

    #[test]
    fn parses_video_and_audio_multitrack_packets() {
        let video = Bytes::from_static(b"\x96\x02hvc1\x00");
        assert!(ValidatedMedia::parse_video(video, EnhancedValidationMode::Strict).is_ok());
        let audio = Bytes::from_static(b"\x95\x02Opus\x00");
        assert!(ValidatedMedia::parse_audio(audio, EnhancedValidationMode::Strict).is_ok());
    }

    #[test]
    fn parses_multichannel_audio_configuration() {
        let audio = Bytes::from_static(b"\x94mp4a\x00\x02");
        assert!(ValidatedMedia::parse_audio(audio, EnhancedValidationMode::Strict).is_ok());
    }

    #[test]
    fn parses_every_enhanced_video_packet_family() {
        for raw in [
            b"\x90vp08config".as_slice(),
            b"\x91av01frame".as_slice(),
            b"\x92hvc1".as_slice(),
            b"\x93av01frame".as_slice(),
            b"\x94av01".as_slice(),
            b"\x95vp08descriptor".as_slice(),
            b"\x96\x02hvc1\x00".as_slice(),
            b"\x97\x02\x00\x00\x01\x02hvc1".as_slice(),
        ] {
            ValidatedMedia::parse_video(
                Bytes::copy_from_slice(raw),
                EnhancedValidationMode::Strict,
            )
            .unwrap_or_else(|error| panic!("valid video family {raw:?} failed: {error}"));
        }
    }

    #[test]
    fn parses_every_enhanced_audio_packet_family() {
        for raw in [
            b"\x90Opusconfig".as_slice(),
            b"\x91Opusframe".as_slice(),
            b"\x92Opus".as_slice(),
            b"\x94mp4a\x00\x02".as_slice(),
            b"\x95\x02Opus\x00".as_slice(),
            b"\x97\x02\x00\x00\x01\x02Opus".as_slice(),
        ] {
            ValidatedMedia::parse_audio(
                Bytes::copy_from_slice(raw),
                EnhancedValidationMode::Strict,
            )
            .unwrap_or_else(|error| panic!("valid audio family {raw:?} failed: {error}"));
        }
    }

    #[test]
    fn unknown_audio_values_are_rejected_or_retained_opaque() {
        for raw in [
            b"\x92zzzz".as_slice(),
            b"\x93Opus".as_slice(),
            b"\x97\x00\x2a\x12Opus".as_slice(),
        ] {
            let raw = Bytes::copy_from_slice(raw);
            assert!(
                ValidatedMedia::parse_audio(raw.clone(), EnhancedValidationMode::Strict).is_err()
            );
            assert!(matches!(
                ValidatedMedia::parse_audio(raw, EnhancedValidationMode::Passthrough)
                    .unwrap()
                    .interpretation,
                MediaInterpretation::Opaque { .. }
            ));
        }
    }

    #[test]
    fn unknown_video_packet_type_is_rejected_or_retained_opaque() {
        let raw = Bytes::from_static(b"\x98hvc1");
        assert!(ValidatedMedia::parse_video(raw.clone(), EnhancedValidationMode::Strict).is_err());
        assert!(matches!(
            ValidatedMedia::parse_video(raw, EnhancedValidationMode::Passthrough)
                .unwrap()
                .interpretation,
            MediaInterpretation::Opaque { .. }
        ));
    }

    #[test]
    fn classifies_enhanced_configurations_coded_frames_and_keyframes() {
        let video_config = ValidatedMedia::parse_video(
            Bytes::from_static(b"\x90vp08config"),
            EnhancedValidationMode::Strict,
        )
        .unwrap();
        assert_eq!(
            video_config.classification(),
            MediaClassification {
                configuration: true,
                ..Default::default()
            }
        );

        let video_frame = ValidatedMedia::parse_video(
            Bytes::from_static(b"\x91vp08frame"),
            EnhancedValidationMode::Strict,
        )
        .unwrap();
        assert_eq!(
            video_frame.classification(),
            MediaClassification {
                coded: true,
                keyframe: true,
                configuration: false
            }
        );

        let audio_config = ValidatedMedia::parse_audio(
            Bytes::from_static(b"\x90Opusconfig"),
            EnhancedValidationMode::Strict,
        )
        .unwrap();
        assert!(audio_config.classification().configuration);
        let audio_frame = ValidatedMedia::parse_audio(
            Bytes::from_static(b"\x91Opusframe"),
            EnhancedValidationMode::Strict,
        )
        .unwrap();
        assert!(audio_frame.classification().coded);
    }

    #[test]
    fn every_prefix_is_strict_error_or_parsed_and_never_panics() {
        let corpus: &[&[u8]] = &[
            b"\x90Opusconfig",
            b"\x91Opusframe",
            b"\x92Opus",
            b"\x94mp4a\x00\x02",
            b"\x95\x02Opus\x00",
            b"\x90vp08config",
            b"\x91av01frame",
            b"\x92hvc1",
            b"\x93av01frame",
            b"\x92zzzz",
            b"\x98hvc1",
            b"\x92vvc1",
            &[0xaf, 0x00, 0x11, 0x88],
            &[0x2f, 0xff, 0xfb],
        ];
        for tag in corpus {
            for len in 0..=tag.len() {
                let raw = Bytes::copy_from_slice(&tag[..len]);
                let strict =
                    ValidatedMedia::parse_audio(raw.clone(), EnhancedValidationMode::Strict);
                let passthrough =
                    ValidatedMedia::parse_audio(raw.clone(), EnhancedValidationMode::Passthrough);
                if strict.is_ok() {
                    assert!(passthrough.is_ok(), "prefix {len} of {tag:?}");
                }
                if let Ok(media) = &passthrough {
                    let _ = media.classification();
                }
                let strict =
                    ValidatedMedia::parse_video(raw.clone(), EnhancedValidationMode::Strict);
                let passthrough =
                    ValidatedMedia::parse_video(raw.clone(), EnhancedValidationMode::Passthrough);
                if strict.is_ok() {
                    assert!(passthrough.is_ok(), "prefix {len} of {tag:?}");
                }
                if let Ok(media) = &passthrough {
                    let _ = media.classification();
                }
            }
        }
    }
}
