//! Minimal FLV audio/video tag parser for RTMP media validation.
//!
//! ParsedAudio and ParsedVideo are the typed views that media validates and
//! elementary reduces to decoder configuration and coded samples. The parser
//! covers legacy FLV audio/video tags and Enhanced RTMP tags (FourCC
//! signalling, multitrack, ModEx) and nothing else: it is not a general FLV
//! file demuxer.
//!
//! The wire layouts were ported by reference from scuffle-flv 0.2.2 (MIT); no
//! scuffle code is copied. Differences from a full demuxer, by design:
//!
//! - Codec configuration records (avcC, hvcC, av1C) are validated
//!   structurally (magic byte plus length bounds) instead of fully decoded.
//! - Enhanced video metadata frames are accepted opaquely instead of being
//!   AMF0-decoded; they carry no facts this crate needs.
//! - Sequence-start payloads are forwarded as raw slices of the input;
//!   nothing is parsed and re-serialized, so relay stays byte-exact.

use bytes::Bytes;
use thiserror::Error;

/// Parse failure for one FLV audio/video tag.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
#[error("malformed FLV tag: {0}")]
pub struct FlvError(pub String);

impl FlvError {
    fn truncated() -> Self {
        Self("truncated tag".to_owned())
    }
}

/// Byte storage accepted by the media parser. Implementations preserve slice contents.
/// Use `Bytes` for owned contiguous views or `PayloadView` for borrowed segments.
pub trait MediaData: crate::Segments + Clone + private::Sealed {
    fn media_slice(&self, range: std::ops::Range<usize>) -> Self;
}
mod private {
    pub trait Sealed {}
    impl Sealed for bytes::Bytes {}
    impl Sealed for crate::PayloadView<'_> {}
}
impl MediaData for Bytes {
    fn media_slice(&self, range: std::ops::Range<usize>) -> Self {
        self.slice(range)
    }
}
impl MediaData for crate::PayloadView<'_> {
    fn media_slice(&self, range: std::ops::Range<usize>) -> Self {
        self.slice(range)
    }
}
struct Cursor<P> {
    raw: P,
    pos: usize,
}
impl<P: MediaData> Cursor<P> {
    fn new(raw: &P) -> Self {
        Self {
            raw: raw.clone(),
            pos: 0,
        }
    }
    fn remaining(&self) -> usize {
        self.raw.len() - self.pos
    }
    fn read_u8(&mut self) -> Result<u8, FlvError> {
        if self.remaining() == 0 {
            return Err(FlvError::truncated());
        }
        if self.pos == self.raw.segment(0).len() {
            self.commit();
        }
        let byte = self.raw.segment(0)[self.pos];
        self.pos += 1;
        Ok(byte)
    }
    fn commit(&mut self) {
        if self.pos != 0 {
            self.raw = self.raw.media_slice(self.pos..self.raw.len());
            self.pos = 0;
        }
    }
    fn read_u16(&mut self) -> Result<u16, FlvError> {
        Ok((u16::from(self.read_u8()?) << 8) | u16::from(self.read_u8()?))
    }
    fn read_u24(&mut self) -> Result<u32, FlvError> {
        Ok((u32::from(self.read_u8()?) << 16)
            | (u32::from(self.read_u8()?) << 8)
            | u32::from(self.read_u8()?))
    }
    fn read_i24(&mut self) -> Result<i32, FlvError> {
        Ok(sign_extend_cts(self.read_u24()?))
    }
    fn read_fourcc(&mut self) -> Result<[u8; 4], FlvError> {
        Ok([
            self.read_u8()?,
            self.read_u8()?,
            self.read_u8()?,
            self.read_u8()?,
        ])
    }
    fn take(&mut self, len: usize) -> Result<P, FlvError> {
        if self.remaining() < len {
            return Err(FlvError::truncated());
        }
        self.commit();
        let bytes = self.raw.media_slice(0..len);
        self.raw = self.raw.media_slice(len..self.raw.len());
        Ok(bytes)
    }
    fn take_rest(&mut self) -> P {
        self.commit();
        let bytes = self.raw.clone();
        self.raw = self.raw.media_slice(self.raw.len()..self.raw.len());
        bytes
    }
}

/// FLV stores composition time as signed 24-bit.
fn sign_extend_cts(value: u32) -> i32 {
    let value = value & 0x00ff_ffff;
    if value & 0x0080_0000 == 0 {
        value as i32
    } else {
        (value | 0xff00_0000) as i32
    }
}

/// Legacy sound format that signals an Enhanced audio tag header.
pub const SOUND_FORMAT_EX_HEADER: u8 = 9;
/// Legacy sound format for AAC audio.
pub const SOUND_FORMAT_AAC: u8 = 10;
/// Legacy video codec id for AVC (H.264).
pub const VIDEO_CODEC_AVC: u8 = 7;

/// Video frame type for a keyframe.
pub const VIDEO_FRAME_KEY: u8 = 1;
/// Video frame type reserved for server-generated keyframes.
pub const VIDEO_FRAME_GENERATED_KEY: u8 = 4;
/// Video frame type for info/command frames.
pub const VIDEO_FRAME_COMMAND: u8 = 5;

/// Enhanced audio packet type for sequence start.
pub const AUDIO_PACKET_SEQUENCE_START: u8 = 0;
/// Enhanced audio packet type for coded frames.
pub const AUDIO_PACKET_CODED_FRAMES: u8 = 1;
/// Enhanced audio packet type for sequence end.
pub const AUDIO_PACKET_SEQUENCE_END: u8 = 2;
/// Enhanced audio packet type for multichannel configuration.
pub const AUDIO_PACKET_MULTICHANNEL_CONFIG: u8 = 4;
/// Enhanced audio packet type that switches to multitrack mode.
pub const AUDIO_PACKET_MULTITRACK: u8 = 5;
/// Enhanced audio packet type for modifier extensions.
pub const AUDIO_PACKET_MODEX: u8 = 7;

/// Enhanced video packet type for sequence start.
pub const VIDEO_PACKET_SEQUENCE_START: u8 = 0;
/// Enhanced video packet type for coded frames.
pub const VIDEO_PACKET_CODED_FRAMES: u8 = 1;
/// Enhanced video packet type for sequence end.
pub const VIDEO_PACKET_SEQUENCE_END: u8 = 2;
/// Enhanced video packet type for coded frames without extra data.
pub const VIDEO_PACKET_CODED_FRAMES_X: u8 = 3;
/// Enhanced video packet type for metadata.
pub const VIDEO_PACKET_METADATA: u8 = 4;
/// Enhanced video packet type for MPEG-2 TS sequence start.
pub const VIDEO_PACKET_MPEG2TS_SEQUENCE_START: u8 = 5;
/// Enhanced video packet type that switches to multitrack mode.
pub const VIDEO_PACKET_MULTITRACK: u8 = 6;
/// Enhanced video packet type for modifier extensions.
pub const VIDEO_PACKET_MODEX: u8 = 7;

/// Multitrack type for a single track.
pub const MULTITRACK_ONE_TRACK: u8 = 0;
/// Multitrack type for many tracks sharing one codec.
pub const MULTITRACK_MANY_TRACKS: u8 = 1;
/// Multitrack type for many tracks with per-track codecs.
pub const MULTITRACK_MANY_CODECS: u8 = 2;

/// Raw audio FourCC; any four bytes parse, known codecs are allowlisted later.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AudioFourCc(pub [u8; 4]);

/// Raw video FourCC; any four bytes parse, known codecs are allowlisted later.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct VideoFourCc(pub [u8; 4]);

/// Typed audio interpretation of one RTMP audio message body.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct ParsedAudio<P = Bytes> {
    pub header: AudioTagHeader,
    pub body: AudioTagBody<P>,
}

/// Typed video interpretation of one RTMP video message body.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct ParsedVideo<P = Bytes> {
    pub header: VideoTagHeader,
    pub body: VideoTagBody<P>,
}

/// Legacy or Enhanced audio tag header.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum AudioTagHeader {
    Legacy(LegacyAudioHeader),
    Enhanced(EnhancedAudioHeader),
}

/// Legacy FLV audio tag header nibbles (E.4.2.1).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct LegacyAudioHeader {
    pub sound_format: u8,
    pub sound_rate: u8,
    pub sound_size: u8,
    pub sound_type: u8,
}

/// Enhanced audio tag header with a resolved packet type.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct EnhancedAudioHeader {
    /// Packet type after ModEx chaining (and the multitrack byte, if any).
    pub packet_type: u8,
    /// Whether any ModEx entry had an unknown extension type.
    pub has_unknown_modex: bool,
    pub content: AudioHeaderContent,
}

/// Multitrack mode selected by an Enhanced audio tag header.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum AudioHeaderContent {
    NoMultitrack(AudioFourCc),
    OneTrack(AudioFourCc),
    ManyTracks(AudioFourCc),
    ManyTracksManyCodecs,
    Unknown {
        multitrack_type: u8,
        four_cc: AudioFourCc,
    },
}

/// Legacy or Enhanced audio tag body.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum AudioTagBody<P = Bytes> {
    Legacy(LegacyAudioBody<P>),
    Enhanced(EnhancedAudioBody<P>),
}

/// Legacy FLV audio tag body.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum LegacyAudioBody<P = Bytes> {
    AacSequenceHeader(P),
    AacRaw(P),
    AacUnknown { packet_type: u8, data: P },
    Other(P),
}

/// Enhanced audio tag body: one packet or one per track.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum EnhancedAudioBody<P = Bytes> {
    NoMultitrack {
        four_cc: AudioFourCc,
        packet: AudioPacket<P>,
    },
    ManyTracks(Vec<AudioTrack<P>>),
}

/// One track of a multitrack Enhanced audio tag.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct AudioTrack<P = Bytes> {
    pub four_cc: AudioFourCc,
    pub track_id: u8,
    pub packet: AudioPacket<P>,
}

/// Enhanced audio packet. Sequence-start bytes are raw slices of the input.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum AudioPacket<P = Bytes> {
    SequenceStart(P),
    CodedFrames(P),
    SequenceEnd,
    MultichannelConfig { channel_count: u8 },
    Unknown { packet_type: u8, data: P },
}

/// Video tag header: frame type plus legacy or Enhanced data.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct VideoTagHeader {
    pub frame_type: u8,
    pub data: VideoTagHeaderData,
}

/// Legacy or Enhanced video tag header data.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum VideoTagHeaderData {
    Legacy(LegacyVideoHeader),
    Enhanced(EnhancedVideoHeader),
}

/// Legacy FLV video tag header (E.4.3.1).
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum LegacyVideoHeader {
    VideoCommand(u8),
    AvcPacket(LegacyAvcPacket),
    Other { codec_id: u8 },
}

/// Legacy AVC packet header. The NALU offset is sign-extended at parse time.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum LegacyAvcPacket {
    SequenceHeader,
    Nalu {
        composition_time_offset: i32,
    },
    EndOfSequence,
    Unknown {
        packet_type: u8,
        composition_time_offset: u32,
    },
}

/// Enhanced video tag header with a resolved packet type.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct EnhancedVideoHeader {
    /// Packet type after ModEx chaining (and the multitrack byte, if any).
    pub packet_type: u8,
    /// Whether any ModEx entry had an unknown extension type.
    pub has_unknown_modex: bool,
    pub content: VideoHeaderContent,
}

/// Multitrack mode (or command) selected by an Enhanced video tag header.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum VideoHeaderContent {
    VideoCommand(u8),
    NoMultitrack(VideoFourCc),
    OneTrack(VideoFourCc),
    ManyTracks(VideoFourCc),
    ManyTracksManyCodecs,
    Unknown {
        multitrack_type: u8,
        four_cc: VideoFourCc,
    },
}

/// Legacy or Enhanced video tag body.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum VideoTagBody<P = Bytes> {
    Legacy(LegacyVideoBody<P>),
    Enhanced(EnhancedVideoBody<P>),
}

/// Legacy FLV video tag body.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum LegacyVideoBody<P = Bytes> {
    Command,
    AvcSequenceHeader(P),
    Other(P),
}

/// Enhanced video tag body: one packet, one per track, or a header command.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum EnhancedVideoBody<P = Bytes> {
    Command,
    NoMultitrack {
        four_cc: VideoFourCc,
        packet: VideoPacket<P>,
    },
    ManyTracks(Vec<VideoTrack<P>>),
}

/// One track of a multitrack Enhanced video tag.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct VideoTrack<P = Bytes> {
    pub four_cc: VideoFourCc,
    pub track_id: u8,
    pub packet: VideoPacket<P>,
}

/// Enhanced video packet. Payloads are raw slices of the input; sequence-start
/// records are validated structurally but never re-serialized.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum VideoPacket<P = Bytes> {
    SequenceStart(P),
    Mpeg2TsSequenceStart(P),
    CodedFrames {
        composition_time_offset: i32,
        data: P,
    },
    CodedFramesX(P),
    Metadata(P),
    SequenceEnd,
    Unknown {
        packet_type: u8,
        data: P,
    },
}

impl<P: MediaData> ParsedAudio<P> {
    /// Demux one RTMP audio message body. Empty or truncated input is an
    /// error, never a panic; trailing bytes follow the legacy rules below.
    pub fn demux(raw: &P) -> Result<Self, FlvError> {
        let mut cursor = Cursor::new(raw);
        let first = cursor.read_u8()?;
        let sound_format = first >> 4;
        if sound_format != SOUND_FORMAT_EX_HEADER {
            let header = LegacyAudioHeader {
                sound_format,
                sound_rate: (first >> 2) & 0x03,
                sound_size: (first >> 1) & 0x01,
                sound_type: first & 0x01,
            };
            let body = if sound_format == SOUND_FORMAT_AAC {
                let packet_type = cursor.read_u8()?;
                let data = cursor.take_rest();
                match packet_type {
                    0 => LegacyAudioBody::AacSequenceHeader(data),
                    1 => LegacyAudioBody::AacRaw(data),
                    _ => LegacyAudioBody::AacUnknown { packet_type, data },
                }
            } else {
                LegacyAudioBody::Other(cursor.take_rest())
            };
            return Ok(Self {
                header: AudioTagHeader::Legacy(header),
                body: AudioTagBody::Legacy(body),
            });
        }

        let header = enhanced_audio_header(&mut cursor, first & 0x0F)?;
        let body = enhanced_audio_body(&header, &mut cursor)?;
        Ok(Self {
            header: AudioTagHeader::Enhanced(header),
            body: AudioTagBody::Enhanced(body),
        })
    }
}

impl<P: MediaData> ParsedVideo<P> {
    /// Demux one RTMP video message body. Empty or truncated input is an
    /// error, never a panic; trailing bytes follow the legacy rules below.
    pub fn demux(raw: &P) -> Result<Self, FlvError> {
        let mut cursor = Cursor::new(raw);
        let first = cursor.read_u8()?;
        let frame_type = (first >> 4) & 0x07;
        if first & 0x80 == 0 {
            let codec_id = first & 0x0F;
            let (header, body) = if codec_id == VIDEO_CODEC_AVC {
                let packet_type = cursor.read_u8()?;
                let composition_time_offset = cursor.read_u24()?;
                let header = match packet_type {
                    0 => LegacyVideoHeader::AvcPacket(LegacyAvcPacket::SequenceHeader),
                    1 => LegacyVideoHeader::AvcPacket(LegacyAvcPacket::Nalu {
                        composition_time_offset: sign_extend_cts(composition_time_offset),
                    }),
                    2 => LegacyVideoHeader::AvcPacket(LegacyAvcPacket::EndOfSequence),
                    _ => LegacyVideoHeader::AvcPacket(LegacyAvcPacket::Unknown {
                        packet_type,
                        composition_time_offset,
                    }),
                };
                let body = match &header {
                    LegacyVideoHeader::AvcPacket(LegacyAvcPacket::SequenceHeader) => {
                        let data = cursor.take_rest();
                        validate_avc_decoder_config(&data)?;
                        LegacyVideoBody::AvcSequenceHeader(data)
                    }
                    _ => LegacyVideoBody::Other(cursor.take_rest()),
                };
                (header, body)
            } else if frame_type == VIDEO_FRAME_COMMAND {
                let command = cursor.read_u8()?;
                (
                    LegacyVideoHeader::VideoCommand(command),
                    LegacyVideoBody::Command,
                )
            } else {
                (
                    LegacyVideoHeader::Other { codec_id },
                    LegacyVideoBody::Other(cursor.take_rest()),
                )
            };
            return Ok(Self {
                header: VideoTagHeader {
                    frame_type,
                    data: VideoTagHeaderData::Legacy(header),
                },
                body: VideoTagBody::Legacy(body),
            });
        }

        let header = enhanced_video_header(&mut cursor, frame_type, first & 0x0F)?;
        let body = enhanced_video_body(&header, &mut cursor)?;
        Ok(Self {
            header: VideoTagHeader {
                frame_type,
                data: VideoTagHeaderData::Enhanced(header),
            },
            body: VideoTagBody::Enhanced(body),
        })
    }
}

/// Resolve the audio packet type through the ModEx chain. Returns the final
/// packet type and whether any extension had an unknown type. The nano-offset
/// extension is length-checked and otherwise ignored.
fn read_audio_modex<P: MediaData>(cursor: &mut Cursor<P>) -> Result<(bool, u8), FlvError> {
    let mut size = usize::from(cursor.read_u8()?) + 1;
    if size == 256 {
        size = usize::from(cursor.read_u16()?) + 1;
    }
    let data = cursor.take(size)?;
    let next = cursor.read_u8()?;
    if next >> 4 == 0 {
        if data.len() < 3 {
            return Err(FlvError(
                "invalid modExData, expected at least 3 bytes".to_owned(),
            ));
        }
        Ok((false, next & 0x0F))
    } else {
        Ok((true, next & 0x0F))
    }
}

/// Resolve the video packet type through the ModEx chain. See read_audio_modex.
fn read_video_modex<P: MediaData>(cursor: &mut Cursor<P>) -> Result<(bool, u8), FlvError> {
    let mut size = usize::from(cursor.read_u8()?) + 1;
    if size == 256 {
        size = usize::from(cursor.read_u16()?) + 1;
    }
    let data = cursor.take(size)?;
    let next = cursor.read_u8()?;
    if next >> 4 == 0 {
        if data.len() < 3 {
            return Err(FlvError(
                "invalid modExData, expected at least 3 bytes".to_owned(),
            ));
        }
        Ok((false, next & 0x0F))
    } else {
        Ok((true, next & 0x0F))
    }
}

fn enhanced_audio_header<P: MediaData>(
    cursor: &mut Cursor<P>,
    initial: u8,
) -> Result<EnhancedAudioHeader, FlvError> {
    let mut packet_type = initial;
    let mut has_unknown_modex = false;
    while packet_type == AUDIO_PACKET_MODEX {
        let (unknown, next) = read_audio_modex(cursor)?;
        has_unknown_modex |= unknown;
        packet_type = next;
    }
    let content = if packet_type == AUDIO_PACKET_MULTITRACK {
        let byte = cursor.read_u8()?;
        let multitrack_type = byte >> 4;
        packet_type = byte & 0x0F;
        if packet_type == AUDIO_PACKET_MULTITRACK {
            return Err(FlvError("nested multitracks are not allowed".to_owned()));
        }
        let four_cc = if multitrack_type == MULTITRACK_MANY_CODECS {
            AudioFourCc([0; 4])
        } else {
            AudioFourCc(cursor.read_fourcc()?)
        };
        match multitrack_type {
            MULTITRACK_ONE_TRACK => AudioHeaderContent::OneTrack(four_cc),
            MULTITRACK_MANY_TRACKS => AudioHeaderContent::ManyTracks(four_cc),
            MULTITRACK_MANY_CODECS => AudioHeaderContent::ManyTracksManyCodecs,
            _ => AudioHeaderContent::Unknown {
                multitrack_type,
                four_cc,
            },
        }
    } else {
        AudioHeaderContent::NoMultitrack(AudioFourCc(cursor.read_fourcc()?))
    };
    Ok(EnhancedAudioHeader {
        packet_type,
        has_unknown_modex,
        content,
    })
}

fn enhanced_video_header<P: MediaData>(
    cursor: &mut Cursor<P>,
    frame_type: u8,
    initial: u8,
) -> Result<EnhancedVideoHeader, FlvError> {
    let mut packet_type = initial;
    let mut has_unknown_modex = false;
    while packet_type == VIDEO_PACKET_MODEX {
        let (unknown, next) = read_video_modex(cursor)?;
        has_unknown_modex |= unknown;
        packet_type = next;
    }
    let content = if packet_type != VIDEO_PACKET_METADATA && frame_type == VIDEO_FRAME_COMMAND {
        VideoHeaderContent::VideoCommand(cursor.read_u8()?)
    } else if packet_type == VIDEO_PACKET_MULTITRACK {
        let byte = cursor.read_u8()?;
        let multitrack_type = byte >> 4;
        packet_type = byte & 0x0F;
        if packet_type == VIDEO_PACKET_MULTITRACK {
            return Err(FlvError("nested multitracks are not allowed".to_owned()));
        }
        let four_cc = if multitrack_type == MULTITRACK_MANY_CODECS {
            VideoFourCc([0; 4])
        } else {
            VideoFourCc(cursor.read_fourcc()?)
        };
        match multitrack_type {
            MULTITRACK_ONE_TRACK => VideoHeaderContent::OneTrack(four_cc),
            MULTITRACK_MANY_TRACKS => VideoHeaderContent::ManyTracks(four_cc),
            MULTITRACK_MANY_CODECS => VideoHeaderContent::ManyTracksManyCodecs,
            _ => VideoHeaderContent::Unknown {
                multitrack_type,
                four_cc,
            },
        }
    } else {
        VideoHeaderContent::NoMultitrack(VideoFourCc(cursor.read_fourcc()?))
    };
    Ok(EnhancedVideoHeader {
        packet_type,
        has_unknown_modex,
        content,
    })
}

/// Take a packet payload: exactly the u24 size prefix for multitrack tracks,
/// otherwise everything that remains.
fn sized_payload<P: MediaData>(cursor: &mut Cursor<P>, size: Option<usize>) -> Result<P, FlvError> {
    match size {
        Some(len) => cursor.take(len),
        None => Ok(cursor.take_rest()),
    }
}

fn enhanced_audio_body<P: MediaData>(
    header: &EnhancedAudioHeader,
    cursor: &mut Cursor<P>,
) -> Result<EnhancedAudioBody<P>, FlvError> {
    if let AudioHeaderContent::NoMultitrack(four_cc) = header.content {
        let packet = audio_packet(header.packet_type, false, cursor)?;
        return Ok(EnhancedAudioBody::NoMultitrack { four_cc, packet });
    }
    let shared_four_cc = match header.content {
        AudioHeaderContent::OneTrack(four_cc)
        | AudioHeaderContent::ManyTracks(four_cc)
        | AudioHeaderContent::Unknown { four_cc, .. } => four_cc,
        AudioHeaderContent::NoMultitrack(_) | AudioHeaderContent::ManyTracksManyCodecs => {
            AudioFourCc([0; 4])
        }
    };
    let single = matches!(header.content, AudioHeaderContent::OneTrack(_));
    let mut tracks = Vec::new();
    loop {
        let four_cc = if matches!(header.content, AudioHeaderContent::ManyTracksManyCodecs) {
            AudioFourCc(cursor.read_fourcc()?)
        } else {
            shared_four_cc
        };
        let track_id = cursor.read_u8()?;
        let sized = !single;
        tracks.push(AudioTrack {
            four_cc,
            track_id,
            packet: audio_packet(header.packet_type, sized, cursor)?,
        });
        if single || cursor.remaining() == 0 {
            break;
        }
    }
    Ok(EnhancedAudioBody::ManyTracks(tracks))
}

fn audio_packet<P: MediaData>(
    packet_type: u8,
    sized: bool,
    cursor: &mut Cursor<P>,
) -> Result<AudioPacket<P>, FlvError> {
    // The multitrack size prefix is consumed up front even for packet types
    // that ignore it, matching the reference demuxer.
    let size = if sized {
        Some(cursor.read_u24()? as usize)
    } else {
        None
    };
    match packet_type {
        AUDIO_PACKET_MULTICHANNEL_CONFIG => {
            let order = cursor.read_u8()?;
            let channel_count = cursor.read_u8()?;
            match order {
                // Custom order carries an explicit per-channel map.
                2 => {
                    cursor.take(usize::from(channel_count))?;
                }
                // Native order carries a 32-bit channel mask.
                1 => {
                    cursor.take(4)?;
                }
                _ => {}
            }
            Ok(AudioPacket::MultichannelConfig { channel_count })
        }
        AUDIO_PACKET_SEQUENCE_END => Ok(AudioPacket::SequenceEnd),
        AUDIO_PACKET_SEQUENCE_START => Ok(AudioPacket::SequenceStart(sized_payload(cursor, size)?)),
        AUDIO_PACKET_CODED_FRAMES => Ok(AudioPacket::CodedFrames(sized_payload(cursor, size)?)),
        _ => Ok(AudioPacket::Unknown {
            packet_type,
            data: sized_payload(cursor, size)?,
        }),
    }
}

fn enhanced_video_body<P: MediaData>(
    header: &EnhancedVideoHeader,
    cursor: &mut Cursor<P>,
) -> Result<EnhancedVideoBody<P>, FlvError> {
    if matches!(header.content, VideoHeaderContent::VideoCommand(_)) {
        return Ok(EnhancedVideoBody::Command);
    }
    if let VideoHeaderContent::NoMultitrack(four_cc) = header.content {
        let packet = video_packet(header.packet_type, four_cc, false, cursor)?;
        return Ok(EnhancedVideoBody::NoMultitrack { four_cc, packet });
    }
    let shared_four_cc = match header.content {
        VideoHeaderContent::OneTrack(four_cc)
        | VideoHeaderContent::ManyTracks(four_cc)
        | VideoHeaderContent::Unknown { four_cc, .. } => four_cc,
        VideoHeaderContent::VideoCommand(_)
        | VideoHeaderContent::NoMultitrack(_)
        | VideoHeaderContent::ManyTracksManyCodecs => VideoFourCc([0; 4]),
    };
    let single = matches!(header.content, VideoHeaderContent::OneTrack(_));
    let mut tracks = Vec::new();
    loop {
        let four_cc = if matches!(header.content, VideoHeaderContent::ManyTracksManyCodecs) {
            VideoFourCc(cursor.read_fourcc()?)
        } else {
            shared_four_cc
        };
        let track_id = cursor.read_u8()?;
        let sized = !single;
        tracks.push(VideoTrack {
            four_cc,
            track_id,
            packet: video_packet(header.packet_type, four_cc, sized, cursor)?,
        });
        if single || cursor.remaining() == 0 {
            break;
        }
    }
    Ok(EnhancedVideoBody::ManyTracks(tracks))
}

fn video_packet<P: MediaData>(
    packet_type: u8,
    four_cc: VideoFourCc,
    sized: bool,
    cursor: &mut Cursor<P>,
) -> Result<VideoPacket<P>, FlvError> {
    // The multitrack size prefix is consumed up front even for packet types
    // that ignore it, matching the reference demuxer.
    let size = if sized {
        Some(cursor.read_u24()? as usize)
    } else {
        None
    };
    match packet_type {
        VIDEO_PACKET_METADATA => Ok(VideoPacket::Metadata(sized_payload(cursor, size)?)),
        VIDEO_PACKET_SEQUENCE_END => Ok(VideoPacket::SequenceEnd),
        VIDEO_PACKET_SEQUENCE_START => {
            let data = sized_payload(cursor, size)?;
            if four_cc.0 == *b"avc1" {
                validate_avc_decoder_config(&data)?;
            } else if four_cc.0 == *b"hvc1" {
                validate_hevc_decoder_config(&data)?;
            } else if four_cc.0 == *b"av01" {
                validate_av1_decoder_config(&data)?;
            }
            Ok(VideoPacket::SequenceStart(data))
        }
        VIDEO_PACKET_MPEG2TS_SEQUENCE_START => Ok(VideoPacket::Mpeg2TsSequenceStart(
            sized_payload(cursor, size)?,
        )),
        VIDEO_PACKET_CODED_FRAMES => {
            if four_cc.0 == *b"avc1" || four_cc.0 == *b"hvc1" {
                let composition_time_offset = cursor.read_i24()?;
                let data = match size {
                    Some(len) => cursor.take(len.saturating_sub(3))?,
                    None => cursor.take_rest(),
                };
                Ok(VideoPacket::CodedFrames {
                    composition_time_offset,
                    data,
                })
            } else {
                Ok(VideoPacket::CodedFrames {
                    composition_time_offset: 0,
                    data: sized_payload(cursor, size)?,
                })
            }
        }
        VIDEO_PACKET_CODED_FRAMES_X => Ok(VideoPacket::CodedFramesX(sized_payload(cursor, size)?)),
        _ => Ok(VideoPacket::Unknown {
            packet_type,
            data: sized_payload(cursor, size)?,
        }),
    }
}

/// Structural check for an AVC decoder configuration record (avcC): version
/// byte plus a plausible length. The record is forwarded raw, never decoded.
fn validate_avc_decoder_config<P: MediaData>(data: &P) -> Result<(), FlvError> {
    if data.len() < 7 || data.segment(0)[0] != 1 {
        return Err(FlvError(
            "invalid AVC decoder configuration record".to_owned(),
        ));
    }
    Ok(())
}

/// Structural check for an HEVC decoder configuration record (hvcC).
fn validate_hevc_decoder_config<P: MediaData>(data: &P) -> Result<(), FlvError> {
    if data.len() < 23 || data.segment(0)[0] != 1 {
        return Err(FlvError(
            "invalid HEVC decoder configuration record".to_owned(),
        ));
    }
    Ok(())
}

/// Structural check for an AV1 codec configuration record (av1C): marker bit
/// plus a plausible length.
fn validate_av1_decoder_config<P: MediaData>(data: &P) -> Result<(), FlvError> {
    if data.len() < 4 || data.segment(0)[0] & 0x80 == 0 {
        return Err(FlvError(
            "invalid AV1 codec configuration record".to_owned(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn audio(bytes: &[u8]) -> Result<ParsedAudio, FlvError> {
        ParsedAudio::demux(&Bytes::copy_from_slice(bytes))
    }

    fn video(bytes: &[u8]) -> Result<ParsedVideo, FlvError> {
        ParsedVideo::demux(&Bytes::copy_from_slice(bytes))
    }

    #[test]
    fn empty_input_is_an_error_not_a_panic() {
        let raw = Bytes::new();
        assert!(ParsedAudio::demux(&raw).is_err());
        assert!(ParsedVideo::demux(&raw).is_err());
    }

    #[test]
    fn legacy_audio_shapes() {
        match audio(&[0xaf, 0x00, 0x11, 0x88]).expect("legacy AAC").body {
            AudioTagBody::Legacy(LegacyAudioBody::AacSequenceHeader(data)) => {
                assert_eq!(data.as_ref(), &[0x11, 0x88]);
            }
            other => panic!("expected AAC sequence header, got {other:?}"),
        }
        match audio(&[0xaf, 0x01, 0xde]).expect("legacy AAC").body {
            AudioTagBody::Legacy(LegacyAudioBody::AacRaw(_)) => {}
            other => panic!("expected AAC raw, got {other:?}"),
        }
        match audio(&[0x2f, 0xff]).expect("legacy MP3").body {
            AudioTagBody::Legacy(LegacyAudioBody::Other(_)) => {}
            other => panic!("expected opaque legacy audio, got {other:?}"),
        }
        // Truncated AAC packet type is an error.
        assert!(audio(&[0xaf]).is_err());
    }

    #[test]
    fn legacy_video_shapes_and_signed_cts() {
        let tag = video(&[0x17, 0x01, 0xff, 0xff, 0xff, 0x65]).expect("legacy AVC");
        match (&tag.header.data, &tag.body) {
            (
                VideoTagHeaderData::Legacy(LegacyVideoHeader::AvcPacket(LegacyAvcPacket::Nalu {
                    composition_time_offset,
                })),
                VideoTagBody::Legacy(LegacyVideoBody::Other(_)),
            ) => assert_eq!(*composition_time_offset, -1),
            other => panic!("expected AVC NALU, got {other:?}"),
        }
        assert!(tag.header.frame_type == VIDEO_FRAME_KEY);
        // Truncated CTS is an error.
        assert!(video(&[0x17, 0x01, 0xff]).is_err());
    }

    #[test]
    fn unknown_values_parse_but_flag() {
        let tag = audio(b"\x92zzzz").expect("unknown FourCC still demuxes");
        match tag.header {
            AudioTagHeader::Enhanced(header) => {
                assert!(!header.has_unknown_modex);
                assert!(matches!(
                    header.content,
                    AudioHeaderContent::NoMultitrack(AudioFourCc(_))
                ));
            }
            other => panic!("expected enhanced header, got {other:?}"),
        }
        let tag = video(b"\x97\x00\x2a\x12hvc1").expect("unknown ModEx still demuxes");
        match tag.header.data {
            VideoTagHeaderData::Enhanced(header) => assert!(header.has_unknown_modex),
            other => panic!("expected enhanced header, got {other:?}"),
        }
    }

    #[test]
    fn every_prefix_of_the_corpus_demuxes_or_errors_without_panicking() {
        let corpus: &[&[u8]] = &[
            b"\x90Opusconfig",
            b"\x91Opusframe",
            b"\x92Opus",
            b"\x94mp4a\x00\x02",
            b"\x95\x02Opus\x00",
            b"\x97\x02\x00\x00\x01\x02Opus",
            b"\x90vp08config",
            b"\x91av01frame",
            b"\x92hvc1",
            b"\x93av01frame",
            b"\x94av01",
            b"\x95vp08descriptor",
            b"\x96\x02hvc1\x00",
            b"\x97\x02\x00\x00\x01\x02hvc1",
            b"\x92zzzz",
            b"\x93Opus",
            b"\x97\x00\x2a\x12Opus",
            b"\x98hvc1",
            b"\x92vvc1",
            &[0xaf, 0x00, 0x11, 0x88],
            &[0xaf, 0x01, 0xde, 0x02, 0x00],
            &[0x2f, 0xff, 0xfb],
            &[
                0x17, 0x01, 0xff, 0xff, 0xff, 0x00, 0x00, 0x00, 0x04, 0x65, 0x88, 0x84, 0x05,
            ],
        ];
        for tag in corpus {
            for len in 0..=tag.len() {
                let raw = Bytes::copy_from_slice(&tag[..len]);
                let _ = ParsedAudio::demux(&raw);
                let _ = ParsedVideo::demux(&raw);
            }
        }
    }
}
