//! Sans-I/O RTMP protocol API with Enhanced RTMP validation.
//!
//! This crate drives the RTMP state machine. Sessions take bytes in and
//! emit bytes out; the caller moves them over TCP, a pipe, or plain memory,
//! so the crate fits any async runtime or proxy without mandating one.
//! The runnable client and server in `examples/` show the TCP glue. The
//! first sample shows the protocol core with no network at all.
//!
//! Chunk framing lives in [`chunk_io`], the handshake in [`handshake`],
//! messages in [`messages`], client and server sessions in [`sessions`],
//! and timestamps in [`time`]. The protocol core derives from RML RTMP
//! (MIT, KallDrexx/rust-media-libs) with hardening for proxy use:
//! cumulative acknowledgements, resource limits, interleaved chunk streams,
//! and verbatim metadata and connect forwarding. See `README.md`
//! ("Changes from RML") for the full list.
//!
//! Enhanced RTMP media is inspected with an in-house FLV parser through
//! [`flv`], [`media`], [`metadata`], [`enhanced`], and [`elementary`].
//! Original bytes stay authoritative for forwarding. An elementary-media
//! view of the same tags serves ingest that does not wrap FLV.
//!
//! # Session input and output
//!
//! Create a session after the handshake. Control actions queue outputs; `receive`
//! returns one packet or event at a time. The caller keeps unread input and can
//! await backpressure between outputs. Media sends return a packet directly.
//!
//! ```
//! use bytes::Bytes;
//! use rtmpx::sessions::{ClientSession, ClientSessionConfig, ClientOutput};
//!
//! let mut client = ClientSession::new(ClientSessionConfig::default())?;
//! client.connect("live")?;
//! let mut input = Bytes::new();
//! let Some(ClientOutput::Packet(packet)) = client.receive(&mut input)? else {
//!     panic!("connect must produce a packet");
//! };
//! // A transport writes packet.io_slices(...), then advances by the written count.
//! // Packet owns its payload and progress, so it can move between tasks.
//! assert!(packet.remaining() > 0);
//! # Ok::<(), rtmpx::sessions::ClientSessionError>(())
//! ```
//!
//! See [`zero_copy_guide`] for the complete receive loop, pooling, and allocation
//! contracts. `examples/serve.rs` and `examples/publish.rs` contain runnable TCP adapters.
//!
//! # Example: inspect one Enhanced RTMP video tag
//!
//! Parse a single HEVC keyframe and read what it carries:
//!
//! ```
//! use bytes::Bytes;
//! use rtmpx::{EnhancedValidationMode, ValidatedMedia};
//!
//! // Extended header plus FourCC: one HEVC keyframe and its coded bytes.
//! // The low nibble selects the packet family: 1 means coded frames.
//! let raw = Bytes::from_static(b"\x91hvc1\x01\x02\x03");
//! let media =
//!     ValidatedMedia::parse_video(raw.clone(), EnhancedValidationMode::Strict).unwrap();
//! let class = media.classification();
//! assert!(class.coded && class.keyframe && !class.configuration);
//! assert_eq!(media.raw(), &raw);
//! ```
//!
//! # Example: inspect one legacy audio tag
//!
//! Parse a single AAC sequence header and read its classification:
//!
//! ```
//! use bytes::Bytes;
//! use rtmpx::{EnhancedValidationMode, ValidatedMedia};
//!
//! let raw = Bytes::from_static(&[0xAF, 0x00, 0x11, 0x88]);
//! let media =
//!     ValidatedMedia::parse_audio(raw.clone(), EnhancedValidationMode::Strict).unwrap();
//! assert!(media.classification().configuration);
//! assert_eq!(media.raw(), &raw);
//! ```

// Protocol core predates strict lints; keep its historical style contained so
// warnings in the validation code stay visible.
#[cfg(test)]
#[macro_use]
mod test_utils {
    #[macro_use]
    pub mod assert_vec_match_macro;
    #[macro_use]
    pub mod assert_vec_contains_macro;
}

pub mod payload;
pub use payload::{Payload, PayloadPool, PayloadPoolConfig, PayloadView, Segments};
pub mod amf;
#[allow(clippy::all)]
pub mod chunk_io;
pub mod elementary;
pub mod enhanced;
pub mod flv;
#[allow(clippy::all)]
pub mod handshake;
pub mod media;
#[allow(clippy::all)]
pub mod messages;
pub mod metadata;
#[allow(clippy::all)]
pub mod sessions;
#[allow(clippy::all)]
pub mod time;

pub use amf::amf0::Amf0Document;
pub use amf::amf3::Amf3Document;
pub use amf::{AmfEncoding, AmfProperties, AmfRead, AmfValue, ObjectId, TreeError, TreeLimits};
// The AMF codecs live under `amf`. Their modules stay re-exported here so
// existing `rtmpx::amf0` and `rtmpx::amf3` paths keep working.
pub use amf::amf0::{Amf0DeserializationError, Amf0Object, Amf0SerializationError, Amf0Value};
pub use amf::amf3::{Amf3DeserializationError, Amf3SerializationError, Amf3Value};
pub use amf::{amf0, amf3};
pub use elementary::{ElementaryCodec, ElementaryUnit};
pub use enhanced::{EnhancedCapabilities, EnhancedValidationMode};
pub use media::{
    MediaClassification, MediaInterpretation, MediaValidationError, ParsedAudio, ParsedVideo,
    ValidatedMedia,
};
pub use metadata::{
    EncoderSummary, MAX_ENCODER_LEN, MetadataCodec, ParsedMetadata, TrackMetadata,
    ValidatedMetadata, normalize_encoder_vendor,
};

/// Ownership and allocation examples.
#[doc = include_str!("../docs/zero-copy.md")]
pub mod zero_copy_guide {}

pub use chunk_io::{DropPolicy, EncodeOptions, HeaderMode, Packet};

#[cfg(test)]
extern crate self as rtmpx;
#[cfg(test)]
#[path = "../tests/support/api.rs"]
mod api;
