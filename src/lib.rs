//! Sans-I/O RTMP protocol API with Enhanced RTMP validation.
//!
//! This crate drives the RTMP state machine. Callers move bytes in and out,
//! so it embeds in any async runtime or proxy without mandating one.
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
//! # Example
//!
//! Parse one legacy AAC audio tag and read its classification:
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

pub use amf::{AmfEncoding, AmfProperties, AmfRead, AmfValue};
// The AMF codecs live under `amf`. Their modules stay re-exported here so
// existing `rtmpx::amf0` and `rtmpx::amf3` paths keep working.
pub use amf::amf0::{Amf0DeserializationError, Amf0Object, Amf0SerializationError, Amf0Value};
pub use amf::amf3::{Amf3DeserializationError, Amf3SerializationError, Amf3Value};
pub use amf::{amf0, amf3};
pub use elementary::{ElementaryCodec, ElementaryUnit};
pub use enhanced::{EnhancedCapabilities, EnhancedValidationMode};
pub use media::{
    MediaClassification, MediaInterpretation, ParsedAudio, ParsedVideo, ValidatedMedia,
};
pub use metadata::{
    EncoderSummary, MAX_ENCODER_LEN, MetadataCodec, ParsedMetadata, TrackMetadata,
    ValidatedMetadata, normalize_encoder_vendor,
};

/// Socket-operation timeouts used by async adapters built around the sans-I/O
/// session API. `None` disables the corresponding timeout.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ServerSessionTimeouts {
    pub handshake_read: Option<std::time::Duration>,
    pub session_read: Option<std::time::Duration>,
    pub write: Option<std::time::Duration>,
}

impl Default for ServerSessionTimeouts {
    fn default() -> Self {
        Self {
            handshake_read: Some(std::time::Duration::from_secs(2)),
            session_read: Some(std::time::Duration::from_millis(2_500)),
            write: Some(std::time::Duration::from_secs(2)),
        }
    }
}
