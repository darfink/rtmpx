//! Role-neutral RTMP protocol API: one crate for RTMP wire
//! protocol plus Enhanced RTMP validation.
//!
//! The sans-I/O chunking, handshake, message, session, and timestamp machinery
//! lives in [`chunk_io`], [`handshake`], [`messages`], [`sessions`], and
//! [`time`]. It derives from RML RTMP (MIT, KallDrexx/rust-media-libs) with
//! hardening for proxy use (cumulative acknowledgements, resource
//! limits, interleaved chunk streams, verbatim metadata/connect forwarding);
//! see `README.md` ("Changes from RML"). Enhanced RTMP media is inspected with
//! an in-house FLV parser via [`flv`], [`media`], [`metadata`], [`enhanced`],
//! and [`elementary`]. Original bytes remain authoritative for forwarding; an
//! elementary-media view of the same tags is available for ingest that does
//! not wrap FLV.

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
pub mod amf0;
pub mod amf3;
pub(crate) mod amf_common;
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

pub use amf::{AmfEncoding, AmfProperties, AmfValue};
pub use amf0::{Amf0DeserializationError, Amf0Object, Amf0SerializationError, Amf0Value};
pub use amf3::{Amf3DeserializationError, Amf3SerializationError, Amf3Value};
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
