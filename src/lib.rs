//! Sans-I/O RTMP protocol API with Enhanced RTMP validation.
//!
//! This crate drives the RTMP state machine. Sessions take bytes in and
//! emit bytes out; the caller moves them over TCP, a pipe, or plain memory,
//! so the crate fits any async runtime or proxy without mandating one.
//! The runnable client and server in `examples/` show the TCP glue. The
//! first sample below shows the protocol core with no network at all.
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
//! # Example: publish into a server with no network
//!
//! Each session result is either bytes for the peer or a raised event.
//! Moving the bytes across until both sides go quiet connects, publishes,
//! and accepts a stream entirely in memory:
//!
//! ```
//! # #![allow(unreachable_patterns)]
//! use rtmpx::sessions::{
//!     ClientSession, ClientSessionConfig, ClientSessionEvent, ClientSessionResult,
//!     PublishRequestType, ServerSession, ServerSessionConfig, ServerSessionEvent,
//!     ServerSessionResult,
//! };
//!
//! fn from_client(
//!     out: Vec<ClientSessionResult>,
//!     client: &mut ClientSession,
//!     server: &mut ServerSession,
//!     client_events: &mut Vec<ClientSessionEvent>,
//!     server_events: &mut Vec<ServerSessionEvent>,
//! ) {
//!     let mut bytes = Vec::new();
//!     for result in out {
//!         match result {
//!             ClientSessionResult::OutboundResponse(packet) => {
//!                 bytes.extend_from_slice(&packet.bytes);
//!             }
//!             ClientSessionResult::RaisedEvent(event) => client_events.push(event),
//!             ClientSessionResult::UnhandleableMessageReceived(_) => {}
//!             // Result enums stay non-exhaustive so new variants cannot
//!             // break callers: unreachable today, but required to compile.
//!             _ => unreachable!("new client session result"),
//!         }
//!     }
//!     if bytes.is_empty() {
//!         return;
//!     }
//!     let back = server
//!         .handle_input(&bytes)
//!         .expect("server reads client bytes");
//!     from_server(back, client, server, client_events, server_events);
//! }
//!
//! fn from_server(
//!     out: Vec<ServerSessionResult>,
//!     client: &mut ClientSession,
//!     server: &mut ServerSession,
//!     client_events: &mut Vec<ClientSessionEvent>,
//!     server_events: &mut Vec<ServerSessionEvent>,
//! ) {
//!     let mut bytes = Vec::new();
//!     for result in out {
//!         match result {
//!             ServerSessionResult::OutboundResponse(packet) => {
//!                 bytes.extend_from_slice(&packet.bytes);
//!             }
//!             ServerSessionResult::RaisedEvent(event) => server_events.push(event),
//!             ServerSessionResult::UnhandleableMessageReceived(_) => {}
//!             _ => unreachable!("new server session result"),
//!         }
//!     }
//!     if bytes.is_empty() {
//!         return;
//!     }
//!     let back = client
//!         .handle_input(&bytes)
//!         .expect("client reads server bytes");
//!     from_client(back, client, server, client_events, server_events);
//! }
//!
//! let (mut client, out) =
//!     ClientSession::new(ClientSessionConfig::new()).expect("client starts");
//! let (mut server, out2) =
//!     ServerSession::new(ServerSessionConfig::new()).expect("server starts");
//! let (mut client_events, mut server_events) = (Vec::new(), Vec::new());
//! from_client(out, &mut client, &mut server, &mut client_events, &mut server_events);
//! from_server(out2, &mut client, &mut server, &mut client_events, &mut server_events);
//!
//! let out = client
//!     .request_connection("live".to_string())
//!     .expect("connect builds");
//! from_client(vec![out], &mut client, &mut server, &mut client_events, &mut server_events);
//! let request = server_events
//!     .iter()
//!     .find_map(|event| match event {
//!         ServerSessionEvent::ConnectionRequested { request_id, .. } => Some(*request_id),
//!         _ => None,
//!     })
//!     .expect("connect raises a request");
//! let out = server.accept_request(request).expect("accept builds");
//! from_server(out, &mut client, &mut server, &mut client_events, &mut server_events);
//! assert!(client_events.iter().any(|event| matches!(
//!     event,
//!     ClientSessionEvent::ConnectionRequestAccepted { .. }
//! )));
//!
//! let out = client
//!     .request_publishing("demo".to_string(), PublishRequestType::Live)
//!     .expect("publish builds");
//! from_client(vec![out], &mut client, &mut server, &mut client_events, &mut server_events);
//! let request = server_events
//!     .iter()
//!     .find_map(|event| match event {
//!         ServerSessionEvent::PublishStreamRequested { request_id, .. } => Some(*request_id),
//!         _ => None,
//!     })
//!     .expect("publish raises a request");
//! let out = server.accept_request(request).expect("accept builds");
//! from_server(out, &mut client, &mut server, &mut client_events, &mut server_events);
//! assert!(client_events.iter().any(|event| matches!(
//!     event,
//!     ClientSessionEvent::PublishRequestAccepted { .. }
//! )));
//! ```
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
