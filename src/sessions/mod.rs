/*!
Session abstractions for RTMP clients and servers.

A session hides the RTMP message flow behind owned input and one packet or event per receive call. Each session owns its own `ChunkEncoder` and `MessageDecoder`.
A single session represents one peer of one RTMP connection. A connection
manager needs one distinct session instance per connection.

Create a session only after the handshake completes.
*/

pub mod client;
pub mod server;

pub use self::client::ClientEvent;
pub use self::client::ClientOutput;
pub use self::client::ClientSession;
pub use self::client::ClientSessionConfig;
pub use self::client::ClientSessionError;
mod streams;
pub use streams::{ClientStreamState, ConnectionState, ServerStreamState, StreamHandle};

mod publish_mode;
pub use self::server::ServerEvent;
pub use self::server::ServerOutput;
pub use self::server::ServerSession;
pub use self::server::ServerSessionConfig;
pub use self::server::ServerSessionError;
pub use publish_mode::PublishMode;

use crate::amf0::Amf0Object;

/// Contains the metadata information a stream may advertise on publishing
#[derive(PartialEq, Debug, Clone, Default)]
pub struct StreamMetadata {
    pub video_width: Option<u32>,
    pub video_height: Option<u32>,
    pub video_codec_id: Option<u32>,
    pub video_frame_rate: Option<f32>,
    pub video_bitrate_kbps: Option<u32>,
    pub audio_codec_id: Option<u32>,
    pub audio_bitrate_kbps: Option<u32>,
    pub audio_sample_rate: Option<u32>,
    pub audio_channels: Option<u32>,
    pub audio_is_stereo: Option<bool>,
    pub encoder: Option<String>,
}

impl StreamMetadata {
    /// Creates a new (and empty) metadata instance.
    pub fn new() -> StreamMetadata {
        Self::default()
    }

    /// Iterates through the passed in hashmap and uses their values to set the metadata
    /// properties. The keys are based on standard metadata property names seen from existing
    /// RTMP encoders.
    pub fn apply_metadata_values(&mut self, mut properties: Amf0Object) {
        for (key, value) in properties.drain(..) {
            match key.as_ref() {
                "width" => match value.get_number() {
                    Some(x) => self.video_width = Some(x as u32),
                    None => (),
                },

                "height" => match value.get_number() {
                    Some(x) => self.video_height = Some(x as u32),
                    None => (),
                },

                "videocodecid" => match value.get_number() {
                    Some(x) => self.video_codec_id = Some(x as u32),
                    None => (),
                },

                "videodatarate" => match value.get_number() {
                    Some(x) => self.video_bitrate_kbps = Some(x as u32),
                    None => (),
                },

                "framerate" => match value.get_number() {
                    Some(x) => self.video_frame_rate = Some(x as f32),
                    None => (),
                },

                "audiocodecid" => match value.get_number() {
                    Some(x) => self.audio_codec_id = Some(x as u32),
                    None => (),
                },

                "audiodatarate" => match value.get_number() {
                    Some(x) => self.audio_bitrate_kbps = Some(x as u32),
                    None => (),
                },

                "audiosamplerate" => match value.get_number() {
                    Some(x) => self.audio_sample_rate = Some(x as u32),
                    None => (),
                },

                "audiochannels" => match value.get_number() {
                    Some(x) => self.audio_channels = Some(x as u32),
                    None => (),
                },

                "stereo" => match value.get_boolean() {
                    Some(x) => self.audio_is_stereo = Some(x),
                    None => (),
                },

                "encoder" => match value.get_string() {
                    Some(x) => self.encoder = Some(x),
                    None => (),
                },

                _ => (),
            }
        }
    }
}

mod data;
mod ids;
pub use data::{DataMessage, DataMessageType};
pub use ids::{RequestId, StreamId};

pub use client::CommandStatus;

mod limits;
pub use limits::{SessionLimitError, SessionLimits};
