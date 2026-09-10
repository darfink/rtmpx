use crate::amf::AmfEncoding;
use crate::chunk_io::ChunkDeserializerConfig;

/// Configuration options that govern how a RTMP client session should operate
#[derive(Clone)]
pub struct ClientSessionConfig {
    pub flash_version: String,
    pub playback_buffer_length_ms: u32,
    pub window_ack_size: u32,
    pub chunk_size: u32,
    pub tc_url: Option<String>,
    /// `objectEncoding` to request in the `connect` command.
    ///
    /// The server answers with the encoding that will actually be used, which
    /// may be lower than this. Defaults to [`AmfEncoding::Amf0`], which every
    /// RTMP peer supports; ask for [`AmfEncoding::Amf3`] only when the far side
    /// is known to want it, since most servers ignore or refuse it.
    pub object_encoding: AmfEncoding,
    /// Limits for untrusted inbound chunk state.
    pub chunk_deserializer: ChunkDeserializerConfig,
}

impl ClientSessionConfig {
    /// Creates a new configuration object with default values
    pub fn new() -> ClientSessionConfig {
        ClientSessionConfig {
            flash_version: "WIN 23,0,0,207".to_string(),
            playback_buffer_length_ms: 2_000,
            window_ack_size: 2_500_000,
            chunk_size: 4096,
            tc_url: None,
            object_encoding: AmfEncoding::Amf0,
            chunk_deserializer: ChunkDeserializerConfig::default(),
        }
    }
}

impl Default for ClientSessionConfig {
    fn default() -> Self {
        Self::new()
    }
}
