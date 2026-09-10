use crate::amf::AmfEncoding;
use crate::chunk_io::ChunkDeserializerConfig;

/// The configuration options that govern how a RTMP server session should operate
#[derive(Clone)]
#[non_exhaustive]
pub struct ServerSessionConfig {
    pub fms_version: String,
    pub chunk_size: u32,
    pub peer_bandwidth: u32,
    pub window_ack_size: u32,
    pub send_on_bw_done_message_on_start: bool,
    /// Highest `objectEncoding` this server will agree to during `connect`.
    ///
    /// The client states the encoding it wants; the response states the one
    /// that will actually be used, which is the client's request clamped to
    /// this ceiling. Setting it to [`AmfEncoding::Amf0`] means the server never
    /// advertises or originates AMF3.
    ///
    /// This governs what is *advertised* and *emitted*, not what is accepted:
    /// inbound type 15/17 messages are decoded either way, so lowering this is
    /// safe against a peer that sends AMF3 regardless.
    pub max_object_encoding: AmfEncoding,
    /// Limits for untrusted inbound chunk state.
    pub chunk_deserializer: ChunkDeserializerConfig,
}

impl ServerSessionConfig {
    /// Creates a new server session config with overridable defaults
    pub fn new() -> ServerSessionConfig {
        ServerSessionConfig {
            fms_version: "FMS/3,0,1,1233".to_string(),
            peer_bandwidth: 2_500_000,
            window_ack_size: 1_073_741_824,
            chunk_size: 4096,
            send_on_bw_done_message_on_start: true,
            max_object_encoding: AmfEncoding::Amf3,
            chunk_deserializer: ChunkDeserializerConfig::default(),
        }
    }
}

impl Default for ServerSessionConfig {
    fn default() -> Self {
        Self::new()
    }
}

impl ServerSessionConfig {
    /// Set `fms_version`.
    pub fn with_fms_version(mut self, value: String) -> Self {
        self.fms_version = value;
        self
    }
    /// Set `chunk_size`.
    pub fn with_chunk_size(mut self, value: u32) -> Self {
        self.chunk_size = value;
        self
    }
    /// Set `peer_bandwidth`.
    pub fn with_peer_bandwidth(mut self, value: u32) -> Self {
        self.peer_bandwidth = value;
        self
    }
    /// Set `window_ack_size`.
    pub fn with_window_ack_size(mut self, value: u32) -> Self {
        self.window_ack_size = value;
        self
    }
    /// Set `send_on_bw_done_message_on_start`.
    pub fn with_send_on_bw_done_message_on_start(mut self, value: bool) -> Self {
        self.send_on_bw_done_message_on_start = value;
        self
    }
    /// Set `max_object_encoding`.
    pub fn with_max_object_encoding(mut self, value: AmfEncoding) -> Self {
        self.max_object_encoding = value;
        self
    }
    /// Set `chunk_deserializer`.
    pub fn with_chunk_deserializer(mut self, value: ChunkDeserializerConfig) -> Self {
        self.chunk_deserializer = value;
        self
    }
}
