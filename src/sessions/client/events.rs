use crate::amf0::{Amf0Object, Amf0Value};
use crate::sessions::DataMessage;
use crate::sessions::StreamMetadata;
use crate::time::RtmpTimestamp;
use bytes::Bytes;

/// Events that can be raised by the client session so that custom business logic can be written
/// to react to it
#[derive(PartialEq, Debug)]
#[non_exhaustive]
pub enum ClientSessionEvent {
    /// Raised when a connection request has been accepted by the server
    #[non_exhaustive]
    ConnectionRequestAccepted {
        /// Server properties from the `_result` command object.
        command_object: Amf0Object,
        /// Server status and Enhanced capability properties.
        additional_properties: Amf0Object,
    },

    /// The server has rejected the connection request
    #[non_exhaustive]
    ConnectionRequestRejected {
        description: String,
        status: CommandStatus,
    },

    /// The server has accepted our request to play video back from a stream key
    #[non_exhaustive]
    PlaybackRequestAccepted { status: CommandStatus },

    /// The server has accepted our request to publish video
    #[non_exhaustive]
    PublishRequestAccepted { status: CommandStatus },

    /// The server has sent over new metadata for the stream
    #[non_exhaustive]
    StreamMetadataReceived {
        metadata: StreamMetadata,
        message: DataMessage,
    },

    /// Script data other than recognized metadata, including undecodable bodies.
    #[non_exhaustive]
    StreamDataReceived { message: DataMessage },

    /// The server has sent over video data for the stream
    #[non_exhaustive]
    VideoDataReceived {
        timestamp: RtmpTimestamp,
        data: Bytes,
    },

    /// The server has sent over audio data for the stream
    #[non_exhaustive]
    AudioDataReceived {
        timestamp: RtmpTimestamp,
        data: Bytes,
    },

    /// The server sent an Amf0 command that was not able to be handled
    #[non_exhaustive]
    UnhandleableAmf0Command {
        stream_id: crate::sessions::StreamId,
        command_name: String,
        transaction_id: f64,
        command_object: Amf0Value,
        additional_values: Vec<Amf0Value>,
    },

    /// The server sent us a result to a transaction that we don't know about
    #[non_exhaustive]
    UnknownTransactionResultReceived {
        transaction_id: f64,
        command_object: Amf0Value,
        additional_values: Vec<Amf0Value>,
    },

    /// The server sent an `onStatus` message with a `code` property that we don't know
    /// how to handle.
    #[non_exhaustive]
    StatusReceived { status: CommandStatus },

    /// The server rejected playback or stream creation.
    #[non_exhaustive]
    PlaybackRequestRejected { status: CommandStatus },
    /// The server rejected publishing or stream creation.
    #[non_exhaustive]
    PublishRequestRejected { status: CommandStatus },
    /// Playback ended on the active stream.
    #[non_exhaustive]
    PlaybackFinished { status: CommandStatus },
    /// Publishing ended on the active stream.
    #[non_exhaustive]
    PublishingFinished { status: CommandStatus },

    /// The client has sent an acknowledgement that they have received the specified number of bytes
    #[non_exhaustive]
    AcknowledgementReceived { bytes_received: u32 },

    /// The client has responded to a ping request
    #[non_exhaustive]
    PingResponseReceived { timestamp: RtmpTimestamp },
}

/// Full server status properties with the originating message stream, if applicable.
#[derive(Clone, Debug, PartialEq)]
pub struct CommandStatus {
    stream_id: Option<crate::sessions::StreamId>,
    properties: Amf0Object,
}
impl CommandStatus {
    pub(crate) fn new(
        stream_id: Option<crate::sessions::StreamId>,
        properties: Amf0Object,
    ) -> Self {
        Self {
            stream_id,
            properties,
        }
    }
    pub fn stream_id(&self) -> Option<crate::sessions::StreamId> {
        self.stream_id
    }
    pub fn properties(&self) -> &Amf0Object {
        &self.properties
    }
    pub fn code(&self) -> Option<&str> {
        self.text("code")
    }
    pub fn description(&self) -> Option<&str> {
        self.text("description")
    }
    pub(crate) fn text(&self, key: &str) -> Option<&str> {
        match self.properties.get(key) {
            Some(Amf0Value::Utf8String(s)) => Some(s),
            _ => None,
        }
    }
}
