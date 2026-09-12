use crate::amf0::{Amf0Object, Amf0Value};
use crate::sessions::{DataMessage, StreamHandle};
use crate::time::RtmpTimestamp;

/// Events that can be raised by the client session so that custom business logic can be written
/// to react to it
#[derive(PartialEq, Debug)]
#[non_exhaustive]
pub enum ClientEvent<D = crate::Payload> {
    /// Raised when a connection request has been accepted by the server
    ConnectionRequestAccepted {
        /// Server properties from the `_result` command object.
        command_object: Amf0Object,
        /// Server status and Enhanced capability properties.
        additional_properties: Amf0Object,
    },

    /// The server has rejected the connection request
    ConnectionRequestRejected {
        description: String,
        status: CommandStatus,
    },

    /// The server has accepted our request to play video back from a stream key
    PlaybackRequestAccepted {
        stream: StreamHandle,
        status: CommandStatus,
    },

    /// The server has accepted our request to publish video
    PublishRequestAccepted {
        stream: StreamHandle,
        status: CommandStatus,
    },

    /// Encoded script data, including metadata, captions, and undecodable bodies.
    StreamDataReceived {
        stream: StreamHandle,
        message: DataMessage<D>,
    },

    /// The server has sent over video data for the stream
    VideoDataReceived {
        stream: StreamHandle,
        timestamp: RtmpTimestamp,
        data: D,
    },

    /// The server has sent over audio data for the stream
    AudioDataReceived {
        stream: StreamHandle,
        timestamp: RtmpTimestamp,
        data: D,
    },

    /// The server sent a command that the session could not handle
    UnhandledCommand {
        stream_id: crate::sessions::StreamId,
        command_name: String,
        transaction_id: f64,
        command_object: Amf0Value,
        additional_values: Vec<Amf0Value>,
    },

    /// The server sent us a result to a transaction that we don't know about
    UnknownTransactionResultReceived {
        transaction_id: f64,
        command_object: Amf0Value,
        additional_values: Vec<Amf0Value>,
    },

    /// The server sent an `onStatus` message with a `code` property that we don't know
    /// how to handle.
    StatusReceived {
        stream: Option<StreamHandle>,
        status: CommandStatus,
    },

    /// The server rejected playback or stream creation.
    PlaybackRequestRejected {
        stream: StreamHandle,
        status: CommandStatus,
    },
    /// The server rejected publishing or stream creation.
    PublishRequestRejected {
        stream: StreamHandle,
        status: CommandStatus,
    },
    /// Playback ended on this stream.
    PlaybackFinished {
        stream: StreamHandle,
        status: CommandStatus,
    },
    /// Publishing ended on this stream.
    PublishingFinished {
        stream: StreamHandle,
        status: CommandStatus,
    },

    /// The server has sent an acknowledgement that they have received the specified number of bytes
    AcknowledgementReceived { bytes_received: u32 },

    /// The server has responded to a ping request
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

impl<D> ClientEvent<D> {
    /// Transform media and script-data storage without changing event semantics.
    pub fn map_payload<T>(self, mut map: impl FnMut(D) -> T) -> ClientEvent<T> {
        match self {
            Self::ConnectionRequestAccepted {
                command_object,
                additional_properties,
            } => ClientEvent::ConnectionRequestAccepted {
                command_object,
                additional_properties,
            },
            Self::ConnectionRequestRejected {
                description,
                status,
            } => ClientEvent::ConnectionRequestRejected {
                description,
                status,
            },
            Self::PlaybackRequestAccepted { stream, status } => {
                ClientEvent::PlaybackRequestAccepted { stream, status }
            }
            Self::PublishRequestAccepted { stream, status } => {
                ClientEvent::PublishRequestAccepted { stream, status }
            }
            Self::StreamDataReceived { stream, message } => ClientEvent::StreamDataReceived {
                stream,
                message: message.map_payload(&mut map),
            },
            Self::VideoDataReceived {
                stream,
                timestamp,
                data,
            } => ClientEvent::VideoDataReceived {
                stream,
                timestamp,
                data: map(data),
            },
            Self::AudioDataReceived {
                stream,
                timestamp,
                data,
            } => ClientEvent::AudioDataReceived {
                stream,
                timestamp,
                data: map(data),
            },
            Self::UnhandledCommand {
                stream_id,
                command_name,
                transaction_id,
                command_object,
                additional_values,
            } => ClientEvent::UnhandledCommand {
                stream_id,
                command_name,
                transaction_id,
                command_object,
                additional_values,
            },
            Self::UnknownTransactionResultReceived {
                transaction_id,
                command_object,
                additional_values,
            } => ClientEvent::UnknownTransactionResultReceived {
                transaction_id,
                command_object,
                additional_values,
            },
            Self::StatusReceived { stream, status } => {
                ClientEvent::StatusReceived { stream, status }
            }
            Self::PlaybackRequestRejected { stream, status } => {
                ClientEvent::PlaybackRequestRejected { stream, status }
            }
            Self::PublishRequestRejected { stream, status } => {
                ClientEvent::PublishRequestRejected { stream, status }
            }
            Self::PlaybackFinished { stream, status } => {
                ClientEvent::PlaybackFinished { stream, status }
            }
            Self::PublishingFinished { stream, status } => {
                ClientEvent::PublishingFinished { stream, status }
            }
            Self::AcknowledgementReceived { bytes_received } => {
                ClientEvent::AcknowledgementReceived { bytes_received }
            }
            Self::PingResponseReceived { timestamp } => {
                ClientEvent::PingResponseReceived { timestamp }
            }
        }
    }
}
