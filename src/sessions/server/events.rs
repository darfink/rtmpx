use super::PublishMode;
use crate::amf0::Amf0Value;
use crate::sessions::{DataMessage, RequestId, StreamHandle, StreamId};
use crate::time::RtmpTimestamp;
use std::sync::Arc;

/// Represents where RTMP playback should start from
#[derive(PartialEq, Debug, Clone)]
#[non_exhaustive]
pub enum PlayStartValue {
    /// If a live stream exists for the specified stream keyplay it, if not
    /// play the recorded stream with a matching name
    LiveOrRecorded,

    /// Only play live streams with the provided stream key
    LiveOnly,

    /// Play the recorded stream for the stream key at the specified start time
    StartTimeInSeconds(u32),
}

/// An event that a server session can raise
#[derive(Debug, PartialEq, Clone)]
#[non_exhaustive]
pub enum ServerEvent<D = crate::Payload> {
    /// The client is changing the maximum size of the RTMP chunks they will be sending
    ClientChunkSizeChanged { new_chunk_size: u32 },

    /// The client is requesting a connection on the specified RTMP application name
    ConnectionRequested {
        request_id: RequestId,
        app_name: Arc<str>,
        /// The remaining `connect` command object properties.
        ///
        /// The session consumes `app` and `objectEncoding` itself and previously
        /// discarded everything else. A proxy needs the rest - in particular the
        /// Enhanced RTMP advertisement (`fourCcList`, `capsEx`, and the FourCC
        /// info maps) - so it can forward the publisher's capabilities to the
        /// backend instead of silently downgrading the negotiation.
        additional_properties: crate::amf0::Amf0Object,
    },

    /// The client is requesting a stream key be released for use.
    ReleaseStreamRequested {
        request_id: RequestId,
        app_name: Arc<str>,
        stream_key: Arc<str>,
    },

    /// The client is requesting the ability to publish on the specified stream key,
    PublishStreamRequested {
        stream: StreamHandle,
        request_id: RequestId,
        app_name: Arc<str>,
        stream_key: Arc<str>,
        mode: PublishMode,
        stream_id: StreamId,
    },

    /// The client is finished publishing on the specified stream key
    PublishStreamFinished {
        stream: StreamHandle,
        stream_id: StreamId,
        app_name: Arc<str>,
        stream_key: Arc<str>,
    },

    /// Encoded script data, including metadata, captions, and undecodable bodies.
    ///
    /// The raw encoded payload is carried so relays can preserve events such as
    /// `onCaption` without decoding and re-encoding their application-specific
    /// fields.
    StreamDataReceived {
        stream: StreamHandle,
        stream_id: StreamId,
        app_name: Arc<str>,
        stream_key: Arc<str>,
        message: DataMessage<D>,
    },

    /// Audio data was received from the client
    AudioDataReceived {
        stream: StreamHandle,
        stream_id: StreamId,
        app_name: Arc<str>,
        stream_key: Arc<str>,
        data: D,
        timestamp: RtmpTimestamp,
    },

    /// Video data received from the client
    VideoDataReceived {
        stream: StreamHandle,
        stream_id: StreamId,
        app_name: Arc<str>,
        stream_key: Arc<str>,
        data: D,
        timestamp: RtmpTimestamp,
    },

    /// The client sent a command that the session could not handle
    UnhandledCommand {
        stream_id: StreamId,
        command_name: String,
        transaction_id: f64,
        command_object: Amf0Value,
        additional_values: Vec<Amf0Value>,
    },

    /// The client is requesting playback of the specified stream
    PlayStreamRequested {
        stream: StreamHandle,
        request_id: RequestId,
        app_name: Arc<str>,
        stream_key: Arc<str>,
        start_at: PlayStartValue,
        duration: Option<u32>,
        reset: bool,
        stream_id: StreamId,
    },

    /// The client is finished with playback of the specified stream
    PlayStreamFinished {
        stream: StreamHandle,
        stream_id: StreamId,
        app_name: Arc<str>,
        stream_key: Arc<str>,
    },

    /// The client has sent an acknowledgement that they have received the specified number of bytes
    AcknowledgementReceived { bytes_received: u32 },

    /// The client has responded to a ping request
    PingResponseReceived { timestamp: RtmpTimestamp },
}

impl<D> ServerEvent<D> {
    /// Transform media and script-data storage without changing event semantics.
    pub fn map_payload<T>(self, mut map: impl FnMut(D) -> T) -> ServerEvent<T> {
        match self {
            Self::ClientChunkSizeChanged { new_chunk_size } => {
                ServerEvent::ClientChunkSizeChanged { new_chunk_size }
            }
            Self::ConnectionRequested {
                request_id,
                app_name,
                additional_properties,
            } => ServerEvent::ConnectionRequested {
                request_id,
                app_name,
                additional_properties,
            },
            Self::ReleaseStreamRequested {
                request_id,
                app_name,
                stream_key,
            } => ServerEvent::ReleaseStreamRequested {
                request_id,
                app_name,
                stream_key,
            },
            Self::PublishStreamRequested {
                stream,
                request_id,
                app_name,
                stream_key,
                mode,
                stream_id,
            } => ServerEvent::PublishStreamRequested {
                stream,
                request_id,
                app_name,
                stream_key,
                mode,
                stream_id,
            },
            Self::PublishStreamFinished {
                stream,
                stream_id,
                app_name,
                stream_key,
            } => ServerEvent::PublishStreamFinished {
                stream,
                stream_id,
                app_name,
                stream_key,
            },
            Self::StreamDataReceived {
                stream,
                stream_id,
                app_name,
                stream_key,
                message,
            } => ServerEvent::StreamDataReceived {
                stream,
                stream_id,
                app_name,
                stream_key,
                message: message.map_payload(&mut map),
            },
            Self::AudioDataReceived {
                stream,
                stream_id,
                app_name,
                stream_key,
                data,
                timestamp,
            } => ServerEvent::AudioDataReceived {
                stream,
                stream_id,
                app_name,
                stream_key,
                data: map(data),
                timestamp,
            },
            Self::VideoDataReceived {
                stream,
                stream_id,
                app_name,
                stream_key,
                data,
                timestamp,
            } => ServerEvent::VideoDataReceived {
                stream,
                stream_id,
                app_name,
                stream_key,
                data: map(data),
                timestamp,
            },
            Self::UnhandledCommand {
                stream_id,
                command_name,
                transaction_id,
                command_object,
                additional_values,
            } => ServerEvent::UnhandledCommand {
                stream_id,
                command_name,
                transaction_id,
                command_object,
                additional_values,
            },
            Self::PlayStreamRequested {
                stream,
                request_id,
                app_name,
                stream_key,
                start_at,
                duration,
                reset,
                stream_id,
            } => ServerEvent::PlayStreamRequested {
                stream,
                request_id,
                app_name,
                stream_key,
                start_at,
                duration,
                reset,
                stream_id,
            },
            Self::PlayStreamFinished {
                stream,
                stream_id,
                app_name,
                stream_key,
            } => ServerEvent::PlayStreamFinished {
                stream,
                stream_id,
                app_name,
                stream_key,
            },
            Self::AcknowledgementReceived { bytes_received } => {
                ServerEvent::AcknowledgementReceived { bytes_received }
            }
            Self::PingResponseReceived { timestamp } => {
                ServerEvent::PingResponseReceived { timestamp }
            }
        }
    }
}
