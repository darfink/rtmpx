use crate::chunk_io::{DecodeError, EncodeError};

use crate::messages::{MessageDeserializationError, MessageSerializationError};
use thiserror::Error;

/// Error state when a server session encounters an error
/// Represents the type of error that occurred
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ServerSessionError {
    #[error(transparent)]
    LimitExceeded(#[from] crate::sessions::SessionLimitError),
    #[error("stream {stream:?} is in state {state:?}")]
    StreamInInvalidState {
        stream: crate::sessions::StreamHandle,
        state: crate::sessions::ServerStreamState,
    },
    #[error("message stream identifiers exhausted")]
    StreamIdsExhausted,
    #[error("stream handle is deleted or belongs to another session")]
    InvalidStreamHandle,
    /// Accept and drain a connection request before sending packets.
    #[error("accept a connection request before sending a packet")]
    NotConnected,

    #[error("drain pending session outputs through receive before sending a packet")]
    PendingOutput,

    /// An earlier input error terminated this session. Close its transport.
    #[error("session terminated after an input error")]
    SessionFailed,
    /// Encountered when an error occurs while deserializing the incoming byte data
    #[error("An error occurred deserializing incoming data: {0}")]
    DecodeError(#[from] DecodeError),

    /// Encountered when an error occurs while serializing outbound messages
    #[error("An error occurred serializing outbound messages: {0}")]
    EncodeError(#[from] EncodeError),

    /// Encountered when an error occurs while turning an RTMP message into an message payload
    #[error(
        "An error occurred while attempting to turn an RTMP message into a message payload: {0}"
    )]
    MessageSerializationError(#[from] MessageSerializationError),

    /// Encountered when an error occurs while turning a message payload into an RTMP message
    #[error(
        "An error occurred while attempting to turn a message payload into an RTMP message: {0}"
    )]
    MessageDeserializationError(#[from] MessageDeserializationError),

    /// A request id that was attempting to be accepted or rejected was not a known
    /// outstanding request.
    #[error(
        "Attempted to accept or reject request id {0} but no outstanding requests have that id"
    )]
    InvalidOutstandingRequest(u32),

    /// A connection request was made without a valid RTMP app name
    #[error("The connection request did not have a non-empty RTMP app name")]
    NoAppNameForConnectionRequest,

    /// The request id specified did not match an outstanding request
    #[error("The request id specified could not be matched to an outstanding request")]
    InvalidRequestId,

    /// An action was attempted to be performed on a inactive stream
    #[error("The '{action}' action was attempted on non-existant stream id {stream_id}")]
    #[non_exhaustive]
    ActionAttemptedOnInactiveStream { action: String, stream_id: u32 },
}
