use crate::chunk_io::{DecodeError, EncodeError};

use crate::messages::{MessageDeserializationError, MessageSerializationError};
use crate::sessions::{ClientStreamState, ConnectionState, StreamHandle};
use thiserror::Error;

/// Error state when a client session encounters an error
/// Represents the type of error that occurred
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ClientSessionError {
    #[error(transparent)]
    LimitExceeded(#[from] crate::sessions::SessionLimitError),
    #[error("stream handle is deleted or belongs to another session")]
    InvalidStreamHandle,
    #[error("stream {stream:?} is in state {state:?}")]
    StreamInInvalidState {
        stream: StreamHandle,
        state: ClientStreamState,
    },
    #[error("server reused an active message stream ID")]
    DuplicateStreamId,

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

    /// Encountered if a connection request is made while we are already connected
    #[error(
        "A connection request was attempted while this session is already in a connected state"
    )]
    CantConnectWhileAlreadyConnected,

    /// Encountered if a request is made, or a response is received for a request while the
    /// client session is not in a valid state for that purpose.
    #[error(
        "The request could not be performed while the session is in the {current_state:?} state"
    )]
    #[non_exhaustive]
    SessionInInvalidState { current_state: ConnectionState },

    /// A response to a `createStream` request should have a numeric as the first parameter
    /// in the additional values property of the amf0 command.  This error is thrown if this is
    /// not present.  Without a stream ID we have no way to know what stream to communicate with
    /// for playback/publishing messages.
    #[error("The server sent a create stream success result without a stream id")]
    CreateStreamResponseHadNoStreamNumber,

    /// When the server sends and `onStatus` message, it is expected that the additional arguments
    /// contains a single value representing an amf0 object.  This is required because the object
    /// should have a `code` property that says the type of operation the status is for.
    #[error("The server sent an onStatus message with invalid arguments")]
    InvalidOnStatusArguments,
}
