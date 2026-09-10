use crate::amf0::Amf0DeserializationError;
use thiserror::Error;

use std::io;

use crate::amf3::Amf3DeserializationError as Amf3Error;

/// Error state when deserialization errors occur
/// Enumeration that represents the various errors that may occur while trying to
/// deserialize a RTMP message
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum MessageDeserializationError {
    /// The bytes or amf0 values contained in the message were not what were expected, and thus
    /// the message could not be parsed.
    #[error("The message was not encoded in an expected format")]
    InvalidMessageFormat,

    /// The bytes in the message that were expected to be AMF0 values were not properly encoded,
    /// and thus could not be read
    #[error("The message did no contain valid Amf0 encoded values: {0}")]
    Amf0DeserializationError(#[from] Amf0DeserializationError),

    /// The bytes in the message that were expected to be AMF3 values were not properly encoded.
    #[error("The message did not contain valid Amf3 encoded values: {0}")]
    Amf3DeserializationError(#[from] Amf3Error),

    /// Failed to read the values from the input buffer
    #[error("An IO error occurred while reading the input: {0}")]
    Io(#[from] io::Error),
}
