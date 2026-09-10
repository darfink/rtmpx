use bytes::Bytes;

use crate::messages::RtmpMessage;
use crate::messages::{MessageDeserializationError, MessageSerializationError};

pub fn serialize_amf0(data: Bytes) -> Result<Bytes, MessageSerializationError> {
    Ok(data)
}

pub fn serialize_amf3(data: Bytes) -> Result<Bytes, MessageSerializationError> {
    Ok(data)
}

pub fn deserialize_amf0(data: Bytes) -> Result<RtmpMessage, MessageDeserializationError> {
    Ok(RtmpMessage::Amf0SharedObject { data })
}

pub fn deserialize_amf3(data: Bytes) -> Result<RtmpMessage, MessageDeserializationError> {
    Ok(RtmpMessage::Amf3SharedObject { data })
}
