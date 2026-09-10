use crate::{
    amf0::{Amf0Object, Amf0Value},
    messages::{MessageDeserializationError, MessagePayload, RtmpMessage},
    time::RtmpTimestamp,
};
use bytes::Bytes;

/// RTMP script-data wire type, independent of negotiated object encoding.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum DataMessageType {
    /// Type 18, normally AMF0 (some peers send bare AMF3 here).
    Amf0,
    /// Type 15, including its original format-selector byte.
    Amf3,
}

impl DataMessageType {
    pub fn type_id(self) -> u8 {
        match self {
            Self::Amf0 => 18,
            Self::Amf3 => 15,
        }
    }
    pub(crate) fn from_type_id(id: u8) -> Option<Self> {
        match id {
            18 => Some(Self::Amf0),
            15 => Some(Self::Amf3),
            _ => None,
        }
    }
}

/// An encoded script-data message. Forwarding does not decode or rewrite it.
///
/// This type preserves even unknown or malformed script data. Parsing is optional
/// and never changes the wire type, timestamp, or original bytes.
#[derive(Clone, Debug, PartialEq)]
pub struct DataMessage {
    wire_type: DataMessageType,
    timestamp: RtmpTimestamp,
    payload: Bytes,
}

impl DataMessage {
    pub fn new(wire_type: DataMessageType, timestamp: RtmpTimestamp, payload: Bytes) -> Self {
        Self {
            wire_type,
            timestamp,
            payload,
        }
    }
    pub fn wire_type(&self) -> DataMessageType {
        self.wire_type
    }
    pub fn timestamp(&self) -> RtmpTimestamp {
        self.timestamp
    }
    pub fn payload(&self) -> &Bytes {
        &self.payload
    }
    pub fn into_payload(self) -> Bytes {
        self.payload
    }

    /// Decode `onMetaData`, with or without `@setDataFrame`.
    /// Returns `None` for other script events or metadata without an object.
    /// AMF3 values are projected into the lossless AMF0 value model.
    pub fn metadata(&self) -> Result<Option<Amf0Object>, MessageDeserializationError> {
        let values = match self.to_message_payload(0).to_rtmp_message()? {
            RtmpMessage::Amf0Data { values } => values,
            RtmpMessage::Amf3Data { values, .. } => values.iter().map(|v| v.to_amf0()).collect(),
            _ => unreachable!("data wire types only"),
        };
        let mut values = values.into_iter();
        let mut name = values.next();
        if matches!(&name, Some(Amf0Value::Utf8String(s)) if s == "@setDataFrame") {
            name = values.next();
        }
        if !matches!(&name, Some(Amf0Value::Utf8String(s)) if s == "onMetaData") {
            return Ok(None);
        }
        Ok(values.next().and_then(|v| v.get_object_properties()))
    }

    pub(crate) fn to_message_payload(&self, stream_id: u32) -> MessagePayload {
        MessagePayload {
            timestamp: self.timestamp,
            type_id: self.wire_type.type_id(),
            message_stream_id: stream_id,
            data: self.payload.clone(),
        }
    }
}
