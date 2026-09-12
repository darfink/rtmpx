use crate::{
    amf0::{Amf0Object, Amf0Value},
    messages::MessageDeserializationError,
    time::RtmpTimestamp,
};

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
pub struct DataMessage<D = crate::Payload> {
    wire_type: DataMessageType,
    timestamp: RtmpTimestamp,
    payload: D,
}

impl<D> DataMessage<D> {
    pub fn new(wire_type: DataMessageType, timestamp: RtmpTimestamp, payload: D) -> Self {
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
    pub fn payload(&self) -> &D {
        &self.payload
    }
    pub fn into_payload(self) -> D {
        self.payload
    }

    pub fn map_payload<T>(self, map: impl FnOnce(D) -> T) -> DataMessage<T> {
        DataMessage {
            wire_type: self.wire_type,
            timestamp: self.timestamp,
            payload: map(self.payload),
        }
    }
}
impl<D: crate::Segments> DataMessage<D> {
    /// Decode metadata directly from contiguous or segmented storage.
    /// AMF values allocate, but the encoded payload is not coalesced.
    pub fn metadata(&self) -> Result<Option<Amf0Object>, MessageDeserializationError> {
        use std::io::Read;
        let reader = || crate::payload::PayloadReader::new(&self.payload);
        let values = match self.wire_type {
            DataMessageType::Amf0 => match crate::amf0::deserialize(&mut reader()) {
                Ok(values) => values,
                Err(first) => match crate::amf3::deserialize(&mut reader()) {
                    Ok(values) => values.iter().map(|v| v.to_amf0()).collect(),
                    Err(_) => return Err(first.into()),
                },
            },
            DataMessageType::Amf3 => {
                let mut reader = reader();
                let mut selector = [0];
                reader
                    .read_exact(&mut selector)
                    .map_err(|_| MessageDeserializationError::InvalidMessageFormat)?;
                match selector[0] {
                    0 => crate::amf0::deserialize(&mut reader)?
                        .iter()
                        .map(|v| v.to_amf3().to_amf0())
                        .collect(),
                    3 => crate::amf3::deserialize(&mut reader)?
                        .iter()
                        .map(|v| v.to_amf0())
                        .collect(),
                    other => {
                        return Err(crate::amf3::Amf3DeserializationError::BadFormatSelector(
                            other,
                        )
                        .into());
                    }
                }
            }
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
}
