use super::types;
use crate::amf::AmfEncoding;
use crate::messages::RtmpMessage;
use crate::messages::{MessageDeserializationError, MessageSerializationError};
use crate::time::RtmpTimestamp;
use bytes::Bytes;
use std::fmt;

/// Represents a raw RTMP message
#[derive(PartialEq, Clone)]
pub struct RawMessage<D = Bytes> {
    pub timestamp: RtmpTimestamp,
    pub type_id: u8,
    pub message_stream_id: u32,
    pub data: D,
}

impl<D> fmt::Debug for RawMessage<D> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "RawMessage {{ timestamp: {:?}, type_id: {:?}, message_stream_id: {:?}, data: [..] }}",
            self.timestamp, self.type_id, self.message_stream_id
        )
    }
}

impl RawMessage {
    /// Creates a new message payload with default values.
    ///
    /// This is mostly used when all information about a message is not known at creation time
    /// but instead is built up over time (e.g. RTMP chunk deserialization process).
    pub fn new() -> RawMessage {
        RawMessage {
            timestamp: RtmpTimestamp::new(0),
            message_stream_id: 0,
            type_id: 0,
            data: Bytes::new(),
        }
    }
}

impl Default for RawMessage {
    fn default() -> Self {
        Self::new()
    }
}

impl RawMessage {
    /// Deserializes the message data in the specified payload into its corresponding
    /// `RtmpMessage`.
    ///
    /// Enhanced RTMP v2-R2 mapping: 20 is AMF0 commands, 17 is AMF3 commands
    /// (both start with format selector `0x00` followed by their respective
    /// encoding); 18 is AMF0 data, 15 is AMF3 data; 19 is AMF0 shared objects
    /// and 16 is AMF3 shared objects (carried opaquely for relay). FLV
    /// TagType 15 (AMF3 script data) is not an RTMP message and stays a
    /// relay/parse concern. A single AMF3 value may appear inside an
    /// otherwise-AMF0 payload behind the AMF0 `0x11` avmplus-object marker;
    /// that wrapping is per-value and non-sticky (see
    /// `crate::amf3::decode_avmplus_wrapped`). Malformed AMF3 is a typed
    /// error, never a silent AMF0 fallback.
    pub fn to_rtmp_message(&self) -> Result<RtmpMessage, MessageDeserializationError> {
        match self.type_id {
            1 => types::set_chunk_size::deserialize(self.data.clone()),
            2 => types::abort::deserialize(self.data.clone()),
            3 => types::acknowledgement::deserialize(self.data.clone()),
            4 => types::user_control::deserialize(self.data.clone()),
            5 => types::window_acknowledgement_size::deserialize(self.data.clone()),
            6 => types::set_peer_bandwidth::deserialize(self.data.clone()),
            8 => types::audio_data::deserialize(self.data.clone()),
            9 => types::video_data::deserialize(self.data.clone()),
            15 => types::amf3_data::deserialize(self.data.clone()),
            16 => types::shared_object::deserialize_amf3(self.data.clone()),
            17 => types::amf3_command::deserialize(self.data.clone()),
            18 => Self::deserialize_data_tolerant(self.data.clone()),
            19 => types::shared_object::deserialize_amf0(self.data.clone()),
            20 => types::amf0_command::deserialize(self.data.clone()),

            _ => Ok(RtmpMessage::Unknown {
                type_id: self.type_id,
                data: self.data.clone(),
            }),
        }
    }

    /// Type 18 is AMF0 on the wire, but peers in the wild mistype AMF3
    /// script-data bodies under it (raw AMF3 values, no format selector).
    /// Try the declared encoding first so well-formed traffic is
    /// unaffected, then bare AMF3 before giving up instead of killing the
    /// session on one mistyped message. A mistyped body normalizes to
    /// `Amf3Data`; the original bytes stay on the payload for relays.
    fn deserialize_data_tolerant(data: Bytes) -> Result<RtmpMessage, MessageDeserializationError> {
        match types::amf0_data::deserialize(data.clone()) {
            Ok(message) => Ok(message),
            Err(first) => {
                let mut cursor = std::io::Cursor::new(data);
                match crate::amf3::deserialize(&mut cursor) {
                    Ok(values) => Ok(RtmpMessage::Amf3Data {
                        values,
                        format: AmfEncoding::Amf3,
                    }),
                    Err(_) => Err(first),
                }
            }
        }
    }

    /// This creates a `RawMessage` from an `RtmpMessage`.
    ///
    /// Since RTMP messages do not contain timestamp or the conversation stream id these must be
    /// provided at the time of creation.
    pub fn from_rtmp_message(
        message: RtmpMessage,
        timestamp: RtmpTimestamp,
        message_stream_id: u32,
    ) -> Result<RawMessage, MessageSerializationError> {
        let type_id = message.get_message_type_id();

        let bytes = match message {
            RtmpMessage::Unknown { type_id: _, data } => data,

            RtmpMessage::Abort { stream_id } => types::abort::serialize(stream_id)?,

            RtmpMessage::Acknowledgement { sequence_number } => {
                types::acknowledgement::serialize(sequence_number)?
            }

            RtmpMessage::Amf0Command {
                command_name,
                transaction_id,
                command_object,
                additional_arguments,
            } => types::amf0_command::serialize(
                command_name,
                transaction_id,
                command_object,
                additional_arguments,
            )?,

            RtmpMessage::Amf0Data { values } => types::amf0_data::serialize(values)?,

            RtmpMessage::Amf3Command {
                command_name,
                transaction_id,
                command_object,
                additional_arguments,
                format,
            } => types::amf3_command::serialize(
                command_name,
                transaction_id,
                command_object,
                additional_arguments,
                format,
            )?,

            RtmpMessage::Amf3Data { values, format } => {
                types::amf3_data::serialize(values, format)?
            }

            RtmpMessage::Amf0SharedObject { data } => types::shared_object::serialize_amf0(data)?,

            RtmpMessage::Amf3SharedObject { data } => types::shared_object::serialize_amf3(data)?,

            RtmpMessage::AudioData { data } => types::audio_data::serialize(data)?,

            RtmpMessage::SetChunkSize { size } => types::set_chunk_size::serialize(size)?,

            RtmpMessage::SetPeerBandwidth { size, limit_type } => {
                types::set_peer_bandwidth::serialize(limit_type, size)?
            }

            RtmpMessage::UserControl {
                event_type,
                stream_id,
                buffer_length,
                timestamp,
            } => types::user_control::serialize(event_type, stream_id, buffer_length, timestamp)?,

            RtmpMessage::VideoData { data } => types::video_data::serialize(data)?,

            RtmpMessage::WindowAcknowledgement { size } => {
                types::window_acknowledgement_size::serialize(size)?
            }
        };

        Ok(RawMessage {
            data: bytes,
            type_id,
            message_stream_id,
            timestamp,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{RawMessage, RtmpMessage};
    use crate::amf0::Amf0Value;
    use crate::messages::{PeerBandwidthLimitType, UserControlEventType};
    use crate::time::RtmpTimestamp;
    use bytes::{BufMut, Bytes, BytesMut};

    #[test]
    fn can_get_payload_from_abort_message() {
        let timestamp = RtmpTimestamp::new(55);
        let stream_id = 52;
        let message = RtmpMessage::Abort { stream_id: 23 };
        let result = RawMessage::from_rtmp_message(message, timestamp, stream_id).unwrap();

        assert_ne!(result.data.len(), 0, "Empty payload data seen");
        assert_eq!(result.type_id, 2, "Incorrect type id");
        assert_eq!(
            result.message_stream_id, stream_id,
            "Incorrect message stream id"
        );
        assert_eq!(result.timestamp, 55, "Incorrect timestamp");
    }

    #[test]
    fn can_get_payload_from_acknowledgement_message() {
        let timestamp = RtmpTimestamp::new(55);
        let stream_id = 52;
        let message = RtmpMessage::Acknowledgement {
            sequence_number: 23,
        };
        let result = RawMessage::from_rtmp_message(message, timestamp, stream_id).unwrap();

        assert_ne!(result.data.len(), 0, "Empty payload data seen");
        assert_eq!(result.type_id, 3, "Incorrect type id");
        assert_eq!(
            result.message_stream_id, stream_id,
            "Incorrect message stream id"
        );
        assert_eq!(result.timestamp, 55, "Incorrect timestamp");
    }

    #[test]
    fn can_get_payload_from_amf0_command_message() {
        let timestamp = RtmpTimestamp::new(55);
        let stream_id = 52;
        let message = RtmpMessage::Amf0Command {
            command_name: "test".to_string(),
            command_object: Amf0Value::Null,
            transaction_id: 23.0,
            additional_arguments: vec![],
        };

        let result = RawMessage::from_rtmp_message(message, timestamp, stream_id).unwrap();

        assert_ne!(result.data.len(), 0, "Empty payload data seen");
        assert_eq!(result.type_id, 20, "Incorrect type id");
        assert_eq!(
            result.message_stream_id, stream_id,
            "Incorrect message stream id"
        );
        assert_eq!(result.timestamp, 55, "Incorrect timestamp");
    }

    #[test]
    fn can_get_payload_from_amf0_data_message() {
        let timestamp = RtmpTimestamp::new(55);
        let stream_id = 52;
        let message = RtmpMessage::Amf0Data {
            values: vec![Amf0Value::Number(23.0)],
        };
        let result = RawMessage::from_rtmp_message(message, timestamp, stream_id).unwrap();

        assert_ne!(result.data.len(), 0, "Empty payload data seen");
        assert_eq!(result.type_id, 18, "Incorrect type id");
        assert_eq!(
            result.message_stream_id, stream_id,
            "Incorrect message stream id"
        );
        assert_eq!(result.timestamp, 55, "Incorrect timestamp");
    }

    #[test]
    fn can_get_payload_from_audio_data_message() {
        let timestamp = RtmpTimestamp::new(55);
        let stream_id = 52;
        let message = RtmpMessage::AudioData {
            data: Bytes::from(vec![33_u8]),
        };
        let result = RawMessage::from_rtmp_message(message, timestamp, stream_id).unwrap();

        assert_ne!(result.data.len(), 0, "Empty payload data seen");
        assert_eq!(result.type_id, 8, "Incorrect type id");
        assert_eq!(
            result.message_stream_id, stream_id,
            "Incorrect message stream id"
        );
        assert_eq!(result.timestamp, 55, "Incorrect timestamp");
    }

    #[test]
    fn can_get_payload_from_set_chunk_size_message() {
        let timestamp = RtmpTimestamp::new(55);
        let stream_id = 52;
        let message = RtmpMessage::SetChunkSize { size: 33 };
        let result = RawMessage::from_rtmp_message(message, timestamp, stream_id).unwrap();

        assert_ne!(result.data.len(), 0, "Empty payload data seen");
        assert_eq!(result.type_id, 1, "Incorrect type id");
        assert_eq!(
            result.message_stream_id, stream_id,
            "Incorrect message stream id"
        );
        assert_eq!(result.timestamp, 55, "Incorrect timestamp");
    }

    #[test]
    fn can_get_payload_from_set_peer_bandwidth_message() {
        let timestamp = RtmpTimestamp::new(55);
        let stream_id = 52;
        let message = RtmpMessage::SetPeerBandwidth {
            size: 33,
            limit_type: PeerBandwidthLimitType::Hard,
        };
        let result = RawMessage::from_rtmp_message(message, timestamp, stream_id).unwrap();

        assert_ne!(result.data.len(), 0, "Empty payload data seen");
        assert_eq!(result.type_id, 6, "Incorrect type id");
        assert_eq!(
            result.message_stream_id, stream_id,
            "Incorrect message stream id"
        );
        assert_eq!(result.timestamp, 55, "Incorrect timestamp");
    }

    #[test]
    fn can_get_payload_from_user_control_message() {
        let timestamp = RtmpTimestamp::new(55);
        let stream_id = 52;
        let message = RtmpMessage::UserControl {
            event_type: UserControlEventType::StreamBegin,
            stream_id: Some(33),
            timestamp: None,
            buffer_length: None,
        };

        let result = RawMessage::from_rtmp_message(message, timestamp, stream_id).unwrap();

        assert_ne!(result.data.len(), 0, "Empty payload data seen");
        assert_eq!(result.type_id, 4, "Incorrect type id");
        assert_eq!(
            result.message_stream_id, stream_id,
            "Incorrect message stream id"
        );
        assert_eq!(result.timestamp, 55, "Incorrect timestamp");
    }

    #[test]
    fn can_get_payload_from_video_data_message() {
        let timestamp = RtmpTimestamp::new(55);
        let stream_id = 52;
        let message = RtmpMessage::VideoData {
            data: Bytes::from(vec![23_u8]),
        };
        let result = RawMessage::from_rtmp_message(message, timestamp, stream_id).unwrap();

        assert_ne!(result.data.len(), 0, "Empty payload data seen");
        assert_eq!(result.type_id, 9, "Incorrect type id");
        assert_eq!(
            result.message_stream_id, stream_id,
            "Incorrect message stream id"
        );
        assert_eq!(result.timestamp, 55, "Incorrect timestamp");
    }

    #[test]
    fn can_get_payload_from_window_acknowledgement_message() {
        let timestamp = RtmpTimestamp::new(55);
        let stream_id = 52;
        let message = RtmpMessage::WindowAcknowledgement { size: 23 };
        let result = RawMessage::from_rtmp_message(message, timestamp, stream_id).unwrap();

        assert_ne!(result.data.len(), 0, "Empty payload data seen");
        assert_eq!(result.type_id, 5, "Incorrect type id");
        assert_eq!(
            result.message_stream_id, stream_id,
            "Incorrect message stream id"
        );
        assert_eq!(result.timestamp, 55, "Incorrect timestamp");
    }

    #[test]
    fn can_get_payload_from_unknown_message() {
        let timestamp = RtmpTimestamp::new(55);
        let stream_id = 52;
        let message = RtmpMessage::Unknown {
            type_id: 33,
            data: Bytes::from(vec![23_u8]),
        };
        let result = RawMessage::from_rtmp_message(message, timestamp, stream_id).unwrap();

        assert_ne!(result.data.len(), 0, "Empty payload data seen");
        assert_eq!(result.type_id, 33, "Incorrect type id");
        assert_eq!(
            result.message_stream_id, stream_id,
            "Incorrect message stream id"
        );
        assert_eq!(result.timestamp, 55, "Incorrect timestamp");
    }

    #[test]
    fn can_get_rtmp_message_for_abort_payload() {
        let message = RtmpMessage::Abort { stream_id: 15 };
        let payload =
            RawMessage::from_rtmp_message(message.clone(), RtmpTimestamp::new(0), 15).unwrap();
        let result = payload.to_rtmp_message().unwrap();

        assert_eq!(result, message);
    }

    #[test]
    fn can_get_rtmp_message_for_acknowledgement_payload() {
        let message = RtmpMessage::Acknowledgement {
            sequence_number: 15,
        };
        let payload =
            RawMessage::from_rtmp_message(message.clone(), RtmpTimestamp::new(0), 15).unwrap();
        let result = payload.to_rtmp_message().unwrap();

        assert_eq!(result, message);
    }

    #[test]
    fn can_get_rtmp_message_for_amf0_command_payload() {
        let message = RtmpMessage::Amf0Command {
            command_name: "test".to_string(),
            transaction_id: 15.0,
            command_object: Amf0Value::Number(23.0),
            additional_arguments: vec![Amf0Value::Null],
        };

        let payload =
            RawMessage::from_rtmp_message(message.clone(), RtmpTimestamp::new(0), 15).unwrap();
        let result = payload.to_rtmp_message().unwrap();

        assert_eq!(result, message);
    }

    #[test]
    fn can_get_rtmp_message_for_amf0_data_payload() {
        let message = RtmpMessage::Amf0Data {
            values: vec![Amf0Value::Number(23.3)],
        };
        let payload =
            RawMessage::from_rtmp_message(message.clone(), RtmpTimestamp::new(0), 15).unwrap();
        let result = payload.to_rtmp_message().unwrap();

        assert_eq!(result, message);
    }

    #[test]
    fn can_get_rtmp_message_for_audio_data_payload() {
        let message = RtmpMessage::AudioData {
            data: Bytes::from(vec![3_u8]),
        };
        let payload =
            RawMessage::from_rtmp_message(message.clone(), RtmpTimestamp::new(0), 15).unwrap();
        let result = payload.to_rtmp_message().unwrap();

        assert_eq!(result, message);
    }

    #[test]
    fn can_get_rtmp_message_for_set_chunk_size_payload() {
        let message = RtmpMessage::SetChunkSize { size: 15 };
        let payload =
            RawMessage::from_rtmp_message(message.clone(), RtmpTimestamp::new(0), 15).unwrap();
        let result = payload.to_rtmp_message().unwrap();

        assert_eq!(result, message);
    }

    #[test]
    fn can_get_rtmp_message_for_set_peer_bandwidth_payload() {
        let message = RtmpMessage::SetPeerBandwidth {
            size: 15,
            limit_type: PeerBandwidthLimitType::Hard,
        };
        let payload =
            RawMessage::from_rtmp_message(message.clone(), RtmpTimestamp::new(0), 15).unwrap();
        let result = payload.to_rtmp_message().unwrap();

        assert_eq!(result, message);
    }

    #[test]
    fn can_get_rtmp_message_for_user_control_payload() {
        let message = RtmpMessage::UserControl {
            stream_id: Some(15),
            buffer_length: None,
            timestamp: None,
            event_type: UserControlEventType::StreamBegin,
        };

        let payload =
            RawMessage::from_rtmp_message(message.clone(), RtmpTimestamp::new(0), 15).unwrap();
        let result = payload.to_rtmp_message().unwrap();

        assert_eq!(result, message);
    }

    #[test]
    fn can_get_rtmp_message_for_video_data_payload() {
        let message = RtmpMessage::VideoData {
            data: Bytes::from(vec![3_u8]),
        };
        let payload =
            RawMessage::from_rtmp_message(message.clone(), RtmpTimestamp::new(0), 15).unwrap();
        let result = payload.to_rtmp_message().unwrap();

        assert_eq!(result, message);
    }

    #[test]
    fn can_get_rtmp_message_for_window_acknowledgement_payload() {
        let message = RtmpMessage::WindowAcknowledgement { size: 25 };
        let payload =
            RawMessage::from_rtmp_message(message.clone(), RtmpTimestamp::new(0), 15).unwrap();
        let result = payload.to_rtmp_message().unwrap();

        assert_eq!(result, message);
    }

    #[test]
    fn can_get_rtmp_message_for_amf0_command_flagged_as_amf3() {
        // A `0x00` format selector means the body is AMF0. This is the framing
        // Flash, librtmp and FFmpeg emit for type 17, so it must decode.
        let message = RtmpMessage::Amf0Command {
            command_name: "test".to_string(),
            transaction_id: 15.0,
            command_object: Amf0Value::Number(23.0),
            additional_arguments: vec![Amf0Value::Null],
        };
        let mut payload =
            RawMessage::from_rtmp_message(message, RtmpTimestamp::new(0), 15).unwrap();
        payload.type_id = 17;
        let mut new_data = BytesMut::with_capacity(payload.data.len() + 1);
        new_data.put_u8(0);
        new_data.extend_from_slice(&payload.data);
        payload.data = new_data.freeze();
        match payload
            .to_rtmp_message()
            .expect("AMF0 body on type 17 must decode")
        {
            RtmpMessage::Amf3Command {
                command_name,
                transaction_id,
                format,
                ..
            } => {
                assert_eq!(command_name, "test");
                assert_eq!(transaction_id, 15.0);
                assert_eq!(format, crate::amf::AmfEncoding::Amf0);
            }
            other => panic!("expected an AMF3 command, got {other:?}"),
        }
    }

    #[test]
    fn can_get_rtmp_message_for_amf0_data_payload_flagged_as_amf3() {
        // Without a leading selector byte the payload is malformed: the first
        // AMF0 marker gets eaten as the selector.
        let message = RtmpMessage::Amf0Data {
            values: vec![Amf0Value::Number(23.3)],
        };
        let mut payload =
            RawMessage::from_rtmp_message(message, RtmpTimestamp::new(0), 15).unwrap();
        payload.type_id = 15;
        assert!(payload.to_rtmp_message().is_err());

        // With one, it decodes.
        let mut with_selector = BytesMut::with_capacity(payload.data.len() + 1);
        with_selector.put_u8(0);
        with_selector.extend_from_slice(&payload.data);
        payload.data = with_selector.freeze();
        match payload
            .to_rtmp_message()
            .expect("AMF0 body on type 15 must decode")
        {
            RtmpMessage::Amf3Data { values, format } => {
                assert_eq!(values, vec![crate::amf3::Amf3Value::Double(23.3)]);
                assert_eq!(format, crate::amf::AmfEncoding::Amf0);
            }
            other => panic!("expected AMF3 data, got {other:?}"),
        }
    }

    #[test]
    fn amf3_values_typed_as_amf0_data_decode() {
        // A mistyping peer sends raw AMF3 values (no format selector)
        // under type 18. The declared AMF0 encoding fails first, then
        // bare AMF3 is tried before giving up.
        let values = vec![
            crate::amf3::Amf3Value::String("@setDataFrame".to_string()),
            crate::amf3::Amf3Value::String("onMetaData".to_string()),
            crate::amf3::Amf3Value::Double(1.5),
        ];
        let payload = RawMessage {
            timestamp: RtmpTimestamp::new(0),
            type_id: 18,
            message_stream_id: 1,
            data: Bytes::from(crate::amf3::serialize(&values).unwrap()),
        };
        match payload
            .to_rtmp_message()
            .expect("AMF3 body on type 18 must decode")
        {
            RtmpMessage::Amf3Data {
                values: got,
                format,
            } => {
                assert_eq!(got, values);
                assert_eq!(format, crate::amf::AmfEncoding::Amf3);
            }
            other => panic!("expected AMF3 data, got {other:?}"),
        }
    }

    #[test]
    fn observed_mistyped_set_data_frame_decodes() {
        // Exact script-data body observed mistyped as type 18: the AMF3
        // `@setDataFrame` probe with its marker object.
        let raw = vec![
            0x06, 0x1b, 0x40, 0x73, 0x65, 0x74, 0x44, 0x61, 0x74, 0x61, 0x46, 0x72, 0x61, 0x6d,
            0x65, 0x06, 0x15, 0x6f, 0x6e, 0x4d, 0x65, 0x74, 0x61, 0x44, 0x61, 0x74, 0x61, 0x0a,
            0x0b, 0x01, 0x1d, 0x72, 0x74, 0x6d, 0x70, 0x78, 0x52, 0x65, 0x64, 0x35, 0x50, 0x72,
            0x6f, 0x62, 0x65, 0x04, 0x2a, 0x0f, 0x65, 0x6e, 0x63, 0x6f, 0x64, 0x65, 0x72, 0x06,
            0x2f, 0x72, 0x74, 0x6d, 0x70, 0x78, 0x2d, 0x72, 0x65, 0x64, 0x35, 0x2d, 0x68, 0x61,
            0x72, 0x6e, 0x65, 0x73, 0x73, 0x2d, 0x61, 0x6d, 0x66, 0x33, 0x01,
        ];
        let payload = RawMessage {
            timestamp: RtmpTimestamp::new(0),
            type_id: 18,
            message_stream_id: 1,
            data: Bytes::from(raw),
        };
        match payload
            .to_rtmp_message()
            .expect("mistyped probe body must decode")
        {
            RtmpMessage::Amf3Data { values, .. } => {
                assert_eq!(
                    values[0],
                    crate::amf3::Amf3Value::String("@setDataFrame".to_string())
                );
                assert_eq!(
                    values[1],
                    crate::amf3::Amf3Value::String("onMetaData".to_string())
                );
            }
            other => panic!("expected AMF3 data, got {other:?}"),
        }
    }
}

impl<D> RawMessage<D> {
    pub fn as_ref(&self) -> RawMessage<&D> {
        RawMessage {
            timestamp: self.timestamp,
            type_id: self.type_id,
            message_stream_id: self.message_stream_id,
            data: &self.data,
        }
    }
    pub fn map_data<T>(self, map: impl FnOnce(D) -> T) -> RawMessage<T> {
        RawMessage {
            timestamp: self.timestamp,
            type_id: self.type_id,
            message_stream_id: self.message_stream_id,
            data: map(self.data),
        }
    }
}
