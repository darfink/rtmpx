use bytes::Bytes;

use crate::amf::AmfEncoding;
use crate::amf3::Amf3Value;
use crate::messages::RtmpMessage;
use crate::messages::{MessageDeserializationError, MessageSerializationError};

pub fn serialize(
    values: Vec<Amf3Value>,
    format: AmfEncoding,
) -> Result<Bytes, MessageSerializationError> {
    super::amf3_command::encode_body(&values, format)
}

pub fn deserialize(data: Bytes) -> Result<RtmpMessage, MessageDeserializationError> {
    let (values, format) = super::amf3_command::decode_body(data.as_ref())?;
    Ok(RtmpMessage::Amf3Data { values, format })
}

#[cfg(test)]
mod tests {
    use super::{deserialize, serialize};
    use crate::amf::AmfEncoding;
    use crate::amf0::Amf0Value;
    use crate::amf3::Amf3Value;
    use crate::messages::RtmpMessage;

    #[test]
    fn round_trips_under_both_selectors() {
        for format in [AmfEncoding::Amf0, AmfEncoding::Amf3] {
            let values = vec![
                Amf3Value::String("onMetaData".to_string()),
                Amf3Value::Double(1.5),
            ];
            let bytes = serialize(values.clone(), format).unwrap();
            assert_eq!(bytes[0], if format.is_amf3() { 0x03 } else { 0x00 });
            assert_eq!(
                deserialize(bytes).unwrap(),
                RtmpMessage::Amf3Data { values, format }
            );
        }
    }

    /// `@setDataFrame` from an AMF3 encoder arrives AMF0-framed in practice.
    #[test]
    fn amf0_framed_set_data_frame_decodes() {
        let values = vec![
            Amf0Value::Utf8String("@setDataFrame".to_string()),
            Amf0Value::Utf8String("onMetaData".to_string()),
            Amf0Value::Object(
                [("width".to_string(), Amf0Value::Number(1920.0))]
                    .into_iter()
                    .collect(),
            ),
        ];
        let mut payload = vec![0x00u8];
        payload.extend_from_slice(&crate::amf0::serialize(&values).unwrap());

        match deserialize(bytes::Bytes::from(payload)).unwrap() {
            RtmpMessage::Amf3Data { values, format } => {
                assert_eq!(format, AmfEncoding::Amf0);
                assert_eq!(values[0], Amf3Value::String("@setDataFrame".to_string()));
                assert_eq!(values[1], Amf3Value::String("onMetaData".to_string()));
            }
            other => panic!("expected AMF3 data, got {other:?}"),
        }
    }
}
