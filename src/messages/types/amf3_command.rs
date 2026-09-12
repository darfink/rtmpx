use bytes::Bytes;
use std::io::Cursor;

use crate::amf::AmfEncoding;
use crate::amf3::{self, Amf3Value, FORMAT_SELECTOR_AMF0, FORMAT_SELECTOR_AMF3};
use crate::messages::RtmpMessage;
use crate::messages::{MessageDeserializationError, MessageSerializationError};

/// Encode the body of a type 15/17 message.
///
/// The leading byte is a *format selector*, not a constant. `0x00` means the
/// values that follow are AMF0 encoded and any value with no AMF0 counterpart
/// is introduced by the AMF0 `avmplus-object-marker` (`0x11`); `0x03` means
/// they are plain AMF3. Flash, librtmp and FFmpeg all emit the `0x00` form, so
/// that is what this crate originates unless asked otherwise.
pub(crate) fn encode_body(
    values: &[Amf3Value],
    format: AmfEncoding,
) -> Result<Bytes, MessageSerializationError> {
    let mut payload = Vec::with_capacity(64);
    match format {
        AmfEncoding::Amf0 => {
            payload.push(FORMAT_SELECTOR_AMF0);
            // AMF3-only values use the AVM+ escape. This structural conversion
            // does not promise the original AMF wire representation.
            let as_amf0: Vec<_> = values.iter().map(|v| v.to_amf0()).collect();
            crate::amf0::serialize_into(&as_amf0, &mut payload)?;
        }
        AmfEncoding::Amf3 => {
            payload.push(FORMAT_SELECTOR_AMF3);
            amf3::serialize_into(values, &mut payload)?;
        }
    }
    Ok(Bytes::from(payload))
}

/// Decode the body of a type 15/17 message, returning the values and the
/// selector they arrived under so a relay can reproduce the framing.
pub(crate) fn decode_body(
    bytes: &[u8],
) -> Result<(Vec<Amf3Value>, AmfEncoding), MessageDeserializationError> {
    let (selector, rest) = bytes
        .split_first()
        .ok_or(MessageDeserializationError::InvalidMessageFormat)?;
    match *selector {
        FORMAT_SELECTOR_AMF0 => {
            let mut cursor = Cursor::new(rest);
            let values = crate::amf0::deserialize(&mut cursor)?;
            // AMF0 -> AMF3 is total, and unwraps any avmplus escape back into
            // the AMF3 value it was carrying.
            Ok((
                values.iter().map(|v| v.to_amf3()).collect(),
                AmfEncoding::Amf0,
            ))
        }
        FORMAT_SELECTOR_AMF3 => {
            let mut cursor = Cursor::new(rest);
            Ok((amf3::deserialize(&mut cursor)?, AmfEncoding::Amf3))
        }
        other => Err(amf3::Amf3DeserializationError::BadFormatSelector(other).into()),
    }
}

pub fn serialize(
    command_name: String,
    transaction_id: f64,
    command_object: Amf3Value,
    mut additional_arguments: Vec<Amf3Value>,
    format: AmfEncoding,
) -> Result<Bytes, MessageSerializationError> {
    let mut values = vec![
        Amf3Value::String(command_name),
        crate::amf0::Amf0Value::Number(transaction_id).to_amf3(),
        command_object,
    ];
    values.append(&mut additional_arguments);
    encode_body(&values, format)
}

pub fn deserialize(data: Bytes) -> Result<RtmpMessage, MessageDeserializationError> {
    let (mut arguments, format) = decode_body(data.as_ref())?;
    if arguments.len() < 3 {
        return Err(amf3::Amf3DeserializationError::TooFewValues(arguments.len(), 3).into());
    }
    let command_name = match arguments.remove(0) {
        Amf3Value::String(v) => v,
        _ => return Err(amf3::Amf3DeserializationError::BadCommandName.into()),
    };
    let transaction_id = match arguments.remove(0) {
        Amf3Value::Integer(v) => v as f64,
        Amf3Value::Double(v) => v,
        _ => return Err(amf3::Amf3DeserializationError::BadTransactionId.into()),
    };
    let command_object = arguments.remove(0);
    Ok(RtmpMessage::Amf3Command {
        command_name,
        transaction_id,
        command_object,
        additional_arguments: arguments,
        format,
    })
}

#[cfg(test)]
mod tests {
    use super::{deserialize, serialize};
    use crate::amf::AmfEncoding;
    use crate::amf0::Amf0Value;
    use crate::amf3::Amf3Value;
    use crate::messages::RtmpMessage;

    fn connect_object() -> Amf3Value {
        Amf3Value::dynamic_object(vec![(
            "app".to_string(),
            Amf3Value::String("live".to_string()),
        )])
    }

    #[test]
    fn round_trips_under_both_selectors() {
        for format in [AmfEncoding::Amf0, AmfEncoding::Amf3] {
            let bytes = serialize(
                "connect".to_string(),
                1.0,
                connect_object(),
                vec![Amf3Value::Boolean(true)],
                format,
            )
            .unwrap();
            assert_eq!(bytes[0], if format.is_amf3() { 0x03 } else { 0x00 });
            let out = deserialize(bytes).unwrap();
            assert_eq!(
                out,
                RtmpMessage::Amf3Command {
                    command_name: "connect".to_string(),
                    transaction_id: 1.0,
                    command_object: connect_object(),
                    additional_arguments: vec![Amf3Value::Boolean(true)],
                    format,
                }
            );
        }
    }

    /// The historical bug: a `0x00` selector followed by an AMF0 body was fed
    /// to the AMF3 decoder, which is what every real client actually sends.
    #[test]
    fn amf0_framed_body_decodes_as_amf0_not_amf3() {
        let values = vec![
            Amf0Value::Utf8String("connect".to_string()),
            Amf0Value::Number(1.0),
            Amf0Value::Object(
                [("app".to_string(), Amf0Value::Utf8String("live".to_string()))]
                    .into_iter()
                    .collect(),
            ),
        ];
        let mut payload = vec![0x00u8];
        payload.extend_from_slice(&crate::amf0::serialize(&values).unwrap());

        let out = deserialize(bytes::Bytes::from(payload)).unwrap();
        match out {
            RtmpMessage::Amf3Command {
                command_name,
                transaction_id,
                command_object,
                format,
                ..
            } => {
                assert_eq!(command_name, "connect");
                assert_eq!(transaction_id, 1.0);
                assert_eq!(command_object, connect_object());
                assert_eq!(format, AmfEncoding::Amf0);
            }
            other => panic!("expected an AMF3 command, got {other:?}"),
        }
    }

    /// An AMF3-only value inside an AMF0-framed body must survive as itself.
    #[test]
    fn avmplus_escaped_value_round_trips_through_amf0_framing() {
        let escaped = Amf3Value::ByteArray(vec![1, 2, 3]);
        let bytes = serialize(
            "onStatus".to_string(),
            0.0,
            Amf3Value::Null,
            vec![escaped.clone()],
            AmfEncoding::Amf0,
        )
        .unwrap();
        assert_eq!(bytes[0], 0x00);
        assert!(
            bytes.contains(&0x11),
            "expected an avmplus escape in the payload"
        );
        match deserialize(bytes).unwrap() {
            RtmpMessage::Amf3Command {
                additional_arguments,
                ..
            } => {
                assert_eq!(additional_arguments, vec![escaped]);
            }
            other => panic!("expected an AMF3 command, got {other:?}"),
        }
    }

    #[test]
    fn too_few_values_is_an_error_not_a_panic() {
        let mut payload = vec![0x00u8];
        payload.extend_from_slice(
            &crate::amf0::serialize(&[Amf0Value::Utf8String("connect".to_string())]).unwrap(),
        );
        assert!(deserialize(bytes::Bytes::from(payload)).is_err());
    }

    #[test]
    fn unknown_selector_is_rejected() {
        assert!(deserialize(bytes::Bytes::from(vec![0x07u8, 0x01])).is_err());
        assert!(deserialize(bytes::Bytes::new()).is_err());
    }
}
