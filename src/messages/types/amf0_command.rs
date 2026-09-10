use crate::amf0::Amf0Value;
use bytes::Bytes;
use std::io::Cursor;

use crate::messages::RtmpMessage;
use crate::messages::{MessageDeserializationError, MessageSerializationError};

pub fn serialize(
    command_name: String,
    transaction_id: f64,
    command_object: Amf0Value,
    mut additional_arguments: Vec<Amf0Value>,
) -> Result<Bytes, MessageSerializationError> {
    let mut values = vec![
        Amf0Value::Utf8String(command_name),
        Amf0Value::Number(transaction_id),
        command_object,
    ];

    values.append(&mut additional_arguments);
    let bytes = crate::amf0::serialize(&values)?;

    Ok(Bytes::from(bytes))
}

pub fn deserialize(data: Bytes) -> Result<RtmpMessage, MessageDeserializationError> {
    let mut cursor = Cursor::new(data);
    let arguments = crate::amf0::deserialize(&mut cursor)?;
    // A peer negotiating `objectEncoding` 3 (notably Red5) keeps framing
    // commands as type 20 but sends each value behind the AMF0
    // `avmplus-object-marker` (0x11), so the wire values arrive as
    // `Amf0Value::AvmPlus`. Project them into the AMF0 model before
    // matching: the projection is total (`Amf3Value::to_amf0` re-wraps only
    // what AMF0 cannot represent), and every downstream consumer of
    // `RtmpMessage::Amf0Command` already speaks plain AMF0.
    let mut arguments: Vec<Amf0Value> = arguments
        .into_iter()
        .map(|v| match v {
            Amf0Value::AvmPlus(inner) => inner.to_amf0(),
            plain => plain,
        })
        .collect();

    // `drain(..3)` panics when the peer sent fewer than three values, which is
    // a single malformed command message away on a public ingest port. Check
    // the length first and fail as a typed error instead.
    if arguments.len() < 3 {
        return Err(MessageDeserializationError::InvalidMessageFormat);
    }

    let command_name = match arguments.remove(0) {
        Amf0Value::Utf8String(value) => value,
        _ => return Err(MessageDeserializationError::InvalidMessageFormat),
    };

    let transaction_id = match arguments.remove(0) {
        Amf0Value::Number(value) => value,
        _ => return Err(MessageDeserializationError::InvalidMessageFormat),
    };

    let command_object = arguments.remove(0);

    Ok(RtmpMessage::Amf0Command {
        command_name,
        transaction_id,
        command_object,
        additional_arguments: arguments,
    })
}

#[cfg(test)]
mod tests {
    use super::{deserialize, serialize};
    use crate::amf0::{Amf0Object, Amf0Value};
    use bytes::Bytes;
    use std::io::Cursor;

    use crate::messages::RtmpMessage;

    #[test]
    fn can_serialize_message() {
        let mut properties1 = Amf0Object::new();
        properties1.insert(
            "prop1".to_string(),
            Amf0Value::Utf8String("abc".to_string()),
        );
        properties1.insert("prop2".to_string(), Amf0Value::Null);

        let mut properties2 = Amf0Object::new();
        properties2.insert(
            "prop1".to_string(),
            Amf0Value::Utf8String("abc".to_string()),
        );
        properties2.insert("prop2".to_string(), Amf0Value::Null);

        let raw_message = serialize(
            "test".to_string(),
            23.0,
            Amf0Value::Object(properties1),
            vec![Amf0Value::Boolean(true), Amf0Value::Number(52.0)],
        )
        .unwrap();

        let mut cursor = Cursor::new(raw_message);
        let result = crate::amf0::deserialize(&mut cursor).unwrap();

        let expected = vec![
            Amf0Value::Utf8String("test".to_string()),
            Amf0Value::Number(23.0),
            Amf0Value::Object(properties2),
            Amf0Value::Boolean(true),
            Amf0Value::Number(52.0),
        ];

        assert_eq!(expected, result);
    }

    #[test]
    fn can_deserialize_message() {
        let mut properties1 = Amf0Object::new();
        properties1.insert(
            "prop1".to_string(),
            Amf0Value::Utf8String("abc".to_string()),
        );
        properties1.insert("prop2".to_string(), Amf0Value::Null);

        let mut properties2 = Amf0Object::new();
        properties2.insert(
            "prop1".to_string(),
            Amf0Value::Utf8String("abc".to_string()),
        );
        properties2.insert("prop2".to_string(), Amf0Value::Null);

        let values = vec![
            Amf0Value::Utf8String("test".to_string()),
            Amf0Value::Number(23.0),
            Amf0Value::Object(properties1),
            Amf0Value::Boolean(true),
            Amf0Value::Number(52.0),
        ];

        let bytes = Bytes::from(crate::amf0::serialize(&values).unwrap());
        let expected = RtmpMessage::Amf0Command {
            command_name: "test".to_string(),
            transaction_id: 23.0,
            command_object: Amf0Value::Object(properties2),
            additional_arguments: vec![Amf0Value::Boolean(true), Amf0Value::Number(52.0)],
        };
        let result = deserialize(bytes).unwrap();

        assert_eq!(expected, result);
    }

    /// Red5 answers an AMF3 `createStream` with a type-20 command whose
    /// values are each behind the avmplus escape (captured wire bytes).
    /// It must parse as a plain AMF0 `_result` carrying stream id 1.
    #[test]
    fn can_deserialize_red5_amf3_create_stream_result() {
        let bytes = Bytes::from(vec![
            0x11, 0x06, 0x0f, 0x5f, 0x72, 0x65, 0x73, 0x75, 0x6c, 0x74, 0x11, 0x04, 0x02, 0x11,
            0x01, 0x11, 0x05, 0x3f, 0xf0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ]);
        let expected = RtmpMessage::Amf0Command {
            command_name: "_result".to_string(),
            transaction_id: 2.0,
            command_object: Amf0Value::Null,
            additional_arguments: vec![Amf0Value::Number(1.0)],
        };
        let result = deserialize(bytes).unwrap();

        assert_eq!(expected, result);
    }
}
