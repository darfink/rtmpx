/*!

Chunk framing for RTMP, as described in section 5.3 of the [official RTMP specification](https://www.adobe.com/content/dam/acom/en/devnet/rtmp/pdf/rtmp_specification_1.0.pdf).

RTMP chunks compress their headers against previously sent and received
chunks. Every inbound chunk must reach the deserializer in the order it
arrived, and every outbound chunk must reach the peer in the order it was
created. A `ChunkSerializer` or `ChunkDeserializer` cannot join mid-stream:
it lacks the history, so it produces errors locally or on the peer.

Inbound chunk bytes become `MessagePayload`s. A payload describes the message
a chunk carried: timestamp, type id, message stream id, and body.

Outbound payloads become `Packet`s. A packet wraps the chunk bytes plus a flag
that tells whether the packet may be dropped. Audio and video packets can
usually be dropped when bandwidth runs out. Any other packet must not be
dropped: a gap causes deserialization errors on the peer.

Inbound and outbound buffers use the [bytes crate](https://crates.io/crates/bytes),
so copies stay minimal.

## Examples

```
# use bytes::Bytes;
# use rtmpx::time::RtmpTimestamp;
# use rtmpx::chunk_io::{ChunkSerializer, ChunkDeserializer};
# use rtmpx::messages::MessagePayload;
# fn main() {
let input1 = MessagePayload {
    timestamp: RtmpTimestamp::new(55),
    message_stream_id: 1,
    type_id: 15,
    data: Bytes::from(vec![1, 2, 3, 4, 5, 6]),
};

let mut serializer = ChunkSerializer::new();
let packet1 = serializer.serialize(&input1, false, false).unwrap();

let mut deserializer = ChunkDeserializer::new();
let output1 = deserializer.get_next_message(&packet1.bytes).unwrap().unwrap();

assert_eq!(output1, input1);
# }

```
*/

mod chunk_header;
mod deserialization_errors;
mod deserializer;
mod serialization_errors;
mod serializer;

pub use self::deserialization_errors::ChunkDeserializationError;
pub use self::deserializer::{ChunkDeserializer, ChunkDeserializerConfig};
pub use self::serialization_errors::ChunkSerializationError;
pub use self::serializer::{ChunkSerializer, Packet};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::messages::MessagePayload;
    use crate::time::RtmpTimestamp;
    use bytes::Bytes;

    #[test]
    fn can_deserialize_messages_serialized_by_chunk_serializer_struct() {
        let input1 = MessagePayload {
            timestamp: RtmpTimestamp::new(55),
            message_stream_id: 1,
            type_id: 15,
            data: Bytes::from(vec![1, 2, 3, 4, 5, 6]),
        };

        let input2 = MessagePayload {
            timestamp: RtmpTimestamp::new(65),
            message_stream_id: 1,
            type_id: 15,
            data: Bytes::from(vec![8, 9, 10]),
        };

        let input3 = MessagePayload {
            timestamp: RtmpTimestamp::new(75),
            message_stream_id: 1,
            type_id: 15,
            data: Bytes::from(vec![1, 2, 3]),
        };

        let mut serializer = ChunkSerializer::new();
        let packet1 = serializer.serialize(&input1, false, false).unwrap();
        let packet2 = serializer.serialize(&input2, false, false).unwrap();
        let packet3 = serializer.serialize(&input3, false, false).unwrap();

        let mut deserializer = ChunkDeserializer::new();
        let output1 = deserializer
            .get_next_message(&packet1.bytes)
            .unwrap()
            .unwrap();
        let output2 = deserializer
            .get_next_message(&packet2.bytes)
            .unwrap()
            .unwrap();
        let output3 = deserializer
            .get_next_message(&packet3.bytes)
            .unwrap()
            .unwrap();

        assert_eq!(
            output1, input1,
            "First message was not deserialized as expected"
        );
        assert_eq!(
            output2, input2,
            "Second message was not deserialized as expected"
        );
        assert_eq!(
            output3, input3,
            "Third message was not deserialized as expected"
        );
    }

    #[test]
    fn can_deserialize_messages_serialized_with_decreasing_time() {
        let input1 = MessagePayload {
            timestamp: RtmpTimestamp::new(65),
            message_stream_id: 1,
            type_id: 15,
            data: Bytes::from(vec![1, 2, 3, 4, 5, 6]),
        };

        let input2 = MessagePayload {
            timestamp: RtmpTimestamp::new(55),
            message_stream_id: 1,
            type_id: 15,
            data: Bytes::from(vec![8, 9, 10]),
        };

        let input3 = MessagePayload {
            timestamp: RtmpTimestamp::new(45),
            message_stream_id: 1,
            type_id: 15,
            data: Bytes::from(vec![1, 2, 3]),
        };

        let mut serializer = ChunkSerializer::new();
        let packet1 = serializer.serialize(&input1, false, false).unwrap();
        let packet2 = serializer.serialize(&input2, false, false).unwrap();
        let packet3 = serializer.serialize(&input3, false, false).unwrap();

        let mut deserializer = ChunkDeserializer::new();
        let output1 = deserializer
            .get_next_message(&packet1.bytes)
            .unwrap()
            .unwrap();
        let output2 = deserializer
            .get_next_message(&packet2.bytes)
            .unwrap()
            .unwrap();
        let output3 = deserializer
            .get_next_message(&packet3.bytes)
            .unwrap()
            .unwrap();

        assert_eq!(
            output1, input1,
            "First message was not deserialized as expected"
        );
        assert_eq!(
            output2, input2,
            "Second message was not deserialized as expected"
        );
        assert_eq!(
            output3, input3,
            "Third message was not deserialized as expected"
        );
    }
    #[test]
    fn can_round_trip_timestamps_across_u32_wraparound() {
        // A stream that stays live past the u32 millisecond rollover (~49 days)
        // must keep monotonically advancing timestamps on the wire: the delta
        // after the wrap is the forward distance mod 2^32, and a later delta
        // larger than 0xFFFFFF must take the extended-timestamp path.
        let input1 = MessagePayload {
            timestamp: RtmpTimestamp::new(u32::MAX - 10),
            message_stream_id: 1,
            type_id: 8,
            data: Bytes::from(vec![0xAF, 0x01, 0x02]),
        };
        let input2 = MessagePayload {
            timestamp: RtmpTimestamp::new(20),
            message_stream_id: 1,
            type_id: 8,
            data: Bytes::from(vec![0xAF, 0x01, 0x03]),
        };
        let input3 = MessagePayload {
            timestamp: RtmpTimestamp::new(20 + 16777225),
            message_stream_id: 1,
            type_id: 8,
            data: Bytes::from(vec![0xAF, 0x01, 0x04]),
        };

        let mut serializer = ChunkSerializer::new();
        let packet1 = serializer.serialize(&input1, false, false).unwrap();
        let packet2 = serializer.serialize(&input2, false, false).unwrap();
        let packet3 = serializer.serialize(&input3, false, false).unwrap();

        let mut deserializer = ChunkDeserializer::new();
        let output1 = deserializer
            .get_next_message(&packet1.bytes)
            .unwrap()
            .unwrap();
        let output2 = deserializer
            .get_next_message(&packet2.bytes)
            .unwrap()
            .unwrap();
        let output3 = deserializer
            .get_next_message(&packet3.bytes)
            .unwrap()
            .unwrap();

        assert_eq!(
            output1, input1,
            "Pre-wrap message was not deserialized as expected"
        );
        assert_eq!(
            output2, input2,
            "Post-wrap message timestamp did not survive the u32 rollover"
        );
        assert_eq!(
            output3, input3,
            "Large post-wrap delta did not survive the extended-timestamp path"
        );
    }
}
