/*!

Chunk framing for RTMP, as described in section 5.3 of the [official RTMP specification](https://www.adobe.com/content/dam/acom/en/devnet/rtmp/pdf/rtmp_specification_1.0.pdf).

RTMP chunks compress their headers against previously sent and received
chunks. Every inbound chunk must reach the deserializer in the order it
arrived, and every outbound chunk must reach the peer in the order it was
created. A `ChunkEncoder` or `ContiguousDecoder` cannot join mid-stream:
it lacks the history, so it produces errors locally or on the peer.

Inbound chunk bytes become `RawMessage`s. A payload describes the message
a chunk carried: timestamp, type id, message stream id, and body.

Outbound payloads become `Packet`s. A packet wraps the chunk bytes plus a flag
that tells whether the packet may be dropped. Audio and video packets can
usually be dropped when bandwidth runs out. Any other packet must not be
dropped: a gap causes deserialization errors on the peer.

Inbound and outbound buffers use the [bytes crate](https://crates.io/crates/bytes),
so copies stay minimal.

## Example

```
use bytes::Bytes;
use rtmpx::{EncodeOptions, chunk_io::{ChunkEncoder, MessageDecoder}, messages::RawMessage, time::RtmpTimestamp};
let message = RawMessage { timestamp: RtmpTimestamp::new(55), message_stream_id: 1,
    type_id: 9, data: Bytes::from_static(b"sample") };
let mut encoder = ChunkEncoder::new();
let packet = encoder.encode(message.as_ref(), EncodeOptions::default())?;
// Explicit transport copy for this in-memory example.
let mut input = Bytes::from(packet.to_vec());
let decoded = MessageDecoder::new().decode(&mut input)?.unwrap();
assert_eq!(decoded.data.into_bytes(), message.data);
# Ok::<(), Box<dyn std::error::Error>>(())
```
*/

mod chunk_header;
mod deserialization_errors;
mod deserializer;
mod serialization_errors;
mod serializer;
mod streaming;
pub use streaming::{ChunkParser, MessageDecoder, MessageFragment, MessageHeader, ParseStep};

pub use self::deserialization_errors::DecodeError;
pub use self::deserializer::{ContiguousDecoder, DecoderLimits};
pub use self::serialization_errors::EncodeError;
pub use self::serializer::{ChunkEncoder, DropPolicy, EncodeOptions, HeaderMode, Packet};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::messages::RawMessage;
    use crate::time::RtmpTimestamp;
    use bytes::Bytes;

    #[test]
    fn can_deserialize_messages_serialized_by_chunk_serializer_struct() {
        let input1 = RawMessage {
            timestamp: RtmpTimestamp::new(55),
            message_stream_id: 1,
            type_id: 15,
            data: Bytes::from(vec![1, 2, 3, 4, 5, 6]),
        };

        let input2 = RawMessage {
            timestamp: RtmpTimestamp::new(65),
            message_stream_id: 1,
            type_id: 15,
            data: Bytes::from(vec![8, 9, 10]),
        };

        let input3 = RawMessage {
            timestamp: RtmpTimestamp::new(75),
            message_stream_id: 1,
            type_id: 15,
            data: Bytes::from(vec![1, 2, 3]),
        };

        let mut serializer = ChunkEncoder::new();
        let packet1 = serializer.serialize(&input1, false, false).unwrap();
        let packet2 = serializer.serialize(&input2, false, false).unwrap();
        let packet3 = serializer.serialize(&input3, false, false).unwrap();

        let mut deserializer = ContiguousDecoder::new();
        let output1 = deserializer
            .get_next_message(&packet1.to_vec())
            .unwrap()
            .unwrap();
        let output2 = deserializer
            .get_next_message(&packet2.to_vec())
            .unwrap()
            .unwrap();
        let output3 = deserializer
            .get_next_message(&packet3.to_vec())
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
        let input1 = RawMessage {
            timestamp: RtmpTimestamp::new(65),
            message_stream_id: 1,
            type_id: 15,
            data: Bytes::from(vec![1, 2, 3, 4, 5, 6]),
        };

        let input2 = RawMessage {
            timestamp: RtmpTimestamp::new(55),
            message_stream_id: 1,
            type_id: 15,
            data: Bytes::from(vec![8, 9, 10]),
        };

        let input3 = RawMessage {
            timestamp: RtmpTimestamp::new(45),
            message_stream_id: 1,
            type_id: 15,
            data: Bytes::from(vec![1, 2, 3]),
        };

        let mut serializer = ChunkEncoder::new();
        let packet1 = serializer.serialize(&input1, false, false).unwrap();
        let packet2 = serializer.serialize(&input2, false, false).unwrap();
        let packet3 = serializer.serialize(&input3, false, false).unwrap();

        let mut deserializer = ContiguousDecoder::new();
        let output1 = deserializer
            .get_next_message(&packet1.to_vec())
            .unwrap()
            .unwrap();
        let output2 = deserializer
            .get_next_message(&packet2.to_vec())
            .unwrap()
            .unwrap();
        let output3 = deserializer
            .get_next_message(&packet3.to_vec())
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
        let input1 = RawMessage {
            timestamp: RtmpTimestamp::new(u32::MAX - 10),
            message_stream_id: 1,
            type_id: 8,
            data: Bytes::from(vec![0xAF, 0x01, 0x02]),
        };
        let input2 = RawMessage {
            timestamp: RtmpTimestamp::new(20),
            message_stream_id: 1,
            type_id: 8,
            data: Bytes::from(vec![0xAF, 0x01, 0x03]),
        };
        let input3 = RawMessage {
            timestamp: RtmpTimestamp::new(20 + 16777225),
            message_stream_id: 1,
            type_id: 8,
            data: Bytes::from(vec![0xAF, 0x01, 0x04]),
        };

        let mut serializer = ChunkEncoder::new();
        let packet1 = serializer.serialize(&input1, false, false).unwrap();
        let packet2 = serializer.serialize(&input2, false, false).unwrap();
        let packet3 = serializer.serialize(&input3, false, false).unwrap();

        let mut deserializer = ContiguousDecoder::new();
        let output1 = deserializer
            .get_next_message(&packet1.to_vec())
            .unwrap()
            .unwrap();
        let output2 = deserializer
            .get_next_message(&packet2.to_vec())
            .unwrap()
            .unwrap();
        let output3 = deserializer
            .get_next_message(&packet3.to_vec())
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
