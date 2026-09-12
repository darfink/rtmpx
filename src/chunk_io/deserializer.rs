use super::{ChunkParser, DecodeError};
use crate::messages::RawMessage;
use bytes::{Bytes, BytesMut};
use std::collections::HashMap;
use std::mem;

/// Resource limits for one inbound RTMP chunk stream parser.
///
/// Defaults mirror the defensive limits used by Scuffle's public listener.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct DecoderLimits {
    pub maximum_chunk_size: usize,
    pub maximum_message_size: usize,
    pub maximum_tracked_chunk_streams: usize,
    pub maximum_partial_messages: usize,
    pub maximum_buffered_bytes: usize,
    /// Maximum retained payload descriptors for one owned segmented message.
    pub maximum_fragments_per_message: usize,
}

impl Default for DecoderLimits {
    fn default() -> Self {
        DecoderLimits {
            maximum_chunk_size: 64 * 1024,
            maximum_message_size: 10 * 1024 * 1024,
            maximum_tracked_chunk_streams: 100,
            maximum_partial_messages: 4,
            maximum_buffered_bytes: 16 * 1024 * 1024,
            maximum_fragments_per_message: 128 * 1024,
        }
    }
}

/// Contiguous compatibility adapter over [`ChunkParser`].
///
/// Supply input once, then call with an empty slice until `None` is returned.
/// Apply SetChunkSize and Abort before requesting the next message. This adapter
/// copies payload bytes into one message allocation. Use [`super::MessageDecoder`]
/// to retain owned receive buffers without copying payloads.
pub struct ContiguousDecoder {
    parser: ChunkParser,
    pending: Vec<u8>,
    partials: HashMap<u32, BytesMut>,
    buffered: usize,
    reserved: usize,
    limits: DecoderLimits,
}
impl Default for ContiguousDecoder {
    fn default() -> Self {
        Self::new()
    }
}
impl ContiguousDecoder {
    pub fn new() -> Self {
        Self::with_limits(DecoderLimits::default())
    }
    pub fn with_limits(limits: DecoderLimits) -> Self {
        Self {
            parser: ChunkParser::with_limits(limits),
            pending: Vec::new(),
            partials: HashMap::new(),
            buffered: 0,
            reserved: 0,
            limits,
        }
    }
    pub fn chunk_size(&self) -> usize {
        self.parser.chunk_size()
    }
    pub fn set_chunk_size(&mut self, size: usize) -> Result<(), DecodeError> {
        self.parser.set_chunk_size(size)
    }
    pub fn abort_chunk_stream(&mut self, csid: u32) {
        self.parser.abort_chunk_stream(csid);
        if let Some(body) = self.partials.remove(&csid) {
            self.buffered -= body.len();
            self.reserved -= body.capacity();
        }
    }
    /// True only at a clean wire boundary with no incomplete message.
    pub fn is_idle(&self) -> bool {
        self.pending.is_empty() && self.parser.is_idle()
    }
    pub fn get_next_message(&mut self, bytes: &[u8]) -> Result<Option<RawMessage>, DecodeError> {
        let attempted = self
            .buffered
            .saturating_add(self.pending.len())
            .saturating_add(bytes.len());
        if attempted > self.limits.maximum_buffered_bytes {
            return Err(DecodeError::ResourceLimitExceeded {
                resource: "buffered bytes",
                attempted,
                maximum: self.limits.maximum_buffered_bytes,
            });
        }
        let mut pending = mem::take(&mut self.pending);
        let from_pending = !pending.is_empty();
        let input = if from_pending {
            pending.extend_from_slice(bytes);
            pending.as_slice()
        } else {
            bytes
        };
        let mut offset = 0;
        let result = loop {
            let step = self.parser.consume(&input[offset..])?;
            offset += step.consumed;
            let Some(fragment) = step.fragment else {
                break None;
            };
            let header = fragment.header;
            let complete = fragment.is_end();
            if fragment.is_start() && complete {
                break Some(RawMessage {
                    timestamp: header.timestamp,
                    type_id: header.type_id,
                    message_stream_id: header.message_stream_id,
                    data: Bytes::copy_from_slice(fragment.data),
                });
            }
            if !self.partials.contains_key(&header.chunk_stream_id) {
                let attempted = self.reserved.saturating_add(header.message_length);
                if attempted > self.limits.maximum_buffered_bytes {
                    return Err(DecodeError::ResourceLimitExceeded {
                        resource: "reserved payload bytes",
                        attempted,
                        maximum: self.limits.maximum_buffered_bytes,
                    });
                }
                let body = BytesMut::with_capacity(header.message_length);
                self.reserved += body.capacity();
                self.partials.insert(header.chunk_stream_id, body);
            }
            let body = self.partials.get_mut(&header.chunk_stream_id).unwrap();
            body.extend_from_slice(fragment.data);
            self.buffered += fragment.data.len();
            if complete {
                let body = self.partials.remove(&header.chunk_stream_id).unwrap();
                self.buffered -= body.len();
                self.reserved -= body.capacity();
                break Some(RawMessage {
                    timestamp: header.timestamp,
                    type_id: header.type_id,
                    message_stream_id: header.message_stream_id,
                    data: body.freeze(),
                });
            }
        };
        if from_pending {
            let remaining = input.len() - offset;
            pending.copy_within(offset.., 0);
            pending.truncate(remaining);
        } else {
            pending.extend_from_slice(&bytes[offset..]);
        }
        self.pending = pending;
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    const INITIAL_MAX_CHUNK_SIZE: usize = 128;
    const MAX_INITIAL_TIMESTAMP: u32 = 0xff_ffff;
    use super::*;
    use crate::time::RtmpTimestamp;
    use byteorder::{BigEndian, LittleEndian, WriteBytesExt};
    use std::io::{Cursor, Write};

    #[test]
    fn can_read_type_0_chunk_with_small_chunk_stream_id_and_small_timestamp() {
        let csid = 50;
        let timestamp = 25u32;
        let message_stream_id = 5u32;
        let type_id = 3;
        let payload = [1_u8, 2_u8, 3_u8];

        let bytes = form_type_0_chunk(
            csid,
            timestamp,
            message_stream_id,
            type_id,
            &payload,
            INITIAL_MAX_CHUNK_SIZE,
        );
        let mut deserializer = ContiguousDecoder::new();
        let result = deserializer.get_next_message(&bytes).unwrap().unwrap();

        assert_eq!(result.type_id, 3, "Incorrect type id");
        assert_eq!(
            result.timestamp,
            RtmpTimestamp::new(timestamp),
            "Incorrect timestamp"
        );
        assert_eq!(&result.data[..], &payload[..], "Incorrect data");
    }

    #[test]
    fn can_read_type_0_chunk_with_medium_chunk_stream_id_and_small_timestamp() {
        let csid = 500;
        let timestamp = 25u32;
        let message_stream_id = 5u32;
        let type_id = 3;
        let payload = [1_u8, 2_u8, 3_u8];

        let bytes = form_type_0_chunk(
            csid,
            timestamp,
            message_stream_id,
            type_id,
            &payload,
            INITIAL_MAX_CHUNK_SIZE,
        );
        let mut deserializer = ContiguousDecoder::new();
        let result = deserializer.get_next_message(&bytes).unwrap().unwrap();

        assert_eq!(result.type_id, 3, "Incorrect type id");
        assert_eq!(
            result.timestamp,
            RtmpTimestamp::new(timestamp),
            "Incorrect timestamp"
        );
        assert_eq!(&result.data[..], &payload[..], "Incorrect data");
    }

    #[test]
    fn can_read_type_0_chunk_with_large_chunk_stream_id_and_small_timestamp() {
        let csid = 50000;
        let timestamp = 25u32;
        let message_stream_id = 5u32;
        let type_id = 3;
        let payload = [1_u8, 2_u8, 3_u8];

        let bytes = form_type_0_chunk(
            csid,
            timestamp,
            message_stream_id,
            type_id,
            &payload,
            INITIAL_MAX_CHUNK_SIZE,
        );
        let mut deserializer = ContiguousDecoder::new();
        let result = deserializer.get_next_message(&bytes).unwrap().unwrap();

        assert_eq!(result.type_id, 3, "Incorrect type id");
        assert_eq!(
            result.timestamp,
            RtmpTimestamp::new(timestamp),
            "Incorrect timestamp"
        );
        assert_eq!(&result.data[..], &payload[..], "Incorrect data");
    }

    #[test]
    fn can_read_type_0_chunk_with_small_chunk_stream_id_and_large_timestamp() {
        let csid = 50;
        let timestamp = 16777216u32;
        let message_stream_id = 5u32;
        let type_id = 3;
        let payload = [1_u8, 2_u8, 3_u8];

        let bytes = form_type_0_chunk(
            csid,
            timestamp,
            message_stream_id,
            type_id,
            &payload,
            INITIAL_MAX_CHUNK_SIZE,
        );
        let mut deserializer = ContiguousDecoder::new();
        let result = deserializer.get_next_message(&bytes).unwrap().unwrap();

        assert_eq!(result.type_id, 3, "Incorrect type id");
        assert_eq!(
            result.timestamp,
            RtmpTimestamp::new(timestamp),
            "Incorrect timestamp"
        );
        assert_eq!(&result.data[..], &payload[..], "Incorrect data");
    }

    #[test]
    fn can_read_type_0_chunk_with_medium_chunk_stream_id_and_large_timestamp() {
        let csid = 500;
        let timestamp = 16777216u32;
        let message_stream_id = 5u32;
        let type_id = 3;
        let payload = [1_u8, 2_u8, 3_u8];

        let bytes = form_type_0_chunk(
            csid,
            timestamp,
            message_stream_id,
            type_id,
            &payload,
            INITIAL_MAX_CHUNK_SIZE,
        );
        let mut deserializer = ContiguousDecoder::new();
        let result = deserializer.get_next_message(&bytes).unwrap().unwrap();

        assert_eq!(result.type_id, 3, "Incorrect type id");
        assert_eq!(
            result.timestamp,
            RtmpTimestamp::new(timestamp),
            "Incorrect timestamp"
        );
        assert_eq!(&result.data[..], &payload[..], "Incorrect data");
    }

    #[test]
    fn can_read_type_0_chunk_with_large_chunk_stream_id_and_large_timestamp() {
        let csid = 50000;
        let timestamp = 16777216u32;
        let message_stream_id = 5u32;
        let type_id = 3;
        let payload = [1_u8, 2_u8, 3_u8];

        let bytes = form_type_0_chunk(
            csid,
            timestamp,
            message_stream_id,
            type_id,
            &payload,
            INITIAL_MAX_CHUNK_SIZE,
        );
        let mut deserializer = ContiguousDecoder::new();
        let result = deserializer.get_next_message(&bytes).unwrap().unwrap();

        assert_eq!(result.type_id, 3, "Incorrect type id");
        assert_eq!(
            result.timestamp,
            RtmpTimestamp::new(timestamp),
            "Incorrect timestamp"
        );
        assert_eq!(&result.data[..], &payload[..], "Incorrect data");
    }

    #[test]
    fn can_read_type_1_chunk_with_small_chunk_stream_id_and_small_timestamp() {
        let csid = 50;
        let timestamp = 25u32;
        let delta = 10_u32;
        let message_stream_id = 5u32;
        let type_id1 = 3;
        let type_id2 = 4;
        let payload = [1_u8, 2_u8, 3_u8];

        let chunk_0_bytes = form_type_0_chunk(
            csid,
            timestamp,
            message_stream_id,
            type_id1,
            &payload,
            INITIAL_MAX_CHUNK_SIZE,
        );
        let chunk_1_bytes = form_type_1_chunk(csid, delta, type_id2, &payload);
        let mut deserializer = ContiguousDecoder::new();
        let _ = deserializer
            .get_next_message(&chunk_0_bytes)
            .unwrap()
            .unwrap();
        let result = deserializer
            .get_next_message(&chunk_1_bytes)
            .unwrap()
            .unwrap();

        assert_eq!(result.type_id, type_id2, "Incorrect type id");
        assert_eq!(
            result.timestamp,
            RtmpTimestamp::new(timestamp + delta),
            "Incorrect timestamp"
        );
        assert_eq!(&result.data[..], &payload[..], "Incorrect data");
    }

    #[test]
    fn can_read_type_2_chunk_with_small_chunk_stream_id_and_small_timestamp() {
        let csid = 50;
        let timestamp = 25u32;
        let delta1 = 10_u32;
        let delta2 = 11_u32;
        let message_stream_id = 5u32;
        let type_id1 = 3;
        let type_id2 = 4;
        let payload = [1_u8, 2_u8, 3_u8];

        let chunk_0_bytes = form_type_0_chunk(
            csid,
            timestamp,
            message_stream_id,
            type_id1,
            &payload,
            INITIAL_MAX_CHUNK_SIZE,
        );
        let chunk_1_bytes = form_type_1_chunk(csid, delta1, type_id2, &payload);
        let chunk_2_bytes = form_type_2_chunk(csid, delta2, &payload);
        let mut deserializer = ContiguousDecoder::new();
        let _ = deserializer
            .get_next_message(&chunk_0_bytes)
            .unwrap()
            .unwrap();
        let _ = deserializer
            .get_next_message(&chunk_1_bytes)
            .unwrap()
            .unwrap();
        let result = deserializer
            .get_next_message(&chunk_2_bytes)
            .unwrap()
            .unwrap();

        assert_eq!(result.type_id, type_id2, "Incorrect type id");
        assert_eq!(
            result.timestamp,
            RtmpTimestamp::new(timestamp + delta1 + delta2),
            "Incorrect timestamp"
        );
        assert_eq!(&result.data[..], &payload[..], "Incorrect data");
    }

    #[test]
    fn can_read_type_2_chunk_with_small_chunk_stream_id_and_large_timestamp() {
        let csid = 50;
        let timestamp = 25u32;
        let delta1 = 10_u32;
        let delta2 = 16777216_u32;
        let message_stream_id = 5u32;
        let type_id1 = 3;
        let type_id2 = 4;
        let payload = [1_u8, 2_u8, 3_u8];

        let chunk_0_bytes = form_type_0_chunk(
            csid,
            timestamp,
            message_stream_id,
            type_id1,
            &payload,
            INITIAL_MAX_CHUNK_SIZE,
        );
        let chunk_1_bytes = form_type_1_chunk(csid, delta1, type_id2, &payload);
        let chunk_2_bytes = form_type_2_chunk(csid, delta2, &payload);
        let mut deserializer = ContiguousDecoder::new();
        let _ = deserializer
            .get_next_message(&chunk_0_bytes)
            .unwrap()
            .unwrap();
        let _ = deserializer
            .get_next_message(&chunk_1_bytes)
            .unwrap()
            .unwrap();
        let result = deserializer
            .get_next_message(&chunk_2_bytes)
            .unwrap()
            .unwrap();

        assert_eq!(result.type_id, type_id2, "Incorrect type id");
        assert_eq!(
            result.timestamp,
            RtmpTimestamp::new(timestamp + delta1 + delta2),
            "Incorrect timestamp"
        );
        assert_eq!(&result.data[..], &payload[..], "Incorrect data");
    }

    #[test]
    fn can_read_type_3_chunk_with_small_chunk_stream_id_and_small_timestamp() {
        let csid = 50;
        let timestamp = 25u32;
        let delta1 = 10_u32;
        let delta2 = 11_u32;
        let message_stream_id = 5u32;
        let type_id1 = 3;
        let type_id2 = 4;
        let payload = [1_u8, 2_u8, 3_u8];

        let chunk_0_bytes = form_type_0_chunk(
            csid,
            timestamp,
            message_stream_id,
            type_id1,
            &payload,
            INITIAL_MAX_CHUNK_SIZE,
        );
        let chunk_1_bytes = form_type_1_chunk(csid, delta1, type_id2, &payload);
        let chunk_2_bytes = form_type_2_chunk(csid, delta2, &payload);
        let chunk_3_bytes = form_type_3_chunk(csid, &payload, INITIAL_MAX_CHUNK_SIZE, None);
        let mut deserializer = ContiguousDecoder::new();
        let _ = deserializer
            .get_next_message(&chunk_0_bytes)
            .unwrap()
            .unwrap();
        let _ = deserializer
            .get_next_message(&chunk_1_bytes)
            .unwrap()
            .unwrap();
        let _ = deserializer
            .get_next_message(&chunk_2_bytes)
            .unwrap()
            .unwrap();
        let result = deserializer
            .get_next_message(&chunk_3_bytes)
            .unwrap()
            .unwrap();

        assert_eq!(result.type_id, type_id2, "Incorrect type id");
        assert_eq!(
            result.timestamp,
            RtmpTimestamp::new(timestamp + delta1 + delta2 + delta2),
            "Incorrect timestamp"
        );
        assert_eq!(&result.data[..], &payload[..], "Incorrect data");
    }

    #[test]
    fn can_read_type_3_chunk_with_small_chunk_stream_id_and_large_timestamp() {
        let csid = 50;
        let timestamp = 10_u32;
        let delta1 = 10_u32;
        let delta2 = 16777216_u32;
        let message_stream_id = 5u32;
        let type_id1 = 3;
        let type_id2 = 4;
        let payload = [1_u8, 2_u8, 3_u8];

        let chunk_0_bytes = form_type_0_chunk(
            csid,
            timestamp,
            message_stream_id,
            type_id1,
            &payload,
            INITIAL_MAX_CHUNK_SIZE,
        );
        let chunk_1_bytes = form_type_1_chunk(csid, delta1, type_id2, &payload);
        let chunk_2_bytes = form_type_2_chunk(csid, delta2, &payload);
        let chunk_3_bytes = form_type_3_chunk(csid, &payload, INITIAL_MAX_CHUNK_SIZE, Some(delta2));
        let mut deserializer = ContiguousDecoder::new();
        let _ = deserializer
            .get_next_message(&chunk_0_bytes)
            .unwrap()
            .unwrap();
        let _ = deserializer
            .get_next_message(&chunk_1_bytes)
            .unwrap()
            .unwrap();
        let _ = deserializer
            .get_next_message(&chunk_2_bytes)
            .unwrap()
            .unwrap();
        let result = deserializer
            .get_next_message(&chunk_3_bytes)
            .unwrap()
            .unwrap();

        assert_eq!(result.type_id, type_id2, "Incorrect type id");
        assert_eq!(
            result.timestamp,
            RtmpTimestamp::new(timestamp + delta1 + delta2 + delta2),
            "Incorrect timestamp"
        );
        assert_eq!(&result.data[..], &payload[..], "Incorrect data");
    }

    #[test]
    fn can_read_message_spread_across_multiple_deserialization_calls() {
        let csid = 50;
        let timestamp = 25u32;
        let message_stream_id = 5u32;
        let type_id = 3;
        let payload = [1_u8, 2_u8, 3_u8];

        let all_bytes = form_type_0_chunk(
            csid,
            timestamp,
            message_stream_id,
            type_id,
            &payload,
            INITIAL_MAX_CHUNK_SIZE,
        );
        let (first, second) = all_bytes.split_at(all_bytes.len() / 2);
        let mut deserializer = ContiguousDecoder::new();
        match deserializer.get_next_message(first).unwrap() {
            Some(x) => panic!("Expected None but received {:?}", x),
            None => (),
        };

        let result = deserializer.get_next_message(second).unwrap().unwrap();

        assert_eq!(result.type_id, 3, "Incorrect type id");
        assert_eq!(
            result.timestamp,
            RtmpTimestamp::new(timestamp),
            "Incorrect timestamp"
        );
        assert_eq!(&result.data[..], &payload[..], "Incorrect data");
    }

    #[test]
    fn can_read_message_exceeding_maximum_chunk_size() {
        let csid = 50;
        let timestamp = 25u32;
        let message_stream_id = 5u32;
        let type_id = 3;
        let payload = [100_u8; 500];
        let max_chunk_size = 100;

        let bytes = form_type_0_chunk(
            csid,
            timestamp,
            message_stream_id,
            type_id,
            &payload,
            max_chunk_size,
        );
        let mut deserializer = ContiguousDecoder::new();
        deserializer.set_chunk_size(max_chunk_size).unwrap();
        let result = deserializer.get_next_message(&bytes).unwrap().unwrap();

        assert_eq!(result.type_id, 3, "Incorrect type id");
        assert_eq!(
            result.timestamp,
            RtmpTimestamp::new(timestamp),
            "Incorrect timestamp"
        );
        assert_eq!(&result.data[..], &payload[..], "Incorrect data");
    }

    #[test]
    fn error_when_setting_chunk_size_too_large() {
        const CHUNK_SIZE_VALUE: usize = 2147483648;
        let mut deserializer = ContiguousDecoder::new();
        match deserializer.set_chunk_size(CHUNK_SIZE_VALUE) {
            Err(DecodeError::InvalidMaxChunkSize {
                chunk_size: CHUNK_SIZE_VALUE,
            }) => {} // success
            x => panic!("Unexpected set max chunk size result of {:?}", x),
        }
    }

    #[test]
    fn type_2_chunk_that_exceeds_max_chunk_size_does_not_keep_applying_delta_to_timestamp() {
        // It was noticed that OBS does not totally conform to the RTMP specification.  It will
        // send a type 1 chunk with a time delta for a video packet, but will send the remaining
        // parts of that chunk with a type 3 header (even though the delta should not be applied).
        // this test verifies we can handle that.

        let chunk1 = [
            0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x09, 0x01, 0x00, 0x00, 0x00, 0x01,
        ];
        let chunk2 = [
            0x44, 0x00, 0x00, 0x21, 0x00, 0x00, 0x05, 0x09, 0x01, 0x02, 0x03, 0x04, 0xc4, 0x05,
        ];

        let mut deserializer = ContiguousDecoder::new();
        deserializer.set_chunk_size(4).unwrap();

        let payload1 = deserializer.get_next_message(&chunk1).unwrap().unwrap();
        assert_eq!(payload1.type_id, 0x09, "Incorrect payload 1 type");
        assert_eq!(
            payload1.timestamp,
            RtmpTimestamp::new(0),
            "Incorrect payload 1 timestamp"
        );
        assert_eq!(&payload1.data[..], &[0x01], "Incorrect payload 1 data");

        let payload2 = deserializer.get_next_message(&chunk2).unwrap().unwrap();
        assert_eq!(payload2.type_id, 0x09, "Incorrect payload 2 type");
        assert_eq!(
            payload2.timestamp,
            RtmpTimestamp::new(33),
            "Incorrect payload 2 timestamp"
        );
        assert_eq!(
            &payload2.data[..],
            &[0x01, 0x02, 0x03, 0x04, 0x05],
            "Incorrect payload 2 data"
        );
    }

    #[test]
    fn can_read_type_3_chunk_that_follows_type_0_has_extended_timestamp() {
        let chunk1 = [
            0x06, 0xff, 0xff, 0xff, 0x00, 0x00, 0x07, 0x09, 0x01, 0x00, 0x00, 0x00, 0x01, 0xff,
            0xff, 0xff, 0x01, 0x02, 0x03, 0x04,
        ];
        let chunk2 = [0xc6, 0x01, 0xff, 0xff, 0xff, 0x05, 0x06, 0x07];
        let mut deserializer = ContiguousDecoder::new();
        deserializer.set_chunk_size(4).unwrap();
        let _ = deserializer.get_next_message(&chunk1).unwrap();
        let payload = deserializer.get_next_message(&chunk2).unwrap().unwrap();
        assert_eq!(payload.type_id, 0x09, "Incorrect payload type");
        assert_eq!(
            payload.timestamp,
            RtmpTimestamp::new(0x1ffffff),
            "Incorrect payload timestamp"
        );
        assert_eq!(
            &payload.data[..],
            &[0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07],
            "Incorrect payload data"
        );
    }

    fn form_type_0_chunk(
        csid: u32,
        timestamp: u32,
        message_stream_id: u32,
        type_id: u8,
        payload: &[u8],
        max_chunk_length: usize,
    ) -> Vec<u8> {
        let mut cursor = Cursor::new(Vec::new());
        if csid < 64 {
            cursor.write_u8(csid as u8).unwrap();
        } else if csid < 319 {
            cursor.write_u8(0_u8).unwrap();
            cursor.write_u8((csid - 64) as u8).unwrap();
        } else {
            cursor.write_u8(1_u8).unwrap();
            cursor.write_u16::<BigEndian>((csid - 64) as u16).unwrap();
        }

        let standard_timestamp = if timestamp >= 16777215 {
            16777215
        } else {
            timestamp
        };
        cursor.write_u24::<BigEndian>(standard_timestamp).unwrap();
        cursor.write_u24::<BigEndian>(payload.len() as u32).unwrap();
        cursor.write_u8(type_id).unwrap();
        cursor.write_u32::<LittleEndian>(message_stream_id).unwrap();

        let mut option_extended_timestamp = None;
        if timestamp > 16777215 {
            cursor.write_u32::<BigEndian>(timestamp).unwrap();
            option_extended_timestamp = Some(timestamp);
        }

        // If the payload is over max_chunk_length, assume we want to form a split message
        // and therefore need to only write the max chunk amount of the payload in this request
        // and append a type 3 chunk with the rest
        if payload.len() > max_chunk_length {
            cursor.write(&payload[..max_chunk_length]).unwrap();

            let next_chunk = form_type_3_chunk(
                csid,
                &payload[max_chunk_length..],
                max_chunk_length,
                option_extended_timestamp,
            );
            cursor.write(&next_chunk).unwrap();
        } else {
            cursor.write(payload).unwrap();
        }

        cursor.into_inner()
    }

    fn form_type_1_chunk(csid: u32, delta: u32, type_id: u8, payload: &[u8]) -> Vec<u8> {
        let mut cursor = Cursor::new(Vec::new());
        if csid < 64 {
            cursor.write_u8((csid as u8) | 0b01000000).unwrap();
        } else if csid < 319 {
            cursor.write_u8(0_u8 | 0b01000000).unwrap();
            cursor.write_u8((csid - 64) as u8).unwrap();
        } else {
            cursor.write_u8(1_u8 | 0b01000000).unwrap();
            cursor.write_u16::<BigEndian>((csid - 64) as u16).unwrap();
        }

        let standard_timestamp = if delta >= 16777215 { 16777215 } else { delta };
        cursor.write_u24::<BigEndian>(standard_timestamp).unwrap();
        cursor.write_u24::<BigEndian>(payload.len() as u32).unwrap();
        cursor.write_u8(type_id).unwrap();

        if delta > 16777215 {
            cursor.write_u32::<BigEndian>(delta).unwrap();
        }

        cursor.write(payload).unwrap();

        cursor.into_inner()
    }

    fn form_type_2_chunk(csid: u32, delta: u32, payload: &[u8]) -> Vec<u8> {
        let mut cursor = Cursor::new(Vec::new());
        if csid < 64 {
            cursor.write_u8((csid as u8) | 0b10000000).unwrap();
        } else if csid < 319 {
            cursor.write_u8(0_u8 | 0b10000000).unwrap();
            cursor.write_u8((csid - 64) as u8).unwrap();
        } else {
            cursor.write_u8(1_u8 | 0b10000000).unwrap();
            cursor.write_u16::<BigEndian>((csid - 64) as u16).unwrap();
        }

        let standard_timestamp = if delta >= 16777215 { 16777215 } else { delta };
        cursor.write_u24::<BigEndian>(standard_timestamp).unwrap();

        if delta > 16777215 {
            cursor.write_u32::<BigEndian>(delta).unwrap();
        }

        cursor.write(payload).unwrap();

        cursor.into_inner()
    }

    fn form_type_3_chunk(
        csid: u32,
        payload: &[u8],
        max_chunk_length: usize,
        option_extended_timestamp: Option<u32>,
    ) -> Vec<u8> {
        let mut cursor = Cursor::new(Vec::new());
        if csid < 64 {
            cursor.write_u8((csid as u8) | 0b11000000).unwrap();
        } else if csid < 319 {
            cursor.write_u8(0_u8 | 0b11000000).unwrap();
            cursor.write_u8((csid - 64) as u8).unwrap();
        } else {
            cursor.write_u8(1_u8 | 0b11000000).unwrap();
            cursor.write_u16::<BigEndian>((csid - 64) as u16).unwrap();
        }

        if option_extended_timestamp != None {
            assert_eq!(
                option_extended_timestamp.unwrap() >= MAX_INITIAL_TIMESTAMP,
                true,
                "timestamp was less than 0xffffff"
            );
            cursor
                .write_u32::<BigEndian>(option_extended_timestamp.unwrap())
                .unwrap();
        }

        // If the payload is over max_chunk_length, assume we want to form a split message
        // and therefore need to only write the max chunk amount of the payload in this request
        // and append a type 3 chunk with the rest
        if payload.len() > max_chunk_length {
            cursor.write(&payload[..max_chunk_length]).unwrap();

            let next_chunk = form_type_3_chunk(
                csid,
                &payload[max_chunk_length..],
                max_chunk_length,
                option_extended_timestamp,
            );
            cursor.write(&next_chunk).unwrap();
        } else {
            cursor.write(payload).unwrap();
        }

        cursor.into_inner()
    }
}

impl DecoderLimits {
    /// Set the maximum number of retained payload segments per message.
    pub fn with_maximum_fragments_per_message(mut self, value: usize) -> Self {
        self.maximum_fragments_per_message = value;
        self
    }
    /// Set `maximum_chunk_size`.
    pub fn with_maximum_chunk_size(mut self, value: usize) -> Self {
        self.maximum_chunk_size = value;
        self
    }
    /// Set `maximum_message_size`.
    pub fn with_maximum_message_size(mut self, value: usize) -> Self {
        self.maximum_message_size = value;
        self
    }
    /// Set `maximum_tracked_chunk_streams`.
    pub fn with_maximum_tracked_chunk_streams(mut self, value: usize) -> Self {
        self.maximum_tracked_chunk_streams = value;
        self
    }
    /// Set `maximum_partial_messages`.
    pub fn with_maximum_partial_messages(mut self, value: usize) -> Self {
        self.maximum_partial_messages = value;
        self
    }
    /// Set `maximum_buffered_bytes`.
    pub fn with_maximum_buffered_bytes(mut self, value: usize) -> Self {
        self.maximum_buffered_bytes = value;
        self
    }
}
