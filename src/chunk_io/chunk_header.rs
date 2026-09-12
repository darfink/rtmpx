use crate::time::RtmpTimestamp;

#[derive(Clone, Copy, PartialEq, Debug)]
pub enum ChunkHeaderFormat {
    Full,                            // Format 0
    TimeDeltaWithoutMessageStreamId, // Format 1
    TimeDeltaOnly,                   // Format 2
    Empty,                           // Format 3
}

#[derive(Clone, Copy, Debug)]
pub struct ChunkHeader {
    pub chunk_stream_id: u32,
    pub timestamp: RtmpTimestamp,
    pub timestamp_field: u32,
    pub message_length: u32,
    pub message_type_id: u8,
    pub message_stream_id: u32,
    pub can_be_dropped: bool,
}
