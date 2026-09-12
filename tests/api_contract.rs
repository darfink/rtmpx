//! External-consumer regression tests for the 2.0 API contracts.
#[path = "support/api.rs"]
mod api;
use crate::api::chunk_io::ChunkEncoder;
use crate::api::messages::{RtmpMessage, UserControlEventType};
use crate::api::sessions::{
    ClientSession, ClientSessionConfig, ClientSessionError, ConnectionState, DataMessage,
    DataMessageType, ServerSession, ServerSessionConfig, ServerSessionError, StreamId,
};
use crate::api::time::RtmpTimestamp;
use crate::api::{
    Amf0Value, AmfRead, EnhancedValidationMode, ValidatedMedia, media::MediaValidationError,
};
use bytes::Bytes;
use std::io::{self, BufReader, Cursor, Read};

#[test]
fn custom_and_buffered_readers_can_use_both_amf_decoders() {
    struct Custom(Cursor<Vec<u8>>);
    impl Read for Custom {
        fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
            self.0.read(bytes)
        }
    }
    impl AmfRead for Custom {}
    let mut reader = Custom(Cursor::new(vec![5]));
    assert_eq!(
        crate::api::amf0::deserialize(&mut reader).unwrap(),
        vec![Amf0Value::Null]
    );
    let mut reader = BufReader::with_capacity(8, Cursor::new(vec![5, 5]));
    assert_eq!(
        crate::api::amf0::deserialize_single(&mut reader).unwrap(),
        Amf0Value::Null
    );
    assert_eq!(reader.remaining_hint(), Some(1));
    assert_eq!(
        crate::api::amf0::deserialize(&mut reader).unwrap(),
        vec![Amf0Value::Null]
    );
    let mut reader = BufReader::new(Custom(Cursor::new(vec![1])));
    assert_eq!(
        crate::api::amf3::deserialize(&mut reader).unwrap(),
        vec![crate::api::Amf3Value::Null]
    );
}

#[test]
fn input_failure_is_terminal_even_after_a_response_was_generated_in_the_same_read() {
    let mut serializer = ChunkEncoder::new();
    let ping = RtmpMessage::UserControl {
        event_type: UserControlEventType::PingRequest,
        stream_id: None,
        timestamp: Some(RtmpTimestamp::new(123)),
        buffer_length: None,
    }
    .into_raw_message(RtmpTimestamp::new(0), 0)
    .unwrap();
    let invalid_window = RtmpMessage::WindowAcknowledgement { size: 0 }
        .into_raw_message(RtmpTimestamp::new(0), 0)
        .unwrap();
    let mut input = serializer.serialize(&ping, false, false).unwrap().to_vec();
    input.extend(
        serializer
            .serialize(&invalid_window, false, false)
            .unwrap()
            .to_vec(),
    );
    let (mut client, _) = ClientSession::new(ClientSessionConfig::default()).unwrap();
    assert!(client.handle_input(&input).is_err());
    assert_eq!(client.state(), ConnectionState::Failed);
    assert!(matches!(
        client.handle_input(&[]),
        Err(ClientSessionError::SessionFailed)
    ));
    assert!(matches!(
        client.send_ping_request(),
        Err(ClientSessionError::SessionFailed)
    ));
    assert!(matches!(
        client.request_connection("live".into()),
        Err(ClientSessionError::SessionFailed)
    ));
    assert!(matches!(
        client.stop_playback(),
        Err(ClientSessionError::SessionFailed)
    ));
    let (mut server, _) = ServerSession::new(ServerSessionConfig::default()).unwrap();
    assert!(server.handle_input(&input).is_err());
    assert!(server.is_failed());
    assert!(matches!(
        server.handle_input(&[]),
        Err(ServerSessionError::SessionFailed)
    ));
    assert!(matches!(
        server.send_ping_request(),
        Err(ServerSessionError::SessionFailed)
    ));
    assert!(matches!(
        server.send_data(
            StreamId::new(1),
            DataMessage::new(DataMessageType::Amf0, RtmpTimestamp::new(0), Bytes::new())
        ),
        Err(ServerSessionError::SessionFailed)
    ));
    assert!(matches!(
        server.send_video_data(StreamId::new(1), Bytes::new(), RtmpTimestamp::new(0), false),
        Err(ServerSessionError::SessionFailed)
    ));
}

#[test]
fn single_unit_access_rejects_multitrack_instead_of_losing_tracks() {
    let mut raw = vec![0x95, 0x10, b'm', b'p', b'4', b'a'];
    for id in [1, 3] {
        raw.extend_from_slice(&[id, 0, 0, 2, 0x11, 0x88]);
    }
    let media =
        ValidatedMedia::parse_audio(Bytes::from(raw), EnhancedValidationMode::Strict).unwrap();
    assert_eq!(media.elementary_units().unwrap().len(), 2);
    assert!(matches!(
        media.elementary_unit(),
        Err(MediaValidationError::MultipleUnits { count: 2, .. })
    ));
    let original = media.raw().clone();
    let (raw, _) = media.into_parts();
    assert_eq!(raw, original);
}

#[test]
fn serial_comparison_handles_wrap_and_reports_half_cycle_ambiguity() {
    use std::cmp::Ordering;
    let before = RtmpTimestamp::new(u32::MAX - 1);
    let after = RtmpTimestamp::new(2);
    assert_eq!(after.wrapping_elapsed_since(before), 4);
    assert_eq!(after.serial_cmp(before), Some(Ordering::Greater));
    assert_eq!(before.serial_cmp(after), Some(Ordering::Less));
    assert_eq!(
        RtmpTimestamp::new(0).serial_cmp(RtmpTimestamp::new(1 << 31)),
        None
    );
    assert_eq!(
        RtmpTimestamp::new(1 << 31).serial_cmp(RtmpTimestamp::new(0)),
        None
    );
}
