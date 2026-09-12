//! Run with `cargo run --example zero_copy`.
use bytes::Bytes;
use rtmpx::{
    Amf3Document, EnhancedValidationMode, Payload, PayloadPool, ValidatedMedia,
    amf3::{self, Amf3Value},
    chunk_io::{ChunkEncoder, MessageDecoder, Packet},
    messages::RawMessage,
    time::RtmpTimestamp,
};
use std::io::{self, IoSlice, Write};

// Works with TcpStream and other vectored writers. The protocol core performs no I/O.
fn write_packet<P: rtmpx::Segments>(
    writer: &mut impl Write,
    mut packet: Packet<P>,
) -> io::Result<()> {
    let cursor = &mut packet;
    while !cursor.is_complete() {
        let mut slices = [IoSlice::new(&[]); 32];
        let count = cursor.io_slices(&mut slices);
        match writer.write_vectored(&slices[..count]) {
            Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
            Ok(written) => cursor.advance(written),
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let data: Payload = [
        Bytes::from_static(b"\x27\x01\x00\x00\x00"),
        Bytes::from(vec![42; 1024]),
    ]
    .into_iter()
    .collect();
    let mut sender = ChunkEncoder::new();
    let plan = sender.encode(
        RawMessage {
            timestamp: RtmpTimestamp::new(42),
            type_id: 9,
            message_stream_id: 1,
            data,
        },
        rtmpx::EncodeOptions {
            headers: if false {
                rtmpx::HeaderMode::Full
            } else {
                rtmpx::HeaderMode::Compressed
            },
            drop_policy: if false {
                rtmpx::DropPolicy::Allowed
            } else {
                rtmpx::DropPolicy::Never
            },
        },
    )?;

    // A Vec stands in for the transport in this example. This step copies wire bytes.
    let mut transport = Vec::new();
    let expected = plan.payload().to_bytes();
    write_packet(&mut transport, plan)?;
    let mut received = Bytes::from(transport);
    let mut decoder = MessageDecoder::new();
    decoder.set_payload_pool(PayloadPool::new(rtmpx::PayloadPoolConfig {
        max_cached_payloads: 16,
        max_descriptors_per_payload: 8193,
    }));
    let message = decoder.decode(&mut received)?.unwrap();
    assert!(received.is_empty());
    assert_eq!(message.data.to_bytes(), expected);

    // Validation borrows segments, including headers split between receive buffers.
    let media = ValidatedMedia::parse_video(message.data.view(), EnhancedValidationMode::Strict)?;
    assert!(media.classification().coded);

    // A relay retains the received payload pieces and creates only new headers.
    let mut relay = ChunkEncoder::new();
    let forwarded = relay.encode(
        message,
        rtmpx::EncodeOptions {
            headers: if false {
                rtmpx::HeaderMode::Full
            } else {
                rtmpx::HeaderMode::Compressed
            },
            drop_policy: if false {
                rtmpx::DropPolicy::Allowed
            } else {
                rtmpx::DropPolicy::Never
            },
        },
    )?;
    println!(
        "Forwarded {} payload bytes in {} segments",
        forwarded.payload().len(),
        forwarded.payload().segments().count()
    );

    // AMF documents own shared objects. A reference can point back to its parent.
    let mut document = Amf3Document::new();
    let id = document.insert(Amf3Value::dynamic_object(Vec::new()));
    *document.get_mut(id).unwrap() =
        Amf3Value::dynamic_object(vec![("self".into(), Amf3Value::Reference(id))]);
    document.roots_mut().push(Amf3Value::Reference(id));
    let wire = document.serialize()?;
    let decoded = amf3::deserialize_document(&mut wire.as_slice())?;
    assert_eq!(decoded.serialize()?, wire);
    println!("AMF graph contains {} object", decoded.objects().len());
    Ok(())
}
