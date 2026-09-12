//! Minimal publisher: cargo run --example publish -- 127.0.0.1:1935 live demo
//! Media bytes are placeholders; substitute actual encoded AVC/AAC frames.
#[path = "support/io.rs"]
mod transport;
use bytes::{Bytes, BytesMut};
use rtmpx::{
    DropPolicy,
    handshake::HandshakeRole,
    sessions::{
        ClientEvent, ClientOutput, ClientSession, ClientSessionConfig, PublishMode, StreamMetadata,
    },
    time::RtmpTimestamp,
};
use tokio::{io::AsyncWriteExt, net::TcpStream};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let address = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:1935".into());
    let app = std::env::args().nth(2).unwrap_or_else(|| "live".into());
    let key = std::env::args().nth(3).unwrap_or_else(|| "demo".into());
    let mut stream = TcpStream::connect(address).await?;
    let mut input = transport::handshake(&mut stream, HandshakeRole::Client).await?;
    let mut buffer = BytesMut::with_capacity(16 * 1024);
    let mut session = ClientSession::new(ClientSessionConfig::default())?;
    session.connect(app)?;
    let mut publishing = false;
    let mut publish_stream = None;
    loop {
        while let Some(output) = session.receive(&mut input)? {
            match output {
                ClientOutput::Packet(packet) => {
                    transport::write_packet(&mut stream, packet).await?
                }
                ClientOutput::Event(ClientEvent::ConnectionRequestAccepted { .. }) => {
                    publish_stream = Some(session.publish(&key, PublishMode::Live)?);
                }
                ClientOutput::Event(ClientEvent::PublishRequestAccepted { .. }) => {
                    publishing = true
                }
                ClientOutput::Event(ClientEvent::ConnectionRequestRejected {
                    description, ..
                }) => return Err(description.into()),
                other => eprintln!("{other:?}"),
            }
        }
        if publishing {
            break;
        }
        input = transport::read_input(&mut stream, &mut buffer)
            .await?
            .ok_or("server closed before publishing")?;
    }
    let publish_stream = publish_stream.ok_or("missing publish stream")?;
    let metadata = StreamMetadata {
        encoder: Some("rtmpx example".into()),
        ..Default::default()
    };
    transport::write_packet(
        &mut stream,
        session.send_metadata(publish_stream, &metadata)?,
    )
    .await?;
    for frame in 0..30 {
        let timestamp = RtmpTimestamp::new(frame * 33);
        transport::write_packet(
            &mut stream,
            session.send_video(
                publish_stream,
                Bytes::from_static(b"\x17\x01\0\0\0sample"),
                timestamp,
                DropPolicy::Never,
            )?,
        )
        .await?;
        transport::write_packet(
            &mut stream,
            session.send_audio(
                publish_stream,
                Bytes::from_static(b"\xaf\x01sample"),
                timestamp,
                DropPolicy::Never,
            )?,
        )
        .await?;
        tokio::time::sleep(std::time::Duration::from_millis(33)).await;
    }
    session.delete_stream(publish_stream)?;
    while let Some(output) = session.receive(&mut input)? {
        if let ClientOutput::Packet(packet) = output {
            transport::write_packet(&mut stream, packet).await?;
        }
    }
    stream.shutdown().await?;
    Ok(())
}
