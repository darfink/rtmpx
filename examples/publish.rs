//! Barebone RTMP publisher: the smallest complete client.
//!
//! Connects, publishes one stream, sends metadata plus a few seconds of
//! placeholder audio/video, then exits. Run a server first (the companion
//! listener below, or any RTMP server), then:
//!
//!     cargo run --example serve
//!     cargo run --example publish -- 127.0.0.1:1935 live demo
//!
//! The media bytes are placeholders, not real encoded frames. Drop real
//! AVC/AAC payloads (legacy) or FourCC-prefixed frames (Enhanced RTMP) into
//! the two marked call sites and the rest of the flow is unchanged. For the
//! shapes real encoders send, see the ffmpeg ingest harness in tests/ffmpeg
//! and the Enhanced round-trip test in tests/enhanced_loopback.rs.
use bytes::Bytes;
use rtmpx::handshake::{Handshake, HandshakeProcessResult, PeerType};
use rtmpx::sessions::{
    ClientSession, ClientSessionConfig, ClientSessionEvent, ClientSessionResult,
    PublishRequestType, StreamMetadata,
};
use rtmpx::time::RtmpTimestamp;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Placeholder media: a legacy AVC keyframe header and an AAC sequence
/// header. The session treats payloads as opaque bytes, so real encoded
/// frames flow through these same two calls unchanged.
const FAKE_VIDEO: &[u8] = &[0x17, 0x01, 0x00, 0x00, 0x00, 0x01, 0x02, 0x03];
const FAKE_AUDIO: &[u8] = &[0xaf, 0x00, 0x12, 0x10, 0x04, 0x60, 0x8c, 0x1c];

/// Send one session result to the wire, returning any raised event.
async fn send(
    stream: &mut TcpStream,
    result: ClientSessionResult,
) -> Result<Option<ClientSessionEvent>, Box<dyn std::error::Error>> {
    match result {
        ClientSessionResult::OutboundResponse(packet) => {
            stream.write_all(&packet.bytes).await?;
            stream.flush().await?;
            Ok(None)
        }
        ClientSessionResult::RaisedEvent(event) => Ok(Some(event)),
        ClientSessionResult::UnhandleableMessageReceived(_) => Ok(None),
    }
}

/// Read server bytes until an event arrives; reject means Err, anything
/// else means the step succeeded.
async fn wait_for_event(
    stream: &mut TcpStream,
    session: &mut ClientSession,
    buf: &mut [u8],
    what: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    loop {
        let n = stream.read(buf).await?;
        if n == 0 {
            return Err("server closed the connection".into());
        }
        for result in session.handle_input(&buf[..n])? {
            if let Some(event) = send(stream, result).await? {
                match event {
                    ClientSessionEvent::ConnectionRequestRejected { description } => {
                        return Err(format!("server rejected {what}: {description}").into());
                    }
                    _ => return Ok(()),
                }
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:1935".to_string());
    let app = std::env::args()
        .nth(2)
        .unwrap_or_else(|| "live".to_string());
    let stream_key = std::env::args()
        .nth(3)
        .unwrap_or_else(|| "demo".to_string());
    let mut buf = vec![0u8; 16 * 1024];

    let mut stream = TcpStream::connect(&addr).await?;
    stream.set_nodelay(true)?;

    // RTMP handshake as client.
    let mut handshake = Handshake::new(PeerType::Client);
    stream
        .write_all(&handshake.generate_outbound_p0_and_p1()?)
        .await?;
    stream.flush().await?;
    // Bytes arriving after the handshake already belong to the session.
    let pending: Vec<u8> = loop {
        let n = stream.read(&mut buf).await?;
        if n == 0 {
            return Err("server closed during handshake".into());
        }
        match handshake.process_bytes(&buf[..n])? {
            HandshakeProcessResult::InProgress { response_bytes } => {
                if !response_bytes.is_empty() {
                    stream.write_all(&response_bytes).await?;
                    stream.flush().await?;
                }
            }
            HandshakeProcessResult::Completed {
                response_bytes,
                remaining_bytes,
            } => {
                if !response_bytes.is_empty() {
                    stream.write_all(&response_bytes).await?;
                    stream.flush().await?;
                }
                break remaining_bytes;
            }
        }
    };

    let mut config = ClientSessionConfig::new();
    config.tc_url = Some(format!("rtmp://{addr}/{app}"));
    let (mut session, _) = ClientSession::new(config)?;
    if !pending.is_empty() {
        for result in session.handle_input(&pending)? {
            send(&mut stream, result).await?;
        }
    }

    // connect -> _result.
    let result = session.request_connection(app.clone())?;
    send(&mut stream, result).await?;
    wait_for_event(&mut stream, &mut session, &mut buf, "connect").await?;
    println!("connected to app '{app}'");

    // createStream -> publish -> NetStream.Publish.Start.
    let result = session.request_publishing(stream_key.clone(), PublishRequestType::Live)?;
    send(&mut stream, result).await?;
    wait_for_event(&mut stream, &mut session, &mut buf, "publish").await?;
    println!("publishing '{stream_key}'");

    // Stream description; players read this first.
    let mut metadata = StreamMetadata::new();
    metadata.video_width = Some(320);
    metadata.video_height = Some(240);
    metadata.encoder = Some("rtmpx publish example".to_string());
    send(&mut stream, session.publish_metadata(&metadata)?).await?;

    // A few seconds of placeholder frames: video at 25 fps with audio
    // alongside. Replace the payloads with real frames.
    for i in 0..150u32 {
        let result = session.publish_video_data(
            Bytes::from_static(FAKE_VIDEO),
            RtmpTimestamp::new(i * 40),
            false,
        )?;
        send(&mut stream, result).await?;
        if i % 2 == 0 {
            let result = session.publish_audio_data(
                Bytes::from_static(FAKE_AUDIO),
                RtmpTimestamp::new(i * 40),
                false,
            )?;
            send(&mut stream, result).await?;
        }
        tokio::time::sleep(std::time::Duration::from_millis(40)).await;
    }
    println!("sent 150 video / 75 audio frames, done");
    Ok(())
}
