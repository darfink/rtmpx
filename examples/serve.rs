//! Barebone RTMP server: the smallest complete listener.
//!
//! Accepts publishers (ffmpeg, OBS, or the companion publish example),
//! logs what arrives, and prints totals per connection:
//!
//!     cargo run --example serve -- 127.0.0.1:1935
//!
//! Then publish into it with ffmpeg:
//!
//!     ffmpeg -re -f lavfi -i testsrc2=s=320x240:r=30 -f lavfi -i sine \
//!       -c:v libx264 -preset ultrafast -tune zerolatency -c:a aac -f flv \
//!       rtmp://127.0.0.1:1935/live/demo
//!
//! or OBS (Service Custom, server rtmp://127.0.0.1:1935/live, key demo),
//! or the companion example above. Incoming payloads are counted and logged,
//! not recorded: wiring them into an FLV muxer or fanning them out with
//! send_video_data / send_audio_data is the natural next step (the relay
//! test in tests/ffmpeg shows the fan-out side).
use rtmpx::handshake::{Handshake, HandshakeProcessResult, PeerType};
use rtmpx::sessions::{
    ServerSession, ServerSessionConfig, ServerSessionEvent, ServerSessionResult,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

struct Stats {
    metadata: usize,
    audio_frames: usize,
    video_frames: usize,
    audio_bytes: u64,
    video_bytes: u64,
}

async fn serve_one(stream: &mut TcpStream) -> Result<(), Box<dyn std::error::Error>> {
    let mut buf = vec![0u8; 16 * 1024];

    // RTMP handshake as server.
    let mut handshake = Handshake::new(PeerType::Server);
    let pending: Vec<u8> = loop {
        let n = stream.read(&mut buf).await?;
        if n == 0 {
            return Err("peer closed during handshake".into());
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

    let (mut session, initial) = ServerSession::new(ServerSessionConfig::new())?;
    let mut stats = Stats {
        metadata: 0,
        audio_frames: 0,
        video_frames: 0,
        audio_bytes: 0,
        video_bytes: 0,
    };
    // Flush the session's opening messages, then feed post-handshake bytes.
    let mut queued = initial;
    if !pending.is_empty() {
        queued.extend(session.handle_input(&pending)?);
    }
    handle_results(stream, &mut session, &mut stats, queued).await?;

    loop {
        let n = stream.read(&mut buf).await?;
        if n == 0 {
            println!(
                "publisher left: {} metadata, {} audio frames ({} bytes), {} video frames ({} bytes)",
                stats.metadata,
                stats.audio_frames,
                stats.audio_bytes,
                stats.video_frames,
                stats.video_bytes,
            );
            return Ok(());
        }
        let results = session.handle_input(&buf[..n])?;
        // accept_request produces follow-up results that must hit the wire
        // too; drain in order (remove(0) is fine for these tiny vectors).
        let mut drain = results;
        while !drain.is_empty() {
            let result = drain.remove(0);
            let mut follow = Vec::new();
            handle_one(stream, &mut session, &mut stats, result, &mut follow).await?;
            drain.extend(follow);
        }
    }
}

async fn handle_results(
    stream: &mut TcpStream,
    session: &mut ServerSession,
    stats: &mut Stats,
    results: Vec<ServerSessionResult>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut drain = results;
    while !drain.is_empty() {
        let result = drain.remove(0);
        let mut follow = Vec::new();
        handle_one(stream, session, stats, result, &mut follow).await?;
        drain.extend(follow);
    }
    Ok(())
}

async fn handle_one(
    stream: &mut TcpStream,
    session: &mut ServerSession,
    stats: &mut Stats,
    result: ServerSessionResult,
    follow: &mut Vec<ServerSessionResult>,
) -> Result<(), Box<dyn std::error::Error>> {
    match result {
        ServerSessionResult::OutboundResponse(packet) => {
            stream.write_all(&packet.bytes).await?;
            stream.flush().await?;
        }
        ServerSessionResult::RaisedEvent(event) => match event {
            ServerSessionEvent::ConnectionRequested {
                request_id,
                app_name,
                ..
            } => {
                println!("connect: app '{app_name}'");
                follow.extend(session.accept_request(request_id)?);
            }
            ServerSessionEvent::PublishStreamRequested {
                request_id,
                stream_key,
                ..
            } => {
                println!("publish: '{stream_key}'");
                follow.extend(session.accept_request(request_id)?);
            }
            ServerSessionEvent::StreamMetadataChanged { metadata, .. } => {
                stats.metadata += 1;
                println!(
                    "metadata: {}x{} encoder={:?}",
                    metadata.video_width.unwrap_or(0),
                    metadata.video_height.unwrap_or(0),
                    metadata.encoder,
                );
            }
            ServerSessionEvent::AudioDataReceived {
                data, timestamp, ..
            } => {
                stats.audio_frames += 1;
                stats.audio_bytes += data.len() as u64;
                if stats.audio_frames == 1 {
                    println!("first audio: {} bytes @ {}", data.len(), timestamp.value);
                }
            }
            ServerSessionEvent::VideoDataReceived {
                data, timestamp, ..
            } => {
                stats.video_frames += 1;
                stats.video_bytes += data.len() as u64;
                if stats.video_frames == 1 {
                    println!("first video: {} bytes @ {}", data.len(), timestamp.value);
                }
            }
            other => {
                // ReleaseStream, FCUnpublish teardowns, play requests
                // (playback fan-out is out of scope here), pings.
                println!("other: {other:?}");
            }
        },
        ServerSessionResult::UnhandleableMessageReceived(_) => {}
        _ => return Err("unsupported protocol result; update the adapter".into()),
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:1935".to_string());
    let listener = TcpListener::bind(&addr).await?;
    println!("listening on {addr} (Ctrl-C to stop)");
    loop {
        let (mut stream, peer) = listener.accept().await?;
        println!("connection from {peer}");
        tokio::spawn(async move {
            if let Err(e) = serve_one(&mut stream).await {
                eprintln!("connection from {peer} ended: {e}");
            }
        });
    }
}
