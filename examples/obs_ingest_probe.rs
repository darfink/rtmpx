//! Manual OBS ingest probe (not run in CI).
//!
//! Binds a loopback RTMP server backed by our `ServerSession` and prints
//! exactly what to put in OBS, then logs what OBS actually sends:
//! connect properties, `releaseStream`/`FCPublish` quirks, metadata shape,
//! and audio/video flow. Use it to validate a new OBS version by hand:
//!
//! ```sh
//! cargo run --example obs_ingest_probe
//! # OBS -> Settings -> Stream -> Service: Custom
//! #   Server: rtmp://127.0.0.1:19350/live
//! #   Stream Key: obs-probe
//! # Press "Start Streaming", watch this terminal, press Ctrl-C to stop.
//! ```
//!
//! Environment: `OBS_PROBE_PORT` (default 19350), `OBS_PROBE_SECS`
//! (default 120), `OBS_PROBE_APP` (default live).
//! The default-suite `tests/obs_ingest.rs` pins the same sequence in CI; this
//! probe is for eyeballing real OBS builds (flashVer, metadata fields,
//! chunking, reconnects) that no fake can reproduce.

use std::time::{Duration, Instant};

#[path = "support/io.rs"]
mod transport;
use bytes::Bytes;
use rtmpx::handshake::{Handshake, HandshakeProgress, HandshakeRole};
use rtmpx::sessions::{ServerEvent, ServerOutput, ServerSession, ServerSessionConfig};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

type ProbeResult<T> = std::result::Result<T, Box<dyn std::error::Error>>;

async fn write_results(
    stream: &mut tokio::net::TcpStream,
    mut input: Bytes,
    session: &mut ServerSession,
) -> ProbeResult<(usize, usize, usize)> {
    let mut meta = 0;
    let mut audio = 0;
    let mut video = 0;
    while let Some(result) = session.receive(&mut input)? {
        match result {
            ServerOutput::Packet(packet) => {
                transport::write_packet(stream, packet).await?;
            }
            ServerOutput::Event(event) => match event {
                ServerEvent::ConnectionRequested {
                    app_name,
                    request_id,
                    additional_properties,
                    ..
                } => {
                    println!("[connect] app={app_name} request={request_id}");
                    for (k, v) in additional_properties.iter() {
                        println!("          prop {k} = {v:?}");
                    }
                    session.accept_request(request_id)?;
                }
                ServerEvent::PublishStreamRequested {
                    stream_key,
                    request_id,
                    mode,
                    stream_id,
                    ..
                } => {
                    println!("[publish] key={stream_key} mode={mode:?} stream={stream_id}");
                    session.accept_request(request_id)?;
                }
                ServerEvent::StreamDataReceived {
                    message,
                    stream_key,
                    ..
                } => {
                    let Some(properties) = message.metadata()? else {
                        continue;
                    };
                    let mut metadata = rtmpx::sessions::StreamMetadata::new();
                    metadata.apply_metadata_values(properties);
                    let is_amf3 = message.wire_type() == rtmpx::sessions::DataMessageType::Amf3;
                    meta += 1;
                    println!(
                        "[metadata #{meta}] key={stream_key} amf3={is_amf3} width={:?} height={:?} vcodec={:?} acodec={:?} encoder={:?}",
                        metadata.video_width,
                        metadata.video_height,
                        metadata.video_codec_id,
                        metadata.audio_codec_id,
                        metadata.encoder
                    );
                }
                ServerEvent::AudioDataReceived {
                    data, timestamp, ..
                } => {
                    audio += 1;
                    if audio <= 3 {
                        println!(
                            "[audio #{audio}] {} bytes ts={} head={:02x?}",
                            data.len(),
                            timestamp.value,
                            &data.to_bytes()[..data.len().min(4)]
                        );
                    }
                }
                ServerEvent::VideoDataReceived {
                    data, timestamp, ..
                } => {
                    video += 1;
                    if video <= 3 {
                        println!(
                            "[video #{video}] {} bytes ts={} head={:02x?}",
                            data.len(),
                            timestamp.value,
                            &data.to_bytes()[..data.len().min(6)]
                        );
                    }
                }
                ServerEvent::UnhandledCommand { command_name, .. } => {
                    println!("[quirk] {command_name} (tolerated, no server semantics)");
                }
                other => println!("[event] {other:?}"),
            },
            ServerOutput::UnhandledMessage(_) => {}
            _ => return Err("unsupported protocol result; update the adapter".into()),
        }
    }
    stream.flush().await?;
    Ok((meta, audio, video))
}

#[tokio::main]
async fn main() -> ProbeResult<()> {
    let port: u16 = std::env::var("OBS_PROBE_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(19350);
    let secs: u64 = std::env::var("OBS_PROBE_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(120);
    let app = std::env::var("OBS_PROBE_APP").unwrap_or_else(|_| "live".to_string());
    let listener = TcpListener::bind(("127.0.0.1", port)).await?;
    println!("rtmpx OBS probe listening on rtmp://127.0.0.1:{port}/{app}");
    println!("OBS -> Settings -> Stream -> Service: Custom");
    println!("  Server:     rtmp://127.0.0.1:{port}/{app}");
    println!("  Stream Key: obs-probe");
    println!("Press Start Streaming in OBS (AMF0-only; exercises quirk tolerance, not AMF3).");
    println!("Waiting up to {secs}s for a publisher... (Ctrl-C to stop)");

    let deadline = Instant::now() + Duration::from_secs(secs);
    let accept = tokio::time::timeout(
        deadline.saturating_duration_since(Instant::now()),
        listener.accept(),
    )
    .await
    .map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            format!("no publisher arrived within {secs}s"),
        )
    })?;
    let (mut stream, peer) = accept?;
    println!("publisher connected from {peer}");
    stream.set_nodelay(true)?;

    // RTMP handshake as server.
    let mut handshake = Handshake::new(HandshakeRole::Server);
    let mut buf = vec![0u8; 16 * 1024];
    let carry = loop {
        let n = stream.read(&mut buf).await?;
        if n == 0 {
            return Err("publisher went away during handshake".into());
        }
        match handshake.process_bytes(&buf[..n])? {
            HandshakeProgress::InProgress { response_bytes } => {
                if !response_bytes.is_empty() {
                    stream.write_all(&response_bytes).await?;
                    stream.flush().await?;
                }
            }
            HandshakeProgress::Completed {
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

    let mut session = ServerSession::new(ServerSessionConfig::new())?;
    let (mut total_meta, mut total_audio, mut total_video) = (0, 0, 0);
    if !carry.is_empty() {
        let results = Bytes::from(carry);
        let (m, a, v) = write_results(&mut stream, results, &mut session).await?;
        total_meta += m;
        total_audio += a;
        total_video += v;
    }
    while Instant::now() < deadline {
        match tokio::time::timeout(
            deadline.saturating_duration_since(Instant::now()),
            stream.read(&mut buf),
        )
        .await
        {
            Ok(Ok(0)) => {
                println!("publisher closed the connection");
                break;
            }
            Ok(Ok(n)) => {
                let results = Bytes::copy_from_slice(&buf[..n]);
                let (m, a, v) = write_results(&mut stream, results, &mut session).await?;
                total_meta += m;
                total_audio += a;
                total_video += v;
                if total_meta + total_audio + total_video > 0 && total_video % 100 == 0 {
                    println!(
                        "... totals: {total_meta} metadata / {total_audio} audio / {total_video} video"
                    );
                }
            }
            Ok(Err(e)) => {
                println!("read failed: {e}");
                break;
            }
            Err(_) => {
                println!("probe window elapsed");
                break;
            }
        }
    }
    println!(
        "OBS probe done: {total_meta} metadata / {total_audio} audio / {total_video} video packets observed."
    );
    println!(
        "Compare against tests/obs_ingest.rs: releaseStream/FCPublish tolerated, onMetaData parsed, A/V byte-exact."
    );
    Ok(())
}
