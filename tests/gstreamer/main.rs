//! Live interop suite: our `ServerSession` against real GStreamer as publisher.
//!
//! Optional by construction: this target only builds with
//! `cargo test --test gstreamer --features gstreamer-live`
//! (see `[[test]]` required-features in Cargo.toml). Tests skip loudly when no
//! `gst-launch-1.0` is on PATH, and need no Red5 server. Default `cargo test`
//! never touches the network or external binaries.
//!
//! Why a second encoder leg when ffmpeg already publishes into our server?
//! Different stack, different quirks: GStreamer's flvmux re-sends `onMetaData`
//! mid-stream, its `rtmpsink` has its own connect/chunking shape, and it tears
//! down via `FCUnpublish` - the same tolerate-don't-choke paths OBS exercises.
//! A green run proves our server accepts an independent encoder, not just
//! ffmpeg's exact byte patterns.

#[path = "../support/api.rs"]
mod api;
#[path = "../common/mod.rs"]
#[allow(dead_code)]
mod common;

use std::process::{Child, Command, Stdio};
use std::time::Duration;

use crate::api::amf::AmfEncoding;
use crate::api::handshake::{Handshake, HandshakeProgress, HandshakeRole};
use crate::api::sessions::{
    ServerSession, ServerSessionConfig, ServerSessionEvent, ServerSessionResult,
};
use common::{Result, run, stream_key};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;

pub struct IngestedMedia {
    pub app: String,
    pub stream_key: String,
    pub metadata_events: usize,
    /// `encoder` strings from every `onMetaData` seen. GStreamer's flvmux
    /// re-sends metadata mid-stream where ffmpeg sends it once, so this both
    /// fingerprints the peer and pins our tolerance for repeats.
    pub encoders: Vec<String>,
    pub audio: Vec<Vec<u8>>,
    pub video: Vec<Vec<u8>>,
    pub negotiated: AmfEncoding,
}

pub fn gst_available() -> bool {
    // Blocking probe, but it runs once per test process and finishes in
    // milliseconds; keeps the harness off tokio's `process` feature.
    Command::new("gst-launch-1.0")
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

async fn write_server_results(
    stream: &mut TcpStream,
    results: Vec<ServerSessionResult>,
    session: &mut ServerSession,
    collected: &mut IngestedMedia,
) -> Result<()> {
    for result in results {
        match result {
            ServerSessionResult::Packet(packet) => {
                stream
                    .write_all(&packet.to_vec())
                    .await
                    .map_err(|e| format!("write to gstreamer failed: {e}"))?;
            }
            ServerSessionResult::Event(event) => match event {
                ServerSessionEvent::ConnectionRequested {
                    request_id,
                    app_name,
                    ..
                } => {
                    collected.app = app_name.to_string();
                    let follow = session
                        .accept_request(request_id)
                        .map_err(|e| format!("accepting gstreamer connect failed: {e:?}"))?;
                    Box::pin(write_server_results(stream, follow, session, collected)).await?;
                }
                ServerSessionEvent::PublishStreamRequested {
                    request_id,
                    stream_key,
                    ..
                } => {
                    collected.stream_key = stream_key.to_string();
                    let follow = session
                        .accept_request(request_id)
                        .map_err(|e| format!("accepting gstreamer publish failed: {e:?}"))?;
                    Box::pin(write_server_results(stream, follow, session, collected)).await?;
                }
                ServerSessionEvent::StreamDataReceived { message, .. } => {
                    let metadata = crate::api::sessions::metadata(&message);
                    collected.metadata_events += 1;
                    collected
                        .encoders
                        .push(metadata.encoder.clone().unwrap_or_default());
                }
                ServerSessionEvent::AudioDataReceived { data, .. } => {
                    collected.audio.push(data.to_vec());
                }
                ServerSessionEvent::VideoDataReceived { data, .. } => {
                    collected.video.push(data.to_vec());
                }
                ServerSessionEvent::ReleaseStreamRequested { .. } => {
                    // Informational only: no outstanding request to accept.
                }
                other => {
                    // `FCUnpublish` on clean EOS lands here: tolerated, like OBS.
                    eprintln!("gstreamer harness: ignoring gstreamer event: {other:?}");
                }
            },
            ServerSessionResult::UnhandledMessage(_) => {}
            #[allow(unreachable_patterns)]
            _ => panic!("unexpected future protocol variant"),
        }
    }
    stream
        .flush()
        .await
        .map_err(|e| format!("flush to gstreamer failed: {e}"))?;
    Ok(())
}

async fn server_handshake(stream: &mut TcpStream, read_buf: &mut [u8]) -> Result<Vec<u8>> {
    let mut handshake = Handshake::new(HandshakeRole::Server);
    loop {
        let n = stream
            .read(read_buf)
            .await
            .map_err(|e| format!("server handshake read failed: {e}"))?;
        if n == 0 {
            return Err("gstreamer closed the connection during handshake".to_string());
        }
        match handshake
            .process_bytes(&read_buf[..n])
            .map_err(|e| format!("server handshake failed: {e:?}"))?
        {
            HandshakeProgress::InProgress { response_bytes } => {
                if !response_bytes.is_empty() {
                    stream
                        .write_all(&response_bytes)
                        .await
                        .map_err(|e| format!("server handshake write failed: {e}"))?;
                    stream
                        .flush()
                        .await
                        .map_err(|e| format!("server handshake flush failed: {e}"))?;
                }
            }
            HandshakeProgress::Completed {
                response_bytes,
                remaining_bytes,
            } => {
                if !response_bytes.is_empty() {
                    stream
                        .write_all(&response_bytes)
                        .await
                        .map_err(|e| format!("server handshake final write failed: {e}"))?;
                    stream
                        .flush()
                        .await
                        .map_err(|e| format!("server handshake final flush failed: {e}"))?;
                }
                return Ok(remaining_bytes);
            }
            #[allow(unreachable_patterns)]
            _ => panic!("unexpected future protocol variant"),
        }
    }
}

/// Bind a one-shot RTMP server on loopback, have `gst-launch-1.0` publish
/// synthetic audio+video to it via `rtmpsink`, and return what our
/// `ServerSession` observed.
pub async fn ingest_from_gst(stream_key: &str, wall_clock: Duration) -> Result<IngestedMedia> {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|e| format!("loopback bind failed: {e}"))?;
    let port = listener
        .local_addr()
        .map_err(|e| format!("local_addr failed: {e}"))?
        .port();
    let url = format!("rtmp://127.0.0.1:{port}/live/{stream_key}");

    let mut child: Child = Command::new("gst-launch-1.0")
        .args([
            "-e",
            "videotestsrc",
            "num-buffers=100",
            "!",
            "video/x-raw,width=128,height=96,framerate=10/1",
            "!",
            "x264enc",
            "tune=zerolatency",
            "bitrate=200",
            "speed-preset=ultrafast",
            "!",
            "h264parse",
            "!",
            "flvmux",
            "name=mux",
            "!",
            "rtmpsink",
            &format!("location={url}"),
            "audiotestsrc",
            "num-buffers=80",
            "wave=sine",
            "freq=440",
            "!",
            "audioconvert",
            "!",
            "audioresample",
            "!",
            "audio/x-raw,rate=44100,channels=2",
            "!",
            "voaacenc",
            "bitrate=64000",
            "!",
            "mux.",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| format!("spawning gst-launch-1.0 failed: {e}"))?;

    let outcome: Result<IngestedMedia> = timeout(wall_clock, async {
        let (mut stream, _) = listener
            .accept()
            .await
            .map_err(|e| format!("accept from gstreamer failed: {e}"))?;
        stream
            .set_nodelay(true)
            .map_err(|e| format!("set_nodelay failed: {e}"))?;
        let mut read_buf = vec![0u8; 16 * 1024];
        let carry = server_handshake(&mut stream, &mut read_buf).await?;

        let (mut session, initial) = ServerSession::new(ServerSessionConfig::new())
            .map_err(|e| format!("server session init failed: {e:?}"))?;
        debug_assert!(initial.is_empty(), "server must not write before connect");

        let mut collected = IngestedMedia {
            app: String::new(),
            stream_key: String::new(),
            metadata_events: 0,
            encoders: Vec::new(),
            audio: Vec::new(),
            video: Vec::new(),
            negotiated: AmfEncoding::Amf0,
        };
        if !carry.is_empty() {
            let results = session
                .handle_input(&carry)
                .map_err(|e| format!("gstreamer sent unreadable RTMP: {e:?}"))?;
            write_server_results(&mut stream, results, &mut session, &mut collected).await?;
        }
        loop {
            if collected.metadata_events >= 1
                && collected.audio.len() >= 2
                && collected.video.len() >= 5
            {
                collected.negotiated = session.negotiated_encoding();
                return Ok::<IngestedMedia, String>(collected);
            }
            let n = stream
                .read(&mut read_buf)
                .await
                .map_err(|e| format!("read from gstreamer failed: {e}"))?;
            if n == 0 {
                return Err(
                    "gstreamer went away before sending metadata + audio + video".to_string(),
                );
            }
            let input = read_buf[..n].to_vec();
            let results = session
                .handle_input(&input)
                .map_err(|e| format!("gstreamer sent unreadable RTMP: {e:?}"))?;
            write_server_results(&mut stream, results, &mut session, &mut collected).await?;
        }
    })
    .await
    .map_err(|_| format!("timed out after {wall_clock:?} waiting for gstreamer ingest to {url}"))?;

    let stderr = child
        .stderr
        .take()
        .map(|mut pipe| {
            use std::io::Read;
            let mut out = String::new();
            let _ = pipe.read_to_string(&mut out);
            out
        })
        .unwrap_or_default();
    let _ = child.kill();
    let _ = child.wait();
    match &outcome {
        Ok(_) => outcome,
        Err(e) => Err(format!("{e}\ngst-launch-1.0 stderr:\n{stderr}")),
    }
}

async fn gstreamer_ingest_body() -> Result<()> {
    if !gst_available() {
        eprintln!(
            "gstreamer harness: SKIP gstreamer_publishes_to_our_server (no gst-launch-1.0 on PATH)"
        );
        return Ok(());
    }
    let key = stream_key("gstreamer");
    let got = ingest_from_gst(&key, Duration::from_secs(60)).await?;
    assert_eq!(got.app, "live", "gstreamer must land on the live app");
    assert_eq!(got.stream_key, key, "stream key must survive ingest");
    assert!(
        got.metadata_events >= 1,
        "must observe gstreamer onMetaData (saw {})",
        got.metadata_events
    );
    assert!(
        got.encoders.iter().any(|e| e.contains("GStreamer")),
        "onMetaData encoder must fingerprint GStreamer, saw {:?}",
        got.encoders
    );
    assert!(
        got.audio.len() >= 2,
        "must observe gstreamer AAC (saw {})",
        got.audio.len()
    );
    assert!(
        got.video.len() >= 5,
        "must observe gstreamer AVC (saw {})",
        got.video.len()
    );
    assert!(
        !got.audio.iter().any(Vec::is_empty) && !got.video.iter().any(Vec::is_empty),
        "media payloads must be non-empty"
    );
    assert_eq!(
        got.negotiated,
        AmfEncoding::Amf0,
        "gstreamer negotiates AMF0"
    );
    // First video packet of an H.264 RTMP stream is the AVC sequence header.
    assert_eq!(got.video[0][0], 0x17, "first video must be keyframe AVC");
    assert_eq!(
        got.video[0][1], 0x00,
        "first video must be a sequence header"
    );
    assert_eq!(
        got.audio[0][0] & 0xF0,
        0xA0,
        "first audio must be AAC ({:02x})",
        got.audio[0][0]
    );
    eprintln!(
        "gstreamer harness: {} onMetaData / {} audio / {} video, encoders={:?}",
        got.metadata_events,
        got.audio.len(),
        got.video.len(),
        got.encoders.iter().take(2).collect::<Vec<_>>()
    );
    Ok(())
}

#[tokio::test]
async fn gstreamer_publishes_to_our_server() {
    run(gstreamer_ingest_body()).await;
}
