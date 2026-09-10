//! Independent-client leg: real ffmpeg publishes into OUR ServerSession.
//!
//! Red5 covers our CLIENT against an independent server; this covers our
//! SERVER against an independent encoder. ffmpeg sends plain AMF0 legacy
//! AVC/AAC, so a green run proves connect -> releaseStream/FCPublish ->
//! createStream -> publish -> metadata/audio/video ingest against a
//! third-party peer. Skips (loudly) when no ffmpeg binary is on PATH.

use std::time::Duration;

use std::process::{Child, Command, Stdio};

use rtmpx::amf::AmfEncoding;
use rtmpx::handshake::{Handshake, HandshakeProcessResult, PeerType};
use rtmpx::sessions::{
    ServerSession, ServerSessionConfig, ServerSessionEvent, ServerSessionResult,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;

use super::common::Result;

pub struct IngestedMedia {
    pub app: String,
    pub stream_key: String,
    pub metadata_events: usize,
    pub audio: Vec<Vec<u8>>,
    pub video: Vec<Vec<u8>>,
    pub negotiated: AmfEncoding,
}

pub async fn ffmpeg_available() -> bool {
    // Blocking probe, but it runs once per test process and finishes in
    // milliseconds; keeps the harness off tokio's `process` feature.
    Command::new("ffmpeg")
        .arg("-version")
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
            ServerSessionResult::OutboundResponse(packet) => {
                stream
                    .write_all(&packet.bytes)
                    .await
                    .map_err(|e| format!("write to ffmpeg failed: {e}"))?;
            }
            ServerSessionResult::RaisedEvent(event) => match event {
                ServerSessionEvent::ConnectionRequested {
                    request_id,
                    app_name,
                    ..
                } => {
                    collected.app = app_name.to_string();
                    let follow = session
                        .accept_request(request_id)
                        .map_err(|e| format!("accepting ffmpeg connect failed: {e:?}"))?;
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
                        .map_err(|e| format!("accepting ffmpeg publish failed: {e:?}"))?;
                    Box::pin(write_server_results(stream, follow, session, collected)).await?;
                }
                ServerSessionEvent::StreamMetadataChanged { .. } => {
                    collected.metadata_events += 1;
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
                    eprintln!("ffmpeg harness: ignoring ffmpeg event: {other:?}");
                }
            },
            ServerSessionResult::UnhandleableMessageReceived(_) => {}
            #[allow(unreachable_patterns)]
            _ => panic!("unexpected future protocol variant"),
        }
    }
    stream
        .flush()
        .await
        .map_err(|e| format!("flush to ffmpeg failed: {e}"))?;
    Ok(())
}

async fn server_handshake(stream: &mut TcpStream, read_buf: &mut [u8]) -> Result<Vec<u8>> {
    let mut handshake = Handshake::new(PeerType::Server);
    loop {
        let n = stream
            .read(read_buf)
            .await
            .map_err(|e| format!("server handshake read failed: {e}"))?;
        if n == 0 {
            return Err("ffmpeg closed the connection during handshake".to_string());
        }
        match handshake
            .process_bytes(&read_buf[..n])
            .map_err(|e| format!("server handshake failed: {e:?}"))?
        {
            HandshakeProcessResult::InProgress { response_bytes } => {
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
            HandshakeProcessResult::Completed {
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

/// Bind a one-shot RTMP server on loopback, have ffmpeg publish synthetic
/// audio+video to it, and return what our ServerSession observed.
pub async fn ingest_from_ffmpeg(stream_key: &str, wall_clock: Duration) -> Result<IngestedMedia> {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|e| format!("loopback bind failed: {e}"))?;
    let port = listener
        .local_addr()
        .map_err(|e| format!("local_addr failed: {e}"))?
        .port();
    let url = format!("rtmp://127.0.0.1:{port}/live/{stream_key}");

    let mut child: Child = Command::new("ffmpeg")
        .args([
            "-hide_banner",
            "-loglevel",
            "warning",
            "-re",
            "-f",
            "lavfi",
            "-i",
            "testsrc2=size=128x96:rate=10:duration=8",
            "-f",
            "lavfi",
            "-i",
            "sine=frequency=440:duration=8",
            "-c:v",
            "libx264",
            "-preset",
            "ultrafast",
            "-tune",
            "zerolatency",
            "-pix_fmt",
            "yuv420p",
            "-g",
            "10",
            "-c:a",
            "aac",
            "-ar",
            "44100",
            "-ac",
            "2",
            "-f",
            "flv",
            &url,
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| format!("spawning ffmpeg failed: {e}"))?;

    let outcome: Result<IngestedMedia> = timeout(wall_clock, async {
        let (mut stream, _) = listener
            .accept()
            .await
            .map_err(|e| format!("accept from ffmpeg failed: {e}"))?;
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
            audio: Vec::new(),
            video: Vec::new(),
            negotiated: AmfEncoding::Amf0,
        };
        if !carry.is_empty() {
            let results = session
                .handle_input(&carry)
                .map_err(|e| format!("ffmpeg sent unreadable RTMP: {e:?}"))?;
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
                .map_err(|e| format!("read from ffmpeg failed: {e}"))?;
            if n == 0 {
                return Err("ffmpeg went away before sending metadata + audio + video".to_string());
            }
            let input = read_buf[..n].to_vec();
            let results = session
                .handle_input(&input)
                .map_err(|e| format!("ffmpeg sent unreadable RTMP: {e:?}"))?;
            write_server_results(&mut stream, results, &mut session, &mut collected).await?;
        }
    })
    .await
    .map_err(|_| format!("timed out after {wall_clock:?} waiting for ffmpeg ingest to {url}"))?;

    let _ = child.kill();
    let _ = child.wait();
    outcome
}

/// True when `ffprobe` is on PATH (ships with ffmpeg, but checked separately
/// so the player leg can skip loudly instead of failing obscurely).
pub async fn ffprobe_available() -> bool {
    Command::new("ffprobe")
        .arg("-version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

/// Independent-encoder Enhanced leg: real ffmpeg publishes HEVC into OUR
/// `ServerSession`. Skips loudly when no ffmpeg is present; fails with
/// ffmpeg's stderr when ffmpeg runs but no Enhanced video arrives (usually
/// an ffmpeg without Enhanced RTMP support — bump the runner's ffmpeg
/// instead of weakening the assert).
pub async fn ingest_enhanced_from_ffmpeg(
    stream_key: &str,
    wall_clock: Duration,
) -> Result<IngestedMedia> {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|e| format!("loopback bind failed: {e}"))?;
    let port = listener
        .local_addr()
        .map_err(|e| format!("local_addr failed: {e}"))?
        .port();
    let url = format!("rtmp://127.0.0.1:{port}/live/{stream_key}");

    let mut child: Child = Command::new("ffmpeg")
        .args([
            "-hide_banner",
            "-loglevel",
            "warning",
            "-re",
            "-f",
            "lavfi",
            "-i",
            "testsrc2=size=128x96:rate=10:duration=8",
            "-f",
            "lavfi",
            "-i",
            "sine=frequency=440:duration=8",
            "-c:v",
            "libx265",
            "-preset",
            "ultrafast",
            "-tune",
            "zerolatency",
            "-pix_fmt",
            "yuv420p",
            "-g",
            "10",
            "-c:a",
            "aac",
            "-ar",
            "44100",
            "-ac",
            "2",
            "-f",
            "flv",
            &url,
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| format!("spawning ffmpeg (hevc) failed: {e}"))?;

    let outcome: Result<IngestedMedia> = timeout(wall_clock, async {
        let (mut stream, _) = listener.accept().await.map_err(|e| format!("accept from ffmpeg failed: {e}"))?;
        stream.set_nodelay(true).map_err(|e| format!("set_nodelay failed: {e}"))?;
        let mut read_buf = vec![0u8; 16 * 1024];
        let carry = server_handshake(&mut stream, &mut read_buf).await?;
        let (mut session, initial) = ServerSession::new(ServerSessionConfig::new())
            .map_err(|e| format!("server session init failed: {e:?}"))?;
        debug_assert!(initial.is_empty(), "server must not write before connect");
        let mut collected = IngestedMedia {
            app: String::new(),
            stream_key: String::new(),
            metadata_events: 0,
            audio: Vec::new(),
            video: Vec::new(),
            negotiated: AmfEncoding::Amf0,
        };
        if !carry.is_empty() {
            let results = session.handle_input(&carry).map_err(|e| format!("ffmpeg sent unreadable RTMP: {e:?}"))?;
            write_server_results(&mut stream, results, &mut session, &mut collected).await?;
        }
        loop {
            let enhanced_seen = collected.video.iter().any(|v| {
                v.len() >= 5 && v[1..5].iter().all(|b| b.is_ascii_alphanumeric()) && v[0] != 0x17 && v[0] != 0x27
            });
            if collected.video.len() >= 2 && enhanced_seen {
                collected.negotiated = session.negotiated_encoding();
                return Ok::<IngestedMedia, String>(collected);
            }
            if collected.video.len() >= 10 {
                return Err(format!("ffmpeg sent {} video packets but none looked Enhanced (first bytes: {:02x?}); is this ffmpeg too old for Enhanced RTMP?", collected.video.len(), collected.video.first().map(|v| v[..v.len().min(6)].to_vec())));
            }
            let n = stream.read(&mut read_buf).await.map_err(|e| format!("read from ffmpeg failed: {e}"))?;
            if n == 0 {
                return Err("ffmpeg went away before sending Enhanced video".to_string());
            }
            let input = read_buf[..n].to_vec();
            let results = session.handle_input(&input).map_err(|e| format!("ffmpeg sent unreadable RTMP: {e:?}"))?;
            write_server_results(&mut stream, results, &mut session, &mut collected).await?;
        }
    })
    .await
    .map_err(|_| format!("timed out after {wall_clock:?} waiting for ffmpeg Enhanced ingest to {url}"))?;

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
        Err(e) => Err(format!("{e}\nffmpeg stderr:\n{stderr}")),
    }
}

/// Third-party player leg: OUR client publishes legacy media into OUR
/// server, OUR server relays it to real ffmpeg as a player, ffprobe verifies
/// the recording. This closes the egress gap the Red5 legs cannot cover:
/// Red5 proves our client against an independent server; the ffmpeg ingest
/// leg proves our server against an independent publisher; only this proves
/// our server can feed an independent player.
///
/// Shape: loopback TCP server with two sequential connections — first our
/// ClientSession publishes (via the shared client driver in tests/common,
/// the same driver the Red5 suite uses), then ffmpeg plays
/// (-c copy to a temp FLV). Media is the exact
/// bytes the server ingested, re-stamped monotonically, so the assertion is
/// end-to-end: publisher bytes -> server -> ffmpeg file -> ffprobe streams.
pub struct PlayedFile {
    /// Temp FLV ffmpeg wrote. Deleted by the test after probing.
    pub path: std::path::PathBuf,
    /// Raw `ffprobe -show_streams` output for diagnostics.
    pub probe: String,
}

static FILE_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

fn temp_flv_path(tag: &str) -> std::path::PathBuf {
    let n = FILE_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    std::env::temp_dir().join(format!("rtmpx-{tag}-{}-{n}.flv", std::process::id()))
}

pub async fn ffprobe_streams(path: &std::path::Path) -> Result<String> {
    let out = Command::new("ffprobe")
        .args([
            "-v",
            "error",
            "-show_entries",
            "stream=codec_type,codec_name",
            "-of",
            "default=noprint_wrappers=1",
            &path.to_string_lossy(),
        ])
        .output()
        .map_err(|e| format!("spawning ffprobe failed: {e}"))?;
    if !out.status.success() {
        return Err(format!(
            "ffprobe failed on {}: {}",
            path.display(),
            String::from_utf8_lossy(&out.stderr)
        ));
    }
    Ok(String::from_utf8_lossy(&out.stdout).to_string())
}

/// Generate a tiny valid FLV with real H.264 + AAC using ffmpeg's own
/// encoders, so the relay player leg feeds ffmpeg decodable bytes. The
/// Red5/default suites use framing-correct fake bytes for byte-exactness;
/// ffmpeg as a *player* needs valid codec headers to write its output file,
/// so this leg extracts real payloads from a generated file and has OUR
/// client publish those (still our bytes on the wire, just valid ones).
pub async fn generate_valid_flv(path: &std::path::Path, duration_secs: u32) -> Result<()> {
    let out = Command::new("ffmpeg")
        .args([
            "-hide_banner",
            "-loglevel",
            "warning",
            "-y",
            "-f",
            "lavfi",
            "-i",
            "testsrc2=size=128x96:rate=10:duration=8",
            "-f",
            "lavfi",
            "-i",
            "sine=frequency=440:duration=8",
            "-c:v",
            "libx264",
            "-preset",
            "ultrafast",
            "-tune",
            "zerolatency",
            "-pix_fmt",
            "yuv420p",
            "-g",
            "10",
            "-c:a",
            "aac",
            "-ar",
            "44100",
            "-ac",
            "2",
            "-t",
            &duration_secs.to_string(),
            "-f",
            "flv",
            &path.to_string_lossy(),
        ])
        .output()
        .map_err(|e| format!("spawning ffmpeg (flv gen) failed: {e}"))?;
    if !out.status.success() {
        return Err(format!(
            "ffmpeg flv gen failed: {}",
            String::from_utf8_lossy(&out.stderr)
        ));
    }
    Ok(())
}

/// Split an FLV file into its audio (type 8) and video (type 9) tag
/// payloads. Those payloads are exactly RTMP media message bodies, so our
/// client can publish them verbatim and the server relays them verbatim.
pub fn extract_flv_media(path: &std::path::Path) -> Result<(Vec<bytes::Bytes>, Vec<bytes::Bytes>)> {
    let raw = std::fs::read(path).map_err(|e| format!("reading {} failed: {e}", path.display()))?;
    if raw.len() < 13 || &raw[0..3] != b"FLV" {
        return Err(format!("{} is not an FLV file", path.display()));
    }
    let mut video = Vec::new();
    let mut audio = Vec::new();
    let mut pos = 9 + 4; // header + first prev-tag-size
    while pos + 11 <= raw.len() {
        let tag_type = raw[pos];
        let data_size = ((raw[pos + 1] as usize) << 16)
            | ((raw[pos + 2] as usize) << 8)
            | (raw[pos + 3] as usize);
        let data_start = pos + 11;
        let data_end = data_start + data_size;
        if data_end + 4 > raw.len() {
            break;
        }
        match tag_type {
            8 => audio.push(bytes::Bytes::copy_from_slice(&raw[data_start..data_end])),
            9 => video.push(bytes::Bytes::copy_from_slice(&raw[data_start..data_end])),
            _ => {}
        }
        pos = data_end + 4;
    }
    if video.is_empty() || audio.is_empty() {
        return Err(format!(
            "{} yielded {} video / {} audio tags; want both",
            path.display(),
            video.len(),
            audio.len()
        ));
    }
    Ok((video, audio))
}

#[allow(clippy::too_many_arguments)]
pub async fn relay_our_publish_to_ffmpeg(
    stream_key: &str,
    metadata: rtmpx::sessions::StreamMetadata,
    video: Vec<bytes::Bytes>,
    audio: Vec<bytes::Bytes>,
    wall_clock: Duration,
) -> Result<PlayedFile> {
    use super::common::driver::{Endpoint, Peer};
    use rtmpx::time::RtmpTimestamp;

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|e| format!("loopback bind failed: {e}"))?;
    let port = listener
        .local_addr()
        .map_err(|e| format!("local_addr failed: {e}"))?
        .port();
    let addr = format!("127.0.0.1:{port}");
    let out_path = temp_flv_path("relay");
    let out_str = out_path.to_string_lossy().to_string();

    // Publisher task: our own client against our own server, via the shared driver.
    let pub_key = stream_key.to_string();
    let pub_addr = addr.clone();
    let pub_meta = metadata.clone();
    let pub_video = video.clone();
    let pub_audio = audio.clone();
    let publisher = tokio::spawn(async move {
        let endpoint = Endpoint {
            addr: pub_addr,
            app: "live".to_string(),
            op_timeout: Duration::from_secs(15),
            unreachable_hint: None,
        };
        let (mut peer, _, _) =
            Peer::connect(&endpoint, AmfEncoding::Amf0, rtmpx::amf0::Amf0Object::new()).await?;
        peer.publish(&pub_key).await?;
        peer.send_metadata(&pub_meta).await?;
        for (i, v) in pub_video.iter().enumerate() {
            peer.send_video(v.clone(), (i as u32) * 40).await?;
        }
        for (i, a) in pub_audio.iter().enumerate() {
            peer.send_audio(a.clone(), (i as u32) * 23).await?;
        }
        // Hold the publish open while the server relays to ffmpeg, then
        // re-send once so a late player still sees keyframes.
        tokio::time::sleep(Duration::from_secs(6)).await;
        for (i, v) in pub_video.iter().enumerate() {
            let _ = peer.send_video(v.clone(), 1000 + (i as u32) * 40).await;
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
        Ok::<(), String>(())
    });

    // Ingest phase: accept the publisher, collect exactly what it sent.
    let ingested_video: Vec<Vec<u8>>;
    let ingested_audio: Vec<Vec<u8>>;
    let play_stream_hint: String;
    {
        let (mut stream, _) = timeout(wall_clock, listener.accept())
            .await
            .map_err(|_| "timed out waiting for our publisher to connect".to_string())?
            .map_err(|e| format!("accept publisher failed: {e}"))?;
        stream
            .set_nodelay(true)
            .map_err(|e| format!("set_nodelay failed: {e}"))?;
        let mut read_buf = vec![0u8; 16 * 1024];
        let carry = server_handshake(&mut stream, &mut read_buf).await?;
        let (mut session, initial) = ServerSession::new(ServerSessionConfig::new())
            .map_err(|e| format!("server init failed: {e:?}"))?;
        debug_assert!(initial.is_empty());
        let mut collected = IngestedMedia {
            app: String::new(),
            stream_key: String::new(),
            metadata_events: 0,
            audio: Vec::new(),
            video: Vec::new(),
            negotiated: AmfEncoding::Amf0,
        };
        if !carry.is_empty() {
            let results = session
                .handle_input(&carry)
                .map_err(|e| format!("publisher sent unreadable RTMP: {e:?}"))?;
            write_server_results(&mut stream, results, &mut session, &mut collected).await?;
        }
        let deadline = std::time::Instant::now() + Duration::from_secs(15);
        while std::time::Instant::now() < deadline {
            if collected.metadata_events >= 1
                && collected.video.len() >= video.len()
                && collected.audio.len() >= audio.len()
            {
                break;
            }
            let n = timeout(Duration::from_secs(5), stream.read(&mut read_buf))
                .await
                .map_err(|_| "timed out reading from our publisher".to_string())?
                .map_err(|e| format!("read publisher failed: {e}"))?;
            if n == 0 {
                break;
            }
            let input = read_buf[..n].to_vec();
            let results = session
                .handle_input(&input)
                .map_err(|e| format!("publisher sent unreadable RTMP: {e:?}"))?;
            write_server_results(&mut stream, results, &mut session, &mut collected).await?;
        }
        if collected.video.len() < video.len() || collected.audio.len() < audio.len() {
            publisher.abort();
            return Err(format!(
                "our publisher did not land: got {} video / {} audio, want {} / {}",
                collected.video.len(),
                collected.audio.len(),
                video.len(),
                audio.len()
            ));
        }
        ingested_video = collected.video;
        ingested_audio = collected.audio;
        play_stream_hint = collected.stream_key;
        assert_eq!(
            play_stream_hint, stream_key,
            "stream key must survive our own ingest"
        );
    }

    // Playout phase: ffmpeg plays what we ingested.
    let url = format!("rtmp://{addr}/live/{stream_key}");
    let mut child: Child = Command::new("ffmpeg")
        .args([
            "-hide_banner",
            "-loglevel",
            "warning",
            "-i",
            &url,
            "-c",
            "copy",
            "-t",
            "4",
            "-y",
            &out_str,
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| format!("spawning ffmpeg player failed: {e}"))?;

    let serve: Result<()> = timeout(Duration::from_secs(25), async {
        let (mut stream, _) = listener
            .accept()
            .await
            .map_err(|e| format!("accept player failed: {e}"))?;
        stream
            .set_nodelay(true)
            .map_err(|e| format!("set_nodelay failed: {e}"))?;
        let mut read_buf = vec![0u8; 32 * 1024];
        let carry = server_handshake(&mut stream, &mut read_buf).await?;
        let (mut session, initial) = ServerSession::new(ServerSessionConfig::new())
            .map_err(|e| format!("server init failed: {e:?}"))?;
        debug_assert!(initial.is_empty());
        let mut play_stream_id: Option<rtmpx::sessions::StreamId> = None;
        let mut sent = false;
        // Handle the player's connect + play, then push the ingested media.
        let mut pending: Vec<u8> = carry;
        let start = std::time::Instant::now();
        loop {
            if start.elapsed() > Duration::from_secs(20) {
                break;
            }
            if !pending.is_empty() {
                let results = session
                    .handle_input(&pending)
                    .map_err(|e| format!("player sent unreadable RTMP: {e:?}"))?;
                pending.clear();
                for result in results {
                    match result {
                        ServerSessionResult::OutboundResponse(packet) => {
                            stream
                                .write_all(&packet.bytes)
                                .await
                                .map_err(|e| format!("write player failed: {e}"))?;
                        }
                        ServerSessionResult::RaisedEvent(event) => match event {
                            ServerSessionEvent::ConnectionRequested { request_id, .. } => {
                                let follow = session
                                    .accept_request(request_id)
                                    .map_err(|e| format!("accept player connect failed: {e:?}"))?;
                                for r in follow {
                                    if let ServerSessionResult::OutboundResponse(packet) = r {
                                        stream
                                            .write_all(&packet.bytes)
                                            .await
                                            .map_err(|e| format!("write player failed: {e}"))?;
                                    }
                                }
                            }
                            ServerSessionEvent::PlayStreamRequested {
                                request_id,
                                stream_id,
                                ..
                            } => {
                                let follow = session
                                    .accept_request(request_id)
                                    .map_err(|e| format!("accept play failed: {e:?}"))?;
                                for r in follow {
                                    if let ServerSessionResult::OutboundResponse(packet) = r {
                                        stream
                                            .write_all(&packet.bytes)
                                            .await
                                            .map_err(|e| format!("write player failed: {e}"))?;
                                    }
                                }
                                play_stream_id = Some(stream_id);
                            }
                            _ => {}
                        },
                        ServerSessionResult::UnhandleableMessageReceived(_) => {}
                        #[allow(unreachable_patterns)]
                        _ => panic!("unexpected future protocol variant"),
                    }
                }
                stream
                    .flush()
                    .await
                    .map_err(|e| format!("flush player failed: {e}"))?;
            }
            if let Some(sid) = play_stream_id
                && !sent
            {
                let meta_packet = session
                    .send_metadata(sid, &metadata)
                    .map_err(|e| format!("send_metadata failed: {e:?}"))?;
                stream
                    .write_all(&meta_packet.bytes)
                    .await
                    .map_err(|e| format!("write meta failed: {e}"))?;
                for (i, v) in ingested_video.iter().enumerate() {
                    let p = session
                        .send_video_data(
                            sid,
                            bytes::Bytes::from(v.clone()),
                            RtmpTimestamp::new((i as u32) * 40),
                            false,
                        )
                        .map_err(|e| format!("send_video failed: {e:?}"))?;
                    stream
                        .write_all(&p.bytes)
                        .await
                        .map_err(|e| format!("write video failed: {e}"))?;
                }
                for (i, a) in ingested_audio.iter().enumerate() {
                    let p = session
                        .send_audio_data(
                            sid,
                            bytes::Bytes::from(a.clone()),
                            RtmpTimestamp::new((i as u32) * 23),
                            false,
                        )
                        .map_err(|e| format!("send_audio failed: {e:?}"))?;
                    stream
                        .write_all(&p.bytes)
                        .await
                        .map_err(|e| format!("write audio failed: {e}"))?;
                }
                stream
                    .flush()
                    .await
                    .map_err(|e| format!("flush media failed: {e}"))?;
                sent = true;
            }
            // Poll ffmpeg liveness; exit once it is done writing.
            if child
                .try_wait()
                .map_err(|e| format!("ffmpeg wait failed: {e}"))?
                .is_some()
            {
                break;
            }
            match timeout(Duration::from_millis(300), stream.read(&mut read_buf)).await {
                Ok(Ok(n)) if n > 0 => {
                    pending.extend_from_slice(&read_buf[..n]);
                }
                _ => {}
            }
        }
        Ok::<(), String>(())
    })
    .await
    .map_err(|_| "timed out serving our relay to ffmpeg".to_string())?;

    // Reap ffmpeg and probe whatever it wrote, even on serve errors, so
    // failures show ffprobe diagnostics instead of just a timeout.
    let _ = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            match child.try_wait() {
                Ok(Some(_)) => break,
                Ok(None) => tokio::time::sleep(Duration::from_millis(200)).await,
                Err(_) => break,
            }
        }
    })
    .await;
    let stderr = child
        .stderr
        .take()
        .map(|mut pipe| {
            use std::io::Read;
            let mut s = String::new();
            let _ = pipe.read_to_string(&mut s);
            s
        })
        .unwrap_or_default();
    let _ = child.kill();
    let _ = child.wait();
    publisher.abort();
    serve?;
    let probe = ffprobe_streams(&out_path)
        .await
        .map_err(|e| format!("{e}\nffmpeg stderr:\n{stderr}"))?;
    Ok(PlayedFile {
        path: out_path,
        probe,
    })
}
