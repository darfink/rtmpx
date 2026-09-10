//! Live interop suite: our ServerSession against real ffmpeg as publisher,
//! plus our publish relayed to real ffmpeg/ffprobe as player.
//!
//! Optional by construction: this target only builds with
//! cargo test --test ffmpeg --features ffmpeg-live
//! (see [[test]] required-features in Cargo.toml). Tests skip loudly when no
//! ffmpeg/ffprobe binaries are on PATH, and need no Red5 server. Default
//! cargo test never touches the network or external binaries.

#[path = "../common/mod.rs"]
mod common;
mod server_harness;

use std::time::Duration;

use common::{Result, legacy_metadata, run, stream_key};
use rtmpx::amf::AmfEncoding;

// --- Independent encoder leg: ffmpeg -> our ServerSession ---------------------

async fn ffmpeg_ingest_body() -> Result<()> {
    if !server_harness::ffmpeg_available().await {
        eprintln!("ffmpeg harness: SKIP ffmpeg_publishes_to_our_server (no ffmpeg on PATH)");
        return Ok(());
    }
    let key = stream_key("ffmpeg");
    let got = server_harness::ingest_from_ffmpeg(&key, Duration::from_secs(60)).await?;
    assert_eq!(got.app, "live", "ffmpeg must land on the live app");
    assert_eq!(got.stream_key, key, "stream key must survive ingest");
    assert!(
        got.metadata_events >= 1,
        "must observe ffmpeg onMetaData (saw {})",
        got.metadata_events
    );
    assert!(
        got.audio.len() >= 2,
        "must observe ffmpeg AAC (saw {})",
        got.audio.len()
    );
    assert!(
        got.video.len() >= 5,
        "must observe ffmpeg AVC (saw {})",
        got.video.len()
    );
    assert!(
        !got.audio.iter().any(Vec::is_empty) && !got.video.iter().any(Vec::is_empty),
        "media payloads must be non-empty"
    );
    assert_eq!(got.negotiated, AmfEncoding::Amf0, "ffmpeg negotiates AMF0");
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
    Ok(())
}

#[tokio::test]
async fn ffmpeg_publishes_to_our_server() {
    run(ffmpeg_ingest_body()).await;
}

// --- Enhanced ingest leg: ffmpeg (HEVC) -> our ServerSession ----------------

async fn ffmpeg_enhanced_ingest_body() -> Result<()> {
    if !server_harness::ffmpeg_available().await {
        eprintln!("ffmpeg harness: SKIP ffmpeg_enhanced_publishes_to_our_server (no ffmpeg on PATH)");
        return Ok(());
    }
    let key = stream_key("ffmpeg-hvc1");
    let got = server_harness::ingest_enhanced_from_ffmpeg(&key, Duration::from_secs(60)).await?;
    assert_eq!(got.app, "live", "ffmpeg must land on the live app");
    assert_eq!(
        got.stream_key, key,
        "stream key must survive Enhanced ingest"
    );
    assert!(
        got.video.len() >= 2,
        "must observe ffmpeg HEVC (saw {})",
        got.video.len()
    );
    // Enhanced video carries the FourCC in bytes 1..5 (e.g. hvc1); legacy
    // AVC would be 0x17 0x00/0x01 with an AVCDecoderConfigurationRecord.
    let first = &got.video[0];
    assert!(
        first.len() >= 5,
        "Enhanced video must carry FourCC, got {first:02x?}"
    );
    assert_ne!(
        first[0], 0x17,
        "HEVC must not arrive as legacy AVC (first byte {0:02x})",
        first[0]
    );
    let fourcc = &first[1..5];
    assert!(
        fourcc == b"hvc1" || fourcc == b"hevc",
        "first Enhanced video must carry hvc1/hevc FourCC, got {fourcc:02x?} (full header {first:02x?})"
    );
    eprintln!(
        "ffmpeg harness: Enhanced ingest saw {} video / {} audio, FourCC={}",
        got.video.len(),
        got.audio.len(),
        String::from_utf8_lossy(fourcc)
    );
    Ok(())
}

#[tokio::test]
async fn ffmpeg_enhanced_publishes_to_our_server() {
    run(ffmpeg_enhanced_ingest_body()).await;
}

// --- Third-party player leg: our client -> our server -> ffmpeg -------------

async fn our_relay_to_ffmpeg_body() -> Result<()> {
    if !server_harness::ffmpeg_available().await {
        eprintln!("ffmpeg harness: SKIP our_server_relays_to_ffmpeg_player (no ffmpeg on PATH)");
        return Ok(());
    }
    if !server_harness::ffprobe_available().await {
        eprintln!("ffmpeg harness: SKIP our_server_relays_to_ffmpeg_player (no ffprobe on PATH)");
        return Ok(());
    }
    let key = stream_key("relay-ffmpeg");
    // ffmpeg-as-player needs valid codec headers, so publish real payloads
    // extracted from a generated FLV (still our client publishing our bytes).
    let src =
        std::env::temp_dir().join(format!("rtmpx-relaysrc-{}-{}.flv", std::process::id(), key));
    server_harness::generate_valid_flv(&src, 3).await?;
    let (video, audio) = server_harness::extract_flv_media(&src)?;
    let _ = std::fs::remove_file(&src);
    // Cap the relay to keep the test fast: first seconds are enough for
    // ffprobe to see both streams.
    let video: Vec<bytes::Bytes> = video.into_iter().take(15).collect();
    let audio: Vec<bytes::Bytes> = audio.into_iter().take(15).collect();
    let metadata = legacy_metadata();
    let played = server_harness::relay_our_publish_to_ffmpeg(
        &key,
        metadata,
        video,
        audio,
        Duration::from_secs(60),
    )
    .await?;
    assert!(
        played.probe.contains("codec_type=video"),
        "ffmpeg recording must contain video, ffprobe said:\n{}",
        played.probe
    );
    assert!(
        played.probe.contains("codec_type=audio"),
        "ffmpeg recording must contain audio, ffprobe said:\n{}",
        played.probe
    );
    assert!(
        played.probe.contains("h264"),
        "relayed video must probe as h264, ffprobe said:\n{}",
        played.probe
    );
    assert!(
        played.probe.contains("aac"),
        "relayed audio must probe as aac, ffprobe said:\n{}",
        played.probe
    );
    eprintln!("ffmpeg harness: relay probe:\n{}", played.probe);
    let _ = std::fs::remove_file(&played.path);
    Ok(())
}

#[tokio::test]
async fn our_server_relays_to_ffmpeg_player() {
    run(our_relay_to_ffmpeg_body()).await;
}
