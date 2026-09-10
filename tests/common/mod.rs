//! Shared helpers for the live interop suites (tests/red5, tests/ffmpeg).
//! Neither suite runs by default; see each suite's README for gating.

pub mod driver;

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use rtmpx::sessions::StreamMetadata;

pub type Result<T> = std::result::Result<T, String>;

static KEY_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Stream key unique across the parallel tests in this process, so tests never
/// see each other's streams on a shared server.
pub fn stream_key(tag: &str) -> String {
    let n = KEY_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("rtmpx-{tag}-{}-{n}", std::process::id())
}

/// Backstop so a wedged peer fails the job instead of hanging it. Normal
/// failures surface via the per-operation endpoint timeouts first.
pub async fn run(body: impl std::future::Future<Output = Result<()>>) {
    tokio::time::timeout(Duration::from_secs(180), body)
        .await
        .expect("interop harness hung: no operation completed or timed out")
        .unwrap();
}

/// onMetaData the harnesses publish: legacy AVC + AAC hints.
pub fn legacy_metadata() -> StreamMetadata {
    let mut m = StreamMetadata::new();
    m.video_width = Some(1280);
    m.video_height = Some(720);
    m.video_codec_id = Some(7); // AVC
    m.video_frame_rate = Some(30.0);
    m.audio_codec_id = Some(10); // AAC
    m.audio_sample_rate = Some(44100);
    m.audio_channels = Some(2);
    m.audio_is_stereo = Some(true);
    m.encoder = Some("rtmpx-interop-harness".to_string());
    m
}
