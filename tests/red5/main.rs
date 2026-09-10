//! Live interop suite: rtmpx against a real Red5 server, plus real
//! ffmpeg publishing into our ServerSession.
//!
//! Optional by construction: this target only builds with
//! `cargo test --test red5 --features red5-live`
//! (see [[test]] required-features in Cargo.toml), and the GitHub workflow
//! runs it against a containerised Red5. Default `cargo test` never touches
//! the network.
//!
//! Matrix rows (see README.md): publish/play x AMF0/AMF3 over legacy
//! media are fully asserted; Enhanced RTMP legs are characterization - Red5
//! has no Enhanced media path, so they assert the connection survives and log
//! what Red5 did with the bytes.

mod fixtures;
mod harness;
mod server_harness;

use std::time::Duration;

use fixtures::*;
use harness::{Peer, Red5, Result, object_encoding_of, stream_key};
use rtmpx::amf::AmfEncoding;
use rtmpx::amf0::{Amf0Object, Amf0Value};
use rtmpx::sessions::ClientSessionEvent;

/// Backstop so a wedged peer fails the job instead of hanging it. Normal
/// failures surface via the per-operation RED5_TIMEOUT_SECS timeouts first.
async fn run(body: impl std::future::Future<Output = Result<()>>) {
    tokio::time::timeout(Duration::from_secs(180), body)
        .await
        .expect("red5 harness hung: no operation completed or timed out")
        .unwrap();
}

/// Red5 answers `connect` with a type-20 `_result` whose command object is
/// always null: the `objectEncoding` confirmation (when present) lives in the
/// trailing information object, i.e. `additional_properties`. For AMF0 Red5
/// omits it entirely, so AMF0 asserts negotiation + absence of an AMF3 claim.
fn assert_amf0_negotiated(peer: &Peer, info: &Amf0Object) {
    assert_eq!(
        peer.session.negotiated_encoding(),
        AmfEncoding::Amf0,
        "AMF0 connect must negotiate AMF0"
    );
    assert_ne!(
        object_encoding_of(info),
        Some(3.0),
        "AMF0 connect must not confirm objectEncoding 3, got {info:?}"
    );
}

fn assert_amf3_negotiated(peer: &Peer, info: &Amf0Object) {
    assert_eq!(
        peer.session.negotiated_encoding(),
        AmfEncoding::Amf3,
        "AMF3 connect must negotiate AMF3 against Red5 (check Red5 still answers objectEncoding 3)"
    );
    assert_eq!(
        object_encoding_of(info),
        Some(3.0),
        "Red5 _result must confirm objectEncoding 3, got {info:?}"
    );
}

/// Publish a fixed legacy sequence: metadata, seq headers, then 3 A/V frames.
async fn publish_legacy_sequence(peer: &mut Peer) -> Result<()> {
    peer.send_metadata(&legacy_metadata()).await?;
    peer.send_video(avc_sequence_header(), 0).await?;
    peer.send_audio(aac_sequence_header(), 0).await?;
    for i in 0..3u8 {
        peer.send_video(avc_coded_frame(0x70 + i), 40 * (u32::from(i) + 1))
            .await?;
        peer.send_audio(aac_raw_frame(0x30 + i), 23 * (u32::from(i) + 1))
            .await?;
    }
    Ok(())
}

fn expected_video() -> Vec<Vec<u8>> {
    let mut v = vec![avc_sequence_header().to_vec()];
    for i in 0..3u8 {
        v.push(avc_coded_frame(0x70 + i).to_vec());
    }
    v
}

fn expected_audio() -> Vec<Vec<u8>> {
    let mut a = vec![aac_sequence_header().to_vec()];
    for i in 0..3u8 {
        a.push(aac_raw_frame(0x30 + i).to_vec());
    }
    a
}

// --- Row 1: publish, AMF0, legacy -------------------------------------------

async fn publish_amf0_legacy_body() -> Result<()> {
    let red5 = Red5::from_env();
    let key = stream_key("pub0");
    let (mut peer, _, info) = Peer::connect(&red5, AmfEncoding::Amf0, Amf0Object::new()).await?;
    assert_amf0_negotiated(&peer, &info);
    peer.publish(&key).await?;
    publish_legacy_sequence(&mut peer).await?;
    Ok(())
}

#[tokio::test]
async fn publish_amf0_legacy() {
    run(publish_amf0_legacy_body()).await;
}

// --- Row 2: play, AMF0, legacy (byte-exact round-trip through Red5) ----------

async fn play_amf0_legacy_roundtrip_body() -> Result<()> {
    let red5 = Red5::from_env();
    let key = stream_key("play0");
    let (mut publ, _, _) = Peer::connect(&red5, AmfEncoding::Amf0, Amf0Object::new()).await?;
    publ.publish(&key).await?;
    // Subscribe BEFORE publishing: live relays only what comes after play.
    let (mut play, _, _) = Peer::connect(&red5, AmfEncoding::Amf0, Amf0Object::new()).await?;
    play.play(&key).await?;
    publish_legacy_sequence(&mut publ).await?;

    let got = play.collect(4, 4, true).await?;
    assert_eq!(got.video, expected_video(), "video must relay byte-exact");
    assert_eq!(got.audio, expected_audio(), "audio must relay byte-exact");
    Ok(())
}

#[tokio::test]
async fn play_amf0_legacy_roundtrip() {
    run(play_amf0_legacy_roundtrip_body()).await;
}

// --- Row 3: publish, AMF3, legacy --------------------------------------------

async fn publish_amf3_legacy_body() -> Result<()> {
    let red5 = Red5::from_env();
    let key = stream_key("pub3");
    let (mut peer, _, info) = Peer::connect(&red5, AmfEncoding::Amf3, Amf0Object::new()).await?;
    assert_amf3_negotiated(&peer, &info);
    peer.publish(&key).await?;
    publish_legacy_sequence(&mut peer).await?;
    Ok(())
}

#[tokio::test]
async fn publish_amf3_legacy() {
    run(publish_amf3_legacy_body()).await;
}

// --- Row 4: play, AMF3, legacy ------------------------------------------------

async fn play_amf3_legacy_roundtrip_body() -> Result<()> {
    let red5 = Red5::from_env();
    let key = stream_key("play3");
    let (mut publ, _, publ_info) =
        Peer::connect(&red5, AmfEncoding::Amf3, Amf0Object::new()).await?;
    assert_amf3_negotiated(&publ, &publ_info);
    publ.publish(&key).await?;
    let (mut play, _, play_info) =
        Peer::connect(&red5, AmfEncoding::Amf3, Amf0Object::new()).await?;
    assert_amf3_negotiated(&play, &play_info);
    play.play(&key).await?;
    publish_legacy_sequence(&mut publ).await?;

    let got = play.collect(4, 4, true).await?;
    assert_eq!(
        got.video,
        expected_video(),
        "video must relay byte-exact over AMF3"
    );
    assert_eq!(
        got.audio,
        expected_audio(),
        "audio must relay byte-exact over AMF3"
    );
    Ok(())
}

#[tokio::test]
async fn play_amf3_legacy_roundtrip() {
    run(play_amf3_legacy_roundtrip_body()).await;
}

// --- Row 9: Enhanced capability advertisement on connect ---------------------

fn enhanced_connect_props() -> Amf0Object {
    let mut extra = Amf0Object::new();
    extra.insert(
        "fourCcList".to_string(),
        Amf0Value::StrictArray(vec![
            Amf0Value::Utf8String("hvc1".to_string()),
            Amf0Value::Utf8String("av01".to_string()),
        ]),
    );
    extra.insert("capsEx".to_string(), Amf0Value::Number(15.0));
    let mut video_map = Amf0Object::new();
    video_map.insert("hvc1".to_string(), Amf0Value::Number(1.0));
    video_map.insert("av01".to_string(), Amf0Value::Number(1.0));
    extra.insert(
        "videoFourCcInfoMap".to_string(),
        Amf0Value::Object(video_map),
    );
    let mut audio_map = Amf0Object::new();
    audio_map.insert("opus".to_string(), Amf0Value::Number(1.0));
    extra.insert(
        "audioFourCcInfoMap".to_string(),
        Amf0Value::Object(audio_map),
    );
    extra.insert("videoFunction".to_string(), Amf0Value::Number(1.0));
    extra
}

async fn enhanced_caps_body(encoding: AmfEncoding, tag: &str) -> Result<()> {
    let red5 = Red5::from_env();
    let key = stream_key(tag);
    let (mut peer, _, _) = Peer::connect(&red5, encoding, enhanced_connect_props()).await?;
    // The point: Red5 must accept a connect carrying the full E-RTMP
    // advertisement and let us publish afterwards - not choke on it.
    peer.publish(&key).await?;
    peer.send_video(avc_sequence_header(), 0).await?;
    Ok(())
}

#[tokio::test]
async fn connect_forwards_enhanced_capabilities_amf0() {
    run(enhanced_caps_body(AmfEncoding::Amf0, "caps0")).await;
}

#[tokio::test]
async fn connect_forwards_enhanced_capabilities_amf3() {
    run(enhanced_caps_body(AmfEncoding::Amf3, "caps3")).await;
}

// --- Row 10: AMF3 script data (type 15) survives Red5 --------------------------

async fn amf3_script_data_body() -> Result<()> {
    let red5 = Red5::from_env();
    let key = stream_key("amf3data");
    let (mut publ, _, _) = Peer::connect(&red5, AmfEncoding::Amf3, Amf0Object::new()).await?;
    publ.publish(&key).await?;
    let (mut play, _, _) = Peer::connect(&red5, AmfEncoding::Amf3, Amf0Object::new()).await?;
    play.play(&key).await?;

    publ.send_raw_amf3_data(amf3_probe_body(), 0).await?;
    // Keep the connection meaningfully alive afterwards.
    publ.send_video(avc_sequence_header(), 0).await?;

    let deadline = std::time::Instant::now() + red5.op_timeout;
    loop {
        if std::time::Instant::now() > deadline {
            return Err("player never saw the AMF3 @setDataFrame probe".to_string());
        }
        for event in play.next_events().await? {
            if let ClientSessionEvent::StreamMetadataReceived { raw_payload, .. } = event {
                let bytes = raw_payload.to_vec();
                assert!(
                    bytes.windows(11).any(|w| w == b"rtmpxRed5Probe"),
                    "relayed AMF3 metadata must carry our marker field, got {bytes:?}"
                );
                return Ok(());
            }
        }
    }
}

#[tokio::test]
async fn amf3_script_data_survives_red5() {
    run(amf3_script_data_body()).await;
}

// --- Rows 5/7: Enhanced media is characterization, not assertion --------------

async fn enhanced_media_body(
    encoding: AmfEncoding,
    tag: &str,
    enhanced: bytes::Bytes,
) -> Result<()> {
    let red5 = Red5::from_env();
    let key = stream_key(tag);
    let (mut publ, _, _) = Peer::connect(&red5, encoding, enhanced_connect_props()).await?;
    publ.publish(&key).await?;
    // Send Enhanced bytes Red5 cannot understand, then prove the connection
    // is still usable by round-tripping legacy media behind it.
    publ.send_video(enhanced.clone(), 0).await?;
    let (mut play, _, _) = Peer::connect(&red5, AmfEncoding::Amf0, Amf0Object::new()).await?;
    play.play(&key).await?;
    publ.send_video(avc_sequence_header(), 40).await?;
    publ.send_audio(aac_sequence_header(), 40).await?;

    let got = play.collect(1, 1, false).await?;
    assert!(
        got.video.contains(&avc_sequence_header().to_vec()),
        "legacy video must still flow after Enhanced input"
    );
    eprintln!(
        "red5 characterization [{tag}]: publish accepted; player saw {} video / {} audio packets (Enhanced relayed: {})",
        got.video.len(),
        got.audio.len(),
        got.video.iter().any(|v| v == &enhanced.to_vec()),
    );
    Ok(())
}

#[tokio::test]
async fn enhanced_hvc1_publish_characterization() {
    run(enhanced_media_body(
        AmfEncoding::Amf0,
        "enh0",
        enhanced_hvc1_sequence_start(),
    ))
    .await;
}

#[tokio::test]
async fn enhanced_av1_publish_characterization() {
    run(enhanced_media_body(
        AmfEncoding::Amf3,
        "enh3",
        enhanced_av1_coded_frame(),
    ))
    .await;
}

// --- Independent encoder leg: ffmpeg -> our ServerSession ---------------------

async fn ffmpeg_ingest_body() -> Result<()> {
    if !server_harness::ffmpeg_available().await {
        eprintln!("red5 harness: SKIP ffmpeg_publishes_to_our_server (no ffmpeg on PATH)");
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
        eprintln!("red5 harness: SKIP ffmpeg_enhanced_publishes_to_our_server (no ffmpeg on PATH)");
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
        "red5 harness: Enhanced ingest saw {} video / {} audio, FourCC={}",
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
        eprintln!("red5 harness: SKIP our_server_relays_to_ffmpeg_player (no ffmpeg on PATH)");
        return Ok(());
    }
    if !server_harness::ffprobe_available().await {
        eprintln!("red5 harness: SKIP our_server_relays_to_ffmpeg_player (no ffprobe on PATH)");
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
    eprintln!("red5 harness: relay probe:\n{}", played.probe);
    let _ = std::fs::remove_file(&played.path);
    Ok(())
}

#[tokio::test]
async fn our_server_relays_to_ffmpeg_player() {
    run(our_relay_to_ffmpeg_body()).await;
}
