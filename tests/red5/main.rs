//! Live interop suite: rtmpx against a real Red5 server.
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

#[path = "../common/mod.rs"]
mod common;
mod fixtures;
mod harness;

use common::driver::Peer;
use common::{Result, legacy_metadata, run, stream_key};
use fixtures::*;
use harness::{object_encoding_of, red5_endpoint};
use rtmpx::amf::AmfEncoding;
use rtmpx::amf0::{Amf0Object, Amf0Value};
use rtmpx::sessions::ClientSessionEvent;

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
    let red5 = red5_endpoint();
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
    let red5 = red5_endpoint();
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
    let red5 = red5_endpoint();
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
    let red5 = red5_endpoint();
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
    let red5 = red5_endpoint();
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
    let red5 = red5_endpoint();
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
            if let ClientSessionEvent::StreamMetadataReceived { message, .. } = event {
                let bytes = message.payload().to_vec();
                let marker = b"rtmpxRed5Probe";
                assert!(
                    bytes.windows(marker.len()).any(|w| w == marker),
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
    let red5 = red5_endpoint();
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

    // Red5 may replay the pre-join Enhanced packet as a cached sequence
    // header, so the first relayed video frame is not always the legacy
    // header. Wait until the legacy header arrives instead of asserting on
    // the first packet.
    let deadline = std::time::Instant::now() + red5.op_timeout;
    let mut videos: Vec<Vec<u8>> = Vec::new();
    let mut audios: Vec<Vec<u8>> = Vec::new();
    while !videos.contains(&avc_sequence_header().to_vec()) || audios.is_empty() {
        if std::time::Instant::now() > deadline {
            break;
        }
        for event in play.next_events().await? {
            match event {
                ClientSessionEvent::VideoDataReceived { data, .. } => {
                    videos.push(data.to_vec());
                }
                ClientSessionEvent::AudioDataReceived { data, .. } => {
                    audios.push(data.to_vec());
                }
                _ => {}
            }
        }
    }
    assert!(
        videos.contains(&avc_sequence_header().to_vec()),
        "legacy video must still flow after Enhanced input"
    );
    assert!(
        !audios.is_empty(),
        "legacy audio must still flow after Enhanced input"
    );
    eprintln!(
        "red5 characterization [{tag}]: publish accepted; player saw {} video / {} audio packets (Enhanced relayed: {})",
        videos.len(),
        audios.len(),
        videos.iter().any(|v| v == &enhanced.to_vec()),
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
