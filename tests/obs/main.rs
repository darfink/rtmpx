//! Live interop suite: our `ServerSession` against real OBS Studio as publisher.
//!
//! Optional by construction: this target only builds with
//! `cargo test --test obs --features obs-live` (see `[[test]]`
//! required-features in Cargo.toml). Default `cargo test` never starts OBS.
//!
//! OBS is the most common publisher, and it carries its own fork of librtmp,
//! so `rtmpsink` in the GStreamer suite only approximates it. This starts the
//! real application with `--startstreaming` and a throwaway profile that
//! streams to a loopback server.
//!
//! Linux only: there OBS keeps its configuration under
//! `$XDG_CONFIG_HOME/obs-studio`, so the profile can live in a temporary
//! directory. On other systems OBS cannot be pointed at a temporary
//! configuration, and the test skips rather than touch the user's own
//! profiles. OBS needs a display; CI runs it under `xvfb-run`. Set
//! `RTMPX_OBS_BIN` to use an `obs` binary that is not on `PATH`, and
//! `RTMPX_OBS_REQUIRED` to fail rather than skip when OBS cannot run.

#[path = "../support/api.rs"]
mod api;
#[path = "../common/mod.rs"]
#[allow(dead_code)]
mod common;

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use crate::api::handshake::{Handshake, HandshakeProgress, HandshakeRole};
use crate::api::sessions::{
    ServerSession, ServerSessionConfig, ServerSessionEvent, ServerSessionResult,
};
use common::{Result, run, stream_key};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;

/// OBS starts slowly under a software-rendered virtual display.
const WALL_CLOCK: Duration = Duration::from_secs(120);
const PROFILE: &str = "rtmpx";

#[derive(Default)]
struct IngestedMedia {
    app: String,
    stream_key: String,
    encoders: Vec<String>,
    audio: Vec<Vec<u8>>,
    video: Vec<Vec<u8>>,
    /// Commands the session answered for itself, in arrival order.
    setup_commands: Vec<String>,
    /// Commands the session refused with `_error`. OBS must never need one.
    refused: Vec<String>,
}

fn obs_binary() -> String {
    std::env::var("RTMPX_OBS_BIN").unwrap_or_else(|_| "obs".to_string())
}

/// Why the test cannot run here, if it cannot.
fn skip_reason() -> Option<String> {
    if !cfg!(target_os = "linux") {
        return Some(
            "OBS can only be given a temporary configuration on Linux, and this test must not \
             touch your own OBS profiles"
                .to_string(),
        );
    }
    if std::env::var_os("DISPLAY").is_none() && std::env::var_os("WAYLAND_DISPLAY").is_none() {
        return Some("no display; run it under xvfb-run".to_string());
    }
    let found = Command::new(obs_binary())
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|status| status.success())
        .unwrap_or(false);
    (!found).then(|| format!("no `{}` binary; set RTMPX_OBS_BIN", obs_binary()))
}

/// A temporary `$XDG_CONFIG_HOME` holding an OBS profile that streams to
/// `rtmp://127.0.0.1:{port}/live/{key}`. Removed on drop.
struct ObsConfig(PathBuf);

impl ObsConfig {
    fn new(port: u16, key: &str) -> Result<Self> {
        let root = std::env::temp_dir().join(format!("rtmpx-obs-{key}"));
        let obs = root.join("obs-studio");
        let profile = obs.join("basic/profiles").join(PROFILE);
        let write = |path: PathBuf, contents: String| -> Result<()> {
            fs::create_dir_all(path.parent().expect("config files have a parent"))
                .map_err(|e| format!("creating {}: {e}", path.display()))?;
            fs::write(&path, contents).map_err(|e| format!("writing {}: {e}", path.display()))
        };
        // `FirstRun=true` records that the first run already happened, which
        // skips the auto-configuration wizard.
        write(
            obs.join("global.ini"),
            format!(
                "[General]\nFirstRun=true\nLastVersion=999999999\nEnableAutoUpdates=false\n\n\
                 [Basic]\nProfile={PROFILE}\nProfileDir={PROFILE}\nSceneCollection={PROFILE}\n\
                 SceneCollectionFile={PROFILE}\nConfirmOnExit=false\n\n\
                 [BasicWindow]\nSysTrayEnabled=false\nWarnBeforeStartingStream=false\n\
                 WarnBeforeStoppingStream=false\n"
            ),
        )?;
        // Small and fast: the encoder runs in software on a CI machine.
        write(
            profile.join("basic.ini"),
            format!(
                "[General]\nName={PROFILE}\n\n[Output]\nMode=Simple\n\n\
                 [SimpleOutput]\nStreamEncoder=x264\nPreset=ultrafast\nVBitrate=500\nABitrate=64\n\n\
                 [Video]\nBaseCX=320\nBaseCY=180\nOutputCX=320\nOutputCY=180\nFPSType=0\n\
                 FPSCommon=30\n\n[Audio]\nSampleRate=44100\nChannelSetup=Stereo\n"
            ),
        )?;
        write(
            profile.join("service.json"),
            format!(
                r#"{{"type":"rtmp_custom","settings":{{"server":"rtmp://127.0.0.1:{port}/live","key":"{key}","use_auth":false,"bwtest":false}}}}"#
            ),
        )?;
        Ok(Self(root))
    }

    /// OBS's own log, which says why it did not stream when it did not.
    fn logs(&self) -> String {
        let dir = self.0.join("obs-studio/logs");
        let mut out = String::new();
        if let Ok(entries) = fs::read_dir(&dir) {
            for entry in entries.flatten() {
                if let Ok(log) = fs::read_to_string(entry.path()) {
                    let tail: Vec<&str> = log.lines().rev().take(80).collect();
                    out.push_str(&format!("--- {}\n", entry.path().display()));
                    for line in tail.into_iter().rev() {
                        out.push_str(line);
                        out.push('\n');
                    }
                }
            }
        }
        out
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for ObsConfig {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
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
                    .map_err(|e| format!("write to OBS failed: {e}"))?;
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
                        .map_err(|e| format!("accepting OBS connect failed: {e:?}"))?;
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
                        .map_err(|e| format!("accepting OBS publish failed: {e:?}"))?;
                    Box::pin(write_server_results(stream, follow, session, collected)).await?;
                }
                ServerSessionEvent::StreamDataReceived { message, .. } => {
                    let metadata = crate::api::sessions::metadata(&message);
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
                ServerSessionEvent::UnhandledCommand { command_name, .. } => {
                    collected.setup_commands.push(command_name);
                }
                ServerSessionEvent::CommandRefused { command_name, .. } => {
                    collected.refused.push(command_name);
                }
                other => eprintln!("obs harness: ignoring OBS event: {other:?}"),
            },
            ServerSessionResult::UnhandledMessage(_) => {}
            #[allow(unreachable_patterns)]
            _ => panic!("unexpected future protocol variant"),
        }
    }
    stream
        .flush()
        .await
        .map_err(|e| format!("flush to OBS failed: {e}"))
}

async fn server_handshake(stream: &mut TcpStream, read_buf: &mut [u8]) -> Result<Vec<u8>> {
    let mut handshake = Handshake::new(HandshakeRole::Server);
    loop {
        let n = stream
            .read(read_buf)
            .await
            .map_err(|e| format!("handshake read from OBS failed: {e}"))?;
        if n == 0 {
            return Err("OBS closed the connection during the handshake".to_string());
        }
        match handshake
            .process_bytes(&read_buf[..n])
            .map_err(|e| format!("handshake with OBS failed: {e:?}"))?
        {
            HandshakeProgress::InProgress { response_bytes } => {
                stream
                    .write_all(&response_bytes)
                    .await
                    .map_err(|e| format!("handshake write to OBS failed: {e}"))?;
            }
            HandshakeProgress::Completed {
                response_bytes,
                remaining_bytes,
            } => {
                stream
                    .write_all(&response_bytes)
                    .await
                    .map_err(|e| format!("handshake write to OBS failed: {e}"))?;
                return Ok(remaining_bytes);
            }
            #[allow(unreachable_patterns)]
            _ => panic!("unexpected future protocol variant"),
        }
    }
}

/// Serve one connection until OBS has sent metadata and enough media.
async fn serve(listener: TcpListener) -> Result<IngestedMedia> {
    let (mut stream, _) = listener
        .accept()
        .await
        .map_err(|e| format!("accept from OBS failed: {e}"))?;
    stream
        .set_nodelay(true)
        .map_err(|e| format!("set_nodelay failed: {e}"))?;
    let mut read_buf = vec![0u8; 16 * 1024];
    let carry = server_handshake(&mut stream, &mut read_buf).await?;
    let (mut session, _) = ServerSession::new(ServerSessionConfig::new())
        .map_err(|e| format!("server session init failed: {e:?}"))?;
    let mut collected = IngestedMedia::default();
    let mut input = carry;
    loop {
        if !input.is_empty() {
            let results = session
                .handle_input(&input)
                .map_err(|e| format!("OBS sent unreadable RTMP: {e:?}"))?;
            write_server_results(&mut stream, results, &mut session, &mut collected).await?;
        }
        if !collected.encoders.is_empty()
            && collected.audio.len() >= 10
            && collected.video.len() >= 10
        {
            return Ok(collected);
        }
        let n = stream
            .read(&mut read_buf)
            .await
            .map_err(|e| format!("read from OBS failed: {e}"))?;
        if n == 0 {
            return Err("OBS went away before sending metadata, audio and video".to_string());
        }
        input = read_buf[..n].to_vec();
    }
}

async fn obs_ingest_body() -> Result<()> {
    if let Some(reason) = skip_reason() {
        // CI sets this, so a missing OBS or display fails instead of passing.
        if std::env::var_os("RTMPX_OBS_REQUIRED").is_some() {
            return Err(format!("OBS interop is required but cannot run: {reason}"));
        }
        eprintln!("obs harness: SKIP obs_publishes_to_our_server ({reason})");
        return Ok(());
    }
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|e| format!("loopback bind failed: {e}"))?;
    let port = listener
        .local_addr()
        .map_err(|e| format!("local_addr failed: {e}"))?
        .port();
    let key = stream_key("obs");
    let config = ObsConfig::new(port, &key)?;

    // OBS writes a lot to stdout; a pipe nobody reads would stall it, so its
    // output goes to files and its log is read on failure.
    let output = |name: &str| -> Result<Stdio> {
        fs::File::create(config.path().join(name))
            .map(Stdio::from)
            .map_err(|e| format!("creating OBS {name}: {e}"))
    };
    let mut obs: Child = Command::new(obs_binary())
        .args([
            "--startstreaming",
            "--multi",
            "--disable-shutdown-check",
            "--profile",
            PROFILE,
            "--collection",
            PROFILE,
        ])
        .env("XDG_CONFIG_HOME", config.path())
        .stdout(output("stdout.log")?)
        .stderr(output("stderr.log")?)
        .spawn()
        .map_err(|e| format!("spawning {} failed: {e}", obs_binary()))?;

    let outcome = timeout(WALL_CLOCK, serve(listener))
        .await
        .unwrap_or_else(|_| Err(format!("timed out after {WALL_CLOCK:?} waiting for OBS")));
    let _ = obs.kill();
    let _ = obs.wait();
    let got = outcome.map_err(|e| {
        let stderr = fs::read_to_string(config.path().join("stderr.log")).unwrap_or_default();
        format!("{e}\nOBS stderr:\n{stderr}\nOBS log:\n{}", config.logs())
    })?;

    assert_eq!(got.app, "live", "OBS must land on the live app");
    assert_eq!(got.stream_key, key, "stream key must survive ingest");
    assert!(
        got.refused.is_empty(),
        "OBS must not need any command refused, refused {:?}",
        got.refused
    );
    for command in ["releaseStream", "FCPublish"] {
        assert!(
            got.setup_commands.iter().any(|c| c == command),
            "OBS sends {command} before createStream, saw {:?}",
            got.setup_commands
        );
    }
    assert!(
        got.encoders
            .iter()
            .any(|e| e.to_ascii_lowercase().contains("obs")),
        "onMetaData encoder must fingerprint OBS, saw {:?}",
        got.encoders
    );
    assert_eq!(
        (got.video[0][0], got.video[0][1]),
        (0x17, 0x00),
        "first video must be the AVC sequence header"
    );
    assert_eq!(
        (got.audio[0][0] & 0xF0, got.audio[0][1]),
        (0xA0, 0x00),
        "first audio must be the AAC sequence header"
    );
    eprintln!(
        "obs harness: {} audio / {} video, encoders={:?}, setup commands={:?}",
        got.audio.len(),
        got.video.len(),
        got.encoders.iter().take(1).collect::<Vec<_>>(),
        got.setup_commands
    );
    Ok(())
}

#[tokio::test]
async fn obs_publishes_to_our_server() {
    run(obs_ingest_body()).await;
}
