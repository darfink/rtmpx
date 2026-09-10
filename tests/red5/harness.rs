//! Shared async driver: TCP + RTMP handshake + ClientSession against Red5.
//!
//! Same sans-I/O glue a proxy uses for its upstream leg
//! (handshake as client, then handle_input in / OutboundResponse out).
//! Every failure says how to fix it.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use rtmpx::amf::AmfEncoding;
use rtmpx::amf0::{Amf0Object, Amf0Value};
use rtmpx::handshake::{Handshake, HandshakeProcessResult, PeerType};
use rtmpx::sessions::{
    ClientSession, ClientSessionConfig, ClientSessionEvent, ClientSessionResult,
    PublishRequestType, StreamMetadata,
};
use rtmpx::time::RtmpTimestamp;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;

pub type Result<T> = std::result::Result<T, String>;

/// How to reach the Red5 under test. Everything honours RED5_* env vars so CI
/// (service container) and local runs (compose file next to this crate) need
/// no code changes.
pub struct Red5 {
    pub addr: String,
    pub app: String,
    pub op_timeout: Duration,
}

impl Red5 {
    pub fn from_env() -> Self {
        let host = std::env::var("RED5_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
        let port: u16 = std::env::var("RED5_PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(1935);
        let app = std::env::var("RED5_APP").unwrap_or_else(|_| "live".to_string());
        let secs: u64 = std::env::var("RED5_TIMEOUT_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(30);
        Self {
            addr: format!("{host}:{port}"),
            app,
            op_timeout: Duration::from_secs(secs),
        }
    }

    pub fn start_hint(&self) -> String {
        format!(
            "Red5 is not reachable at {} (app '{}'). Start it first:
  docker compose -f tests/red5/docker-compose.yml up -d --build",
            self.addr, self.app
        )
    }

    pub fn tc_url(&self) -> String {
        format!("rtmp://{}/{}", self.addr, self.app)
    }
}

static KEY_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Stream key unique across the parallel tests in this process, so tests never
/// see each other's streams on the shared Red5.
pub fn stream_key(tag: &str) -> String {
    let n = KEY_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("rtmpx-{tag}-{}-{n}", std::process::id())
}

/// A connected RTMP client: socket plus the sans-I/O session driving it.
pub struct Peer {
    stream: TcpStream,
    pub session: ClientSession,
    read_buf: Vec<u8>,
    op_timeout: Duration,
}

/// Media observed by a player, in arrival order.
#[derive(Default, Debug)]
pub struct PlayedMedia {
    pub video: Vec<Vec<u8>>,
    pub audio: Vec<Vec<u8>>,
    /// Number of StreamMetadataReceived events.
    pub meta: usize,
}

impl Peer {
    async fn write_results(
        &mut self,
        results: Vec<ClientSessionResult>,
        events: &mut Vec<ClientSessionEvent>,
    ) -> Result<()> {
        for result in results {
            match result {
                ClientSessionResult::OutboundResponse(packet) => {
                    self.stream
                        .write_all(&packet.bytes)
                        .await
                        .map_err(|e| format!("write to Red5 failed (peer went away?): {e}"))?;
                }
                ClientSessionResult::RaisedEvent(event) => events.push(event),
                ClientSessionResult::UnhandleableMessageReceived(_) => {}
            }
        }
        self.stream
            .flush()
            .await
            .map_err(|e| format!("flush to Red5 failed: {e}"))?;
        Ok(())
    }

    async fn read_results(&mut self, events: &mut Vec<ClientSessionEvent>) -> Result<()> {
        let n = self
            .stream
            .read(&mut self.read_buf)
            .await
            .map_err(|e| format!("read from Red5 failed: {e}"))?;
        if n == 0 {
            return Err("Red5 closed the connection mid-test".to_string());
        }
        let input = self.read_buf[..n].to_vec();
        let results = self
            .session
            .handle_input(&input)
            .map_err(|e| format!("session rejected Red5 bytes: {e:?}"))?;
        self.write_results(results, events).await
    }

    async fn handshake(&mut self) -> Result<()> {
        let mut handshake = Handshake::new(PeerType::Client);
        let c0c1 = handshake
            .generate_outbound_p0_and_p1()
            .map_err(|e| format!("handshake init failed: {e:?}"))?;
        self.stream
            .write_all(&c0c1)
            .await
            .map_err(|e| format!("handshake write failed: {e}"))?;
        self.stream
            .flush()
            .await
            .map_err(|e| format!("handshake flush failed: {e}"))?;
        loop {
            let n = self
                .stream
                .read(&mut self.read_buf)
                .await
                .map_err(|e| format!("handshake read failed: {e}"))?;
            if n == 0 {
                return Err("Red5 closed the connection during handshake".to_string());
            }
            match handshake
                .process_bytes(&self.read_buf[..n])
                .map_err(|e| format!("handshake failed: {e:?}"))?
            {
                HandshakeProcessResult::InProgress { response_bytes } => {
                    if !response_bytes.is_empty() {
                        self.stream
                            .write_all(&response_bytes)
                            .await
                            .map_err(|e| format!("handshake response write failed: {e}"))?;
                        self.stream
                            .flush()
                            .await
                            .map_err(|e| format!("handshake response flush failed: {e}"))?;
                    }
                }
                HandshakeProcessResult::Completed {
                    response_bytes,
                    remaining_bytes,
                } => {
                    if !response_bytes.is_empty() {
                        self.stream
                            .write_all(&response_bytes)
                            .await
                            .map_err(|e| format!("handshake final write failed: {e}"))?;
                        self.stream
                            .flush()
                            .await
                            .map_err(|e| format!("handshake final flush failed: {e}"))?;
                    }
                    if !remaining_bytes.is_empty() {
                        let results = self
                            .session
                            .handle_input(&remaining_bytes)
                            .map_err(|e| format!("session rejected post-handshake bytes: {e:?}"))?;
                        self.write_results(results, &mut Vec::new()).await?;
                    }
                    return Ok(());
                }
            }
        }
    }

    /// Connect, handshake, and complete connect -> _result. Returns the peer
    /// plus the server's _result command object and status properties, so
    /// tests can assert on objectEncoding and capability echoes.
    pub async fn connect(
        red5: &Red5,
        encoding: AmfEncoding,
        extra: Amf0Object,
    ) -> Result<(Self, Amf0Object, Amf0Object)> {
        let stream = timeout(red5.op_timeout, TcpStream::connect(&red5.addr))
            .await
            .map_err(|_| {
                format!(
                    "timed out connecting to {}\n{}",
                    red5.addr,
                    red5.start_hint()
                )
            })?
            .map_err(|e| {
                format!(
                    "cannot connect to {}: {e}\n{}",
                    red5.addr,
                    red5.start_hint()
                )
            })?;
        stream
            .set_nodelay(true)
            .map_err(|e| format!("set_nodelay failed: {e}"))?;

        let mut config = ClientSessionConfig::new();
        config.object_encoding = encoding;
        config.tc_url = Some(red5.tc_url());
        let (session, _) =
            ClientSession::new(config).map_err(|e| format!("session init failed: {e:?}"))?;
        let mut peer = Self {
            stream,
            session,
            read_buf: vec![0u8; 16 * 1024],
            op_timeout: red5.op_timeout,
        };

        timeout(peer.op_timeout, peer.handshake())
            .await
            .map_err(|_| "timed out during RTMP handshake".to_string())??;

        let request = peer
            .session
            .request_connection_with_properties(red5.app.clone(), extra)
            .map_err(|e| format!("building connect failed: {e:?}"))?;
        peer.write_results(vec![request], &mut Vec::new()).await?;

        timeout(peer.op_timeout, async {
            loop {
                let mut events = Vec::new();
                peer.read_results(&mut events).await?;
                for event in events {
                    match event {
                        ClientSessionEvent::ConnectionRequestAccepted {
                            command_object,
                            additional_properties,
                        } => {
                            return Ok::<(Amf0Object, Amf0Object), String>((
                                command_object,
                                additional_properties,
                            ));
                        }
                        ClientSessionEvent::ConnectionRequestRejected { description } => {
                            return Err(format!("Red5 rejected connect: {description}"));
                        }
                        _ => {}
                    }
                }
            }
        })
        .await
        .map_err(|_| "timed out waiting for connect _result".to_string())?
        .map(|accepted| {
            let (command_object, additional_properties) = accepted;
            (peer, command_object, additional_properties)
        })
    }

    /// createStream -> publish, waiting for NetStream.Publish.Start.
    pub async fn publish(&mut self, stream_key: &str) -> Result<()> {
        let request = self
            .session
            .request_publishing(stream_key.to_string(), PublishRequestType::Live)
            .map_err(|e| format!("building publish failed: {e:?}"))?;
        self.write_results(vec![request], &mut Vec::new()).await?;
        timeout(self.op_timeout, async {
            loop {
                let mut events = Vec::new();
                self.read_results(&mut events).await?;
                for event in events {
                    if matches!(event, ClientSessionEvent::PublishRequestAccepted) {
                        return Ok::<(), String>(());
                    }
                }
            }
        })
        .await
        .map_err(|_| format!("timed out waiting for publish accept on '{stream_key}'"))?
    }

    /// createStream -> play, waiting for NetStream.Play.Start.
    pub async fn play(&mut self, stream_key: &str) -> Result<()> {
        let request = self
            .session
            .request_playback(stream_key.to_string())
            .map_err(|e| format!("building play failed: {e:?}"))?;
        self.write_results(vec![request], &mut Vec::new()).await?;
        timeout(self.op_timeout, async {
            loop {
                let mut events = Vec::new();
                self.read_results(&mut events).await?;
                for event in events {
                    if matches!(event, ClientSessionEvent::PlaybackRequestAccepted) {
                        return Ok::<(), String>(());
                    }
                }
            }
        })
        .await
        .map_err(|_| format!("timed out waiting for play accept on '{stream_key}'"))?
    }

    pub async fn send_video(&mut self, data: Bytes, timestamp: u32) -> Result<()> {
        let result = self
            .session
            .publish_video_data(data, RtmpTimestamp::new(timestamp), false)
            .map_err(|e| format!("publish_video_data failed: {e:?}"))?;
        self.write_results(vec![result], &mut Vec::new()).await
    }

    pub async fn send_audio(&mut self, data: Bytes, timestamp: u32) -> Result<()> {
        let result = self
            .session
            .publish_audio_data(data, RtmpTimestamp::new(timestamp), false)
            .map_err(|e| format!("publish_audio_data failed: {e:?}"))?;
        self.write_results(vec![result], &mut Vec::new()).await
    }

    pub async fn send_metadata(&mut self, metadata: &StreamMetadata) -> Result<()> {
        let result = self
            .session
            .publish_metadata(metadata)
            .map_err(|e| format!("publish_metadata failed: {e:?}"))?;
        self.write_results(vec![result], &mut Vec::new()).await
    }

    pub async fn send_raw_amf3_data(&mut self, body: Bytes, timestamp: u32) -> Result<()> {
        let result = self
            .session
            .publish_raw_amf3_data_payload(body, RtmpTimestamp::new(timestamp))
            .map_err(|e| format!("publish_raw_amf3_data_payload failed: {e:?}"))?;
        self.write_results(vec![result], &mut Vec::new()).await
    }

    /// Read until at least the wanted media arrived. Red5's live app only
    /// relays media published AFTER the player subscribed, so subscribe the
    /// player first and publish afterwards.
    pub async fn collect(
        &mut self,
        want_video: usize,
        want_audio: usize,
        want_meta: bool,
    ) -> Result<PlayedMedia> {
        timeout(self.op_timeout, async {
            let mut got = PlayedMedia::default();
            loop {
                let mut events = Vec::new();
                self.read_results(&mut events).await?;
                for event in events {
                    match event {
                        ClientSessionEvent::VideoDataReceived { data, .. } => {
                            got.video.push(data.to_vec());
                        }
                        ClientSessionEvent::AudioDataReceived { data, .. } => {
                            got.audio.push(data.to_vec());
                        }
                        ClientSessionEvent::StreamMetadataReceived { .. } => {
                            got.meta += 1;
                        }
                        _ => {}
                    }
                }
                if got.video.len() >= want_video
                    && got.audio.len() >= want_audio
                    && (!want_meta || got.meta >= 1)
                {
                    return Ok::<PlayedMedia, String>(got);
                }
            }
        })
        .await
        .map_err(|_| {
            format!(
                "timed out waiting for Red5 to relay {want_video} video / {want_audio} audio / meta={want_meta}"
            )
        })?
    }

    /// One socket read's worth of raised events. Lets tests observe traffic
    /// that collect() ignores (acks, pings, metadata payloads).
    pub async fn next_events(&mut self) -> Result<Vec<ClientSessionEvent>> {
        timeout(self.op_timeout, async {
            let mut events = Vec::new();
            self.read_results(&mut events).await?;
            Ok::<Vec<ClientSessionEvent>, String>(events)
        })
        .await
        .map_err(|_| "timed out waiting for any server traffic".to_string())?
    }
}

/// Read a numeric objectEncoding out of a _result command object.
pub fn object_encoding_of(command_object: &Amf0Object) -> Option<f64> {
    command_object
        .get("objectEncoding")
        .and_then(Amf0Value::get_number)
}
