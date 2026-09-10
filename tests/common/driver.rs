//! Shared async client driver for the live interop suites: TCP + RTMP
//! handshake + ClientSession against any RTMP server (Red5, our own loopback
//! server, ...).
//!
//! Same sans-I/O glue a proxy uses for its upstream leg
//! (handshake as client, then handle_input in / OutboundResponse out).
//! Every failure says how to fix it.

// Each live suite uses a different slice of this driver (play/collect only in
// red5, publish-only plus loopback in ffmpeg), so per-target builds would
// otherwise warn on the other suite's half.
#![allow(dead_code)]

use std::time::Duration;

use bytes::Bytes;
use rtmpx::amf::AmfEncoding;
use rtmpx::amf0::Amf0Object;
use rtmpx::handshake::{Handshake, HandshakeProcessResult, PeerType};
use rtmpx::sessions::{
    ClientSession, ClientSessionConfig, ClientSessionEvent, ClientSessionResult,
    PublishRequestType, StreamMetadata,
};
use rtmpx::time::RtmpTimestamp;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;

use super::Result;

/// How to reach the server under test: address, app, and per-operation
/// timeout. Suites that need a start hint (Red5 via docker compose) set
/// unreachable_hint; loopback endpoints leave it None.
pub struct Endpoint {
    pub addr: String,
    pub app: String,
    pub op_timeout: Duration,
    pub unreachable_hint: Option<String>,
}

impl Endpoint {
    pub fn tc_url(&self) -> String {
        format!("rtmp://{}/{}", self.addr, self.app)
    }

    fn connect_error(&self, what: String) -> String {
        match &self.unreachable_hint {
            Some(hint) => format!("{what}\n{hint}"),
            None => what,
        }
    }
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
                        .map_err(|e| format!("write to server failed (peer went away?): {e}"))?;
                }
                ClientSessionResult::RaisedEvent(event) => events.push(event),
                ClientSessionResult::UnhandleableMessageReceived(_) => {}
                #[allow(unreachable_patterns)]
                _ => panic!("unexpected future protocol variant"),
            }
        }
        self.stream
            .flush()
            .await
            .map_err(|e| format!("flush to server failed: {e}"))?;
        Ok(())
    }

    async fn read_results(&mut self, events: &mut Vec<ClientSessionEvent>) -> Result<()> {
        let n = self
            .stream
            .read(&mut self.read_buf)
            .await
            .map_err(|e| format!("read from server failed: {e}"))?;
        if n == 0 {
            return Err("server closed the connection mid-test".to_string());
        }
        let input = self.read_buf[..n].to_vec();
        let results = self
            .session
            .handle_input(&input)
            .map_err(|e| format!("session rejected server bytes: {e:?}"))?;
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
                return Err("server closed the connection during handshake".to_string());
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
                #[allow(unreachable_patterns)]
                _ => panic!("unexpected future protocol variant"),
            }
        }
    }

    /// Connect, handshake, and complete connect -> _result. Returns the peer
    /// plus the server's _result command object and status properties, so
    /// tests can assert on objectEncoding and capability echoes.
    pub async fn connect(
        endpoint: &Endpoint,
        encoding: AmfEncoding,
        extra: Amf0Object,
    ) -> Result<(Self, Amf0Object, Amf0Object)> {
        let stream = timeout(endpoint.op_timeout, TcpStream::connect(&endpoint.addr))
            .await
            .map_err(|_| {
                endpoint.connect_error(format!("timed out connecting to {}", endpoint.addr))
            })?
            .map_err(|e| {
                endpoint.connect_error(format!("cannot connect to {}: {e}", endpoint.addr))
            })?;
        stream
            .set_nodelay(true)
            .map_err(|e| format!("set_nodelay failed: {e}"))?;

        let mut config = ClientSessionConfig::new();
        config.object_encoding = encoding;
        config.tc_url = Some(endpoint.tc_url());
        let (session, _) =
            ClientSession::new(config).map_err(|e| format!("session init failed: {e:?}"))?;
        let mut peer = Self {
            stream,
            session,
            read_buf: vec![0u8; 16 * 1024],
            op_timeout: endpoint.op_timeout,
        };

        timeout(peer.op_timeout, peer.handshake())
            .await
            .map_err(|_| "timed out during RTMP handshake".to_string())??;

        let request = peer
            .session
            .request_connection_with_properties(endpoint.app.clone(), extra)
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
                            ..
                        } => {
                            return Ok::<(Amf0Object, Amf0Object), String>((
                                command_object,
                                additional_properties,
                            ));
                        }
                        ClientSessionEvent::ConnectionRequestRejected { description, .. } => {
                            return Err(format!("server rejected connect: {description}"));
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
                    if matches!(event, ClientSessionEvent::PublishRequestAccepted { .. }) {
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
                    if matches!(event, ClientSessionEvent::PlaybackRequestAccepted { .. }) {
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
            .publish_data(rtmpx::sessions::DataMessage::new(
                rtmpx::sessions::DataMessageType::Amf3,
                RtmpTimestamp::new(timestamp),
                body,
            ))
            .map_err(|e| format!("publish_data failed: {e:?}"))?;
        self.write_results(vec![result], &mut Vec::new()).await
    }

    /// Read until at least the wanted media arrived. Live apps typically only
    /// relay media published AFTER the player subscribed, so subscribe the
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
                "timed out waiting for server to relay {want_video} video / {want_audio} audio / meta={want_meta}"
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
