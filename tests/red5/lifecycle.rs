//! Multiple stream lifecycles over one TCP connection, using the public pull API.
use super::{fixtures::*, harness::red5_endpoint};
use crate::common::{Result, legacy_metadata, run, stream_key};
use bytes::{Bytes, BytesMut};
use rtmpx::{
    AmfEncoding, DropPolicy, Packet,
    handshake::HandshakeRole,
    sessions::{
        ClientEvent, ClientOutput, ClientSession, ClientSessionConfig, ConnectionState,
        PublishMode, StreamHandle,
    },
    time::RtmpTimestamp,
};
use std::{collections::VecDeque, net::SocketAddr, time::Duration};
use tokio::{
    net::TcpStream,
    time::{sleep, timeout},
};

#[path = "../../examples/support/io.rs"]
mod transport;

fn error(e: impl std::fmt::Display) -> String {
    e.to_string()
}

struct Peer {
    socket: TcpStream,
    session: ClientSession,
    input: Bytes,
    buffer: BytesMut,
    events: VecDeque<ClientEvent>,
    deadline: Duration,
    address: SocketAddr,
    connect_events: usize,
}
impl Peer {
    async fn connect(encoding: AmfEncoding) -> Result<Self> {
        let endpoint = red5_endpoint();
        let mut socket = timeout(endpoint.op_timeout, TcpStream::connect(&endpoint.addr))
            .await
            .map_err(error)?
            .map_err(error)?;
        socket.set_nodelay(true).map_err(error)?;
        let address = socket.local_addr().map_err(error)?;
        let input = timeout(
            endpoint.op_timeout,
            transport::handshake(&mut socket, HandshakeRole::Client),
        )
        .await
        .map_err(error)?
        .map_err(error)?;
        let session = ClientSession::new(ClientSessionConfig {
            object_encoding: encoding,
            tc_url: Some(endpoint.tc_url()),
            ..Default::default()
        })
        .map_err(error)?;
        let mut peer = Self {
            socket,
            session,
            input,
            buffer: BytesMut::new(),
            events: VecDeque::new(),
            deadline: endpoint.op_timeout,
            address,
            connect_events: 0,
        };
        peer.session.connect(&endpoint.app).map_err(error)?;
        peer.flush().await?;
        timeout(peer.deadline, async {
            loop {
                match peer.next_event().await? {
                    ClientEvent::ConnectionRequestAccepted { .. } => return Ok::<(), String>(()),
                    ClientEvent::ConnectionRequestRejected { description, .. } => {
                        return Err(description);
                    }
                    _ => {}
                }
            }
        })
        .await
        .map_err(error)??;
        assert_eq!(peer.session.negotiated_encoding(), encoding);
        Ok(peer)
    }
    async fn write(&mut self, packet: Packet) -> Result<()> {
        timeout(
            self.deadline,
            transport::write_packet(&mut self.socket, packet),
        )
        .await
        .map_err(error)?
        .map_err(error)
    }
    async fn flush(&mut self) -> Result<()> {
        while let Some(output) = self.session.receive(&mut self.input).map_err(error)? {
            match output {
                ClientOutput::Packet(packet) => self.write(packet).await?,
                ClientOutput::Event(event) => {
                    if matches!(event, ClientEvent::ConnectionRequestAccepted { .. }) {
                        self.connect_events += 1;
                    }
                    self.events.push_back(event);
                }
                _ => {}
            }
        }
        Ok(())
    }
    async fn next_event(&mut self) -> Result<ClientEvent> {
        loop {
            if let Some(event) = self.events.pop_front() {
                return Ok(event);
            }
            self.input = transport::read_input(&mut self.socket, &mut self.buffer)
                .await
                .map_err(error)?
                .ok_or("Red5 closed the existing connection")?;
            self.flush().await?;
        }
    }
    async fn started(&mut self, stream: StreamHandle, publishing: bool) -> Result<()> {
        self.flush().await?;
        timeout(self.deadline, async {
            loop {
                match self.next_event().await? {
                    ClientEvent::PublishRequestAccepted { stream: h, .. }
                        if publishing && h == stream =>
                    {
                        return Ok(());
                    }
                    ClientEvent::PlaybackRequestAccepted { stream: h, .. }
                        if !publishing && h == stream =>
                    {
                        return Ok(());
                    }
                    ClientEvent::PublishRequestRejected { status, .. }
                    | ClientEvent::PlaybackRequestRejected { status, .. } => {
                        return Err(format!("Red5 rejected stream: {status:?}"));
                    }
                    _ => {}
                }
            }
        })
        .await
        .map_err(error)?
    }
    async fn publish(&mut self, key: &str) -> Result<StreamHandle> {
        let stream = self
            .session
            .publish(key, PublishMode::Live)
            .map_err(error)?;
        self.started(stream, true).await?;
        Ok(stream)
    }
    async fn play(&mut self, key: &str) -> Result<StreamHandle> {
        let stream = self.session.play(key).map_err(error)?;
        self.started(stream, false).await?;
        Ok(stream)
    }
    // Leave deletion queued deliberately: the next play/publish must preserve wire order.
    fn delete(&mut self, stream: StreamHandle) -> Result<()> {
        self.session.delete_stream(stream).map_err(error)?;
        assert_eq!(self.session.stream_state(stream), None);
        assert_eq!(self.session.state(), ConnectionState::Connected);
        Ok(())
    }
    async fn send_sequence(
        &mut self,
        stream: StreamHandle,
        marker: u8,
    ) -> Result<(Vec<Bytes>, Vec<Bytes>)> {
        let metadata = self
            .session
            .send_metadata(stream, &legacy_metadata())
            .map_err(error)?;
        self.write(metadata).await?;
        let mut video = vec![avc_sequence_header()];
        let mut audio = vec![aac_sequence_header()];
        for i in 0..6 {
            video.push(avc_coded_frame(marker + i));
            audio.push(aac_raw_frame(marker + i));
        }
        for (i, (v, a)) in video.iter().zip(&audio).enumerate() {
            let t = RtmpTimestamp::new(i as u32 * 40);
            let packet = self
                .session
                .send_video(stream, v.clone(), t, DropPolicy::Never)
                .map_err(error)?;
            self.write(packet).await?;
            let packet = self
                .session
                .send_audio(stream, a.clone(), t, DropPolicy::Never)
                .map_err(error)?;
            self.write(packet).await?;
            if i != 6 {
                sleep(Duration::from_millis(40)).await;
            }
        }
        Ok((video, audio))
    }
    async fn collect(
        &mut self,
        stream: StreamHandle,
        expected: (Vec<Bytes>, Vec<Bytes>),
    ) -> Result<()> {
        timeout(self.deadline, async {
            let (mut video, mut audio) = (Vec::new(), Vec::new());
            while video.len() < expected.0.len() || audio.len() < expected.1.len() {
                match self.next_event().await? {
                    ClientEvent::VideoDataReceived {
                        stream: h, data, ..
                    } => {
                        assert_eq!(h, stream);
                        video.push(data.into_bytes());
                    }
                    ClientEvent::AudioDataReceived {
                        stream: h, data, ..
                    } => {
                        assert_eq!(h, stream);
                        audio.push(data.into_bytes());
                    }
                    ClientEvent::PlaybackRequestRejected { status, .. } => {
                        return Err(format!("playback rejected: {status:?}"));
                    }
                    _ => {}
                }
            }
            assert_eq!(video, expected.0, "video after stream transition");
            assert_eq!(audio, expected.1, "audio after stream transition");
            Ok(())
        })
        .await
        .map_err(error)?
    }
    fn assert_same_connection(&self) {
        assert_eq!(self.socket.local_addr().unwrap(), self.address);
        assert_eq!(self.connect_events, 1);
        assert_eq!(self.session.state(), ConnectionState::Connected);
        assert_eq!(self.session.streams().count(), 1);
    }
}

async fn lifecycle(encoding: AmfEncoding) -> Result<()> {
    let mut primary = Peer::connect(encoding).await?;
    let mut counterpart = Peer::connect(encoding).await?;
    let keys = [
        stream_key("lifecycle-first"),
        stream_key("lifecycle-second"),
        stream_key("lifecycle-third"),
    ];
    let first = primary.publish(&keys[0]).await?;
    let observer = counterpart.play(&keys[0]).await?;
    let expected = primary.send_sequence(first, 0x20).await?;
    counterpart.collect(observer, expected).await?;

    counterpart.delete(observer)?;
    let source = counterpart.publish(&keys[1]).await?;
    primary.delete(first)?;
    let playback = primary.play(&keys[1]).await?;
    assert_ne!(first, playback);
    let expected = counterpart.send_sequence(source, 0x40).await?;
    primary.collect(playback, expected).await?;

    primary.delete(playback)?;
    let last = primary.publish(&keys[2]).await?;
    counterpart.delete(source)?;
    let final_observer = counterpart.play(&keys[2]).await?;
    assert_ne!(first, last);
    assert_ne!(playback, last);
    let expected = primary.send_sequence(last, 0x60).await?;
    counterpart.collect(final_observer, expected).await?;
    primary.assert_same_connection();
    counterpart.assert_same_connection();
    primary.delete(last)?;
    primary.flush().await?;
    counterpart.delete(final_observer)?;
    counterpart.flush().await?;
    eprintln!(
        "Red5 {encoding:?}: publish -> delete -> play -> delete -> publish, three byte-exact media phases on one connection"
    );
    Ok(())
}
#[tokio::test]
async fn one_connection_stream_lifecycle_amf0() {
    run(lifecycle(AmfEncoding::Amf0)).await;
}
#[tokio::test]
async fn one_connection_stream_lifecycle_amf3() {
    run(lifecycle(AmfEncoding::Amf3)).await;
}
