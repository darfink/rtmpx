//! Fixture adapter for the historical wire corpus. Not part of the library API.
//! It coalesces outputs so existing protocol assertions remain easy to compare.
#![allow(dead_code, unused_imports)]
pub use rtmpx::*;
pub mod chunk_io {
    pub use rtmpx::chunk_io::*;
    pub struct ChunkEncoder(rtmpx::chunk_io::ChunkEncoder);
    impl std::ops::Deref for ChunkEncoder {
        type Target = rtmpx::chunk_io::ChunkEncoder;
        fn deref(&self) -> &Self::Target {
            &self.0
        }
    }
    impl std::ops::DerefMut for ChunkEncoder {
        fn deref_mut(&mut self) -> &mut Self::Target {
            &mut self.0
        }
    }
    impl ChunkEncoder {
        pub fn new() -> Self {
            Self(rtmpx::chunk_io::ChunkEncoder::new())
        }
        pub fn serialize<D: crate::api::Segments + Clone + Into<crate::api::Payload>>(
            &mut self,
            message: &crate::api::messages::RawMessage<D>,
            full: bool,
            drop: bool,
        ) -> Result<Packet, EncodeError> {
            self.0.encode(
                message.clone().map_data(Into::into),
                EncodeOptions {
                    headers: if full {
                        HeaderMode::Full
                    } else {
                        HeaderMode::Compressed
                    },
                    drop_policy: if drop {
                        DropPolicy::Allowed
                    } else {
                        DropPolicy::Never
                    },
                },
            )
        }
        pub fn set_chunk_size(
            &mut self,
            size: u32,
            time: crate::api::time::RtmpTimestamp,
        ) -> Result<Packet, EncodeError> {
            self.0.set_chunk_size(size, time)
        }
    }
}
pub mod sessions {
    use bytes::Bytes;
    pub use rtmpx::sessions::*;
    use rtmpx::{DropPolicy, Packet, Payload, amf0::Amf0Object, time::RtmpTimestamp};
    pub type DataMessage<D = Bytes> = rtmpx::sessions::DataMessage<D>;
    pub type ServerSessionEvent<D = Bytes> = ServerEvent<D>;
    pub type ClientSessionEvent<D = Bytes> = ClientEvent<D>;
    pub type ServerSessionResult<D = Bytes> = ServerOutput<D>;
    pub type ClientSessionResult<D = Bytes> = ClientOutput<D>;
    pub struct ServerSession(rtmpx::sessions::ServerSession);
    pub struct ClientSession(rtmpx::sessions::ClientSession);
    macro_rules! shared {
        ($session:ident,$output:ident,$error:ident,$config:ident) => {
            impl std::ops::Deref for $session {
                type Target = rtmpx::sessions::$session;
                fn deref(&self) -> &Self::Target {
                    &self.0
                }
            }
            impl std::ops::DerefMut for $session {
                fn deref_mut(&mut self) -> &mut Self::Target {
                    &mut self.0
                }
            }
            impl $session {
                pub fn new(config: $config) -> Result<(Self, Vec<$output<Bytes>>), $error> {
                    Ok((Self(rtmpx::sessions::$session::new(config)?), Vec::new()))
                }
                pub fn into_inner(self) -> rtmpx::sessions::$session {
                    self.0
                }
                fn drain(&mut self) -> Result<Vec<$output<Bytes>>, $error> {
                    let mut out = Vec::new();
                    let mut input = Bytes::new();
                    while let Some(output) = self.0.receive(&mut input)? {
                        out.push(output.map_payload(Payload::into_bytes));
                    }
                    Ok(out)
                }
                pub fn handle_bytes(
                    &mut self,
                    mut input: Bytes,
                    mut emit: impl FnMut($output<Payload>),
                ) -> Result<(), $error> {
                    while let Some(output) = self.0.receive(&mut input)? {
                        emit(output);
                    }
                    Ok(())
                }
            }
        };
    }
    shared!(
        ServerSession,
        ServerOutput,
        ServerSessionError,
        ServerSessionConfig
    );
    shared!(
        ClientSession,
        ClientOutput,
        ClientSessionError,
        ClientSessionConfig
    );
    impl ServerSession {
        fn handle(&self, id: StreamId) -> Result<StreamHandle, ServerSessionError> {
            if self.is_failed() {
                return Err(ServerSessionError::SessionFailed);
            }
            self.0
                .streams()
                .find(|(_, wire)| *wire == id)
                .map(|(h, _)| h)
                .ok_or(ServerSessionError::InvalidStreamHandle)
        }
        pub fn send_metadata(
            &mut self,
            id: StreamId,
            data: &StreamMetadata,
        ) -> Result<Packet, ServerSessionError> {
            self.0.send_metadata(self.handle(id)?, data)
        }
        pub fn complete_playback(&mut self, id: StreamId) -> Result<Packet, ServerSessionError> {
            self.0.complete_playback(self.handle(id)?)
        }

        pub fn handle_input(
            &mut self,
            bytes: &[u8],
        ) -> Result<Vec<ServerSessionResult>, ServerSessionError> {
            let mut input = Bytes::copy_from_slice(bytes);
            let mut out = Vec::new();
            while let Some(output) = self.0.receive(&mut input)? {
                let output = output.map_payload(Payload::into_bytes);
                out.push(output);
            }
            Ok(out)
        }
        pub fn accept_request(
            &mut self,
            id: RequestId,
        ) -> Result<Vec<ServerSessionResult>, ServerSessionError> {
            self.0.accept_request(id)?;
            self.drain()
        }
        pub fn accept_request_with_properties(
            &mut self,
            id: RequestId,
            p: Amf0Object,
        ) -> Result<Vec<ServerSessionResult>, ServerSessionError> {
            self.0.accept_request_with_properties(id, p)?;
            self.drain()
        }
        pub fn reject_request(
            &mut self,
            id: RequestId,
            code: &str,
            description: &str,
        ) -> Result<Vec<ServerSessionResult>, ServerSessionError> {
            self.0.reject_request(id, code, description)?;
            self.drain()
        }
        pub fn send_video_data(
            &mut self,
            id: StreamId,
            data: Bytes,
            time: RtmpTimestamp,
            drop: bool,
        ) -> Result<Packet, ServerSessionError> {
            self.0
                .send_video(self.handle(id)?, data, time, drop_policy(drop))
        }
        pub fn send_audio_data(
            &mut self,
            id: StreamId,
            data: Bytes,
            time: RtmpTimestamp,
            drop: bool,
        ) -> Result<Packet, ServerSessionError> {
            self.0
                .send_audio(self.handle(id)?, data, time, drop_policy(drop))
        }
        pub fn send_data(
            &mut self,
            id: StreamId,
            data: DataMessage,
        ) -> Result<Packet, ServerSessionError> {
            self.0.send_data(self.handle(id)?, data)
        }
    }
    pub fn metadata<D: rtmpx::Segments>(message: &DataMessage<D>) -> StreamMetadata {
        let mut metadata = StreamMetadata::default();
        metadata.apply_metadata_values(message.metadata().unwrap().unwrap());
        metadata
    }
    fn drop_policy(drop: bool) -> DropPolicy {
        if drop {
            DropPolicy::Allowed
        } else {
            DropPolicy::Never
        }
    }
    impl ClientSession {
        fn handle(&self) -> Result<StreamHandle, ClientSessionError> {
            self.0
                .streams()
                .next()
                .map(|(h, _)| h)
                .ok_or(ClientSessionError::InvalidStreamHandle)
        }
        pub fn active_stream_id(&self) -> Option<StreamId> {
            self.handle().ok().and_then(|h| self.0.stream_id(h))
        }

        pub fn handle_input(
            &mut self,
            bytes: &[u8],
        ) -> Result<Vec<ClientSessionResult>, ClientSessionError> {
            let mut input = Bytes::copy_from_slice(bytes);
            let mut out = Vec::new();
            while let Some(output) = self.0.receive(&mut input)? {
                let output = output.map_payload(Payload::into_bytes);
                out.push(output);
            }
            Ok(out)
        }
        pub fn request_connection(
            &mut self,
            app: String,
        ) -> Result<ClientSessionResult, ClientSessionError> {
            self.0.connect(app)?;
            Ok(self.drain()?.remove(0))
        }
        pub fn request_connection_with_properties(
            &mut self,
            app: String,
            p: Amf0Object,
        ) -> Result<ClientSessionResult, ClientSessionError> {
            self.0.connect_with_properties(app, p)?;
            Ok(self.drain()?.remove(0))
        }
        pub fn request_publishing(
            &mut self,
            key: String,
            kind: PublishMode,
        ) -> Result<ClientSessionResult, ClientSessionError> {
            self.0.publish(key, kind)?;
            Ok(self.drain()?.remove(0))
        }
        pub fn request_playback(
            &mut self,
            key: String,
        ) -> Result<ClientSessionResult, ClientSessionError> {
            self.0.play(key)?;
            Ok(self.drain()?.remove(0))
        }
        pub fn stop_publishing(&mut self) -> Result<Vec<ClientSessionResult>, ClientSessionError> {
            if let Ok(h) = self.handle() {
                self.0.delete_stream(h)?;
            }
            self.drain()
        }
        pub fn stop_playback(&mut self) -> Result<Vec<ClientSessionResult>, ClientSessionError> {
            if let Ok(h) = self.handle() {
                self.0.delete_stream(h)?;
            }
            self.drain()
        }
        pub fn publish_video_data(
            &mut self,
            data: Bytes,
            time: RtmpTimestamp,
            drop: bool,
        ) -> Result<ClientSessionResult, ClientSessionError> {
            Ok(ClientOutput::Packet(self.0.send_video(
                self.handle()?,
                data,
                time,
                drop_policy(drop),
            )?))
        }
        pub fn publish_audio_data(
            &mut self,
            data: Bytes,
            time: RtmpTimestamp,
            drop: bool,
        ) -> Result<ClientSessionResult, ClientSessionError> {
            Ok(ClientOutput::Packet(self.0.send_audio(
                self.handle()?,
                data,
                time,
                drop_policy(drop),
            )?))
        }
        pub fn publish_data(
            &mut self,
            data: DataMessage,
        ) -> Result<ClientSessionResult, ClientSessionError> {
            Ok(ClientOutput::Packet(
                self.0.send_data(self.handle()?, data)?,
            ))
        }
        pub fn publish_metadata(
            &mut self,
            data: &StreamMetadata,
        ) -> Result<ClientSessionResult, ClientSessionError> {
            Ok(ClientOutput::Packet(
                self.0.send_metadata(self.handle()?, data)?,
            ))
        }
    }
}
