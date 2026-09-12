mod config;
mod errors;
mod events;
mod outstanding_transaction;

mod result;

#[cfg(test)]
mod tests;

pub use self::config::ClientSessionConfig;
pub use self::errors::ClientSessionError;
pub use self::events::{ClientEvent, CommandStatus};
pub use self::result::ClientOutput;
use crate::sessions::PublishMode;
type ClientSessionResult<D = Bytes> = ClientOutput<D>;
use crate::sessions::streams::Streams;
use crate::sessions::{ClientStreamState, ConnectionState, StreamHandle};

use self::outstanding_transaction::{OutstandingTransaction, TransactionPurpose};
use crate::amf::AmfEncoding;
use crate::amf0::{Amf0Object, Amf0Value};
use crate::chunk_io::{ChunkEncoder, DecodeError, MessageDecoder, Packet};
use crate::messages::RawMessage;
use crate::messages::{RtmpMessage, UserControlEventType};
use crate::sessions::{
    DataMessage as GenericDataMessage, DataMessageType, StreamId, StreamMetadata,
};
use crate::time::RtmpTimestamp;
use bytes::Bytes;
use std::collections::HashMap;
use std::time::Instant;

type ClientResult = Result<Vec<ClientSessionResult>, ClientSessionError>;

/// A session that represents the client side of a single RTMP connection.
///
/// The `ClientSession` connects to an application on the server and then
/// requests publishing or playback. It also reacts to the events and
/// responses the server sends.
///
/// Each play or publish operation owns an independent stream handle.
/// Several streams can operate concurrently on one connection.
///
/// RTMP chunk headers compress against earlier chunks, so this is required:
///
/// * Pass all bytes received **after** the completed handshake into the
/// `ClientSession` in the order they arrived
/// * Send all responses the session generates to the server **in order**
/// * Pass no extraneous bytes into the session, and send only bytes the
/// session generated to the server
///
/// Any violation of these rules can cause RTMP chunk parsing errors on the
/// `ClientSession` or on the peer.
pub struct ClientSession {
    failed: bool,
    start_time: Instant,
    serializer: ChunkEncoder,
    deserializer: MessageDecoder,
    pending: std::collections::VecDeque<ClientOutput>,
    config: ClientSessionConfig,
    next_transaction_id: u32,
    outstanding_transactions: HashMap<u32, OutstandingTransaction>,
    current_state: ConnectionState,
    streams: Streams<ClientStreamState>,
    connected_app_name: Option<String>,
    peer_window_ack_size: Option<u32>,
    // see the matching field in `sessions/server/mod.rs`.
    self_window_ack_size: Option<u32>,
    bytes_received: u64,
    bytes_received_since_last_ack: u32,
    /// The encoding the server confirmed in its `connect` response, clamped to
    /// what this crate supports.
    negotiated_encoding: AmfEncoding,
    /// True once the server has actually used a type 15/17 message.
    peer_uses_amf3_framing: bool,
}

impl ClientSession {
    /// Whether an input error has permanently terminated this session.
    pub fn is_failed(&self) -> bool {
        self.failed
    }

    fn ensure_active(&self) -> Result<(), ClientSessionError> {
        if self.failed {
            Err(ClientSessionError::SessionFailed)
        } else {
            Ok(())
        }
    }

    /// Creates a new client session with the specified configuration
    ///
    /// The initial output list is empty. Connection negotiation produces the
    /// required flow-control messages after the server accepts the connection.
    pub fn new(config: ClientSessionConfig) -> Result<Self, ClientSessionError> {
        if config.window_ack_size == 0 || config.chunk_size == 0 {
            return Err(DecodeError::ResourceLimitExceeded {
                resource: "client flow-control value",
                attempted: 0,
                maximum: u32::MAX as usize,
            }
            .into());
        }
        let session = ClientSession {
            failed: false,
            start_time: Instant::now(),
            serializer: ChunkEncoder::new(),
            deserializer: {
                let mut decoder = MessageDecoder::with_limits(config.decoder_limits);
                if let Some(pool) = &config.payload_pool {
                    decoder.set_payload_pool(pool.clone());
                }
                decoder
            },
            pending: std::collections::VecDeque::new(),
            next_transaction_id: 1,
            outstanding_transactions: HashMap::new(),
            current_state: ConnectionState::Disconnected,
            streams: Streams::new(),
            connected_app_name: None,
            peer_window_ack_size: None,
            self_window_ack_size: Some(config.window_ack_size),
            bytes_received: 0,
            bytes_received_since_last_ack: 0,
            negotiated_encoding: AmfEncoding::Amf0,
            peer_uses_amf3_framing: false,
            config,
        };

        Ok(session)
    }

    /// Connection state. Each media stream has an independent lifecycle.
    pub fn state(&self) -> ConnectionState {
        self.current_state
    }

    pub fn stream_state(&self, stream: StreamHandle) -> Option<ClientStreamState> {
        self.streams.get(stream).map(|s| s.state)
    }
    pub fn stream_id(&self, stream: StreamHandle) -> Option<StreamId> {
        self.streams.get(stream).and_then(|s| s.wire_id)
    }
    pub fn streams(&self) -> impl Iterator<Item = (StreamHandle, ClientStreamState)> + '_ {
        self.streams.iter().map(|(h, e)| (h, e.state))
    }
    fn publishing_id(&self, stream: StreamHandle) -> Result<u32, ClientSessionError> {
        self.ensure_active()?;
        let entry = self
            .streams
            .get(stream)
            .ok_or(ClientSessionError::InvalidStreamHandle)?;
        if entry.state != ClientStreamState::Publishing {
            return Err(ClientSessionError::StreamInInvalidState {
                stream,
                state: entry.state,
            });
        }
        Ok(entry
            .wire_id
            .expect("publishing stream has a wire ID")
            .get())
    }

    /// The AMF encoding the server confirmed in its `connect` response,
    /// clamped to what this crate supports.
    ///
    /// Mirrors [`crate::sessions::ServerSession::negotiated_encoding`]. The
    /// live Red5 interop suite (`tests/red5`, feature `red5-live`) asserts on
    /// this to prove `objectEncoding` negotiation against an independent peer.
    pub fn negotiated_encoding(&self) -> AmfEncoding {
        self.negotiated_encoding
    }

    /// Recycle payload descriptors after consumers release their messages.
    /// Cloned pools can be shared with other sessions and relay tasks.
    pub fn set_payload_pool(&mut self, pool: crate::PayloadPool) {
        self.deserializer.set_payload_pool(pool);
    }

    /// Return one output without retaining unread transport input.
    /// Call again with the same buffer after handling the output. `None` means more
    /// input is required. Drain pending outputs even when the input buffer is empty.
    /// Errors are terminal; discard the session and unsent packets.
    pub fn receive(
        &mut self,
        input: &mut Bytes,
    ) -> Result<Option<ClientOutput>, ClientSessionError> {
        self.ensure_active()?;
        let result = self.receive_inner(input);
        if result.is_err() {
            self.failed = true;
            self.current_state = ConnectionState::Failed;
            self.pending.clear();
        }
        result
    }
    fn receive_inner(
        &mut self,
        input: &mut Bytes,
    ) -> Result<Option<ClientOutput>, ClientSessionError> {
        if let Some(output) = self.pending.pop_front() {
            return Ok(Some(output));
        }
        loop {
            let before = input.len();
            let payload = self.deserializer.decode(input)?;
            let consumed = before - input.len();
            self.bytes_received = self.bytes_received.wrapping_add(consumed as u64);
            let effective_ack_size = self.peer_window_ack_size.or(self.self_window_ack_size);
            if let Some(peer_ack_size) = effective_ack_size {
                self.bytes_received_since_last_ack = self
                    .bytes_received_since_last_ack
                    .wrapping_add(consumed as u32);
                if self.bytes_received_since_last_ack >= peer_ack_size {
                    // see the matching note in `sessions/server/mod.rs`.
                    // The acknowledgement sequence number is cumulative bytes
                    // received, not bytes since the previous ack. Wraps at 2^32.
                    let ack_message = RtmpMessage::Acknowledgement {
                        sequence_number: self.bytes_received as u32,
                    };
                    let ack_payload = ack_message.into_raw_message(self.get_epoch(), 0)?;
                    let ack_packet = self.serializer.serialize(&ack_payload, false, false)?;

                    self.bytes_received_since_last_ack %= peer_ack_size;
                    self.pending.push_back(ClientOutput::Packet(ack_packet));
                }
            }

            let Some(payload) = payload else {
                return Ok(self.pending.pop_front());
            };
            // Acknowledgements precede outputs caused by the acknowledged message.
            let acknowledgement = self.pending.pop_front();
            let output = self.process_payload(payload)?;
            if let Some(ack) = acknowledgement {
                if let Some(output) = output {
                    self.pending.push_front(output);
                }
                return Ok(Some(ack));
            }
            if output.is_some() {
                return Ok(output);
            }
        }
    }
    fn process_payload(
        &mut self,
        payload: RawMessage<crate::Payload>,
    ) -> Result<Option<ClientOutput>, ClientSessionError> {
        if let Some(wire_type) = DataMessageType::from_type_id(payload.type_id) {
            if wire_type == DataMessageType::Amf3 {
                self.peer_uses_amf3_framing = true;
            }
            let message = DataMessage::new(wire_type, payload.timestamp, payload.data);
            let Some(stream) = self.streams.find(payload.message_stream_id) else {
                return Ok(None);
            };
            let event = ClientSessionEvent::StreamDataReceived { stream, message };
            return Ok(Some(ClientOutput::Event(event)));
        }
        let event = match payload.type_id {
            8 => self.media_event(
                payload.message_stream_id,
                payload.data,
                payload.timestamp,
                false,
            ),
            9 => self.media_event(
                payload.message_stream_id,
                payload.data,
                payload.timestamp,
                true,
            ),
            _ => {
                if !matches!(payload.type_id, 1..=6 | 17 | 20) {
                    return Ok(Some(ClientOutput::UnhandledMessage(payload)));
                }
                let payload = RawMessage {
                    timestamp: payload.timestamp,
                    type_id: payload.type_id,
                    message_stream_id: payload.message_stream_id,
                    data: payload.data.into_bytes(),
                };
                let outputs = self.handle_message(payload)?;
                self.pending.extend(
                    outputs
                        .into_iter()
                        .map(|output| output.map_payload(crate::Payload::from)),
                );
                return Ok(self.pending.pop_front());
            }
        };
        Ok(event.map(ClientOutput::Event))
    }
    fn handle_message(
        &mut self,
        payload: RawMessage,
    ) -> Result<Vec<ClientSessionResult>, ClientSessionError> {
        let message = payload.to_rtmp_message()?;
        let message_results = match message {
            RtmpMessage::Abort { stream_id } => {
                self.deserializer.abort_chunk_stream(stream_id);
                Vec::new()
            }
            RtmpMessage::Acknowledgement { sequence_number } => {
                self.handle_acknowledgement(sequence_number)?
            }

            RtmpMessage::Amf0Command {
                command_name,
                transaction_id,
                command_object,
                additional_arguments,
            } => self.handle_amf0_command(
                payload.message_stream_id,
                command_name,
                transaction_id,
                command_object,
                additional_arguments,
            )?,

            RtmpMessage::Amf3Command {
                command_name,
                transaction_id,
                command_object,
                additional_arguments,
                format: _,
            } => {
                // Same commands, different encoding: project into
                // the AMF0 value model and reuse the one set of
                // handlers rather than maintaining a parallel
                // implementation.
                self.peer_uses_amf3_framing = true;
                self.handle_amf0_command(
                    payload.message_stream_id,
                    command_name,
                    transaction_id,
                    command_object.to_amf0(),
                    additional_arguments.iter().map(|v| v.to_amf0()).collect(),
                )?
            }

            RtmpMessage::Amf0SharedObject { data: _ }
            | RtmpMessage::Amf3SharedObject { data: _ } => {
                vec![ClientSessionResult::UnhandledMessage(payload)]
            }

            RtmpMessage::AudioData { data } => self
                .media_event(payload.message_stream_id, data, payload.timestamp, false)
                .map(ClientSessionResult::Event)
                .into_iter()
                .collect(),

            RtmpMessage::VideoData { data } => self
                .media_event(payload.message_stream_id, data, payload.timestamp, true)
                .map(ClientSessionResult::Event)
                .into_iter()
                .collect(),

            RtmpMessage::UserControl {
                event_type,
                timestamp,
                stream_id,
                buffer_length,
            } => self.handle_user_control(event_type, timestamp, stream_id, buffer_length)?,

            RtmpMessage::WindowAcknowledgement { size } => self.handle_window_ack_size(size)?,

            RtmpMessage::SetChunkSize { size } => self.handle_set_chunk_size(size)?,

            _ => vec![ClientSessionResult::UnhandledMessage(payload)],
        };

        Ok(message_results)
    }

    /// Forms an RTMP message requesting a connection to the specified application on the server.
    /// An event will be raised when the request is accepted or rejected.
    fn request_connection_inner(
        &mut self,
        app_name: String,
    ) -> Result<ClientSessionResult, ClientSessionError> {
        self.ensure_active()?;
        // Delegate to the extensible variant so upstream behaviour is
        // preserved exactly while proxies can add their own connect properties.
        self.request_connection_with_properties_inner(app_name, Amf0Object::new())
    }

    /// Same as `request_connection`, but merges `extra_properties` into the
    /// `connect` command object.
    ///
    /// Added for proxy use. A proxy must forward the publisher's
    /// Enhanced RTMP capability advertisement (`fourCcList`, `capsEx`,
    /// `videoFourCcInfoMap`, `audioFourCcInfoMap`) to the backend ingester.
    /// Without it an OBS 30+ publisher that negotiated HEVC or AV1 silently
    /// falls back to H.264 across the hop.
    ///
    /// Properties the session sets itself (`app`, `flashVer`, `objectEncoding`,
    /// `tcUrl`) take precedence and cannot be overridden, so a malformed or
    /// hostile connect object cannot corrupt our own session state.
    fn request_connection_with_properties_inner(
        &mut self,
        app_name: String,
        extra_properties: Amf0Object,
    ) -> Result<ClientSessionResult, ClientSessionError> {
        self.ensure_active()?;
        match self.current_state {
            ConnectionState::Disconnected => (),
            _ => {
                return Err(ClientSessionError::CantConnectWhileAlreadyConnected);
            }
        }

        self.config
            .session_limits
            .check_requests(self.outstanding_transactions.len())?;
        let transaction_id = self.get_next_transaction_id();
        let transaction = OutstandingTransaction::ConnectionRequested {
            app_name: app_name.clone(),
        };

        // Seed with the caller's properties, then let the session's
        // own values overwrite them.
        let mut properties = extra_properties;
        properties.insert("app".to_string(), Amf0Value::Utf8String(app_name));
        properties.insert(
            "flashVer".to_string(),
            Amf0Value::Utf8String(self.config.flash_version.clone()),
        );
        properties.insert(
            "objectEncoding".to_string(),
            Amf0Value::Number(self.config.object_encoding.as_object_encoding()),
        );

        // Some implementations require a tcUrl to be sent up with the connection request
        match &self.config.tc_url {
            Some(tc_url) => {
                properties.insert("tcUrl".to_string(), Amf0Value::Utf8String(tc_url.clone()));
            }
            None => (),
        };

        let message = RtmpMessage::Amf0Command {
            command_name: "connect".to_string(),
            command_object: Amf0Value::Object(properties),
            additional_arguments: vec![],
            transaction_id: transaction_id as f64,
        };

        let payload = message.into_raw_message(self.get_epoch(), 0)?;
        let packet = self.serializer.serialize(&payload, false, false)?;

        self.outstanding_transactions
            .insert(transaction_id, transaction);
        self.current_state = ConnectionState::Connecting;

        Ok(ClientSessionResult::Packet(packet))
    }

    /// Starts the process of requesting playback on the server for the specified stream key.  An
    /// event will be raised when the request is accepted or rejected.  Once accepted we will
    /// receive audio, video, and metadata information via `ClientSessionEvent`s.
    fn create_stream(
        &mut self,
        purpose: TransactionPurpose,
    ) -> Result<StreamHandle, ClientSessionError> {
        self.ensure_active()?;
        if self.current_state != ConnectionState::Connected {
            return Err(ClientSessionError::SessionInInvalidState {
                current_state: self.current_state,
            });
        }
        self.config
            .session_limits
            .check_streams(self.streams.iter().count())?;
        self.config
            .session_limits
            .check_requests(self.outstanding_transactions.len())?;
        let transaction_id = self.get_next_transaction_id();
        let message = RtmpMessage::Amf0Command {
            command_name: "createStream".into(),
            transaction_id: transaction_id as f64,
            command_object: Amf0Value::Null,
            additional_arguments: Vec::new(),
        };
        let payload = message.into_raw_message(self.get_epoch(), 0)?;
        let packet = self.serializer.serialize(&payload, false, false)?;
        let stream = self.streams.insert(ClientStreamState::Creating, None);
        self.outstanding_transactions.insert(
            transaction_id,
            OutstandingTransaction::CreateStream { stream, purpose },
        );
        self.pending.push_back(ClientOutput::Packet(packet));
        Ok(stream)
    }

    /// Delete a stream or cancel its pending creation. The handle becomes invalid immediately.
    /// A late createStream result is deleted automatically without starting play or publish.
    pub fn delete_stream(&mut self, stream: StreamHandle) -> Result<(), ClientSessionError> {
        self.ensure_active()?;
        let entry = self
            .streams
            .get(stream)
            .ok_or(ClientSessionError::InvalidStreamHandle)?;
        let packet = if let Some(wire_id) = entry.wire_id {
            Some(self.delete_packet(wire_id.get())?)
        } else {
            None
        };
        self.streams.remove(stream);
        if let Some(packet) = packet {
            self.pending.push_back(ClientOutput::Packet(packet));
        }
        Ok(())
    }
    fn delete_packet(&mut self, stream_id: u32) -> Result<Packet, ClientSessionError> {
        let message = RtmpMessage::Amf0Command {
            command_name: "deleteStream".into(),
            transaction_id: 0.0,
            command_object: Amf0Value::Null,
            additional_arguments: vec![Amf0Value::Number(stream_id as f64)],
        };
        let payload = message.into_raw_message(self.get_epoch(), 0)?;
        Ok(self.serializer.serialize(&payload, false, false)?)
    }

    /// Sends a ping request to the server.  An event will be raised when we get a response back
    pub fn send_ping_request(&mut self) -> Result<(Packet, RtmpTimestamp), ClientSessionError> {
        self.ensure_output_drained()?;
        self.ensure_active()?;
        let current_epoch = self.get_epoch();
        let message = RtmpMessage::UserControl {
            event_type: UserControlEventType::PingRequest,
            buffer_length: None,
            stream_id: None,
            timestamp: Some(current_epoch.clone()),
        };

        let payload = message.into_raw_message(self.get_epoch(), 0)?;
        let packet = self.serializer.serialize(&payload, false, false)?;
        Ok((packet, current_epoch))
    }

    /// If publishing, this allows us to send encoder metadata to the server to send to all
    /// players.
    fn publish_metadata_inner(
        &mut self,
        stream: StreamHandle,
        metadata: &StreamMetadata,
    ) -> Result<ClientSessionResult, ClientSessionError> {
        self.ensure_active()?;
        let active_stream_id = self.publishing_id(stream)?;

        let mut properties = Amf0Object::new();
        if let Some(x) = metadata.video_width {
            properties.insert("width".to_string(), Amf0Value::Number(x as f64));
        }

        if let Some(x) = metadata.video_height {
            properties.insert("height".to_string(), Amf0Value::Number(x as f64));
        }

        if let Some(x) = metadata.video_codec_id {
            properties.insert("videocodecid".to_string(), Amf0Value::Number(x as f64));
        }

        if let Some(x) = metadata.video_frame_rate {
            properties.insert("framerate".to_string(), Amf0Value::Number(x as f64));
        }

        if let Some(x) = metadata.video_bitrate_kbps {
            properties.insert("videodatarate".to_string(), Amf0Value::Number(x as f64));
        }

        if let Some(x) = metadata.audio_codec_id {
            properties.insert("audiocodecid".to_string(), Amf0Value::Number(x as f64));
        }

        if let Some(x) = metadata.audio_bitrate_kbps {
            properties.insert("audiodatarate".to_string(), Amf0Value::Number(x as f64));
        }

        if let Some(x) = metadata.audio_sample_rate {
            properties.insert("audiosamplerate".to_string(), Amf0Value::Number(x as f64));
        }

        if let Some(x) = metadata.audio_channels {
            properties.insert("audiochannels".to_string(), Amf0Value::Number(x as f64));
        }

        if let Some(x) = metadata.audio_is_stereo {
            properties.insert("stereo".to_string(), Amf0Value::Boolean(x));
        }

        if let Some(ref x) = metadata.encoder {
            properties.insert("encoder".to_string(), Amf0Value::Utf8String(x.clone()));
        }

        let message = RtmpMessage::Amf0Data {
            values: vec![
                Amf0Value::Utf8String("@setDataFrame".to_string()),
                Amf0Value::Utf8String("onMetaData".to_string()),
                Amf0Value::Object(properties),
            ],
        };

        let payload = message.into_raw_message(self.get_epoch(), active_stream_id)?;
        let packet = self.serializer.serialize(&payload, false, false)?;

        Ok(ClientSessionResult::Packet(packet))
    }

    fn media_event<D>(
        &self,
        stream_id: u32,
        data: D,
        timestamp: RtmpTimestamp,
        video: bool,
    ) -> Option<ClientSessionEvent<D>> {
        let stream = self.streams.find(stream_id)?;
        let state = self.streams.get(stream)?.state;
        if !matches!(
            state,
            ClientStreamState::StartingPlayback | ClientStreamState::Playing
        ) {
            return None;
        }
        Some(if video {
            ClientSessionEvent::VideoDataReceived {
                stream,
                data,
                timestamp,
            }
        } else {
            ClientSessionEvent::AudioDataReceived {
                stream,
                data,
                timestamp,
            }
        })
    }

    fn handle_amf0_command(
        &mut self,
        stream_id: u32,
        name: String,
        transaction_id: f64,
        command_object: Amf0Value,
        additional_args: Vec<Amf0Value>,
    ) -> ClientResult {
        match name.as_str() {
            "_result" => self.handle_amf0_command_success_result(
                transaction_id,
                command_object,
                additional_args,
            ),
            "_error" => self.handle_amf0_command_failed_result(
                transaction_id,
                command_object,
                additional_args,
            ),
            "onStatus" => self.handle_on_status_command(stream_id, additional_args),

            _ => {
                let event = ClientSessionEvent::UnhandledCommand {
                    stream_id: StreamId::new(stream_id),
                    command_name: name,
                    additional_values: additional_args,
                    command_object,
                    transaction_id,
                };

                Ok(vec![ClientSessionResult::Event(event)])
            }
        }
    }

    fn handle_amf0_command_failed_result(
        &mut self,
        transaction_id: f64,
        command_object: Amf0Value,
        additional_args: Vec<Amf0Value>,
    ) -> ClientResult {
        let outstanding_transaction = match self
            .outstanding_transactions
            .remove(&(transaction_id as u32))
        {
            Some(transaction) => transaction,
            None => {
                let event = ClientSessionEvent::UnknownTransactionResultReceived {
                    additional_values: additional_args,
                    command_object,
                    transaction_id,
                };

                return Ok(vec![ClientSessionResult::Event(event)]);
            }
        };

        let properties = additional_args
            .into_iter()
            .next()
            .and_then(|v| v.get_object_properties())
            .unwrap_or_default();
        let status = CommandStatus::new(None, properties);
        let event = match outstanding_transaction {
            OutstandingTransaction::ConnectionRequested { .. } => {
                self.current_state = ConnectionState::Disconnected;
                ClientSessionEvent::ConnectionRequestRejected {
                    description: status.description().unwrap_or("").to_string(),
                    status,
                }
            }
            OutstandingTransaction::CreateStream { stream, purpose } => {
                if self.streams.remove(stream).is_none() {
                    return Ok(Vec::new());
                }
                match purpose {
                    TransactionPurpose::PlayRequest { .. } => {
                        ClientSessionEvent::PlaybackRequestRejected { stream, status }
                    }
                    TransactionPurpose::PublishRequest { .. } => {
                        ClientSessionEvent::PublishRequestRejected { stream, status }
                    }
                }
            }
        };
        Ok(vec![ClientSessionResult::Event(event)])
    }

    fn handle_amf0_command_success_result(
        &mut self,
        transaction_id: f64,
        command_object: Amf0Value,
        additional_args: Vec<Amf0Value>,
    ) -> ClientResult {
        let outstanding_transaction = match self
            .outstanding_transactions
            .remove(&(transaction_id as u32))
        {
            Some(transaction) => transaction,
            None => {
                let event = ClientSessionEvent::UnknownTransactionResultReceived {
                    additional_values: additional_args,
                    command_object,
                    transaction_id,
                };

                return Ok(vec![ClientSessionResult::Event(event)]);
            }
        };

        match outstanding_transaction {
            OutstandingTransaction::ConnectionRequested { app_name } => {
                self.current_state = ConnectionState::Connected;
                self.connected_app_name = Some(app_name);

                let message = RtmpMessage::WindowAcknowledgement {
                    size: self.config.window_ack_size,
                };
                let payload = message.into_raw_message(self.get_epoch(), 0)?;
                let packet = self.serializer.serialize(&payload, false, false)?;
                let command_object = match command_object {
                    Amf0Value::Object(properties) => properties,
                    _ => Amf0Object::new(),
                };
                let additional_properties = additional_args
                    .into_iter()
                    .next()
                    .and_then(|value| match value {
                        Amf0Value::Object(properties) => Some(properties),
                        _ => None,
                    })
                    .unwrap_or_default();
                // The server's response is the authoritative half of the
                // `objectEncoding` handshake: it states the encoding that will
                // actually be used, which may be lower than what we asked for.
                // It is clamped against our own request as well, so a server
                // answering with more than we asked for cannot push us into an
                // encoding we did not opt into.
                self.negotiated_encoding = AmfEncoding::negotiate(
                    additional_properties
                        .get("objectEncoding")
                        .and_then(|value| value.get_number())
                        .map(AmfEncoding::from_object_encoding)
                        .unwrap_or_default(),
                    self.config.object_encoding,
                );
                let event = ClientSessionEvent::ConnectionRequestAccepted {
                    command_object,
                    additional_properties,
                };

                let chunk_size_packet = self
                    .serializer
                    .set_chunk_size(self.config.chunk_size, RtmpTimestamp::new(0))?;

                Ok(vec![
                    ClientSessionResult::Packet(packet),
                    ClientSessionResult::Event(event),
                    ClientSessionResult::Packet(chunk_size_packet),
                ])
            }

            OutstandingTransaction::CreateStream { stream, purpose } => {
                if additional_args.len() == 0 {
                    return Err(ClientSessionError::CreateStreamResponseHadNoStreamNumber);
                }

                let stream_id = match additional_args[0] {
                    Amf0Value::Number(number)
                        if number.is_finite()
                            && number.fract() == 0.0
                            && number >= 1.0
                            && number <= u32::MAX as f64 =>
                    {
                        number as u32
                    }
                    _ => {
                        return Err(ClientSessionError::CreateStreamResponseHadNoStreamNumber);
                    }
                };

                if self.streams.find(stream_id).is_some() {
                    return Err(ClientSessionError::DuplicateStreamId);
                }
                if self.streams.get(stream).is_none() {
                    let packet = self.delete_packet(stream_id)?;
                    return Ok(vec![ClientSessionResult::Packet(packet)]);
                }
                self.streams.bind(stream, StreamId::new(stream_id));

                match purpose {
                    TransactionPurpose::PlayRequest { stream_key } => {
                        self.streams.get_mut(stream).unwrap().state =
                            ClientStreamState::StartingPlayback;

                        let buffer_message = RtmpMessage::UserControl {
                            event_type: UserControlEventType::SetBufferLength,
                            buffer_length: Some(self.config.playback_buffer_length_ms),
                            stream_id: Some(stream_id),
                            timestamp: None,
                        };

                        let buffer_payload =
                            buffer_message.into_raw_message(self.get_epoch(), 0)?;
                        let buffer_packet =
                            self.serializer.serialize(&buffer_payload, false, false)?;

                        let play_message = RtmpMessage::Amf0Command {
                            command_name: "play".to_string(),
                            transaction_id: 0.0,
                            command_object: Amf0Value::Null,
                            // Always send the `start` argument (-2 = live first, then
                            // recorded, the RTMP default). Red5 silently ignores a
                            // single-argument `play`; ffmpeg and other players always
                            // include `start`, so mirror that for interop.
                            additional_arguments: vec![
                                Amf0Value::Utf8String(stream_key),
                                Amf0Value::Number(-2.0),
                            ],
                        };

                        let play_payload =
                            play_message.into_raw_message(self.get_epoch(), stream_id)?;
                        let play_packet = self.serializer.serialize(&play_payload, false, false)?;

                        Ok(vec![
                            ClientSessionResult::Packet(buffer_packet),
                            ClientSessionResult::Packet(play_packet),
                        ])
                    }

                    TransactionPurpose::PublishRequest {
                        stream_key,
                        request_type,
                    } => {
                        self.streams.get_mut(stream).unwrap().state =
                            ClientStreamState::StartingPublish;

                        let publish_type_string = match request_type {
                            PublishMode::Live => "live".to_string(),
                            PublishMode::Record => "record".to_string(),
                            PublishMode::Append => "append".to_string(),
                        };

                        let publish_message = RtmpMessage::Amf0Command {
                            command_name: "publish".to_string(),
                            transaction_id: 0.0,
                            command_object: Amf0Value::Null,
                            additional_arguments: vec![
                                Amf0Value::Utf8String(stream_key),
                                Amf0Value::Utf8String(publish_type_string),
                            ],
                        };

                        let publish_payload =
                            publish_message.into_raw_message(self.get_epoch(), stream_id)?;
                        let publish_packet =
                            self.serializer.serialize(&publish_payload, false, false)?;
                        Ok(vec![ClientSessionResult::Packet(publish_packet)])
                    }
                }
            }
        }
    }

    fn handle_on_status_command(
        &mut self,
        stream_id: u32,
        arguments: Vec<Amf0Value>,
    ) -> ClientResult {
        let properties = arguments
            .into_iter()
            .next()
            .and_then(|v| v.get_object_properties())
            .ok_or(ClientSessionError::InvalidOnStatusArguments)?;
        let status = CommandStatus::new(Some(StreamId::new(stream_id)), properties);
        let code = status
            .code()
            .ok_or(ClientSessionError::InvalidOnStatusArguments)?;
        let Some(stream) = self.streams.find(stream_id) else {
            return Ok(vec![ClientSessionResult::Event(
                ClientSessionEvent::StatusReceived {
                    stream: None,
                    status,
                },
            )]);
        };
        let state = self.streams.get(stream).unwrap().state;
        let event = match code {
            "NetStream.Play.Start" if state == ClientStreamState::StartingPlayback => {
                self.streams.get_mut(stream).unwrap().state = ClientStreamState::Playing;
                return Ok(vec![ClientSessionResult::Event(
                    ClientSessionEvent::PlaybackRequestAccepted { stream, status },
                )]);
            }
            "NetStream.Publish.Start" if state == ClientStreamState::StartingPublish => {
                self.streams.get_mut(stream).unwrap().state = ClientStreamState::Publishing;
                return Ok(vec![ClientSessionResult::Event(
                    ClientSessionEvent::PublishRequestAccepted { stream, status },
                )]);
            }
            "NetStream.Play.StreamNotFound"
            | "NetStream.Play.Failed"
            | "NetStream.Play.FileStructureInvalid"
            | "NetStream.Play.NoSupportedTrackFound"
                if state == ClientStreamState::StartingPlayback =>
            {
                ClientSessionEvent::PlaybackRequestRejected { stream, status }
            }
            "NetStream.Publish.BadName"
            | "NetStream.Publish.Denied"
            | "NetStream.Publish.Failed"
                if state == ClientStreamState::StartingPublish =>
            {
                ClientSessionEvent::PublishRequestRejected { stream, status }
            }
            "NetStream.Play.Complete" | "NetStream.Play.Stop"
                if matches!(
                    state,
                    ClientStreamState::Playing | ClientStreamState::StartingPlayback
                ) =>
            {
                ClientSessionEvent::PlaybackFinished { stream, status }
            }
            "NetStream.Unpublish.Success" | "NetStream.Publish.Stop"
                if matches!(
                    state,
                    ClientStreamState::Publishing | ClientStreamState::StartingPublish
                ) =>
            {
                ClientSessionEvent::PublishingFinished { stream, status }
            }
            _ if status.text("level") == Some("error")
                && state == ClientStreamState::StartingPlayback =>
            {
                ClientSessionEvent::PlaybackRequestRejected { stream, status }
            }
            _ if status.text("level") == Some("error")
                && state == ClientStreamState::StartingPublish =>
            {
                ClientSessionEvent::PublishRequestRejected { stream, status }
            }
            _ => {
                return Ok(vec![ClientSessionResult::Event(
                    ClientSessionEvent::StatusReceived {
                        stream: Some(stream),
                        status,
                    },
                )]);
            }
        };
        let packet = self.delete_packet(stream_id)?;
        self.streams.remove(stream);
        Ok(vec![
            ClientSessionResult::Packet(packet),
            ClientSessionResult::Event(event),
        ])
    }

    fn handle_acknowledgement(&mut self, sequence_number: u32) -> ClientResult {
        let event = ClientSessionEvent::AcknowledgementReceived {
            bytes_received: sequence_number,
        };
        Ok(vec![ClientSessionResult::Event(event)])
    }

    fn handle_window_ack_size(&mut self, size: u32) -> ClientResult {
        if size == 0 {
            return Err(DecodeError::ResourceLimitExceeded {
                resource: "acknowledgement window",
                attempted: 0,
                maximum: u32::MAX as usize,
            }
            .into());
        }
        self.peer_window_ack_size = Some(size);
        Ok(Vec::new())
    }

    fn handle_user_control(
        &mut self,
        event_type: UserControlEventType,
        timestamp: Option<RtmpTimestamp>,
        _stream_id: Option<u32>,
        _buffer_length: Option<u32>,
    ) -> ClientResult {
        match event_type {
            UserControlEventType::PingRequest => self.handle_ping_request(timestamp),
            UserControlEventType::PingResponse => self.handle_ping_response(timestamp),
            _ => Ok(Vec::new()),
        }
    }

    fn handle_ping_request(&mut self, timestamp: Option<RtmpTimestamp>) -> ClientResult {
        let message = RtmpMessage::UserControl {
            event_type: UserControlEventType::PingResponse,
            buffer_length: None,
            stream_id: None,
            timestamp,
        };

        let payload = message.into_raw_message(self.get_epoch(), 0)?;
        let packet = self.serializer.serialize(&payload, false, false)?;
        Ok(vec![ClientSessionResult::Packet(packet)])
    }

    fn handle_ping_response(&mut self, timestamp: Option<RtmpTimestamp>) -> ClientResult {
        let timestamp = timestamp.unwrap_or(RtmpTimestamp::new(0));
        let event = ClientSessionEvent::PingResponseReceived { timestamp };
        Ok(vec![ClientSessionResult::Event(event)])
    }

    fn handle_set_chunk_size(&mut self, size: u32) -> ClientResult {
        self.deserializer.set_chunk_size(size as usize)?;
        Ok(Vec::new())
    }

    fn get_epoch(&self) -> RtmpTimestamp {
        RtmpTimestamp::new(self.start_time.elapsed().as_millis() as u32)
    }

    fn get_next_transaction_id(&mut self) -> u32 {
        let transaction_id = self.next_transaction_id;
        self.next_transaction_id = self.next_transaction_id.wrapping_add(1);
        transaction_id
    }
}

impl ClientSession {
    /// Prepare audio without copying. Accepts `Bytes`, segmented `Payload`, or owned vectors.
    pub fn send_audio(
        &mut self,
        stream: StreamHandle,
        data: impl Into<crate::Payload>,
        timestamp: RtmpTimestamp,
        drop_policy: crate::chunk_io::DropPolicy,
    ) -> Result<crate::chunk_io::Packet, ClientSessionError> {
        self.ensure_output_drained()?;
        self.ensure_active()?;
        let stream_id = self.publishing_id(stream)?;
        Ok(self.serializer.encode(
            RawMessage {
                timestamp,
                type_id: 8,
                message_stream_id: stream_id,
                data: data.into(),
            },
            crate::chunk_io::EncodeOptions {
                drop_policy,
                ..Default::default()
            },
        )?)
    }
}

impl ClientSession {
    /// Prepare video without copying. Accepts `Bytes`, segmented `Payload`, or owned vectors.
    pub fn send_video(
        &mut self,
        stream: StreamHandle,
        data: impl Into<crate::Payload>,
        timestamp: RtmpTimestamp,
        drop_policy: crate::chunk_io::DropPolicy,
    ) -> Result<crate::chunk_io::Packet, ClientSessionError> {
        self.ensure_output_drained()?;
        self.ensure_active()?;
        let stream_id = self.publishing_id(stream)?;
        Ok(self.serializer.encode(
            RawMessage {
                timestamp,
                type_id: 9,
                message_stream_id: stream_id,
                data: data.into(),
            },
            crate::chunk_io::EncodeOptions {
                drop_policy,
                ..Default::default()
            },
        )?)
    }
}

impl ClientSession {
    /// Prepare encoded script data verbatim, without decoding or copying its body.
    pub fn send_data<D: Into<crate::Payload>>(
        &mut self,
        stream: StreamHandle,
        message: DataMessage<D>,
    ) -> Result<crate::chunk_io::Packet, ClientSessionError> {
        self.ensure_output_drained()?;
        self.ensure_active()?;
        let stream_id = self.publishing_id(stream)?;
        let timestamp = message.timestamp();
        let type_id = message.wire_type().type_id();
        Ok(self.serializer.encode(
            RawMessage {
                timestamp,
                type_id,
                message_stream_id: stream_id,
                data: message.into_payload().into(),
            },
            crate::chunk_io::EncodeOptions::default(),
        )?)
    }
}

type ClientSessionEvent<D = Bytes> = ClientEvent<D>;

impl ClientSession {
    /// Queue the protocol outputs. Drain them through `receive`, including with empty input.
    pub fn connect(&mut self, app_name: impl Into<String>) -> Result<(), ClientSessionError> {
        let outputs = self.request_connection_inner(app_name.into())?;
        self.pending
            .push_back(outputs.map_payload(crate::Payload::from));
        Ok(())
    }
}

impl ClientSession {
    /// Queue the protocol outputs. Drain them through `receive`, including with empty input.
    pub fn connect_with_properties(
        &mut self,
        app_name: impl Into<String>,
        extra_properties: Amf0Object,
    ) -> Result<(), ClientSessionError> {
        let outputs =
            self.request_connection_with_properties_inner(app_name.into(), extra_properties)?;
        self.pending
            .push_back(outputs.map_payload(crate::Payload::from));
        Ok(())
    }
}

impl ClientSession {
    /// Create a playback stream. The returned handle is usable before the peer responds.
    pub fn play(
        &mut self,
        stream_key: impl Into<String>,
    ) -> Result<StreamHandle, ClientSessionError> {
        self.create_stream(TransactionPurpose::PlayRequest {
            stream_key: stream_key.into(),
        })
    }
    /// Create a publishing stream. Acceptance or rejection arrives through `receive`.
    pub fn publish(
        &mut self,
        stream_key: impl Into<String>,
        mode: PublishMode,
    ) -> Result<StreamHandle, ClientSessionError> {
        self.create_stream(TransactionPurpose::PublishRequest {
            stream_key: stream_key.into(),
            request_type: mode,
        })
    }
}

impl ClientSession {
    /// Encode metadata and return its outbound packet.
    pub fn send_metadata(
        &mut self,
        stream: StreamHandle,
        metadata: &StreamMetadata,
    ) -> Result<Packet, ClientSessionError> {
        self.ensure_output_drained()?;
        match self.publish_metadata_inner(stream, metadata)? {
            ClientSessionResult::Packet(packet) => Ok(packet),
            _ => unreachable!("metadata produces one packet"),
        }
    }
}

impl ClientSession {
    fn ensure_output_drained(&self) -> Result<(), ClientSessionError> {
        self.ensure_active()?;
        if self.pending.is_empty() {
            Ok(())
        } else {
            Err(ClientSessionError::PendingOutput)
        }
    }
}

type DataMessage<D = Bytes> = GenericDataMessage<D>;
