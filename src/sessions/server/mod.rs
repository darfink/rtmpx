use crate::sessions::streams::Streams;
use crate::sessions::{CallId, ConnectionState, ServerStreamState, StreamHandle};
mod active_stream;
mod config;
mod errors;
mod events;
mod outstanding_requests;

mod result;
mod session_state;

#[cfg(test)]
mod tests;

use self::active_stream::{ActiveStream, StreamState};
use self::outstanding_requests::OutstandingRequest;
use self::session_state::SessionState;
use crate::amf::{self, AmfEncoding};
use crate::amf0::{Amf0Object, Amf0Value};
use crate::chunk_io::{ChunkEncoder, DecodeError, EncodeError, MessageDecoder, Packet};
use crate::messages::RawMessage;
use crate::messages::{PeerBandwidthLimitType, RtmpMessage, UserControlEventType};
use crate::sessions::{
    DataMessage as GenericDataMessage, DataMessageType, RequestId, StreamId, StreamMetadata,
};
use crate::time::RtmpTimestamp;
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

pub use self::config::ServerSessionConfig;
pub use self::errors::ServerSessionError;
pub use self::events::{PlayStartValue, ServerEvent};
pub use self::result::ServerOutput;
use crate::sessions::PublishMode;
type ServerSessionResult<D = Bytes> = ServerOutput<D>;

/// A session that represents the server side of a single RTMP connection.
///
/// The `ServerSession` parses inbound RTMP chunks into messages and runs the
/// common server workflows for them. Each receive call returns an owned packet
/// or event. Applications accept or reject requests, then drain queued outputs.
/// Media sends return packets with resumable write progress.
///
/// Request and media events identify server-local stream handles.
/// Independent streams share connection negotiation and flow control.
/// Completing playback reports stream completion without closing the connection.
///
/// The `ServerSession` does not move bytes itself. The application reads
/// inbound chunk bytes and writes the returned responses.
///
/// RTMP chunk headers compress against earlier chunks, so this is required:
/// pass all bytes received **after** the completed handshake into the
/// `ServerSession`, send all returned responses to the client **in order**,
/// and send no other bytes to the client. Any violation of these rules can
/// cause RTMP chunk parsing errors on the peer or on the `ServerSession`
/// itself.
pub struct ServerSession {
    failed: bool,
    start_time: Instant,
    serializer: ChunkEncoder,
    deserializer: MessageDecoder,
    pending: std::collections::VecDeque<ServerOutput>,
    connected_app_name: Option<Arc<str>>,
    outstanding_requests: HashMap<u32, OutstandingRequest>,
    next_request_number: u32,
    current_state: SessionState,
    fms_version: String,
    /// The encoding agreed at `connect`: what the client asked for, clamped to
    /// what this crate supports. Everything this side originates uses it.
    negotiated_encoding: AmfEncoding,
    /// Ceiling from [`ServerSessionConfig::max_object_encoding`].
    max_object_encoding: AmfEncoding,
    /// True once the peer has actually used a type 15/17 message. Tracked
    /// separately from the negotiated encoding because a peer that negotiated
    /// `objectEncoding` 3 may still send some or all messages as AMF0.
    peer_uses_amf3_framing: bool,
    /// Framing to use for the message currently being produced.
    ///
    /// RTMP peers mirror each other rather than switching unilaterally: a
    /// `connect` that arrives as AMF0 is answered as AMF0 even when
    /// `objectEncoding` 3 was negotiated, which is what FMS and every encoder
    /// that talks to it expect. Negotiation decides what is *permitted* and
    /// what value is echoed; this decides what actually goes out.
    response_encoding: AmfEncoding,
    active_streams: HashMap<u32, ActiveStream>,
    stream_handles: Streams<()>,
    next_stream_id: u32,
    session_limits: crate::sessions::SessionLimits,
    peer_window_ack_size: Option<u32>,
    // The window size we advertised to the peer, used as the
    // acknowledgement trigger when the peer never advertises its own.
    self_window_ack_size: Option<u32>,
    bytes_received: u64,
    bytes_received_since_last_ack: u32,
    // Protocol control is emitted only once the peer's `connect` request is
    // answered. Sending it from `new` causes pre-connect writes and diverges
    // from RTMP server behaviour in FFmpeg, OBS, and FMS. It is serialized
    // then too; see `take_connection_preamble`.
    connection_preamble: Option<ConnectionPreamble>,
    /// Send `onBWDone` after the connect `_result`. It is serialized there,
    /// not with the control preamble, because chunk headers are compressed
    /// against the previous message: packets must leave in the order they
    /// were serialized.
    send_on_bw_done: bool,
    /// Commands the application answers; see [`ServerSession::delegate_command`].
    delegated_commands: Vec<String>,
    /// Calls made with [`ServerSession::call`] that the client has not answered.
    outstanding_calls: std::collections::HashSet<u32>,
    next_call_id: u32,
}

impl ServerSession {
    /// Connection state, independent of individual message streams.
    pub fn state(&self) -> ConnectionState {
        if self.failed {
            ConnectionState::Failed
        } else if self.current_state == SessionState::Connected {
            ConnectionState::Connected
        } else if self
            .outstanding_requests
            .values()
            .any(|r| matches!(r, OutstandingRequest::ConnectionRequest { .. }))
        {
            ConnectionState::Connecting
        } else {
            ConnectionState::Disconnected
        }
    }
    pub fn stream_id(&self, stream: StreamHandle) -> Option<StreamId> {
        self.stream_handles.get(stream).and_then(|e| e.wire_id)
    }
    pub fn stream_state(&self, stream: StreamHandle) -> Option<ServerStreamState> {
        let wire = self.stream_id(stream)?;
        Some(match &self.active_streams.get(&wire.get())?.current_state {
            StreamState::Created => ServerStreamState::Created,
            StreamState::Playing { .. } => ServerStreamState::Playing,
            StreamState::Publishing { .. } => ServerStreamState::Publishing,
            StreamState::Completed => ServerStreamState::Completed,
        })
    }
    fn playback_id(&self, stream: StreamHandle) -> Result<StreamId, ServerSessionError> {
        let state = self
            .stream_state(stream)
            .ok_or(ServerSessionError::InvalidStreamHandle)?;
        if state != ServerStreamState::Playing {
            return Err(ServerSessionError::StreamInInvalidState { stream, state });
        }
        Ok(self.stream_id(stream).expect("live playback stream"))
    }
    pub fn streams(&self) -> impl Iterator<Item = (StreamHandle, StreamId)> + '_ {
        self.stream_handles
            .iter()
            .filter_map(|(h, e)| e.wire_id.map(|id| (h, id)))
    }

    /// Whether an input error has permanently terminated this session.
    pub fn is_failed(&self) -> bool {
        self.failed
    }

    fn ensure_active(&self) -> Result<(), ServerSessionError> {
        if self.failed {
            Err(ServerSessionError::SessionFailed)
        } else {
            Ok(())
        }
    }

    /// Cumulative bytes handed to `handle_input`, exposed so tests
    /// can assert the acknowledgement sequence number exactly.
    #[cfg(test)]
    pub(crate) fn bytes_received_for_test(&self) -> u64 {
        self.bytes_received
    }

    /// Creates a new server session.
    ///
    /// The initial output list is empty. Required control messages are retained
    /// until the application accepts the client's connection request.
    pub fn new(config: ServerSessionConfig) -> Result<Self, ServerSessionError> {
        if config.window_ack_size == 0 {
            return Err(DecodeError::ResourceLimitExceeded {
                resource: "acknowledgement window",
                attempted: 0,
                maximum: u32::MAX as usize,
            }
            .into());
        }
        let session = ServerSession {
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
            connected_app_name: None,
            outstanding_requests: HashMap::new(),
            next_request_number: 0,
            current_state: SessionState::Started,
            fms_version: config.fms_version,
            negotiated_encoding: AmfEncoding::Amf0,
            max_object_encoding: config.max_object_encoding,
            peer_uses_amf3_framing: false,
            response_encoding: AmfEncoding::Amf0,
            active_streams: HashMap::new(),
            stream_handles: Streams::new(),
            next_stream_id: 1,
            session_limits: config.session_limits,
            peer_window_ack_size: None,
            self_window_ack_size: Some(config.window_ack_size),
            bytes_received: 0,
            bytes_received_since_last_ack: 0,
            connection_preamble: Some(ConnectionPreamble {
                chunk_size: config.chunk_size,
                window_ack_size: config.window_ack_size,
                peer_bandwidth: config.peer_bandwidth,
            }),
            send_on_bw_done: config.send_on_bw_done_message_on_start,
            delegated_commands: Vec::new(),
            outstanding_calls: std::collections::HashSet::new(),
            next_call_id: 1,
        };

        Ok(session)
    }

    /// The control messages that precede the answer to `connect`, serialized
    /// now and only once.
    ///
    /// They are serialized when the answer is, not when the session is
    /// created: each chunk header is compressed against the previous message
    /// on its chunk stream, and a new chunk size applies from the moment it
    /// is serialized. Anything sent before `connect` is answered, such as an
    /// acknowledgement or a ping response, must not be encoded against
    /// preamble packets the peer has not received.
    fn take_connection_preamble(&mut self) -> Result<Vec<ServerSessionResult>, ServerSessionError> {
        let Some(preamble) = self.connection_preamble.take() else {
            return Ok(Vec::new());
        };
        let mut results = Vec::with_capacity(4);

        let chunk_size_packet = self
            .serializer
            .set_chunk_size(preamble.chunk_size, RtmpTimestamp::new(0))?;
        results.push(ServerSessionResult::Packet(chunk_size_packet));

        // RTMP 1.0 section 7.2.1.1 orders the connect flow as Window
        // Acknowledgement Size, Set Peer Bandwidth, StreamBegin, `_result`.
        let messages = [
            RtmpMessage::WindowAcknowledgement {
                size: preamble.window_ack_size,
            },
            RtmpMessage::SetPeerBandwidth {
                size: preamble.peer_bandwidth,
                limit_type: PeerBandwidthLimitType::Dynamic,
            },
            RtmpMessage::UserControl {
                event_type: UserControlEventType::StreamBegin,
                stream_id: Some(0),
                timestamp: None,
                buffer_length: None,
            },
        ];
        for message in messages {
            let payload = self.to_payload(message, 0)?;
            let packet = self.serializer.serialize(&payload, true, false)?;
            results.push(ServerSessionResult::Packet(packet));
        }
        Ok(results)
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
    ) -> Result<Option<ServerOutput>, ServerSessionError> {
        self.ensure_active()?;
        let result = self.receive_inner(input);
        if result.is_err() {
            self.failed = true;
            self.pending.clear();
        }
        result
    }
    fn receive_inner(
        &mut self,
        input: &mut Bytes,
    ) -> Result<Option<ServerOutput>, ServerSessionError> {
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
                    // The RTMP spec defines the acknowledgement sequence
                    // number as the *cumulative* number of bytes received on the
                    // connection, not the count since the last ack. Upstream sent
                    // the per-window delta, which effectively reports a stalled
                    // byte counter to the peer. Strict senders that track the
                    // acknowledged window will throttle or drop the connection.
                    // The value wraps at 2^32 by design.
                    let ack_message = RtmpMessage::Acknowledgement {
                        sequence_number: self.bytes_received as u32,
                    };
                    let ack_payload = self.to_payload(ack_message, 0)?;
                    let ack_packet = self.serializer.serialize(&ack_payload, false, false)?;

                    self.bytes_received_since_last_ack %= peer_ack_size;
                    self.pending.push_back(ServerOutput::Packet(ack_packet));
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
    ) -> Result<Option<ServerOutput>, ServerSessionError> {
        if let Some(wire_type) = DataMessageType::from_type_id(payload.type_id) {
            if wire_type == DataMessageType::Amf3 {
                self.peer_uses_amf3_framing = true;
            }
            let message = DataMessage::new(wire_type, payload.timestamp, payload.data);
            let Some(app_name) = self.connected_app_name.clone() else {
                return Ok(None);
            };
            let Some(ActiveStream {
                current_state: StreamState::Publishing { stream_key, .. },
            }) = self.active_streams.get(&payload.message_stream_id)
            else {
                return Ok(None);
            };
            let event = ServerSessionEvent::StreamDataReceived {
                stream: self
                    .stream_handles
                    .find(payload.message_stream_id)
                    .expect("known stream"),
                stream_id: StreamId::new(payload.message_stream_id),
                app_name,
                stream_key: stream_key.clone(),
                message,
            };
            return Ok(Some(ServerOutput::Event(event)));
        }
        let event = match payload.type_id {
            8 => self.audio_event(payload.data, payload.message_stream_id, payload.timestamp)?,
            9 => self.video_event(payload.data, payload.message_stream_id, payload.timestamp)?,
            _ => {
                if !matches!(payload.type_id, 1..=6 | 17 | 20) {
                    return Ok(Some(ServerOutput::UnhandledMessage(payload)));
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
        Ok(event.map(ServerOutput::Event))
    }
    fn handle_message(
        &mut self,
        payload: RawMessage,
    ) -> Result<Vec<ServerSessionResult>, ServerSessionError> {
        let message = payload.to_rtmp_message()?;

        let message_results = match message {
            RtmpMessage::Abort { stream_id } => self.handle_abort_message(stream_id)?,

            RtmpMessage::Acknowledgement { sequence_number } => {
                self.handle_acknowledgement_message(sequence_number)?
            }

            RtmpMessage::Amf0Command {
                command_name,
                transaction_id,
                command_object,
                additional_arguments,
            } => {
                self.response_encoding = AmfEncoding::Amf0;
                self.handle_amf0_command(
                    payload.message_stream_id,
                    command_name,
                    transaction_id,
                    command_object,
                    additional_arguments,
                )?
            }

            RtmpMessage::Amf3Command {
                command_name,
                transaction_id,
                command_object,
                additional_arguments,
                format: _,
            } => {
                // Type 17 and type 20 carry the same NetConnection
                // and NetStream commands; only the encoding
                // differs. Project into the AMF0 value model and
                // run the one set of handlers, so an AMF3 client
                // gets identical behaviour - including Enhanced
                // RTMP capability validation - instead of a
                // parallel implementation that drifts.
                self.peer_uses_amf3_framing = true;
                self.response_encoding = AmfEncoding::Amf3;
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
                vec![ServerSessionResult::UnhandledMessage(payload)]
            }

            RtmpMessage::AudioData { data } => self
                .audio_event(data, payload.message_stream_id, payload.timestamp)?
                .map(ServerSessionResult::Event)
                .into_iter()
                .collect(),

            RtmpMessage::SetChunkSize { size } => self.handle_set_chunk_size(size)?,

            RtmpMessage::SetPeerBandwidth { size, limit_type } => {
                self.handle_set_peer_bandwidth(size, limit_type)?
            }

            RtmpMessage::UserControl {
                event_type,
                stream_id,
                buffer_length,
                timestamp,
            } => self.handle_user_control(event_type, stream_id, buffer_length, timestamp)?,

            RtmpMessage::VideoData { data } => self
                .video_event(data, payload.message_stream_id, payload.timestamp)?
                .map(ServerSessionResult::Event)
                .into_iter()
                .collect(),

            RtmpMessage::WindowAcknowledgement { size } => {
                self.handle_window_acknowledgement(size)?
            }

            _ => vec![ServerSessionResult::UnhandledMessage(payload)],
        };

        Ok(message_results)
    }

    /// Tells the server session that it should accept an outstanding request
    fn accept_request_inner(
        &mut self,
        request_id: RequestId,
    ) -> Result<Vec<ServerSessionResult>, ServerSessionError> {
        self.ensure_active()?;
        self.accept_request_with_properties_inner(request_id, Amf0Object::new())
    }

    /// Accept a request and add server capability fields to a successful
    /// `connect` result. Properties are ignored for non-connection requests.
    fn accept_request_with_properties_inner(
        &mut self,
        request_id: RequestId,
        response_properties: Amf0Object,
    ) -> Result<Vec<ServerSessionResult>, ServerSessionError> {
        self.ensure_active()?;
        if self.is_command_request(request_id) {
            // RTMP 1.0 section 7.2.1.2: the answer carries a Response value;
            // `undefined` is the void one, as the no-op commands send.
            return self.answer_command(request_id, vec![Amf0Value::Undefined], None);
        }
        let request_id = request_id.get();
        // Look, don't take: if the answer cannot be built, the request stays
        // outstanding and the application can still answer it another way.
        let request = match self.outstanding_requests.get(&request_id) {
            Some(x) => x.clone(),
            None => return Err(ServerSessionError::InvalidRequestId),
        };

        let results = match request {
            OutstandingRequest::ConnectionRequest {
                app_name,
                transaction_id,
                encoding,
            } => {
                self.response_encoding = encoding;
                self.accept_connection_request(app_name, transaction_id, response_properties)
            }

            OutstandingRequest::PublishRequested {
                stream_key,
                mode,
                stream_id,
                encoding,
            } => {
                self.response_encoding = encoding;
                self.accept_publish_request(stream_id, stream_key, mode)
            }

            OutstandingRequest::PlayRequested {
                stream_key,
                stream_id,
                encoding,
            } => {
                self.response_encoding = encoding;
                self.accept_play_request(stream_id, stream_key)
            }

            // Answered by `answer_command` above.
            OutstandingRequest::CommandRequested { .. } => {
                Err(ServerSessionError::InvalidOutstandingRequest(request_id))
            }
        }?;
        self.outstanding_requests.remove(&request_id);
        Ok(results)
    }

    /// Answer a delegated command with `_result(null, values...)`, or with
    /// `_error` when `refusal` is given.
    ///
    /// The request is only given up once its answer is built: if `values`
    /// cannot be encoded, the request stays outstanding and the application
    /// can still answer it another way.
    fn answer_command(
        &mut self,
        request_id: RequestId,
        values: Vec<Amf0Value>,
        refusal: Option<(&str, &str)>,
    ) -> Result<Vec<ServerSessionResult>, ServerSessionError> {
        self.ensure_active()?;
        let Some(OutstandingRequest::CommandRequested {
            transaction_id,
            stream_id,
            encoding,
        }) = self.outstanding_requests.get(&request_id.get())
        else {
            return Err(ServerSessionError::InvalidRequestId);
        };
        let (transaction_id, stream_id, encoding) = (*transaction_id, *stream_id, *encoding);
        self.response_encoding = encoding;
        let packet = match refusal {
            None => {
                self.create_success_response(transaction_id, Amf0Value::Null, values, stream_id)?
            }
            Some((code, description)) => {
                self.create_error_packet(code, description, transaction_id, stream_id)?
            }
        };
        self.outstanding_requests.remove(&request_id.get());
        Ok(vec![ServerSessionResult::Packet(packet)])
    }

    fn is_command_request(&self, request_id: RequestId) -> bool {
        matches!(
            self.outstanding_requests.get(&request_id.get()),
            Some(OutstandingRequest::CommandRequested { .. })
        )
    }

    /// The AMF encoding agreed with the peer during `connect`.
    pub fn negotiated_encoding(&self) -> AmfEncoding {
        self.negotiated_encoding
    }

    /// Framing for a message this side originates rather than one that answers
    /// a request.
    ///
    /// There is nothing to mirror, so AMF3 is used only when the peer both
    /// negotiated it and has demonstrably sent a type 15/17 message. A peer may
    /// negotiate `objectEncoding` 3 and still never use AMF3 framing, and
    /// pushing type 15 at it unprompted is a needless compatibility risk.
    fn server_initiated_encoding(&self) -> AmfEncoding {
        if self.negotiated_encoding.is_amf3() && self.peer_uses_amf3_framing {
            AmfEncoding::Amf3
        } else {
            AmfEncoding::Amf0
        }
    }

    /// Build a command response in the negotiated encoding.
    ///
    /// Responses are authored once against the AMF0 value model; this is the
    /// only place that has to know which encoding goes on the wire.
    fn command_message(
        &self,
        command_name: &str,
        transaction_id: f64,
        command_object: Amf0Value,
        additional_arguments: Vec<Amf0Value>,
    ) -> RtmpMessage {
        self.encode_response(RtmpMessage::Amf0Command {
            command_name: command_name.to_string(),
            transaction_id,
            command_object,
            additional_arguments,
        })
    }

    /// Convert an AMF0-authored response into the negotiated encoding.
    ///
    /// The type 15/17 body is written with the `0x00` format selector - AMF0
    /// values, with an `avmplus` escape for anything that has no AMF0 form.
    /// That is what Flash, librtmp and FFmpeg emit and what they reliably
    /// parse, so it is the safer of the two legal framings to originate.
    fn encode_response(&self, message: RtmpMessage) -> RtmpMessage {
        // AMF3 framing needs both halves: the peer must have negotiated
        // `objectEncoding` 3, and this particular exchange must already be
        // using AMF3. Either alone is not enough.
        if !(self.negotiated_encoding.is_amf3() && self.response_encoding.is_amf3()) {
            return message;
        }
        match message {
            RtmpMessage::Amf0Command {
                command_name,
                transaction_id,
                command_object,
                additional_arguments,
            } => RtmpMessage::Amf3Command {
                command_name,
                transaction_id,
                command_object: command_object.to_amf3(),
                additional_arguments: additional_arguments.iter().map(|v| v.to_amf3()).collect(),
                format: AmfEncoding::Amf0,
            },
            RtmpMessage::Amf0Data { values } => RtmpMessage::Amf3Data {
                values: values.iter().map(|v| v.to_amf3()).collect(),
                format: AmfEncoding::Amf0,
            },
            other => other,
        }
    }

    /// Re-encode an outbound message and turn it into a payload at the current
    /// epoch. Media and protocol-control messages pass through untouched.
    fn to_payload(
        &self,
        message: RtmpMessage,
        stream_id: u32,
    ) -> Result<crate::messages::RawMessage, ServerSessionError> {
        Ok(self
            .encode_response(message)
            .into_raw_message(self.get_epoch(), stream_id)?)
    }

    /// Tells the server session that it should reject an outstanding request
    fn reject_request_inner(
        &mut self,
        request_id: RequestId,
        code: &str,
        description: &str,
    ) -> Result<Vec<ServerSessionResult>, ServerSessionError> {
        self.ensure_active()?;
        if self.is_command_request(request_id) {
            return self.answer_command(request_id, Vec::new(), Some((code, description)));
        }
        let request_id = request_id.get();
        // Look, don't take; see `accept_request_with_properties_inner`.
        let request = match self.outstanding_requests.get(&request_id) {
            Some(x) => x.clone(),
            None => return Err(ServerSessionError::InvalidRequestId),
        };

        let (transaction_id, stream_id) = match request {
            OutstandingRequest::ConnectionRequest {
                transaction_id,
                encoding,
                ..
            } => {
                self.response_encoding = encoding;
                (transaction_id, 0)
            }
            OutstandingRequest::PublishRequested {
                stream_id,
                encoding,
                ..
            } => {
                self.response_encoding = encoding;
                (0.0, stream_id)
            }
            OutstandingRequest::PlayRequested {
                stream_id,
                encoding,
                ..
            } => {
                self.response_encoding = encoding;
                (0.0, stream_id)
            }
            // Answered by `answer_command` before the request is removed.
            OutstandingRequest::CommandRequested { .. } => {
                return Err(ServerSessionError::InvalidOutstandingRequest(request_id));
            }
        };

        // NetConnection and NetStream rejections use different
        // command names.
        //
        // `connect` is a request/response transaction, so a refusal is `_error`
        // carrying its transaction id. `publish` and `play` are not: the server
        // answers them with an `onStatus` event at `level: "error"`, which is
        // what the accept paths in this file already emit for
        // `NetStream.Publish.Start` and `NetStream.Play.Start`.
        //
        // Upstream sent `_error` for all three. Encoders looking for `onStatus`
        // on a publish - which is where they expect `NetStream.Publish.*` - miss
        // the rejection entirely and fall back to a generic connection error.
        //
        // A refused connect is the first thing the peer hears from us, so it needs
        // the same protocol preamble (chunk size, window acknowledgement, ...)
        // the accept path prepends. The `_error` is chunked at the chunk size
        // that preamble announces; without it the peer decodes with the
        // default of 128, cannot read the refusal, and observes a bare drop.
        // `onBWDone` is not part of the preamble: it follows only an accepted
        // connect.
        // Build the answer before anything is serialized: that is the step
        // that can fail, and nothing may change if it does.
        let is_connect = matches!(request, OutstandingRequest::ConnectionRequest { .. });
        let payload = if is_connect {
            self.error_payload(code, description, transaction_id, stream_id)?
        } else {
            let status = amf::status_object::<Amf0Value>("error", code, description);
            let message = self.command_message("onStatus", 0.0, Amf0Value::Null, vec![status]);
            self.to_payload(message, stream_id)?
        };
        check_lengths(std::slice::from_ref(&payload))?;

        let mut results = if is_connect {
            self.take_connection_preamble()?
        } else {
            Vec::new()
        };
        results.extend(self.serialize_all(vec![payload])?);
        self.outstanding_requests.remove(&request_id);
        Ok(results)
    }

    /// Prepares metadata information to be sent to the client
    pub fn send_metadata(
        &mut self,
        stream: StreamHandle,
        metadata: &StreamMetadata,
    ) -> Result<Packet, ServerSessionError> {
        self.ensure_output_drained()?;
        let stream_id = self.playback_id(stream)?;
        self.ensure_active()?;
        let stream_id = stream_id.get();
        self.response_encoding = self.server_initiated_encoding();
        let mut properties = Amf0Object::with_capacity(11);

        metadata
            .video_width
            .map(|x| properties.insert("width".to_string(), Amf0Value::Number(x as f64)));

        metadata
            .video_height
            .map(|x| properties.insert("height".to_string(), Amf0Value::Number(x as f64)));

        metadata
            .video_codec_id
            .map(|x| properties.insert("videocodecid".to_string(), Amf0Value::Number(x as f64)));

        metadata
            .video_bitrate_kbps
            .map(|x| properties.insert("videodatarate".to_string(), Amf0Value::Number(x as f64)));

        metadata
            .video_frame_rate
            .map(|x| properties.insert("framerate".to_string(), Amf0Value::Number(x as f64)));

        metadata
            .audio_codec_id
            .map(|x| properties.insert("audiocodecid".to_string(), Amf0Value::Number(x as f64)));

        metadata
            .audio_bitrate_kbps
            .map(|x| properties.insert("audiodatarate".to_string(), Amf0Value::Number(x as f64)));

        metadata
            .audio_sample_rate
            .map(|x| properties.insert("audiosamplerate".to_string(), Amf0Value::Number(x as f64)));

        metadata
            .audio_channels
            .map(|x| properties.insert("audiochannels".to_string(), Amf0Value::Number(x as f64)));

        metadata
            .audio_is_stereo
            .map(|x| properties.insert("stereo".to_string(), Amf0Value::Boolean(x)));

        metadata
            .encoder
            .as_ref()
            .map(|x| properties.insert("encoder".to_string(), Amf0Value::Utf8String(x.clone())));

        let message = RtmpMessage::Amf0Data {
            values: vec![
                Amf0Value::Utf8String("onMetaData".to_string()),
                Amf0Value::Object(properties),
            ],
        };

        let payload = self.to_payload(message, stream_id)?;
        let packet = self.serializer.serialize(&payload, false, false)?;
        Ok(packet)
    }

    /// Sends a ping request to the client
    pub fn send_ping_request(&mut self) -> Result<(Packet, RtmpTimestamp), ServerSessionError> {
        self.ensure_output_drained()?;
        self.ensure_active()?;
        let epoch = self.get_epoch();
        let message = RtmpMessage::UserControl {
            event_type: UserControlEventType::PingRequest,
            timestamp: Some(epoch.clone()),
            buffer_length: None,
            stream_id: None,
        };

        let payload = message.into_raw_message(epoch.clone(), 0)?;
        let packet = self.serializer.serialize(&payload, false, false)?;
        Ok((packet, epoch))
    }

    /// Changes stream to Completed, and sends out an
    /// `onStatus(code: NetStream.Play.Complete)`
    pub fn complete_playback(
        &mut self,
        stream: StreamHandle,
    ) -> Result<Packet, ServerSessionError> {
        self.ensure_output_drained()?;
        let stream_id = self
            .stream_id(stream)
            .ok_or(ServerSessionError::InvalidStreamHandle)?;
        self.ensure_active()?;
        let stream_id = stream_id.get();
        self.response_encoding = self.server_initiated_encoding();
        let stream_key = match self.active_streams.get_mut(&stream_id) {
            Some(ActiveStream {
                current_state: state,
            }) => {
                let k = match state {
                    StreamState::Playing { stream_key: k } => k.clone(),
                    _ => {
                        return Err(ServerSessionError::ActionAttemptedOnInactiveStream {
                            action: "complete".to_string(),
                            stream_id,
                        });
                    }
                };
                *state = StreamState::Completed;
                k
            }
            _ => {
                return Err(ServerSessionError::ActionAttemptedOnInactiveStream {
                    action: "complete".to_string(),
                    stream_id,
                });
            }
        };

        let description = format!("Stream playback is completed for {}", stream_key);
        let status_message = RtmpMessage::Amf0Command {
            command_name: "onStatus".to_string(),
            transaction_id: 0.0,
            command_object: Amf0Value::Null,
            additional_arguments: vec![Amf0Value::Object(create_status_object(
                "status",
                "NetStream.Play.Complete",
                description.as_ref(),
            ))],
        };

        let payload = self.to_payload(status_message, stream_id)?;

        Ok(self.serializer.serialize(&payload, false, false)?)
    }

    fn handle_abort_message(
        &mut self,
        stream_id: u32,
    ) -> Result<Vec<ServerSessionResult>, ServerSessionError> {
        self.deserializer.abort_chunk_stream(stream_id);
        Ok(Vec::new())
    }

    fn handle_acknowledgement_message(
        &self,
        sequence_number: u32,
    ) -> Result<Vec<ServerSessionResult>, ServerSessionError> {
        let event = ServerSessionEvent::AcknowledgementReceived {
            bytes_received: sequence_number,
        };
        Ok(vec![ServerSessionResult::Event(event)])
    }

    fn handle_amf0_command(
        &mut self,
        stream_id: u32,
        name: String,
        transaction_id: f64,
        command_object: Amf0Value,
        additional_args: Vec<Amf0Value>,
    ) -> Result<Vec<ServerSessionResult>, ServerSessionError> {
        let results = match name.as_str() {
            "connect" => self.handle_command_connect(transaction_id, command_object)?,
            "closeStream" => self.handle_command_close_stream(additional_args)?,
            "createStream" => self.handle_command_create_stream(transaction_id)?,
            "deleteStream" => self.handle_command_delete_stream(additional_args)?,
            "play" => self.handle_command_play(stream_id, transaction_id, additional_args)?,
            "publish" => self.handle_command_publish(stream_id, transaction_id, additional_args)?,
            _ => self.handle_unimplemented_command(
                stream_id,
                name,
                transaction_id,
                command_object,
                additional_args,
            )?,
        };

        Ok(results)
    }

    fn handle_command_connect(
        &mut self,
        transaction_id: f64,
        command_object: Amf0Value,
    ) -> Result<Vec<ServerSessionResult>, ServerSessionError> {
        // AMF3 handlers are defined above; this is the AMF0 connect path.
        let mut properties = match command_object {
            Amf0Value::Object(properties) => properties,
            _ => return Err(ServerSessionError::NoAppNameForConnectionRequest),
        };

        let app_name: Arc<str> = match properties.shift_remove("app") {
            Some(value) => match value {
                Amf0Value::Utf8String(mut app) => {
                    if app.ends_with("/") {
                        app.pop();
                    }

                    app
                }
                _ => return Err(ServerSessionError::NoAppNameForConnectionRequest),
            },
            None => return Err(ServerSessionError::NoAppNameForConnectionRequest),
        }
        .into();

        // Actually honour `objectEncoding` instead of echoing it.
        //
        // The client states the highest encoding it wants; the server answers
        // with the encoding that will actually be used. Echoing the request
        // back unchanged tells a client it may switch to AMF3 whether or not
        // this side can decode it, so the request is clamped to what this crate
        // supports and the clamped value is what gets sent in the response and
        // used for every message this side originates.
        let requested = properties
            .shift_remove("objectEncoding")
            .and_then(|value| value.get_number())
            .map(AmfEncoding::from_object_encoding)
            .unwrap_or_default();
        self.negotiated_encoding = AmfEncoding::negotiate(requested, self.max_object_encoding);

        let request = OutstandingRequest::ConnectionRequest {
            app_name: app_name.clone(),
            transaction_id,
            encoding: self.response_encoding,
        };

        let request_number = self.register_request(request)?;

        let event = ServerSessionEvent::ConnectionRequested {
            app_name: app_name,
            request_id: RequestId(request_number),
            // Hand the caller whatever the client sent beyond the
            // fields we consumed above, rather than dropping it.
            additional_properties: properties,
        };

        Ok(vec![ServerSessionResult::Event(event)])
    }

    /// Answer a command this session does not implement.
    ///
    /// RTMP 1.0 section 7.2: the receiver answers a command with `_result`
    /// or `_error` carrying the same transaction id, and a transaction id of 0
    /// means no answer is expected. A command left unanswered leaves a client
    /// that waits for it stalled with nothing on the wire to say why.
    ///
    /// In order:
    /// - `_result`/`_error` answering a [`ServerSession::call`] raise
    ///   [`ServerSessionEvent::CallAnswered`].
    /// - A delegated command that expects an answer raises
    ///   [`ServerSessionEvent::CommandRequested`] for the application.
    /// - [`ServerSession::NO_OP_COMMANDS`] are accepted with `_result`.
    /// - Any other command that expects an answer is refused with `_error`
    ///   (`NetConnection.Call.Failed`) and raises
    ///   [`ServerSessionEvent::CommandRefused`].
    ///
    /// Before `connect` is accepted nothing is answered: the connection does
    /// not exist yet, and its control preamble has not been sent. Whatever is
    /// not answered surfaces as [`ServerSessionEvent::UnhandledCommand`].
    fn handle_unimplemented_command(
        &mut self,
        stream_id: u32,
        name: String,
        transaction_id: f64,
        command_object: Amf0Value,
        additional_args: Vec<Amf0Value>,
    ) -> Result<Vec<ServerSessionResult>, ServerSessionError> {
        // `_result` and `_error` answer this side's calls; they are never
        // requests themselves.
        if name == "_result" || name == "_error" {
            // Calls are made on the NetConnection, so only an answer on
            // message stream 0 can belong to one.
            if stream_id == 0
                && let Some(call_id) = call_id_of(transaction_id)
                && self.outstanding_calls.remove(&call_id)
            {
                let event = ServerSessionEvent::CallAnswered {
                    call_id: CallId(call_id),
                    accepted: name == "_result",
                    command_object,
                    additional_values: additional_args,
                };
                return Ok(vec![ServerSessionResult::Event(event)]);
            }
            return Ok(vec![unhandled(
                stream_id,
                name,
                transaction_id,
                command_object,
                additional_args,
            )]);
        }

        // A non-finite id cannot be echoed meaningfully, so it is treated like
        // 0: no answer expected.
        let expects_answer = self.current_state == SessionState::Connected
            && transaction_id.is_finite()
            && transaction_id != 0.0;
        if !expects_answer {
            return Ok(vec![unhandled(
                stream_id,
                name,
                transaction_id,
                command_object,
                additional_args,
            )]);
        }

        if self
            .delegated_commands
            .iter()
            .any(|delegated| *delegated == name)
        {
            let request_number = self.register_request(OutstandingRequest::CommandRequested {
                transaction_id,
                stream_id,
                encoding: self.response_encoding,
            })?;
            let event = ServerSessionEvent::CommandRequested {
                request_id: RequestId(request_number),
                stream_id: StreamId::new(stream_id),
                command_name: name,
                command_object,
                additional_values: additional_args,
            };
            return Ok(vec![ServerSessionResult::Event(event)]);
        }

        if Self::NO_OP_COMMANDS.contains(&name.as_str()) {
            let packet = self.create_success_response(
                transaction_id,
                Amf0Value::Null,
                vec![Amf0Value::Undefined],
                stream_id,
            )?;
            return Ok(vec![
                ServerSessionResult::Packet(packet),
                unhandled(
                    stream_id,
                    name,
                    transaction_id,
                    command_object,
                    additional_args,
                ),
            ]);
        }

        // The name is the peer's own; bound how much of it is echoed.
        let shown: String = name.chars().take(MAX_ECHOED_COMMAND_NAME).collect();
        let packet = self.create_error_packet(
            "NetConnection.Call.Failed",
            &format!("Method not found ({shown})."),
            transaction_id,
            stream_id,
        )?;
        let event = ServerSessionEvent::CommandRefused {
            stream_id: StreamId::new(stream_id),
            command_name: name,
            transaction_id,
        };
        Ok(vec![
            ServerSessionResult::Packet(packet),
            ServerSessionResult::Event(event),
        ])
    }

    fn handle_command_close_stream(
        &mut self,
        mut arguments: Vec<Amf0Value>,
    ) -> Result<Vec<ServerSessionResult>, ServerSessionError> {
        if self.current_state != SessionState::Connected {
            return Ok(Vec::new());
        }

        let app_name = match self.connected_app_name {
            Some(ref name) => name.clone(),
            None => return Ok(Vec::new()),
        };

        // First argument should be the stream id to close
        if arguments.len() == 0 {
            return Ok(Vec::new());
        }

        let stream_id = match arguments.remove(0) {
            Amf0Value::Number(x) => x as u32,
            _ => return Ok(Vec::new()),
        };

        let stream = match self.active_streams.get_mut(&stream_id) {
            Some(x) => x,
            None => return Ok(Vec::new()),
        };

        // Before we change the stream state we need to grab the info from it for any
        // events that need to be raised
        let results = match stream.current_state {
            StreamState::Publishing {
                ref stream_key,
                mode: _,
            } => {
                let event = ServerSessionEvent::PublishStreamFinished {
                    stream: self.stream_handles.find(stream_id).expect("known stream"),
                    stream_id: StreamId::new(stream_id),
                    app_name,
                    stream_key: stream_key.clone(),
                };

                vec![ServerSessionResult::Event(event)]
            }

            StreamState::Playing { ref stream_key } => {
                let event = ServerSessionEvent::PlayStreamFinished {
                    stream: self.stream_handles.find(stream_id).expect("known stream"),
                    stream_id: StreamId::new(stream_id),
                    app_name,
                    stream_key: stream_key.clone(),
                };

                vec![ServerSessionResult::Event(event)]
            }

            _ => Vec::new(),
        };

        // As afar as we are concerned, a created and closed stream are equivalent.  Both allow
        // reusing the stream
        stream.current_state = StreamState::Created;

        Ok(results)
    }

    fn handle_command_create_stream(
        &mut self,
        transaction_id: f64,
    ) -> Result<Vec<ServerSessionResult>, ServerSessionError> {
        self.session_limits
            .check_streams(self.active_streams.len())?;
        let new_stream_id = self.next_stream_id;
        self.next_stream_id = self
            .next_stream_id
            .checked_add(1)
            .ok_or(ServerSessionError::StreamIdsExhausted)?;

        let new_stream = ActiveStream {
            current_state: StreamState::Created,
        };

        self.active_streams.insert(new_stream_id, new_stream);
        self.stream_handles
            .insert((), Some(StreamId::new(new_stream_id)));

        let packet = self.create_success_response(
            transaction_id,
            Amf0Value::Null,
            vec![Amf0Value::Number(new_stream_id as f64)],
            0,
        )?; // Stream create result must always be on stream 0 for flash clients

        Ok(vec![ServerSessionResult::Packet(packet)])
    }

    fn handle_command_delete_stream(
        &mut self,
        mut arguments: Vec<Amf0Value>,
    ) -> Result<Vec<ServerSessionResult>, ServerSessionError> {
        // Not sure if I need to send a response
        if self.current_state != SessionState::Connected {
            return Ok(Vec::new());
        }

        let app_name = match self.connected_app_name {
            Some(ref name) => name.clone(),
            None => return Ok(Vec::new()),
        };

        if arguments.len() == 0 {
            return Ok(Vec::new());
        }

        // First argument is expected to be the stream id
        let stream_id = match arguments.remove(0) {
            Amf0Value::Number(x) => x as u32,
            // GStreamer's RTMP sink sends deleteStream's id as an
            // AMF string. Accept the decimal representation for compatibility.
            Amf0Value::Utf8String(value) => match value.parse::<u32>() {
                Ok(value) => value,
                Err(_) => return Ok(Vec::new()),
            },
            _ => return Ok(Vec::new()),
        };

        let stream = match self.active_streams.remove(&stream_id) {
            Some(stream) => stream,
            None => return Ok(Vec::new()),
        };

        self.outstanding_requests
            .retain(|_, request| match request {
                OutstandingRequest::PlayRequested { stream_id: id, .. }
                | OutstandingRequest::PublishRequested { stream_id: id, .. }
                | OutstandingRequest::CommandRequested { stream_id: id, .. } => *id != stream_id,
                _ => true,
            });
        let result = match stream.current_state {
            StreamState::Publishing {
                ref stream_key,
                mode: _,
            } => {
                let event = ServerSessionEvent::PublishStreamFinished {
                    stream: self.stream_handles.find(stream_id).expect("known stream"),
                    stream_id: StreamId::new(stream_id),
                    stream_key: stream_key.clone(),
                    app_name,
                };

                vec![ServerSessionResult::Event(event)]
            }

            StreamState::Playing { ref stream_key } => {
                let event = ServerSessionEvent::PlayStreamFinished {
                    stream: self.stream_handles.find(stream_id).expect("known stream"),
                    stream_id: StreamId::new(stream_id),
                    app_name,
                    stream_key: stream_key.clone(),
                };

                vec![ServerSessionResult::Event(event)]
            }
            _ => Vec::new(),
        };

        if let Some(handle) = self.stream_handles.find(stream_id) {
            self.stream_handles.remove(handle);
        }
        Ok(result)
    }

    fn handle_command_publish(
        &mut self,
        stream_id: u32,
        transaction_id: f64,
        mut arguments: Vec<Amf0Value>,
    ) -> Result<Vec<ServerSessionResult>, ServerSessionError> {
        if arguments.len() < 2 {
            let packet = self.create_error_packet(
                "NetStream.Publish.Start",
                "Invalid publish arguments",
                transaction_id,
                stream_id,
            )?;
            return Ok(vec![ServerSessionResult::Packet(packet)]);
        }

        if self.current_state != SessionState::Connected {
            let packet = self.create_error_packet(
                "NetStream.Publish.Start",
                "Can't publish before connecting",
                transaction_id,
                stream_id,
            )?;
            return Ok(vec![ServerSessionResult::Packet(packet)]);
        }

        if self.stream_handles.find(stream_id).is_none() {
            let packet = self.create_error_packet(
                "NetStream.Failed",
                "Unknown message stream",
                transaction_id,
                stream_id,
            )?;
            return Ok(vec![ServerSessionResult::Packet(packet)]);
        }

        let app_name = match self.connected_app_name {
            Some(ref name) => name.clone(),
            None => {
                let packet = self.create_error_packet(
                    "NetStream.Publish.Start",
                    "Can't publish before connecting",
                    transaction_id,
                    stream_id,
                )?;
                return Ok(vec![ServerSessionResult::Packet(packet)]);
            }
        };

        let stream_key: Arc<str> = match arguments.remove(0) {
            Amf0Value::Utf8String(stream_key) => stream_key.into(),
            _ => {
                let packet = self.create_error_packet(
                    "NetStream.Publish.Start",
                    "Invalid publish arguments",
                    transaction_id,
                    stream_id,
                )?;
                return Ok(vec![ServerSessionResult::Packet(packet)]);
            }
        };

        let mode = match arguments.remove(0) {
            Amf0Value::Utf8String(raw_mode) => match raw_mode.to_ascii_lowercase().as_ref() {
                "live" => PublishMode::Live,
                "append" => PublishMode::Append,
                "record" => PublishMode::Record,
                _ => {
                    let error_properties = create_status_object(
                        "error",
                        "NetStream.Publish.Start",
                        "Invalid publish mode given",
                    );
                    let packet = self.create_error_response(
                        transaction_id,
                        Amf0Value::Null,
                        vec![Amf0Value::Object(error_properties)],
                        stream_id,
                    )?;

                    return Ok(vec![ServerSessionResult::Packet(packet)]);
                }
            },

            _ => {
                let packet = self.create_error_packet(
                    "NetStream.Publish.Start",
                    "Invalid publish arguments",
                    transaction_id,
                    stream_id,
                )?;
                return Ok(vec![ServerSessionResult::Packet(packet)]);
            }
        };

        let request = OutstandingRequest::PublishRequested {
            stream_key: stream_key.clone(),
            mode: mode.clone(),
            stream_id,
            encoding: self.response_encoding,
        };

        let request_number = self.register_request(request)?;

        let event = ServerSessionEvent::PublishStreamRequested {
            stream: self.stream_handles.find(stream_id).expect("known stream"),
            request_id: RequestId(request_number),
            app_name,
            stream_key,
            mode,
            stream_id: StreamId::new(stream_id),
        };

        Ok(vec![ServerSessionResult::Event(event)])
    }

    fn handle_command_play(
        &mut self,
        stream_id: u32,
        transaction_id: f64,
        mut arguments: Vec<Amf0Value>,
    ) -> Result<Vec<ServerSessionResult>, ServerSessionError> {
        if arguments.len() < 1 {
            let packet = self.create_error_packet(
                "NetStream.Play.Start",
                "Invalid play arguments",
                transaction_id,
                stream_id,
            )?;
            return Ok(vec![ServerSessionResult::Packet(packet)]);
        }

        if self.current_state != SessionState::Connected {
            let packet = self.create_error_packet(
                "NetStream.Play.Start",
                "Can't play before connecting",
                transaction_id,
                stream_id,
            )?;
            return Ok(vec![ServerSessionResult::Packet(packet)]);
        }

        if self.stream_handles.find(stream_id).is_none() {
            let packet = self.create_error_packet(
                "NetStream.Failed",
                "Unknown message stream",
                transaction_id,
                stream_id,
            )?;
            return Ok(vec![ServerSessionResult::Packet(packet)]);
        }

        let app_name = match self.connected_app_name {
            Some(ref name) => name.clone(),
            None => {
                let packet = self.create_error_packet(
                    "NetStream.Play.Start",
                    "Can't play before connecting",
                    transaction_id,
                    stream_id,
                )?;
                return Ok(vec![ServerSessionResult::Packet(packet)]);
            }
        };

        let stream_key: Arc<str> = match arguments.remove(0) {
            Amf0Value::Utf8String(stream_key) => stream_key.into(),
            _ => {
                let packet = self.create_error_packet(
                    "NetStream.Play.Start",
                    "Invalid play arguments",
                    transaction_id,
                    stream_id,
                )?;
                return Ok(vec![ServerSessionResult::Packet(packet)]);
            }
        };

        let start_at = if arguments.len() >= 1 {
            match arguments.remove(0) {
                Amf0Value::Number(x) => {
                    if x == -2.0 {
                        PlayStartValue::LiveOrRecorded
                    } else if x == -1.0 {
                        PlayStartValue::LiveOnly
                    } else if x >= 0.0 {
                        PlayStartValue::StartTimeInSeconds(x as u32)
                    } else {
                        PlayStartValue::LiveOrRecorded // Invalid value so return default
                    }
                }

                _ => PlayStartValue::LiveOrRecorded,
            }
        } else {
            PlayStartValue::LiveOrRecorded
        };

        let duration = if arguments.len() >= 1 {
            match arguments.remove(0) {
                Amf0Value::Number(x) => {
                    if x >= 0.0 {
                        Some(x as u32)
                    } else {
                        None
                    }
                }

                _ => None,
            }
        } else {
            None
        };

        let reset = if arguments.len() >= 1 {
            match arguments.remove(0) {
                Amf0Value::Boolean(x) => x,
                _ => false,
            }
        } else {
            false
        };

        let request = OutstandingRequest::PlayRequested {
            stream_key: stream_key.clone(),
            stream_id,
            encoding: self.response_encoding,
        };

        let request_number = self.register_request(request)?;

        let event = ServerSessionEvent::PlayStreamRequested {
            stream: self.stream_handles.find(stream_id).expect("known stream"),
            request_id: RequestId(request_number),
            app_name,
            stream_key,
            start_at,
            duration,
            reset,
            stream_id: StreamId::new(stream_id),
        };

        Ok(vec![ServerSessionResult::Event(event)])
    }

    fn audio_event<D>(
        &self,
        data: D,
        stream_id: u32,
        timestamp: RtmpTimestamp,
    ) -> Result<Option<ServerSessionEvent<D>>, ServerSessionError> {
        if self.current_state != SessionState::Connected {
            // Audio data sent before connected, just ignore it.
            return Ok(None);
        }

        let app_name = match self.connected_app_name {
            Some(ref x) => x.clone(),
            None => return Ok(None), // No app name so we aren't in a valid connection state.
        };

        let publish_stream_key = match self.active_streams.get(&stream_id) {
            Some(ref stream) => {
                match stream.current_state {
                    StreamState::Publishing {
                        ref stream_key,
                        mode: _,
                    } => stream_key.clone(),
                    _ => return Ok(None), // Not a publishing stream so ignore it
                }
            }

            None => return Ok(None), // Audio sent over an invalid stream, ignore it
        };

        let event = ServerSessionEvent::AudioDataReceived {
            stream: self.stream_handles.find(stream_id).expect("known stream"),
            stream_id: StreamId::new(stream_id),
            stream_key: publish_stream_key,
            app_name,
            timestamp,
            data,
        };

        Ok(Some(event))
    }

    fn handle_set_chunk_size(
        &mut self,
        size: u32,
    ) -> Result<Vec<ServerSessionResult>, ServerSessionError> {
        self.deserializer.set_chunk_size(size as usize)?;
        Ok(Vec::new())
    }

    fn handle_set_peer_bandwidth(
        &self,
        _size: u32,
        _limit_type: PeerBandwidthLimitType,
    ) -> Result<Vec<ServerSessionResult>, ServerSessionError> {
        Ok(Vec::new())
    }

    fn handle_user_control(
        &mut self,
        event_type: UserControlEventType,
        _stream_id: Option<u32>,
        _buffer_length: Option<u32>,
        timestamp: Option<RtmpTimestamp>,
    ) -> Result<Vec<ServerSessionResult>, ServerSessionError> {
        match event_type {
            UserControlEventType::PingRequest => {
                let message = RtmpMessage::UserControl {
                    event_type: UserControlEventType::PingResponse,
                    stream_id: None,
                    buffer_length: None,
                    timestamp,
                };

                let payload = self.to_payload(message, 0)?;
                let response = self.serializer.serialize(&payload, false, false)?;
                Ok(vec![ServerSessionResult::Packet(response)])
            }

            UserControlEventType::PingResponse => {
                let timestamp = timestamp.unwrap_or(RtmpTimestamp::new(0));
                let event = ServerSessionEvent::PingResponseReceived { timestamp };
                Ok(vec![ServerSessionResult::Event(event)])
            }

            _ => Ok(Vec::new()),
        }
    }

    fn video_event<D>(
        &self,
        data: D,
        stream_id: u32,
        timestamp: RtmpTimestamp,
    ) -> Result<Option<ServerSessionEvent<D>>, ServerSessionError> {
        if self.current_state != SessionState::Connected {
            // Video data sent before connected, just ignore it.
            return Ok(None);
        }

        let app_name = match self.connected_app_name {
            Some(ref x) => x.clone(),
            None => return Ok(None), // No app name so we aren't in a valid connection state.
        };

        let publish_stream_key = match self.active_streams.get(&stream_id) {
            Some(ref stream) => {
                match stream.current_state {
                    StreamState::Publishing {
                        ref stream_key,
                        mode: _,
                    } => stream_key.clone(),
                    _ => return Ok(None), // Not a publishing stream so ignore it
                }
            }

            None => return Ok(None), // Video sent over an invalid stream, ignore it
        };

        let event = ServerSessionEvent::VideoDataReceived {
            stream: self.stream_handles.find(stream_id).expect("known stream"),
            stream_id: StreamId::new(stream_id),
            stream_key: publish_stream_key,
            app_name,
            timestamp,
            data,
        };

        Ok(Some(event))
    }

    fn handle_window_acknowledgement(
        &mut self,
        size: u32,
    ) -> Result<Vec<ServerSessionResult>, ServerSessionError> {
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

    fn accept_connection_request(
        &mut self,
        app_name: Arc<str>,
        transaction_id: f64,
        response_properties: Amf0Object,
    ) -> Result<Vec<ServerSessionResult>, ServerSessionError> {
        let mut command_object_properties = Amf0Object::new();
        command_object_properties.insert(
            "fmsVer".to_string(),
            Amf0Value::Utf8String(self.fms_version.clone()),
        );
        command_object_properties.insert("capabilities".to_string(), Amf0Value::Number(31.0));

        let description = format!("Successfully connected on app: {app_name}");
        let mut additional_properties = create_status_object(
            "status",
            "NetConnection.Connect.Success",
            description.as_ref(),
        );
        additional_properties.insert(
            "objectEncoding".to_string(),
            Amf0Value::Number(self.negotiated_encoding.as_object_encoding()),
        );
        additional_properties.extend(response_properties);

        let message = RtmpMessage::Amf0Command {
            command_name: "_result".to_string(),
            transaction_id: transaction_id,
            command_object: Amf0Value::Object(command_object_properties),
            additional_arguments: vec![Amf0Value::Object(additional_properties)],
        };

        // Build every payload first. The application's properties may not
        // encode, and if they don't, nothing has changed yet.
        let mut payloads = vec![self.to_payload(message, 0)?];
        // `onBWDone` is a call on the client's NetConnection, which exists
        // only once the connect `_result` has been sent.
        if self.send_on_bw_done {
            // AMF0 regardless of the connect's framing, as it was when this
            // was part of the preamble.
            payloads.push(
                RtmpMessage::Amf0Command {
                    command_name: "onBWDone".to_string(),
                    transaction_id: 0.0,
                    command_object: Amf0Value::Null,
                    additional_arguments: vec![Amf0Value::Number(8192_f64)],
                }
                .into_raw_message(self.get_epoch(), 0)?,
            );
        }
        check_lengths(&payloads)?;

        // Nothing below can fail.
        self.connected_app_name = Some(app_name);
        self.current_state = SessionState::Connected;
        // The preamble goes first on the wire, so it is serialized first: the
        // result is chunked at the chunk size the preamble announces.
        let mut results = self.take_connection_preamble()?;
        results.extend(self.serialize_all(payloads)?);
        Ok(results)
    }

    fn accept_publish_request(
        &mut self,
        stream_id: u32,
        stream_key: Arc<str>,
        mode: PublishMode,
    ) -> Result<Vec<ServerSessionResult>, ServerSessionError> {
        if !self.active_streams.contains_key(&stream_id) {
            return Err(ServerSessionError::ActionAttemptedOnInactiveStream {
                action: "publish".to_string(),
                stream_id,
            });
        }

        let description = format!(
            "Successfully started publishing on stream key {}",
            stream_key
        );

        let stream_begin_message = RtmpMessage::UserControl {
            event_type: UserControlEventType::StreamBegin,
            stream_id: Some(stream_id),
            buffer_length: None,
            timestamp: None,
        };

        // RTMP 1.0 section 6.2: User Control messages use message stream 0;
        // the stream they concern is in the event data.
        let stream_begin_payload = self.to_payload(stream_begin_message, 0)?;

        let status_object =
            create_status_object("status", "NetStream.Publish.Start", description.as_ref());
        let publish_start_message = RtmpMessage::Amf0Command {
            command_name: "onStatus".to_string(),
            transaction_id: 0.0,
            command_object: Amf0Value::Null,
            additional_arguments: vec![Amf0Value::Object(status_object)],
        };

        let publish_start_payload = self.to_payload(publish_start_message, stream_id)?;
        let payloads = vec![stream_begin_payload, publish_start_payload];
        check_lengths(&payloads)?;

        // Nothing below can fail.
        if let Some(active_stream) = self.active_streams.get_mut(&stream_id) {
            active_stream.current_state = StreamState::Publishing { stream_key, mode };
        }
        self.serialize_all(payloads)
    }

    fn accept_play_request(
        &mut self,
        stream_id: u32,
        stream_key: Arc<str>,
    ) -> Result<Vec<ServerSessionResult>, ServerSessionError> {
        if !self.active_streams.contains_key(&stream_id) {
            return Err(ServerSessionError::ActionAttemptedOnInactiveStream {
                action: "play".to_string(),
                stream_id,
            });
        }

        let reset_status_object =
            create_status_object("status", "NetStream.Play.Reset", "Reset stream");
        let reset_message = RtmpMessage::Amf0Command {
            command_name: "onStatus".to_string(),
            transaction_id: 0.0,
            command_object: Amf0Value::Null,
            additional_arguments: vec![Amf0Value::Object(reset_status_object)],
        };

        let stream_begin_message = RtmpMessage::UserControl {
            event_type: UserControlEventType::StreamBegin,
            stream_id: Some(stream_id),
            buffer_length: None,
            timestamp: None,
        };

        let description = format!("Successfully started playback on stream key {}", stream_key);
        let start_status_object =
            create_status_object("status", "NetStream.Play.Start", description.as_ref());
        let start_message = RtmpMessage::Amf0Command {
            command_name: "onStatus".to_string(),
            transaction_id: 0.0,
            command_object: Amf0Value::Null,
            additional_arguments: vec![Amf0Value::Object(start_status_object)],
        };

        let data1_message = RtmpMessage::Amf0Data {
            values: vec![
                Amf0Value::Utf8String("|RtmpSampleAccess".to_string()),
                Amf0Value::Boolean(false),
                Amf0Value::Boolean(false),
            ],
        };

        let mut data_start_properties = Amf0Object::new();
        data_start_properties.insert(
            "code".to_string(),
            Amf0Value::Utf8String("NetStream.Data.Start".to_string()),
        );

        let data2_message = RtmpMessage::Amf0Data {
            values: vec![
                Amf0Value::Utf8String("onStatus".to_string()),
                Amf0Value::Object(data_start_properties),
            ],
        };

        // Built in the order the packets are sent, and serialized in that
        // order: each chunk header is compressed against the previous message
        // on its chunk stream.
        let payloads = vec![
            self.to_payload(reset_message, stream_id)?,
            // RTMP 1.0 section 6.2: User Control messages use message stream
            // 0; the stream they concern is in the event data.
            self.to_payload(stream_begin_message, 0)?,
            self.to_payload(start_message, stream_id)?,
            self.to_payload(data1_message, stream_id)?,
            self.to_payload(data2_message, stream_id)?,
        ];
        check_lengths(&payloads)?;

        // Nothing below can fail.
        if let Some(active_stream) = self.active_streams.get_mut(&stream_id) {
            active_stream.current_state = StreamState::Playing { stream_key };
        }
        self.serialize_all(payloads)
    }

    /// Serialize messages that were all built beforehand, in order.
    ///
    /// Serializing can only fail for a message over the RTMP length limit,
    /// which [`check_lengths`] rules out first. So a caller that builds every
    /// payload before changing session state either sends all of it or
    /// changes nothing.
    fn serialize_all(
        &mut self,
        payloads: Vec<RawMessage>,
    ) -> Result<Vec<ServerSessionResult>, ServerSessionError> {
        check_lengths(&payloads)?;
        let mut results = Vec::with_capacity(payloads.len());
        for payload in &payloads {
            let packet = self.serializer.serialize(payload, false, false)?;
            results.push(ServerSessionResult::Packet(packet));
        }
        Ok(results)
    }

    fn create_success_response(
        &mut self,
        transaction_id: f64,
        command_object: Amf0Value,
        additional_arguments: Vec<Amf0Value>,
        stream_id: u32,
    ) -> Result<Packet, ServerSessionError> {
        let message = RtmpMessage::Amf0Command {
            command_name: "_result".to_string(),
            transaction_id,
            command_object,
            additional_arguments,
        };

        let payload = self.to_payload(message, stream_id)?;
        let packet = self.serializer.serialize(&payload, false, false)?;
        Ok(packet)
    }

    fn create_error_response(
        &mut self,
        transaction_id: f64,
        command_object: Amf0Value,
        additional_arguments: Vec<Amf0Value>,
        stream_id: u32,
    ) -> Result<Packet, ServerSessionError> {
        let message = RtmpMessage::Amf0Command {
            command_name: "_error".to_string(),
            transaction_id,
            command_object,
            additional_arguments,
        };

        let payload = self.to_payload(message, stream_id)?;
        let packet = self.serializer.serialize(&payload, false, false)?;
        Ok(packet)
    }

    fn get_epoch(&self) -> RtmpTimestamp {
        RtmpTimestamp::new(self.start_time.elapsed().as_millis() as u32)
    }

    /// An `_error` answer, built but not yet serialized.
    fn error_payload(
        &self,
        code: &str,
        description: &str,
        transaction_id: f64,
        stream_id: u32,
    ) -> Result<RawMessage, ServerSessionError> {
        // RTMP 1.0 allows "warning", "status" or "error" as the level;
        // `_error` is the command name, not a level.
        let status_object = create_status_object("error", code, description);
        let message = RtmpMessage::Amf0Command {
            command_name: "_error".to_string(),
            transaction_id,
            command_object: Amf0Value::Null,
            additional_arguments: vec![Amf0Value::Object(status_object)],
        };
        self.to_payload(message, stream_id)
    }

    fn create_error_packet(
        &mut self,
        code: &str,
        description: &str,
        transaction_id: f64,
        stream_id: u32,
    ) -> Result<Packet, ServerSessionError> {
        let payload = self.error_payload(code, description, transaction_id, stream_id)?;
        Ok(self.serializer.serialize(&payload, false, false)?)
    }
}

/// The RTMP message length field is 24 bits.
const MAX_MESSAGE_LENGTH: usize = 0xff_ffff;

/// Fail before anything is serialized if a message is too long to send.
fn check_lengths(payloads: &[RawMessage]) -> Result<(), ServerSessionError> {
    match payloads
        .iter()
        .find(|payload| payload.data.len() > MAX_MESSAGE_LENGTH)
    {
        Some(payload) => Err(EncodeError::MessageTooLong {
            size: payload.data.len().min(u32::MAX as usize) as u32,
        }
        .into()),
        None => Ok(()),
    }
}

/// Values for the control messages sent before the answer to `connect`.
struct ConnectionPreamble {
    chunk_size: u32,
    window_ack_size: u32,
    peer_bandwidth: u32,
}

/// How much of an unknown command's name an `_error` description repeats.
const MAX_ECHOED_COMMAND_NAME: usize = 64;

/// The [`CallId`] a client's `_result`/`_error` transaction id refers to,
/// if it can be one: calls are numbered with positive integers.
fn call_id_of(transaction_id: f64) -> Option<u32> {
    (transaction_id.fract() == 0.0 && transaction_id >= 1.0 && transaction_id <= u32::MAX as f64)
        .then(|| transaction_id as u32)
}

fn unhandled(
    stream_id: u32,
    name: String,
    transaction_id: f64,
    command_object: Amf0Value,
    additional_values: Vec<Amf0Value>,
) -> ServerSessionResult {
    ServerSessionResult::Event(ServerSessionEvent::UnhandledCommand {
        stream_id: StreamId::new(stream_id),
        command_name: name,
        transaction_id,
        command_object,
        additional_values,
    })
}

impl ServerSession {
    /// Legacy setup commands that this session answers with `_result` and
    /// otherwise ignores.
    ///
    /// They are not part of RTMP 1.0. Flash Media Live Encoder introduced
    /// them, and OBS, FFmpeg, librtmp and most hardware encoders send them
    /// before `createStream`, and `FCUnpublish` before `deleteStream`. This
    /// session keeps no state for them, so a successful no-op is the honest
    /// answer. `releaseStream` asks the server to free a stream name a
    /// previous connection may still hold, which only the application can do;
    /// one that tracks stream names can take it over with
    /// [`delegate_command`](Self::delegate_command). Every other command the
    /// session does not implement is refused with `_error`.
    pub const NO_OP_COMMANDS: &'static [&'static str] =
        &["releaseStream", "FCPublish", "FCUnpublish"];
}

fn create_status_object(level: &str, code: &str, description: &str) -> Amf0Object {
    let mut properties = Amf0Object::new();
    properties.insert(
        "level".to_string(),
        Amf0Value::Utf8String(level.to_string()),
    );
    properties.insert("code".to_string(), Amf0Value::Utf8String(code.to_string()));
    properties.insert(
        "description".to_string(),
        Amf0Value::Utf8String(description.to_string()),
    );
    properties
}

impl ServerSession {
    /// Prepare audio without copying. Accepts `Bytes`, segmented `Payload`, or owned vectors.
    pub fn send_audio(
        &mut self,
        stream: StreamHandle,
        data: impl Into<crate::Payload>,
        timestamp: RtmpTimestamp,
        drop_policy: crate::chunk_io::DropPolicy,
    ) -> Result<crate::chunk_io::Packet, ServerSessionError> {
        self.ensure_output_drained()?;
        let stream_id = self.playback_id(stream)?;
        self.ensure_active()?;
        let stream_id = stream_id.get();
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

impl ServerSession {
    /// Prepare video without copying. Accepts `Bytes`, segmented `Payload`, or owned vectors.
    pub fn send_video(
        &mut self,
        stream: StreamHandle,
        data: impl Into<crate::Payload>,
        timestamp: RtmpTimestamp,
        drop_policy: crate::chunk_io::DropPolicy,
    ) -> Result<crate::chunk_io::Packet, ServerSessionError> {
        self.ensure_output_drained()?;
        let stream_id = self.playback_id(stream)?;
        self.ensure_active()?;
        let stream_id = stream_id.get();
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

impl ServerSession {
    /// Prepare encoded script data verbatim, without decoding or copying its body.
    pub fn send_data<D: Into<crate::Payload>>(
        &mut self,
        stream: StreamHandle,
        message: DataMessage<D>,
    ) -> Result<crate::chunk_io::Packet, ServerSessionError> {
        self.ensure_output_drained()?;
        let stream_id = self.playback_id(stream)?;
        self.ensure_active()?;
        let stream_id = stream_id.get();
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

type ServerSessionEvent<D = Bytes> = ServerEvent<D>;

impl ServerSession {
    /// Queue the protocol outputs. Drain them through `receive`, including with empty input.
    pub fn accept_request(&mut self, request_id: RequestId) -> Result<(), ServerSessionError> {
        let outputs = self.accept_request_inner(request_id)?;
        self.pending.extend(
            outputs
                .into_iter()
                .map(|output| output.map_payload(crate::Payload::from)),
        );
        Ok(())
    }
}

impl ServerSession {
    /// Queue the protocol outputs. Drain them through `receive`, including with empty input.
    pub fn accept_request_with_properties(
        &mut self,
        request_id: RequestId,
        response_properties: Amf0Object,
    ) -> Result<(), ServerSessionError> {
        let outputs = self.accept_request_with_properties_inner(request_id, response_properties)?;
        self.pending.extend(
            outputs
                .into_iter()
                .map(|output| output.map_payload(crate::Payload::from)),
        );
        Ok(())
    }
}

impl ServerSession {
    /// Queue the protocol outputs. Drain them through `receive`, including with empty input.
    pub fn reject_request(
        &mut self,
        request_id: RequestId,
        code: &str,
        description: &str,
    ) -> Result<(), ServerSessionError> {
        let outputs = self.reject_request_inner(request_id, code, description)?;
        self.pending.extend(
            outputs
                .into_iter()
                .map(|output| output.map_payload(crate::Payload::from)),
        );
        Ok(())
    }
}

/// Answering commands in the application.
///
/// None of this is needed for an ordinary server: the session answers every
/// command itself. It is for applications that implement a command the
/// session does not, such as a bandwidth check (`_checkbw`), or that want
/// their own behaviour for one it answers as a no-op, such as `FCPublish`.
impl ServerSession {
    /// Let the application answer the client's `name` command.
    ///
    /// From now on, a `name` call that expects an answer raises
    /// [`ServerEvent::CommandRequested`] instead of being answered by the
    /// session. Answer it with [`accept_command`](Self::accept_command),
    /// [`accept_request`](Self::accept_request) (an empty `_result`) or
    /// [`reject_request`](Self::reject_request) (an `_error`); the client
    /// waits until you do. A call with transaction id 0 expects no answer and
    /// still raises [`ServerEvent::UnhandledCommand`].
    ///
    /// Commands the session implements, such as `connect`, `createStream`,
    /// `publish` and `play`, cannot be delegated, and neither can `_result`
    /// and `_error`, which answer calls rather than make them; naming one has
    /// no effect.
    pub fn delegate_command(&mut self, name: impl Into<String>) {
        let name = name.into();
        if !self.delegated_commands.contains(&name) {
            self.delegated_commands.push(name);
        }
    }

    /// Answer a [`ServerEvent::CommandRequested`] with
    /// `_result(null, values...)`. Drain the output through `receive`.
    ///
    /// Fails with [`ServerSessionError::InvalidRequestId`] when `request_id`
    /// is not an outstanding command request. If `values` cannot be encoded,
    /// the request stays outstanding and can still be answered.
    pub fn accept_command(
        &mut self,
        request_id: RequestId,
        values: Vec<Amf0Value>,
    ) -> Result<(), ServerSessionError> {
        let outputs = self.answer_command(request_id, values, None)?;
        self.pending.extend(
            outputs
                .into_iter()
                .map(|output| output.map_payload(crate::Payload::from)),
        );
        Ok(())
    }

    /// Call `name` on the client and expect an answer. Drain the output
    /// through `receive`.
    ///
    /// The client's `_result` or `_error` raises [`ServerEvent::CallAnswered`]
    /// with the returned [`CallId`]. An unanswered call holds a place in
    /// [`SessionLimits::max_pending_requests`](crate::sessions::SessionLimits::max_pending_requests),
    /// shared with requests awaiting the application, until it is answered or
    /// [forgotten](Self::forget_call). RTMP has no way to cancel a call.
    pub fn call(
        &mut self,
        name: &str,
        arguments: Vec<Amf0Value>,
    ) -> Result<CallId, ServerSessionError> {
        self.ensure_client_callable()?;
        self.session_limits
            .check_requests(self.pending_requests())?;
        // Ids stay positive so they never read as "no answer expected", and
        // skip any still awaiting an answer.
        let mut call_id = self.next_call_id;
        while call_id == 0 || self.outstanding_calls.contains(&call_id) {
            call_id = call_id.wrapping_add(1);
        }
        let packet = self.command_to_client(name, f64::from(call_id), arguments)?;
        self.pending.push_back(ServerOutput::Packet(packet));
        self.next_call_id = call_id.wrapping_add(1);
        self.outstanding_calls.insert(call_id);
        Ok(CallId(call_id))
    }

    /// Stop waiting for the answer to `call_id`, for example after the
    /// application's own timeout, and free its place in
    /// [`SessionLimits::max_pending_requests`](crate::sessions::SessionLimits::max_pending_requests).
    /// A late answer then raises [`ServerEvent::UnhandledCommand`]. Returns
    /// whether the call was still awaiting an answer.
    pub fn forget_call(&mut self, call_id: CallId) -> bool {
        self.outstanding_calls.remove(&call_id.0)
    }

    /// Call `name` on the client without expecting an answer (transaction id
    /// 0), for example `onBWDone` or `onFCPublish`. Drain the output through
    /// `receive`.
    pub fn notify(
        &mut self,
        name: &str,
        arguments: Vec<Amf0Value>,
    ) -> Result<(), ServerSessionError> {
        self.ensure_client_callable()?;
        let packet = self.command_to_client(name, 0.0, arguments)?;
        self.pending.push_back(ServerOutput::Packet(packet));
        Ok(())
    }

    fn ensure_client_callable(&self) -> Result<(), ServerSessionError> {
        self.ensure_active()?;
        if self.current_state == SessionState::Connected {
            Ok(())
        } else {
            Err(ServerSessionError::NotConnected)
        }
    }

    /// A command to the client's NetConnection (message stream 0).
    fn command_to_client(
        &mut self,
        name: &str,
        transaction_id: f64,
        arguments: Vec<Amf0Value>,
    ) -> Result<Packet, ServerSessionError> {
        self.response_encoding = self.server_initiated_encoding();
        let message = self.command_message(name, transaction_id, Amf0Value::Null, arguments);
        let payload = message.into_raw_message(self.get_epoch(), 0)?;
        Ok(self.serializer.serialize(&payload, false, false)?)
    }
}

impl ServerSession {
    /// Requests and calls awaiting an answer, in either direction. They share
    /// [`SessionLimits::max_pending_requests`](crate::sessions::SessionLimits::max_pending_requests).
    fn pending_requests(&self) -> usize {
        self.outstanding_requests.len() + self.outstanding_calls.len()
    }

    /// Record a request awaiting the application's decision. Numbers still in
    /// use are skipped, so a long-lived request is never overwritten.
    fn register_request(&mut self, request: OutstandingRequest) -> Result<u32, ServerSessionError> {
        self.session_limits
            .check_requests(self.pending_requests())?;
        let mut request_number = self.next_request_number;
        while self.outstanding_requests.contains_key(&request_number) {
            request_number = request_number.wrapping_add(1);
        }
        self.next_request_number = request_number.wrapping_add(1);
        self.outstanding_requests.insert(request_number, request);
        Ok(request_number)
    }

    fn ensure_output_drained(&self) -> Result<(), ServerSessionError> {
        self.ensure_active()?;
        if !self.pending.is_empty() {
            return Err(ServerSessionError::PendingOutput);
        }
        if self.current_state != SessionState::Connected {
            return Err(ServerSessionError::NotConnected);
        }
        Ok(())
    }
}

type DataMessage<D = Bytes> = GenericDataMessage<D>;
