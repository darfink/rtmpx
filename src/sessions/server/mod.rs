use crate::sessions::streams::Streams;
use crate::sessions::{ConnectionState, ServerStreamState, StreamHandle};
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
use crate::chunk_io::{ChunkEncoder, DecodeError, MessageDecoder, Packet};
use crate::messages::RawMessage;
use crate::messages::{PeerBandwidthLimitType, RtmpMessage, UserControlEventType};
use crate::sessions::{
    DataMessage as GenericDataMessage, DataMessageType, RequestId, StreamId, StreamMetadata,
};
use crate::time::RtmpTimestamp;
use bytes::Bytes;
use std::collections::HashMap;
use std::mem;
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
    // Protocol control is emitted only after the peer's `connect` request is
    // accepted. Sending it from `new` causes pre-connect writes and diverges
    // from RTMP server behaviour in FFmpeg, OBS, and FMS.
    pending_connection_control: Vec<ServerSessionResult>,
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
        let mut session = ServerSession {
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
            pending_connection_control: Vec::new(),
        };

        let mut results = Vec::with_capacity(4);

        let chunk_size_packet = session
            .serializer
            .set_chunk_size(config.chunk_size, RtmpTimestamp::new(0))?;
        results.push(ServerSessionResult::Packet(chunk_size_packet));

        let window_ack_message = RtmpMessage::WindowAcknowledgement {
            size: config.window_ack_size,
        };
        let window_ack_payload = session.to_payload(window_ack_message, 0)?;
        let window_ack_packet = session
            .serializer
            .serialize(&window_ack_payload, true, false)?;
        results.push(ServerSessionResult::Packet(window_ack_packet));

        let begin_message = RtmpMessage::UserControl {
            event_type: UserControlEventType::StreamBegin,
            stream_id: Some(0),
            timestamp: None,
            buffer_length: None,
        };

        let begin_payload = session.to_payload(begin_message, 0)?;
        let begin_packet = session.serializer.serialize(&begin_payload, true, false)?;
        results.push(ServerSessionResult::Packet(begin_packet));

        let peer_message = RtmpMessage::SetPeerBandwidth {
            size: config.peer_bandwidth,
            limit_type: PeerBandwidthLimitType::Dynamic,
        };
        let peer_payload = session.to_payload(peer_message, 0)?;
        let peer_packet = session.serializer.serialize(&peer_payload, true, false)?;
        results.push(ServerSessionResult::Packet(peer_packet));

        if config.send_on_bw_done_message_on_start {
            let bw_done_message = RtmpMessage::Amf0Command {
                command_name: "onBWDone".to_string(),
                transaction_id: 0.0,
                command_object: Amf0Value::Null,
                additional_arguments: vec![Amf0Value::Number(8192_f64)],
            };

            let bw_done_payload = session.to_payload(bw_done_message, 0)?;
            let bw_done_packet = session
                .serializer
                .serialize(&bw_done_payload, true, false)?;
            results.push(ServerSessionResult::Packet(bw_done_packet));
        }

        session.pending_connection_control = results;
        Ok(session)
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
        let request_id = request_id.get();
        let request = match self.outstanding_requests.remove(&request_id) {
            Some(x) => x,
            None => return Err(ServerSessionError::InvalidRequestId),
        };

        match request {
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
        }
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
        let request_id = request_id.get();
        let request = match self.outstanding_requests.remove(&request_id) {
            Some(x) => x,
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
        // the same protocol preamble (chunk size, window acknowledgement,
        // onBWDone, ...) the accept path prepends. Without it the _error reuses
        // the preamble's chunk stream as a continuation the peer never saw and
        // cannot decode, and the client observes a bare drop instead of the
        // rejection.
        let mut results = if matches!(request, OutstandingRequest::ConnectionRequest { .. }) {
            mem::replace(&mut self.pending_connection_control, Vec::new())
        } else {
            Vec::new()
        };
        let packet = match request {
            OutstandingRequest::ConnectionRequest { .. } => {
                self.create_error_packet(code, description, transaction_id, stream_id)?
            }
            OutstandingRequest::PublishRequested { .. }
            | OutstandingRequest::PlayRequested { .. } => {
                let status = amf::status_object::<Amf0Value>("error", code, description);
                let message = self.command_message("onStatus", 0.0, Amf0Value::Null, vec![status]);
                let payload = self.to_payload(message, stream_id)?;
                self.serializer.serialize(&payload, false, false)?
            }
        };

        results.push(ServerSessionResult::Packet(packet));
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

            _ => vec![ServerSessionResult::Event(
                ServerSessionEvent::UnhandledCommand {
                    stream_id: StreamId::new(stream_id),
                    command_name: name,
                    additional_values: additional_args,
                    transaction_id,
                    command_object,
                },
            )],
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

        let request_number = self.next_request_number;
        self.next_request_number = self.next_request_number.wrapping_add(1);
        self.session_limits
            .check_requests(self.outstanding_requests.len())?;
        self.outstanding_requests.insert(request_number, request);

        let event = ServerSessionEvent::ConnectionRequested {
            app_name: app_name,
            request_id: RequestId(request_number),
            // Hand the caller whatever the client sent beyond the
            // fields we consumed above, rather than dropping it.
            additional_properties: properties,
        };

        Ok(vec![ServerSessionResult::Event(event)])
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
                | OutstandingRequest::PublishRequested { stream_id: id, .. } => *id != stream_id,
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

        let request_number = self.next_request_number;
        self.next_request_number = self.next_request_number.wrapping_add(1);
        self.session_limits
            .check_requests(self.outstanding_requests.len())?;
        self.outstanding_requests.insert(request_number, request);

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

        let request_number = self.next_request_number;
        self.next_request_number = self.next_request_number.wrapping_add(1);
        self.session_limits
            .check_requests(self.outstanding_requests.len())?;
        self.outstanding_requests.insert(request_number, request);

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
        self.connected_app_name = Some(app_name.clone());
        self.current_state = SessionState::Connected;

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

        let payload = self.to_payload(message, 0)?;
        let packet = self.serializer.serialize(&payload, false, false)?;

        let mut results = mem::replace(&mut self.pending_connection_control, Vec::new());
        results.push(ServerSessionResult::Packet(packet));
        Ok(results)
    }

    fn accept_publish_request(
        &mut self,
        stream_id: u32,
        stream_key: Arc<str>,
        mode: PublishMode,
    ) -> Result<Vec<ServerSessionResult>, ServerSessionError> {
        match self.active_streams.get_mut(&stream_id) {
            Some(active_stream) => {
                active_stream.current_state = StreamState::Publishing {
                    stream_key: stream_key.clone(),
                    mode,
                };
            }

            None => {
                return Err(ServerSessionError::ActionAttemptedOnInactiveStream {
                    action: "publish".to_string(),
                    stream_id,
                });
            }
        };

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

        let stream_begin_payload = self.to_payload(stream_begin_message, stream_id)?;
        let stream_begin_packet = self
            .serializer
            .serialize(&stream_begin_payload, false, false)?;

        let status_object =
            create_status_object("status", "NetStream.Publish.Start", description.as_ref());
        let publish_start_message = RtmpMessage::Amf0Command {
            command_name: "onStatus".to_string(),
            transaction_id: 0.0,
            command_object: Amf0Value::Null,
            additional_arguments: vec![Amf0Value::Object(status_object)],
        };

        let publish_start_payload = self.to_payload(publish_start_message, stream_id)?;
        let publish_packet = self
            .serializer
            .serialize(&publish_start_payload, false, false)?;

        Ok(vec![
            ServerSessionResult::Packet(stream_begin_packet),
            ServerSessionResult::Packet(publish_packet),
        ])
    }

    fn accept_play_request(
        &mut self,
        stream_id: u32,
        stream_key: Arc<str>,
    ) -> Result<Vec<ServerSessionResult>, ServerSessionError> {
        match self.active_streams.get_mut(&stream_id) {
            Some(active_stream) => {
                active_stream.current_state = StreamState::Playing {
                    stream_key: stream_key.clone(),
                };
            }

            None => {
                return Err(ServerSessionError::ActionAttemptedOnInactiveStream {
                    action: "play".to_string(),
                    stream_id,
                });
            }
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

        let stream_begin_payload = self.to_payload(stream_begin_message, stream_id)?;
        let stream_begin_packet = self
            .serializer
            .serialize(&stream_begin_payload, false, false)?;

        let start_payload = self.to_payload(start_message, stream_id)?;
        let start_packet = self.serializer.serialize(&start_payload, false, false)?;

        let data1_payload = self.to_payload(data1_message, stream_id)?;
        let data1_packet = self.serializer.serialize(&data1_payload, false, false)?;

        let data2_payload = self.to_payload(data2_message, stream_id)?;
        let data2_packet = self.serializer.serialize(&data2_payload, false, false)?;

        let reset_payload = self.to_payload(reset_message, stream_id)?;
        let reset_packet = self.serializer.serialize(&reset_payload, false, false)?;

        Ok(vec![
            ServerSessionResult::Packet(reset_packet),
            ServerSessionResult::Packet(stream_begin_packet),
            ServerSessionResult::Packet(start_packet),
            ServerSessionResult::Packet(data1_packet),
            ServerSessionResult::Packet(data2_packet),
        ])
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

    fn create_error_packet(
        &mut self,
        code: &str,
        description: &str,
        transaction_id: f64,
        stream_id: u32,
    ) -> Result<Packet, ServerSessionError> {
        let status_object = create_status_object("_error", code, description);
        let packet = self.create_error_response(
            transaction_id,
            Amf0Value::Null,
            vec![Amf0Value::Object(status_object)],
            stream_id,
        )?;
        Ok(packet)
    }
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

impl ServerSession {
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
