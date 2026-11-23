# Changelog

## Unreleased (4.0.0)

Once `connect` is accepted, the server session now answers every command it does not implement when the command has a finite, nonzero transaction id, as RTMP 1.0 section 7.2 requires. Previously these commands got no answer, so a client that waited for one stalled. Commands the session implements keep their own contracts: `createStream` gets `_result`, `publish` and `play` get `onStatus`, and `deleteStream` and `closeStream` get no answer.

This is a major release because behaviour changes on upgrade without a compile error. Review the first three entries below.

### Breaking behaviour

- Refuse any unimplemented command other than the no-ops below with `_error` (`NetConnection.Call.Failed`, "Method not found (name).") and raise the new `ServerEvent::CommandRefused`. Refused commands no longer raise `UnhandledCommand`; handle `CommandRefused` where you logged or counted them.
- `send_on_bw_done_message_on_start` now defaults to `false`. `onBWDone` is not part of RTMP 1.0, and librtmp and FFmpeg answer it with a `_checkbw` the session would refuse. Set it to keep sending `onBWDone`; it now follows the connect `_result`, and a refused `connect` does not get it.
- Answer `releaseStream`, `FCPublish` and `FCUnpublish` with a no-op `_result(null, undefined)`. They are listed in `ServerSession::NO_OP_COMMANDS`.
- Remove `ServerEvent::ReleaseStreamRequested`. It was never raised.

### Fixes

- Send the connect flow in the order of RTMP 1.0 section 7.2.1.1: Window Acknowledgement Size, Set Peer Bandwidth, StreamBegin, `_result`.
- Serialize that preamble when `connect` is answered rather than when the session is created. An acknowledgement or ping response sent before then was compressed against preamble packets the client had not received, and could not be decoded.
- Accepting or rejecting a request no longer gives it up when its answer cannot be encoded, for example because of `accept_request_with_properties` values. The call fails, the request stays outstanding, and the session state is unchanged, so the application can still answer it.
- Send StreamBegin on message stream 0 when accepting `publish` and `play`, as RTMP 1.0 section 6.2 specifies for User Control messages.
- Report a refused `connect` at level `error`. It was `_error`, which is not an RTMP 1.0 status level.
- Fix `play` acceptance sending `NetStream.Play.Reset` out of serialization order, which compressed its chunk header against a message the client had not received yet.
- Transaction id 0, non-finite transaction ids, `_result`/`_error` that answer no server call, and commands before `connect` is accepted are not answered. They raise `UnhandledCommand`, which is now informational.

### Added

These are only needed by applications that implement commands the session does not. Like `accept_request`, each queues its output; drain it through `receive`.

- `ServerSession::delegate_command(name)` hands `name` to the application. Calls to it raise `ServerEvent::CommandRequested`; answer with `accept_command(request_id, values)`, `accept_request` (`_result(null, undefined)`) or `reject_request` (`_error`). A request whose answer cannot be encoded stays outstanding, and deleting its stream expires it.
- `ServerSession::call(name, arguments)` calls a method on the client and returns a `CallId`. The client's answer on message stream 0 raises `ServerEvent::CallAnswered`. `forget_call(call_id)` stops waiting for one the client never answers.
- `ServerSession::notify(name, arguments)` calls a method on the client with transaction id 0, for notifications such as `onBWDone` or `onFCPublish`.
- Delegated requests and unanswered calls share `SessionLimits::max_pending_requests`.

## 3.1.2 — 2026-11-19

- Reorganize the README and polish crate documentation, the ownership guide, and the Enhanced RTMP compliance matrix.
- Fix broken test-suite references and align CI documentation with the active jobs.

## 3.1.1 — 2026-10-02

- Accept GStreamer's little-endian FourCC codec ids in `onMetaData`. `eflvmux` writes `avc1` as `1cva`, so strict validation rejected every GStreamer multitrack publication.
- Limit numeric `onMetaData` codec labels to known FourCCs, as string labels already were.

## 3.1.0 — 2026-09-20

- Add elementary mappings for FLAC, AC-3, E-AC-3, MP3, VP8, VP9, and VVC.
- Map legacy MP3, including the 8 kHz format identifier.
- Add ordered elementary events for sequence ends and typed skipped-message reasons.
- Preserve signed VVC composition offsets and reject undersized multitrack offset fields.
- Add codec mapping, control-event, multitrack, and AMF0/AMF3 session regression tests.

These mappings preserve opaque codec payloads. They do not add codec decoding or HLS packaging.

## 3.0.1 — 2026-09-14

- Refresh the readme introduction around production readiness and allocation behavior.
- Drop the license badge from the readme header.

## 3.0.0 — 2026-09-12

Migrate callers before upgrading. Version 3 replaces the version 2 public API.

### Session API

- Client and server sessions expose a pull loop through `receive(&mut Bytes)`.
- Session operations queue control outputs. Media sends return an owned `Packet`.
- Local `StreamHandle` values identify stream lifetimes independently of wire `StreamId` values.
- Clients can create, delete, play, and publish separate streams on one connection.
- Servers expose matching stream state, request handling, and playback completion.
- Session limits bound streams and pending requests.
- Socket deadlines belong to the caller. The transport timeout type was removed.

### Payload ownership

- Streaming chunk parsing retains slices of owned input buffers.
- Pooled payload descriptors reduce repeated allocations for fragmented messages.
- Packet cursors emit inline chunk headers and payload slices through vectored writes.
- Cursors retain progress across partial writes.
- Borrowed media inspection supports segmented payloads.
- Elementary-unit visitors avoid a temporary result collection.
- Input buffers, control messages, TLS, and explicit coalescing can still allocate.

### AMF

- AMF0 and AMF3 graph APIs preserve references, shared objects, and cycles.
- Tree APIs remain available for callers that do not need object identity.

### Migration

| Version 2 | Version 3 |
|---|---|
| `ChunkSerializer` | `ChunkEncoder` |
| `ChunkDeserializer` | `ContiguousDecoder`, `MessageDecoder`, or `ChunkParser` |
| `MessagePayload` | `RawMessage` |
| `handle_input` and result vectors | `receive` and individual outputs |
| `ServerSessionResult` / `ClientSessionResult` | `ServerOutput` / `ClientOutput` |
| `ServerSessionEvent` / `ClientSessionEvent` | `ServerEvent` / `ClientEvent` |
| `PeerType` / `HandshakeProcessResult` | `HandshakeRole` / `HandshakeProgress` |
| Automatic metadata events | `StreamDataReceived` with explicit metadata inspection |

See [the ownership guide](docs/zero-copy.md) and the examples for complete call sequences.
