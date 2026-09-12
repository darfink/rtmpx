# RTMPX API and ownership guide

RTMPX performs no network I/O. Sessions own protocol state. Applications own transports and scheduling.
The default session payload is `Payload`, which retains receive segments without copying their bytes.

## Start with sessions

`ClientSession::new(config)` and `ServerSession::new(config)` return one session. Create them after the RTMP handshake.
Connection and request-decision actions return `Result<()>` and queue their outputs:

- Client: `connect`, `connect_with_properties`, `delete_stream`
- Server: `accept_request`, `accept_request_with_properties`, `reject_request`

Both sessions expose `receive(&mut Bytes) -> Result<Option<Output>, Error>`.
The concrete output types are `ClientOutput` and `ServerOutput`:

- `Packet(packet)` contains outbound wire data
- `Event(event)` contains a `ClientEvent` or `ServerEvent`
- `UnhandledMessage(message)` preserves an unhandled message and its segmented body

Each call returns at most one output. Unread input stays with the caller.
The output does not borrow the session or input variable. It can move into an asynchronous queue.
`None` means that pending outputs are drained and more input is required.
Even when input is empty, call `receive`. Control actions and received commands can queue several outputs.

```rust
use bytes::Bytes;
use rtmpx::sessions::{ServerSession, ServerSessionConfig, ServerOutput, ServerEvent};

let mut session = ServerSession::new(ServerSessionConfig::default())?;
let mut input = Bytes::new(); // Replace with owned transport input after the handshake.
while let Some(output) = session.receive(&mut input)? {
    match output {
        ServerOutput::Packet(packet) => {
            // Hand the packet to the transport writer.
            # let _ = packet;
        }
        ServerOutput::Event(ServerEvent::ConnectionRequested { request_id, .. }) => {
            session.accept_request(request_id)?;
            // The next receive call returns the queued response.
        }
        ServerOutput::Event(ServerEvent::VideoDataReceived { data, timestamp, .. }) => {
            // Move data into a bounded relay queue; awaiting here is permitted.
            # let _ = (data, timestamp);
        }
        _ => {}
    }
}
# Ok::<(), rtmpx::sessions::ServerSessionError>(())
```

The callback and batch-ingestion APIs are removed. No temporary event vector is required.
Acknowledgements count consumed wire bytes. Draining outputs does not count unread input again.
If input fails, close the transport and discard the session and unsent packets.
Earlier returned outputs remain observable. A failed session rejects later operations.

## Connections and streams

`state()` reports `ConnectionState`. It stays `Connected` while individual streams start, run, or end.
The connection owns negotiation, acknowledgements, ping, and framing. Message stream ID 0 is implicit in those operations.
`StreamId::CONTROL` names that wire channel for low-level code. RTMP chunk streams are a separate framing concept.

`play(key)` and `publish(key, PublishMode)` return `Result<StreamHandle>` immediately and queue `createStream`.
Each operation has an independent lifecycle. Multiple playback and publishing streams can share one session.
Acceptance and rejection events carry the original handle, even when responses arrive out of order.
Media and script-data events also identify their stream.

`StreamHandle` is a 16-byte local value. It exists before the server assigns a wire ID.
Handles are scoped to one session. A handle from another session is rejected.
`stream_id(handle)` returns the optional wire `StreamId`. `stream_state(handle)` reports its current state.
Client states use `ClientStreamState`. Server states use `ServerStreamState`. `streams()` iterates live handles without allocating.
Server request events provide server-local handles. Those handles differ from the handles held by the client.

`delete_stream(handle)` invalidates a client handle immediately. It queues deletion for an established wire stream.
If creation is pending, its eventual response is deleted without starting the cancelled operation.
No deletion acknowledgement is required. A replacement operation can start immediately:

```rust
use rtmpx::sessions::{ClientSession, ClientSessionError, PublishMode, StreamHandle};

fn replace_playback(
    client: &mut ClientSession,
    playback: StreamHandle,
) -> Result<StreamHandle, ClientSessionError> {
    client.delete_stream(playback)?;
    client.publish("replacement", PublishMode::Live)
}
```

Drain the queued packets through `receive`. Wait for `PublishRequestAccepted` before sending media on the returned handle.
Deletion preserves other streams and the connection. Reused storage slots and wire IDs do not revive deleted handles.
A late status for an unknown wire stream produces `StatusReceived { stream: None, .. }`.

The server uses `complete_playback(handle)` to report playback completion. This differs from deleting the client's stream.
RTMPX clients delete the completed stream automatically. The server retains it until deletion arrives from the peer.
Request rejection or playback/publishing completion releases the affected client handle. The corresponding event retains its identity.

Stream storage grows during creation and reuses vacant slots. Wire-ID lookup uses a hash index.
Neither handle validation nor media routing allocates per frame. Connection setup and AMF commands can still allocate.

## Bound session state

Both configurations expose `session_limits: SessionLimits`, independently of `decoder_limits`.
The defaults allow 128 live streams and 128 pending requests per connection.
`max_streams` includes client streams awaiting creation. `max_pending_requests` bounds unanswered client transactions and server requests awaiting an application decision.
Cancelled client creations count until the peer answers. This prevents repeated cancellation from bypassing the pending-request bound.
Deleting streams and resolving requests release their capacity.

Client actions return `ClientSessionError::LimitExceeded` without terminating the session. They can be retried after capacity becomes available.
If inbound requests exceed server limits, `receive` returns `ServerSessionError::LimitExceeded` and terminates the session.
`SessionLimitError` distinguishes stream limits from pending-request limits. Zero disables the corresponding operation.
These limits bound protocol state. The application still bounds transport buffers, outgoing queues, and retained payloads.

## Send and resume packets

Both sessions expose `send_audio`, `send_video`, and `send_data`.
Audio and video accept owned `Bytes`, `Vec<u8>`, or `Payload`, plus a timestamp and `DropPolicy`.
Both client and server sends require a `StreamHandle`.
Clients send on publishing streams. Servers send on playback streams.
`send_data` accepts `DataMessage` and preserves its timestamp, wire type, and encoded body.
`send_metadata` encodes typed metadata. Sending encoded data does not decode it.

All sends and control outputs use `Packet<Payload>`. There is no separate prepared-packet type.
Packet owns both its payload and write progress. Store it directly in a task or queue.

For each transport write:

1. Fill a stack array with `packet.io_slices(&mut slices)`
2. Write those slices
3. Advance the packet by the number of bytes successfully written
4. Retain the packet until `packet.is_complete()` is true

`Packet` also implements `bytes::Buf`. `remaining()` excludes bytes already advanced.
`wire_len()` reports the original total wire length. Headers use inline storage.
`to_vec()` explicitly copies the remaining wire bytes. `copy_to(&mut Vec<u8>)` reuses the destination capacity.
Neither copying helper advances the packet.

Server sends require an accepted connection.
Drain pending session outputs before calling a send method. Otherwise, the method returns `PendingOutput` without encoding a new packet.
Send packets in production order. The caller must preserve that order across its own queues.
`DropPolicy::Never` forbids omission. `DropPolicy::Allowed` permits omission only before transmission starts.
`packet.can_drop()` becomes false after the first advance. A partially transmitted packet must finish.

TCP and TLS writers have separate capabilities and buffering costs.
The examples use asynchronous vectored writes and handle short writes without a borrowed cursor object.

## Validate media and inspect metadata

```rust
use bytes::Bytes;
use rtmpx::{EnhancedValidationMode, Payload, ValidatedMedia};

let payload: Payload = [
    Bytes::from_static(b"\x27\x01"),
    Bytes::from_static(b"\x00\x00\x00sample"),
].into_iter().collect();
let media = ValidatedMedia::parse_video(payload.view(), EnhancedValidationMode::Strict)?;
assert!(media.classification().coded);
# Ok::<(), rtmpx::MediaValidationError>(())
```

`parse_audio` accepts the same storage types. Both parsers also accept contiguous `Bytes`.
Header fields can cross segment boundaries. Parsed media ranges borrow the original payload without copying bytes or descriptors.
Release borrowed interpretations before moving their payloads. Classification is a small owned value.

Single-track validation does not allocate for valid segmented media. Multitrack parsing allocates its typed track vector.
Error and opaque-result strings can allocate. Elementary extraction from contiguous media offers `visit_elementary_units` without a result vector.

Every script-data message produces `StreamDataReceived`, including metadata. There are no separate automatic metadata events.
Call `message.metadata()` for properties, or `ValidatedMetadata::parse(message, mode)` for Enhanced RTMP metadata validation.
Both inspect segmented storage directly. AMF values allocate, but encoded input is not coalesced.

## Recycle descriptors

```rust
use rtmpx::{PayloadPool, PayloadPoolConfig, sessions::{ServerSession, ServerSessionConfig}};

let pool = PayloadPool::new(PayloadPoolConfig {
    max_cached_payloads: 16,
    max_descriptors_per_payload: 4096,
});
let config = ServerSessionConfig { payload_pool: Some(pool), ..Default::default() };
let session = ServerSession::new(config)?;
# let _ = session;
# Ok::<(), rtmpx::sessions::ServerSessionError>(())
```

`PayloadPool::default()` uses those limits. Clones share the cache.
Both session configurations accept `payload_pool`. Sessions and `MessageDecoder` also expose `set_payload_pool` for later changes.

The first segment stays inline. Additional segments use a descriptor vector.
Dropping a pooled payload releases its receive buffers, then returns the empty vector to the pool.
This works across relay tasks and threads. A mutex protects cache checkout and return, not parsing or writing.
The descriptor-capacity limit excludes the inline segment. Oversized vectors and vectors returned to a full cache are freed.

Cache limits do not restrict live messages. Bound relay queues and configure `decoder_limits` separately.
Warmup must cover concurrent messages and their fragment counts. New peaks can require more descriptor storage.
Move payloads through queues. Cloning a fragmented payload can allocate another descriptor vector.

A small segment can retain a large receive allocation. Use bounded source buffers and release consumed messages promptly.
Transport buffer allocation, reuse, and ownership metadata remain the application's responsibility.

## Public names and layers

`Payload` stores message-body segments. `RawMessage` adds the RTMP type, timestamp, and message stream ID.
`RtmpMessage` represents interpreted protocol messages. `RtmpMessage::into_raw_message` encodes their bodies.
`Packet` adds chunk framing and owns outbound write progress.

`PublishMode` is shared by client requests and server events. `DropPolicy` controls omission, without promising transport delivery.
`UnhandledCommand` preserves commands that sessions cannot interpret, including commands received with AMF3 framing.
`HandshakeRole` specifies the local client/server role. `HandshakeProgress` describes the result of processing handshake input.
Socket timeouts belong to the transport adapter. The unused `ServerSessionTimeouts` export is removed.

## Advanced chunk APIs

| Need | API |
|---|---|
| Own protocol-free message assembly | `MessageDecoder::decode(&mut Bytes)` |
| Process borrowed fragments immediately | `ChunkParser::consume(&[u8])` |
| Assemble borrowed input contiguously | `MessageDecoder::decode_slice(&mut &[u8])` |
| Retain unread borrowed input internally | `ContiguousDecoder::get_next_message(&[u8])` |
| Encode owned or borrowed message storage | `ChunkEncoder::encode(message, EncodeOptions)` |
| Change outbound chunk size | `ChunkEncoder::set_chunk_size(size, timestamp)` |

`RawMessage::as_ref()` borrows its body for low-level encoding. Its `map_data` method changes storage while preserving message identity.
`EncodeOptions` names the drop policy and header mode. Sessions manage header compression automatically.
A chunk-size change returns a control packet encoded with the old size. Send it before subsequent packets.

Apply decoded SetChunkSize and Abort messages before decoding the next message. Sessions do this automatically.
`DecoderLimits` bounds message size, chunk size, tracked streams, partial messages, retained bytes, and fragment descriptors.
Borrowed assembly also charges reserved message capacity against the byte limit.
The byte limit does not measure complete backing allocations retained by external `Bytes` owners.

`Payload::into_bytes()` returns existing contiguous storage or explicitly coalesces fragmented storage.
`Payload::reader()` reads AMF across segments. `PayloadView` provides borrowed ranges with constant-time segment access.

## Allocation contract

`tests/allocations.rs` measures server ingestion, segmented validation, client sending, and partial-write traversal.
It also measures client media routing across concurrent playback streams.
After warmup, this path makes zero allocations and reallocations with a descriptor pool and preallocated transport storage.
Coverage includes 256-byte audio, 16 KiB video, and 256 KiB video, with 128-byte and 4 KiB inbound chunks.
The emitted wire is also decoded through a receiving session to validate the relayed bytes.

The explicit contiguous decoder needs one allocation for a 256 KiB message, with no reallocations across 16 KiB reads.
These counts exclude transport buffer creation, connection setup, AMF commands, acknowledgements, and application metrics.

## Use AMF graphs

```rust
use rtmpx::{Amf3Document, amf3::Amf3Value};

let mut document = Amf3Document::new();
let id = document.insert(Amf3Value::dynamic_object(Vec::new()));
*document.get_mut(id).unwrap() = Amf3Value::dynamic_object(vec![
    ("self".into(), Amf3Value::Reference(id)),
]);
document.roots_mut().push(Amf3Value::Reference(id));
let encoded = document.serialize()?;
# Ok::<(), rtmpx::amf3::Amf3SerializationError>(())
```

`Amf0Document` and `Amf3Document` own complex values in an arena.
References contain document-local `ObjectId` values. Moving a document preserves its IDs.
`resolve` borrows an inline value or its referenced object.
Copying an ID into another document does not import its target.

The decoder registers each complex object before its children. This permits shared objects and cycles without reference-counting leaks.
The serializer emits references by identity. Equal but distinct inline objects remain distinct.
AMF3 string and trait references remain supported.

AMF0 references cover objects, typed objects, and arrays. Dates and XML documents do not occupy AMF0 object-reference slots.
AMF3 supports references for all its complex types, including vectors, dictionaries, and registered externalizable classes.

`Amf0Document::embed_amf3` imports a single-root AMF3 document and relocates its IDs.
`get_amf3` resolves embedded IDs. Each encoded AVM+ value starts a fresh AMF3 reference context.

`to_tree(TreeLimits)` explicitly expands shared values. It rejects cycles and validates node, byte, and depth budgets before expansion.
The byte budget estimates owned value storage, not allocator overhead or exact resident memory.
The existing `deserialize` functions use these bounded tree conversions.
An unbound reference cannot be serialized without its document.
Value-level encoding conversions do not import an arena. Convert to a bounded tree before those conversions.

AMF strings and typed values still allocate. This work removes deep copies from object-reference storage, not all AMF allocations.
`serialize_into` reuses output capacity and restores output length on error.
`as_str` and `as_object` borrow values for inspection.
