# RTMPX API and ownership guide

RTMPX performs no network I/O. Sessions manage protocol state, while your application owns transports, TLS, timeouts, and scheduling.
The default session payload type is `Payload`, which keeps slices of your receive buffers without copying their bytes.

## Starting with sessions

Create `ClientSession::new(config)` or `ServerSession::new(config)` once the RTMP handshake completes.
Control and request-decision methods return `Result<()>` and queue their outbound packets inside the session:

- **Client:** `connect`, `connect_with_properties`, `delete_stream`
- **Server:** `accept_request`, `accept_request_with_properties`, `reject_request`, `accept_command`, `call`, `notify`

Both sessions drive progress through a single pull method, `receive(&mut Bytes) -> Result<Option<Output>, Error>`, yielding `ClientOutput` or `ServerOutput`:

- `Packet(packet)`: outbound framed wire data ready for your transport writer
- `Event(event)`: a `ClientEvent` or `ServerEvent`
- `UnhandledMessage(message)`: an unhandled RTMP message with its segmented body intact

Each `receive` call returns at most one item and leaves any unread input in your buffer. Because the returned value does not borrow the session or input buffer, you can move it straight into an async channel or await backpressure before calling `receive` again.
Keep calling `receive` until it returns `Ok(None)`—even when your input buffer is empty—because a single control action or incoming command can queue several outputs.

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

Acknowledgements count only consumed wire bytes, so draining outputs never double-counts unread input.
If `receive` returns an error, close the transport and discard the session and any unsent packets; a failed session rejects further calls (earlier returned outputs stay valid).

## Connections and streams

`state()` returns `ConnectionState`, which stays `Connected` as individual streams start, run, and end.
Connection-level messages (negotiation, acknowledgements, ping, chunk framing) implicitly use message stream ID `0` (`StreamId::CONTROL` in low-level APIs), which is separate from RTMP chunk stream IDs.

On the client, `play(key)` and `publish(key, PublishMode)` return `Result<StreamHandle>` immediately and queue `createStream`.
Multiple playback and publishing streams can run independently over one connection: acceptance, rejection, media, and script-data events all carry the original `StreamHandle`, even if server responses arrive out of order.

`StreamHandle` is a 16-byte session-local value assigned before the server picks a wire ID (handles from another session are rejected).
Use `stream_id(handle)` for the optional wire `StreamId`, `stream_state(handle)` for `ClientStreamState` / `ServerStreamState`, and `streams()` to iterate live handles without allocating. Server request events use server-local handles independent of the client's handles.

Calling `delete_stream(handle)` on a client invalidates the handle immediately and queues deletion for an established stream (if `createStream` is still pending, its eventual response is deleted automatically without starting the cancelled operation). You do not need to wait for a deletion acknowledgement before starting a replacement stream:

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

Drain the queued packets through `receive()`, and wait for `PublishRequestAccepted` before sending media on the new handle.
Deleting a stream leaves other streams and the connection untouched; reused slots and wire IDs never revive deleted handles, and a late status for an unknown wire stream surfaces as `StatusReceived { stream: None, .. }`.

On the server, call `complete_playback(handle)` to signal playback completion. RTMPX clients automatically delete a completed stream, while the server keeps it until the peer sends `deleteStream`. Request rejection or stream completion releases the affected client handle while preserving its identity on the final event.

Stream storage reuses vacant slots and indexes wire IDs in a hash table, so neither handle validation nor media routing allocates per frame.

## Bounding session state

Both session configs expose `session_limits: SessionLimits` alongside `decoder_limits`.
By default, each connection allows up to 128 live streams (`max_streams`, including pending client creations) and 128 pending requests (`max_pending_requests`, covering unanswered client transactions and server requests awaiting a decision).
Cancelled client creations remain counted until the peer responds, preventing repeated cancellation from bypassing the cap; deleting streams and resolving requests free capacity.

- On the client, exceeding a limit returns `ClientSessionError::LimitExceeded` without killing the session, so you can retry once capacity frees up.
- On the server, exceeding a limit on inbound requests returns `ServerSessionError::LimitExceeded` and terminates the session.
- `SessionLimitError` distinguishes stream limits from pending-request limits; setting a limit to `0` disables that operation.

These limits cap protocol state only—your application still owns bounds on socket buffers, outbound queues, and retained payloads.

## Sending and resuming packets

Both sessions provide `send_audio`, `send_video`, and `send_data`, each taking a `StreamHandle` (clients send on publish streams; servers send on playback streams).
Audio and video sends accept owned `Bytes`, `Vec<u8>`, or `Payload`, along with a timestamp and `DropPolicy`.
`send_data` sends a `DataMessage` preserving its timestamp, wire type, and encoded body without decoding it; `send_metadata` encodes typed metadata.

Both media sends and control outputs return `Packet<Payload>`, which owns its payload, inline chunk headers, and partial-write cursor so it can live directly in a task or queue. To write a packet to a transport:

1. Fill a stack slice array with `packet.io_slices(&mut slices)`.
2. Perform a vectored write.
3. Advance the packet by the bytes actually written.
4. Keep writing until `packet.is_complete()` is true.

`Packet` also implements `bytes::Buf`: `remaining()` tracks unwritten bytes, while `wire_len()` reports the original total length. `to_vec()` and `copy_to(&mut Vec<u8>)` copy remaining wire bytes without advancing the cursor.

Drain all pending session outputs via `receive()` before calling a send method, or the send returns `PendingOutput` without encoding a packet. Always write packets in the order produced. `DropPolicy::Never` forbids dropping a packet; `DropPolicy::Allowed` allows dropping only before any byte is written (`packet.can_drop()` becomes `false` after the first advance, so partially written packets must finish).

## Validating media and inspecting metadata

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

`ValidatedMedia::parse_video` and `parse_audio` accept both segmented `PayloadView` and contiguous `Bytes`.
Headers can span segment boundaries, and parsed views borrow directly from the original payload without copying bytes or descriptors—just drop borrowed views before moving the payload. `classification()` returns a small owned value.

Single-track validation does not allocate for valid segmented media; multitrack parsing allocates its track vector, and error/opaque strings can allocate. When extracting elementary units from contiguous bytes, `visit_elementary_units` avoids allocating a temporary result vector.

Every script-data message arrives as `StreamDataReceived` (including `onMetaData`). Call `message.metadata()` to read properties, or `ValidatedMetadata::parse(message, mode)` for Enhanced RTMP metadata validation. Both read segmented storage directly without coalescing the payload (though decoded AMF values allocate). Check `metadata()` first before calling the strict metadata validator, which rejects non-metadata script messages.

## Recycling descriptors

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

`PayloadPool::default()` uses these same limits, and cloning a pool shares the underlying cache. Pass it in `ClientSessionConfig` / `ServerSessionConfig`, or attach it later with `set_payload_pool` on sessions and `MessageDecoder`.

Because a `Payload` stores its first segment inline, only multi-segment payloads need a descriptor vector. When a pooled payload drops, it releases its receive buffers and returns the cleared vector to the pool—even from another thread or relay task (a mutex guards checkout/return only, never parsing or writing). Vectors larger than `max_descriptors_per_payload` (excluding the inline segment) or returned to a full cache are freed normally.

Pool limits control cache size, not live message count—bound relay queues and `decoder_limits` separately. Move payloads rather than cloning fragmented ones (cloning can allocate a fresh descriptor vector), and keep source receive buffers reasonably sized so a small slice does not pin a large allocation.

## Core types

- `Payload`: segmented message body retaining slices of receive buffers.
- `RawMessage`: adds RTMP message type, timestamp, and message stream ID.
- `RtmpMessage`: decoded protocol message (`into_raw_message` encodes it back).
- `Packet`: adds chunk framing and tracks outbound write progress.
- `PublishMode`: shared across client publish requests and server events.
- `DropPolicy`: marks whether a packet may be skipped before transmission starts.
- `UnhandledCommand`: preserves commands sessions do not interpret, including AMF3-framed commands.
- `HandshakeRole` / `HandshakeProgress`: drive the RTMP handshake independently of session state.

## Low-level chunk APIs

| Need | API |
| --- | --- |
| Protocol-free segmented message assembly | `MessageDecoder::decode(&mut Bytes)` |
| Immediate borrowed chunk fragments | `ChunkParser::consume(&[u8])` |
| Contiguous assembly from a borrowed slice | `MessageDecoder::decode_slice(&mut &[u8])` |
| Contiguous assembly retaining unread bytes internally | `ContiguousDecoder::get_next_message(&[u8])` |
| Encoding owned or borrowed messages | `ChunkEncoder::encode(message, EncodeOptions)` |
| Changing outbound chunk size | `ChunkEncoder::set_chunk_size(size, timestamp)` |

`RawMessage::as_ref()` borrows its body for encoding, and `map_data` transforms payload storage while keeping header metadata. `EncodeOptions` sets the drop policy and header compression mode (sessions manage compression automatically). Changing outbound chunk size returns a control packet framed with the old chunk size—send it before any subsequent packets.

When using `MessageDecoder`, `ChunkParser`, or `ContiguousDecoder` directly, apply decoded `SetChunkSize` and `Abort` messages before feeding more input. `DecoderLimits` caps message size, chunk size, tracked streams, partial messages, retained bytes, and fragment descriptors (borrowed assembly also counts reserved capacity against the byte limit, though it cannot see the full backing allocation of external `Bytes` slices).

Use `Payload::into_bytes()` to return an already-contiguous buffer or coalesce fragments once, `Payload::reader()` to read AMF across segments, and `PayloadView` for O(1) borrowed segment access.

## Allocation contract

`tests/allocations.rs` measures server ingestion, segmented validation, client sending, partial-write traversal, and concurrent client playback routing across 256-byte audio, 16 KiB video, and 256 KiB video (with 128-byte and 4 KiB inbound chunks), decoding the relayed wire bytes to confirm correctness.

After warmup with a `PayloadPool` and preallocated transport buffers, that media path performs **zero allocations and zero reallocations**.
The explicit `ContiguousDecoder` performs one allocation for a 256 KiB message with zero reallocations across 16 KiB reads.
These counts exclude transport buffer creation, connection setup, AMF commands, acknowledgements, and application metrics.

## Application boundaries

- **Forwarding relays** can validate payloads via borrowed views, drop the view, move the `Payload` through a bounded queue, and send it—retaining the original receive buffers all the way until socket write completes.
- **Ingest pipelines** (such as HLS packagers) that need contiguous codec input can call `Payload::into_bytes()` once at their boundary (a no-op when already contiguous, a single copy when fragmented) and extract samples via `visit_elementary_units` without allocating a temporary vector.

## AMF graphs

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

`Amf0Document` and `Amf3Document` store complex values in an arena referenced by document-local `ObjectId`s (IDs stay valid when a document moves, but copying an ID into another document does not import its target). Call `resolve` to borrow an inline value or its referenced object.

- Decoding registers each complex object before its children, handling shared objects and cycles without `Rc`/`Arc` leaks.
- Serialization emits references by object identity, so equal but distinct inline objects stay distinct. AMF3 string and trait references are preserved.
- AMF0 references cover objects, typed objects, and arrays (dates and XML documents do not take reference slots). AMF3 references cover all complex types, including vectors, dictionaries, and registered externalizable classes.
- `Amf0Document::embed_amf3` imports a single-root AMF3 document and relocates its IDs; `get_amf3` resolves them (each encoded AVM+ value starts a fresh AMF3 reference context).
- `to_tree(TreeLimits)` expands shared values into a tree, rejecting cycles and checking node, byte, and depth budgets first (note that the byte budget estimates owned value storage, not allocator overhead). The standard `deserialize` helpers use these bounded tree conversions.
- Unbound references cannot be serialized outside their owning document; convert to a bounded tree before value-level AMF0/AMF3 conversions.

Graph documents eliminate deep copies from shared object references, though AMF strings and typed values still allocate. Use `serialize_into` to reuse an output buffer (restoring its length on error), and `as_str` / `as_object` for borrowed inspection.
