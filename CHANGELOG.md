# Changelog

## 3.0.0 — unreleased

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
