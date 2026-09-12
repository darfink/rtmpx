<div align="center">

# `rtmpx`

## Sans-I/O RTMP and Enhanced RTMP for Rust

[![GitHub CI Status][github-shield]][github]
[![crates.io version][crate-shield]][crate]
[![Documentation][docs-shield]][docs]
[![License][license-shield]][license]

</div>

RTMPX provides client and server sessions, chunk framing, handshakes, AMF codecs, and typed media inspection.
Applications own sockets, TLS, scheduling, backpressure, and media delivery policy.
The library works with any runtime and can also run entirely in memory.

Version 3 redesigns the public API around explicit payload ownership and independent stream lifecycles.
It replaces the version 2 API without a compatibility layer.
The version is prepared in this repository; its release tag and crates.io publication are pending.
See the [migration notes](CHANGELOG.md) and [API and ownership guide](docs/zero-copy.md).

## What the API provides

- **Pull-based sessions:** `receive(&mut Bytes)` returns one owned packet, event, or unhandled message.
- **Independent streams:** One connection can carry several publishing and playback operations, each identified by a local `StreamHandle`.
- **Segmented input:** Messages retain receive-buffer slices, including messages split across reads or interleaved chunk streams.
- **Resumable output:** `Packet` owns payload storage and write progress, and exposes chunk headers and payload slices for vectored writes.
- **Borrowed inspection:** Media headers can cross segment boundaries without forcing a contiguous message copy.
- **Bounded reuse:** Descriptor pools reduce repeated allocations; decoder and session limits bound protocol state.
- **AMF graphs:** AMF0 and AMF3 document APIs preserve shared objects and cycles through document-local references.
- **Lossless forwarding:** Encoded media and script-data bodies remain authoritative. Inspecting them does not require rewriting them.

## Connections, streams, and ownership

A session represents one side of one RTMP connection after its handshake.
Connection state covers negotiation, acknowledgements, ping, and framing.
Publishing and playback have separate stream state.

Client `play` and `publish` operations return handles before the server assigns wire stream IDs.
Events carry those handles through acceptance, rejection, media delivery, and completion.
Deleting a stream invalidates its handle immediately, including when creation is still pending.
A client can then start another operation without reconnecting.

Server request events provide server-local handles.
Applications accept or reject requests and send media on playback streams.
`complete_playback` reports completion while other streams continue.
Local handles, RTMP message stream IDs, and chunk stream IDs are distinct concepts.

Control operations queue outputs for `receive`. Media sends return packets directly.
Applications drain pending outputs and preserve packet order.
A packet retains progress across short writes and can move into a task or bounded queue.

## Allocation and copy boundaries

The media path can avoid payload copies between owned socket input and outbound packet writes.
The first payload segment is inline; additional segments use a descriptor vector.
A bounded `PayloadPool` recycles that vector after payload release, including across threads.

The allocation contract has explicit boundaries:

- Warmed media paths use pooled descriptors and preallocated transport storage.
- Borrowed single-track validation does not allocate for valid media.
- Multitrack validation, AMF values, control messages, and errors can allocate.
- Socket buffers, TLS, and application queues have their own allocation costs.
- Cloning fragmented payloads can allocate descriptors and retain source buffers.
- Explicit conversion to contiguous bytes copies fragmented payloads.

Routmp exercises segmented forwarding from publisher to upstream.
Rushls exercises the contiguous codec-input boundary needed by its HLS pipeline.
Elementary-unit visitors remove FLV framing without a temporary result vector.
RTMPX does not decode codecs, package HLS, cache playback media, or implement reconnect policy.

See the [allocation contract](docs/zero-copy.md#allocation-contract) for measured paths and exclusions.

## Choose a layer

| Application need | API |
|---|---|
| Handle connections and stream requests | `ClientSession`, `ServerSession` |
| Assemble owned receive buffers | `MessageDecoder` |
| Process borrowed chunk fragments immediately | `ChunkParser` |
| Assemble contiguous messages from borrowed input | `ContiguousDecoder` |
| Frame owned or borrowed messages | `ChunkEncoder`, `Packet` |
| Inspect media without rewriting its body | `ValidatedMedia`, `PayloadView` |
| Inspect encoded script data explicitly | `DataMessage`, `ValidatedMetadata` |
| Extract contiguous codec samples and configuration | `ElementaryUnit`, `visit_elementary_units` |
| Preserve AMF object identity and cycles | `Amf0Document`, `Amf3Document` |

Sessions apply chunk-size changes, aborts, and flow-control messages automatically.
Low-level decoder users apply those protocol messages before decoding subsequent input.
Transport deadlines and bounds on queued or retained application data remain caller responsibilities.

## Changes from RML

RTMPX derives from `rml_rtmp` 0.8.0, upstream commit `953fc41d` dated 2023-05-31.
Its AMF0 codec derives from `rml_amf0` 0.3.0.
The MIT license retains that provenance.

The changes extend beyond protocol fixes and Enhanced RTMP support:

| Area | RTMPX changes |
|---|---|
| Public API | Owned pull outputs replace input-result batches; storage and write progress are explicit |
| Stream lifecycle | Concurrent client operations, symmetric server handles, cancellation, deletion, completion, and stale-handle rejection |
| Chunk input | Streaming parsing, segmented assembly, per-chunk-stream partial state, and explicit contiguous adapters |
| Chunk output | Inline headers, payload-preserving packet cursors, short-write progress, and explicit drop policy |
| Resource control | Limits for decoding, fragments, streams, and pending requests; bounded descriptor caches |
| AMF | AMF3 support, graph documents, identity-based references, cycles, and bounded graph-to-tree expansion |
| Media inspection | In-house legacy and Enhanced FLV parsing, borrowed segmented views, and elementary-unit visitors |
| Script data | Encoded bodies retained; metadata inspection is explicit rather than an automatic event conversion |
| Interoperability | Configurable connect properties, capability forwarding, complete status properties, and independent-peer tests |

Protocol fixes include cumulative acknowledgements, acknowledgement-window fallback, timestamp rollover, and safe interleaved message assembly.
Abort messages release partial state, and malformed lengths return errors.
Request rejection uses command-specific response shapes.
Interop handling covers explicit playback start values, decimal-string deletion IDs, and case-insensitive publish modes.

AMF tree codecs use depth and collection limits, exact reads, and bounded reference expansion.
Graph codecs preserve identity without deep-copying referenced objects.
Value-level AMF0/AMF3 conversions do not import graph arenas or guarantee identical wire representations.

## Enhanced RTMP

The [compliance matrix](docs/enhanced-rtmp.md) documents support against VSO Enhanced RTMP v2 r2, revision `v2-2026-01-31-r2`.
The parser covers legacy media, Enhanced packet families, FourCCs, multitrack, multichannel audio, and ModEx.
Strict validation rejects unsupported or malformed structures; passthrough retains opaque payloads for forwarding.
Codec admission and decoding remain application concerns.

## Examples

- [Publisher](examples/publish.rs): Connect, publish, and send metadata and placeholder media.
- [Listener](examples/serve.rs): Handshake, accept requests, and inspect media through borrowed views.
- [Ownership example](examples/zero_copy.rs): Segmented parsing, packet traversal, descriptor pooling, and AMF graphs.
- [Transport helpers](examples/support/io.rs): Asynchronous vectored writes with partial-write handling.
- [OBS probe](examples/obs_ingest_probe.rs): Inspect a real OBS connection and its messages.

Run the listener, then the publisher:

```sh
cargo run --example serve
cargo run --example publish -- 127.0.0.1:1935 live demo
```

## Validation

The default tests cover framing, handshakes, AMF trees and graphs, session lifecycles, resource limits, and ownership behavior.
Allocation tests measure warmed media paths separately from transport and control-plane work.

```sh
cargo test
cargo test --release --test allocations
cargo clippy --all-targets --all-features -- -D warnings
```

Optional interoperability suites use independent implementations:

- [Red5](tests/red5/README.md): AMF0/AMF3 publish and playback, including publish/delete/play/delete/publish on one connection.
- FFmpeg: Legacy AVC/AAC and Enhanced HEVC ingest, plus relay playback verified with ffprobe.
- GStreamer: Connect properties, chunking, repeated metadata, and publication teardown.
- OBS: Automated connect-sequence coverage and a manual live probe.

The consumer migrations also exercise TCP backpressure, TLS, byte-exact forwarding, codec-sample preservation, and HLS output.
These checks validate specific paths; they do not establish that all application workloads allocate nothing.

## License

MIT. See [LICENSE](LICENSE), including the upstream RML copyright.

<!-- Links -->
[github-shield]: https://img.shields.io/github/actions/workflow/status/darfink/rtmpx/ci.yml?branch=main&label=actions&logo=github&style=for-the-badge
[github]: https://github.com/darfink/rtmpx/actions/workflows/ci.yml?query=branch%3Amain
[crate-shield]: https://img.shields.io/crates/v/rtmpx.svg?style=for-the-badge
[crate]: https://crates.io/crates/rtmpx
[docs-shield]: https://img.shields.io/badge/docs-crates-green.svg?style=for-the-badge
[docs]: https://docs.rs/rtmpx/
[license-shield]: https://img.shields.io/crates/l/rtmpx.svg?style=for-the-badge
[license]: https://github.com/darfink/rtmpx

