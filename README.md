<div align="center">

# `rtmpx`

## Sans-I/O RTMP and Enhanced RTMP for Rust

[![GitHub CI Status][github-shield]][github]
[![crates.io version][crate-shield]][crate]
[![Documentation][docs-shield]][docs]

</div>

RTMPX is a sans-I/O Rust library for RTMP and Enhanced RTMP. It provides client and server sessions, chunk framing, handshakes, AMF0/AMF3 codecs, and typed media inspection—while leaving sockets, TLS, scheduling, backpressure, and media delivery policy to your application. It works with any async or sync runtime, or entirely in memory.

RTMPX is used in production for both publisher ingest and player delivery. It handles AMF0 and AMF3 script data, Enhanced RTMP multitrack and multichannel audio, and segmented payloads that relay media without copying or re-encoding bytes. Once descriptor pools and transport buffers are warmed up, hot media paths can run with zero allocations.

## What it provides

- **Pull-based sessions:** `receive(&mut Bytes)` yields one owned packet, event, or unhandled message at a time, so you can apply backpressure or move outputs across async tasks freely.
- **Independent streams:** One RTMP connection can run multiple publish and play operations concurrently, each tracked by a local `StreamHandle`.
- **Segmented input:** Received messages keep slices of your receive buffers—even across short reads or interleaved chunk streams—without forcing a copy.
- **Resumable output:** `Packet` owns its payload and write cursor, exposing chunk headers and payload slices for vectored (`write_vectored`) I/O across partial writes.
- **Borrowed media inspection:** Legacy and Enhanced FLV headers can be parsed directly across segment boundaries without coalescing the message.
- **Bounded reuse and limits:** `PayloadPool` recycles fragment descriptor vectors across threads, while `DecoderLimits` and `SessionLimits` cap protocol state.
- **AMF0 and AMF3 graphs:** Document APIs preserve shared object identity and cycles through document-local references without reference-counting leaks.
- **Lossless forwarding:** Raw media and script-data payloads stay authoritative—inspecting them never requires re-encoding them.

## Connections, streams, and ownership

A session manages one side of an RTMP connection after the handshake: negotiation, acknowledgements, ping, and chunk framing live on the connection, while each publishing or playback stream has its own independent lifecycle.

On the client side, `play` and `publish` return a `StreamHandle` immediately—before the server assigns a wire stream ID—and carry that handle through acceptance, rejection, media, and completion. Calling `delete_stream` invalidates the handle at once (even while creation is still in flight), letting you start a new stream on the same connection without reconnecting.

On the server side, incoming requests carry server-local handles. Your application accepts or rejects requests, sends media on playback streams, and calls `complete_playback` to finish one stream while others keep running. Local handles, RTMP message stream IDs, and chunk stream IDs remain distinct concepts.

Control actions queue outbound packets for `receive()`, while media sends return a `Packet` directly. Drain pending outputs before sending media and preserve packet order across your own queues; each `Packet` tracks partial-write progress and can move safely into a task or bounded queue.

## Allocation and copy boundaries

The media path avoids copying payload bytes between owned socket input and outbound packet writes. A `Payload` stores its first segment inline and keeps any additional segments in a descriptor vector that a bounded `PayloadPool` recycles when the payload drops, including across threads.

What allocates and what does not:

- **Zero-allocation after warmup:** Warmed media relay and playback paths with pooled descriptors and preallocated transport buffers, plus borrowed single-track media validation for valid input.
- **Can allocate:** Multitrack validation, AMF values, control commands, errors, cloning fragmented payloads, and explicitly coalescing a fragmented payload into contiguous bytes (`Payload::into_bytes()`).
- **Application-owned:** Socket buffers, TLS framing, and your own queues.

A forwarding relay can pass segmented payloads straight from publisher to upstream. An ingest pipeline (such as an HLS packager) can coalesce payloads once at its codec boundary and extract samples with `visit_elementary_units` without allocating a temporary vector. RTMPX does not decode codecs, package HLS, cache playback sequences, or manage reconnect policy.

See the [API and ownership guide](docs/zero-copy.md#allocation-contract) for the exact measured paths.

## Choosing a layer

| Need | API |
| --- | --- |
| Full connections and stream lifecycles | `ClientSession`, `ServerSession` |
| Owned segmented message assembly | `MessageDecoder` |
| Immediate borrowed chunk fragments | `ChunkParser` |
| Contiguous message assembly from slices | `ContiguousDecoder` |
| Framing owned or borrowed messages | `ChunkEncoder`, `Packet` |
| Inspecting media without rewriting bytes | `ValidatedMedia`, `PayloadView` |
| Inspecting script data and metadata | `DataMessage`, `ValidatedMetadata` |
| Extracting codec samples and configuration | `ElementaryUnit`, `visit_elementary_units` (AVC, HEVC, AV1, VP8, VP9, VVC, AAC, Opus, FLAC, AC-3, E-AC-3, MP3) |
| Observing sequence ends and skipped messages | `ElementaryEvent`, `visit_elementary_events` |
| Preserving AMF object identity and cycles | `Amf0Document`, `Amf3Document` |

Sessions handle chunk-size updates, aborts, and flow control automatically. If you use the low-level decoders directly, apply decoded `SetChunkSize` and `Abort` messages before feeding subsequent bytes. Transport timeouts and queue bounds always stay under your application's control.

## Enhanced RTMP and elementary media

The [Enhanced RTMP compliance matrix](docs/enhanced-rtmp.md) tracks coverage against VSO Enhanced RTMP v2 r2 (`v2-2026-01-31-r2`).
The parser covers legacy FLV, Enhanced packet families, FourCCs, multitrack, multichannel audio, and ModEx. Strict mode rejects malformed or unknown structures; passthrough mode preserves opaque payloads for lossless forwarding. Codec admission and decoding remain up to your application.

For ingest pipelines extracting codec bitstreams:

- `elementary_units` and `visit_elementary_units` emit codec configurations and coded samples.
- `elementary_events` and `visit_elementary_events` also emit sequence ends and skipped messages in wire track order (distinguishing unsupported codecs, unknown packet types, metadata, video commands, channel configs, and MPEG-TS configs). Malformed or opaque input still returns an error.
- Extracted payloads borrow slices of the input bytes. RTMPX validates container framing, not codec decodability: a payload can contain multiple codec frames, and video keyframe flags reflect publisher signaling rather than a proven decoder restart point.
- VVC, AVC, and HEVC preserve signed composition offsets; other supported Enhanced video codecs use zero offsets. Legacy MP3 handles both standard MP3 and the 8 kHz format ID.

## Changes from RML

RTMPX is derived from `rml_rtmp` 0.8.0 (commit `953fc41d`, 2023-05-31) and `rml_amf0` 0.3.0 under the MIT license. Beyond protocol fixes and Enhanced RTMP support, it redesigns the core around explicit ownership and bounded state:

| Area | What changed |
| --- | --- |
| Public API | Pull-based owned outputs replace input-result batches; payload storage and write progress are explicit |
| Stream lifecycle | Concurrent client operations, symmetric server handles, cancellation, deletion, completion, and stale-handle rejection |
| Chunk input | Streaming parsing, segmented assembly, per-chunk-stream partial state, and explicit contiguous adapters |
| Chunk output | Inline headers, payload-preserving packet cursors, partial-write progress, and explicit drop policy |
| Resource limits | Limits for decoding, fragments, streams, and pending requests, plus bounded descriptor pools |
| AMF | AMF3 support, graph documents, identity-based references, cycles, and bounded graph-to-tree expansion |
| Media inspection | In-house legacy and Enhanced FLV parser, borrowed segmented views, and elementary-unit visitors |
| Script data | Raw encoded bodies stay authoritative; metadata inspection is explicit instead of automatic event conversion |
| Interoperability | Configurable connect properties, capability forwarding, complete status maps, and live third-party interop suites |

Protocol hardening includes cumulative acknowledgements, acknowledgement-window fallback, timestamp rollover handling, abort cleanup of partial messages, strict length checks, command-specific rejection shapes, explicit playback start values, decimal-string deletion IDs, and case-insensitive publish modes.

## Examples

- [`examples/publish.rs`](examples/publish.rs): Connect, publish, and send metadata and placeholder media.
- [`examples/serve.rs`](examples/serve.rs): Handshake, accept requests, and inspect media through borrowed views.
- [`examples/zero_copy.rs`](examples/zero_copy.rs): Segmented parsing, packet traversal, descriptor pooling, and AMF graphs.
- [`examples/support/io.rs`](examples/support/io.rs): Async vectored writes with partial-write handling.
- [`examples/obs_ingest_probe.rs`](examples/obs_ingest_probe.rs): Inspect a live OBS Studio connection and its messages.

Run the listener in one terminal and the publisher in another:

```sh
cargo run --example serve
cargo run --example publish -- 127.0.0.1:1935 live demo
```

## Testing

The default test suite runs with no network or external binaries and covers framing, handshakes, AMF trees and graphs, session lifecycles, limits, and allocation bounds:

```sh
cargo test
cargo test --release --test allocations
cargo clippy --all-targets --all-features -- -D warnings
```

Opt-in interoperability suites test against independent implementations:

- **[Red5](tests/red5/README.md)** (`--test red5 --features red5-live`): AMF0/AMF3 publish and playback, including publish → delete → play → delete → publish on a single connection.
- **FFmpeg** (`--test ffmpeg --features ffmpeg-live`): Legacy AVC/AAC and Enhanced HEVC ingest plus relay playback verified with `ffprobe`.
- **GStreamer** (`--test gstreamer --features gstreamer-live`): Connect properties, chunking, repeated metadata, and teardown, through both `rtmpsink` (librtmp) and `rtmp2sink` (GStreamer's own client).
- **OBS** (`--test obs --features obs-live`, Linux under `xvfb-run`): Real OBS Studio streaming to the server from a throwaway profile, plus a manual live probe (`examples/obs_ingest_probe.rs`).
- Every encoder leg fails if the server refuses any command the encoder sends.

## License

MIT. See [LICENSE](LICENSE), which retains the upstream RML copyright.

<!-- Links -->
[github-shield]: https://img.shields.io/github/actions/workflow/status/darfink/rtmpx/ci.yml?branch=main&label=actions&logo=github&style=for-the-badge
[github]: https://github.com/darfink/rtmpx/actions/workflows/ci.yml?query=branch%3Amain
[crate-shield]: https://img.shields.io/crates/v/rtmpx.svg?style=for-the-badge
[crate]: https://crates.io/crates/rtmpx
[docs-shield]: https://img.shields.io/badge/docs-crates-green.svg?style=for-the-badge
[docs]: https://docs.rs/rtmpx/
