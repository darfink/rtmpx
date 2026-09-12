<div align="center">

# `rtmpx`

## Sans-I/O (Enhanced) RTMP protocol API

[![GitHub CI Status][github-shield]][github]
[![crates.io version][crate-shield]][crate]
[![Documentation][docs-shield]][docs]
[![License][license-shield]][license]

</div>

Several RTMP crates exist for Rust. `rtmpx` is a fork and extension of the RML
RTMP crates (from
[KallDrexx/rust-media-libs](https://github.com/KallDrexx/rust-media-libs)),
built to add Enhanced RTMP support (HEVC/AV1, multitrack, FourCC signalling)
on a reliable core. It is used in production.

Sans-I/O protocol API: chunk framing, handshake, command and data messages,
plus client/server sessions and typed inspection of Enhanced RTMP media.
Callers move bytes in and out while the crate drives the state machine, so it
embeds in any async runtime or proxy without mandating one.
The AMF0/AMF3 codecs are intentionally inlined into this crate: they exist to
serve its RTMP framing, not as general-purpose libraries.

Client and server sessions expose one `receive(&mut Bytes)` loop. Each media operation has a local `StreamHandle`.
Clients can play and publish concurrently, or delete a stream and immediately start another operation.
Media sends return an owned `Packet` with resumable vectored writes. See the [API and ownership guide](docs/zero-copy.md).

Version 3 replaces the version 2 public API. See the [release notes](CHANGELOG.md) for migration details.

## Testing

Comprehensively tested. Around 300 tests in the default suite (`cargo test`, no network,
no external binaries) cover chunking, handshake, AMF0/AMF3, sessions,
interleaved streams, resource limits, and encoder quirks -- plus live legs:

- Interop against Red5, one of the few full-fledged independent RTMP servers, across publish/play x AMF0/AMF3 (`cargo test --test red5 --features red5-live`; see `tests/red5/README.md`),
- Real ffmpeg ingest (legacy AVC/AAC and Enhanced HEVC) into the server, and ffmpeg playback of the relay, verified with ffprobe,
- Real GStreamer ingest (flvmux quirks: repeated onMetaData, own connect/chunking, FCUnpublish teardown) into the server,
- OBS connect-sequence coverage in the default suite plus a manual OBS probe (`cargo run --example obs_ingest_probe`).

Beyond the suite above, it ingests production traffic from a wide array of
publishers, encoders, and RTMP clients.

## Changes from RML

Based on `rml_rtmp` 0.8.0 (upstream master `953fc41d`, 2023-05-31), with the
AMF0 codec derived from `rml_amf0` 0.3.0.

- `ClientSession::connect_with_properties` merges extra AMF0 properties into the `connect` command object. Proxies can forward E-RTMP capabilities (`fourCcList`, `capsEx`, FourCC info maps). `connect` uses the default properties.
- `ServerSession` keeps the original metadata message (`DataMessage`) and exposes unconsumed connect fields (`additional_properties`), so proxies relay metadata and connect properties verbatim instead of dropping them.
- Acknowledgements are cumulative (total bytes received, per spec) and fall back to the advertised window when the peer never sends one (ffmpeg never does); overshoot is kept modulo the window, zero windows are rejected.
- Timestamp deltas across the u32 rollover (~49 days of continuous streaming) are computed with wrapping arithmetic, so long-lived publishes keep correct deltas past the boundary; pinned by round-trip regression tests.
- The deserializer keeps partial payloads per chunk stream (fixing corruption when messages interleave), turns a shrinking message length into `MessageLengthSmallerThanBufferedPayload` instead of panicking, and bounds chunk size, message size, tracked streams, concurrent partials, and buffered bytes; `Abort` releases the target partial.
- `reject_request` answers `connect` with `_error` but `publish`/`play` with `onStatus` errors -- the shape encoders actually watch for.
- The AMF0 codec is hardened (depth/collection caps, exact reads, strict arrays, long-string/date/xml/typed-object/avmplus markers, no `drain(..3)` panic); a new AMF3 codec converts losslessly both ways; Enhanced RTMP is validated with an in-house FLV parser with original bytes authoritative for relay.
- Also: `deleteStream` accepts GStreamer decimal-string stream ids, clients see full result property maps, publish modes match case-insensitively.
- Playback interop: `play` always sends `start` (`-2.0`, live-first default) - strict servers ignore single-argument `play` -- and script data still arrives when mistyped (explicit metadata inspection handles `@setDataFrame` and falls back to bare AMF3 for type 18).

## Examples

Minimal client and server to copy the sans-I/O glue from:

- Barebone publisher (`examples/publish.rs`): connect, publish, metadata, audio/video. Media bytes are placeholders; swap in real frames.
- Barebone listener (`examples/serve.rs`): handshake, accept, borrowed media inspection. Accepts ffmpeg, OBS, or the publisher above.
- Manual OBS probe (`examples/obs_ingest_probe.rs`): logs exactly what a real OBS build sends, for eyeballing new OBS versions.

Run the listener, then publish into it:

    cargo run --example serve
    cargo run --example publish -- 127.0.0.1:1935 live demo

## Enhanced RTMP

Validated against Enhanced RTMP v2 r2 (VSO, 2026-01-31); see
`docs/enhanced-rtmp.md` for the compliance matrix.

## License

MIT (see `LICENSE`), covering both this fork and the upstream RML code it
derives from (Copyright 2017 Matthew Shapiro).

<!-- Links -->
[github-shield]: https://img.shields.io/github/actions/workflow/status/darfink/rtmpx/ci.yml?branch=main&label=actions&logo=github&style=for-the-badge
[github]: https://github.com/darfink/rtmpx/actions/workflows/ci.yml?query=branch%3Amain
[crate-shield]: https://img.shields.io/crates/v/rtmpx.svg?style=for-the-badge
[crate]: https://crates.io/crates/rtmpx
[docs-shield]: https://img.shields.io/badge/docs-crates-green.svg?style=for-the-badge
[docs]: https://docs.rs/rtmpx/
[license-shield]: https://img.shields.io/crates/l/rtmpx.svg?style=for-the-badge
[license]: https://github.com/darfink/rtmpx

## Zero-copy APIs

The [allocation and ownership guide](docs/zero-copy.md) covers packet cursors, borrowed input, segmented session events, and AMF object graphs.
Run `cargo run --example zero_copy` for a complete example.
