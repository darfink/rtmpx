# Red5 live interoperability suite

An opt-in end-to-end test suite exercising `rtmpx`'s `ClientSession` against a real Red5 server.
For the reverse direction (`ServerSession` against real encoders and players), see [`tests/ffmpeg/main.rs`](../ffmpeg/main.rs) and [`tests/gstreamer/main.rs`](../gstreamer/main.rs).

## Why test against Red5

Unit and loopback tests prove self-consistency, but cannot catch a framing bug that both our client and server share. Red5 is an independent Java RTMP server supporting AMF0 and AMF3 over plaintext RTMP whose `live` app relays publisher bytes opaquely—making it a clean independent peer for client framing, `objectEncoding` negotiation, and multi-stream lifecycle behavior.

## Opt-in gating

The `red5` target is gated behind `required-features = ["red5-live"]` in `Cargo.toml`, so plain `cargo test` neither compiles it nor touches the network:

- `cargo test` runs only the offline default suite.
- `cargo test --test red5 --features red5-live` compiles and runs the live Red5 tests against `RED5_HOST:RED5_PORT`.

## Layout

```text
rtmpx/
  tests/
    amf3_loopback.rs          # default: AMF3 ClientSession -> ServerSession
    enhanced_loopback.rs      # default: Enhanced hvc1/av01 byte-exact relay
    obs_ingest.rs             # default: OBS connect/releaseStream/FCPublish sequence
    session_quirks.rs         # default: timestamp rollover, string deleteStream, error shapes
    common/                   # shared async TCP + handshake + ClientSession driver
      mod.rs
      driver.rs
    ffmpeg/                   # opt-in FFmpeg/ffprobe server interop (`--features ffmpeg-live`)
    gstreamer/                # opt-in GStreamer server interop (`--features gstreamer-live`)
    red5/
      README.md               # this file
      red5.dockerfile         # pinned Red5 2.0.40 image (built from release tarball)
      docker-compose.yml      # local and CI entrypoint
      main.rs                 # live test matrix
      lifecycle.rs            # publish -> delete -> play -> delete -> publish on one connection
      harness.rs              # Red5 endpoint config (RED5_*) and objectEncoding checks
      fixtures.rs             # legacy AVC/AAC, Enhanced hvc1/av01, AMF3 probe payloads
  examples/
    obs_ingest_probe.rs       # manual loopback OBS logger (not run in CI)
  .github/workflows/ci.yml    # CI: default, red5, ffmpeg, and gstreamer jobs
```

## Test matrix

`P` = publish leg (client publishes, Red5 accepts). `R` = round-trip leg (a second client subscribes first, then the first publishes, asserting byte-exact relay—Red5's `live` app only relays media sent after the player subscribes).

| Row | Direction | AMF | Media | Test | What it checks |
| --- | --- | --- | --- | --- | --- |
| 1 | P | AMF0 | Legacy AVC/AAC | `publish_amf0_legacy` | AMF0 negotiation (`objectEncoding 0`), publish accepted, metadata + sequence headers + 3 A/V frames sent |
| 2 | R | AMF0 | Legacy AVC/AAC | `play_amf0_legacy_roundtrip` | 4 video + 4 audio frames relayed byte-exact, metadata received |
| 3 | P | AMF3 | Legacy AVC/AAC | `publish_amf3_legacy` | AMF3 negotiation (`objectEncoding 3`), publish accepted |
| 4 | R | AMF3 | Legacy AVC/AAC | `play_amf3_legacy_roundtrip` | Same byte-exact relay as row 2 over an AMF3 command plane |
| 5 | P | AMF0 | Enhanced `hvc1` | `enhanced_hvc1_publish_characterization` | Characterization: publish accepted and legacy media still flows afterward; logs Red5 relay behavior |
| 6 | P | AMF3 | Enhanced `av01` | `enhanced_av1_publish_characterization` | Characterization: same check for `av01` over AMF3 |
| 7 | P | AMF0 + AMF3 | Capabilities | `connect_forwards_enhanced_capabilities_amf0/_amf3` | Full Enhanced RTMP connect advertisement (`fourCcList`, `capsEx`, `videoFourCcInfoMap`, `audioFourCcInfoMap`, `videoFunction`) accepted |
| 8 | R | AMF3 | Type-15 script data | `amf3_script_data_survives_red5` | `@setDataFrame`/`onMetaData` with `rtmpxRed5Probe` marker relayed; connection remains healthy |

In addition, `one_connection_stream_lifecycle_amf0` and `one_connection_stream_lifecycle_amf3` exercise three sequential phases over one TCP connection (each sending sequence headers and six A/V frame pairs paced at 40 ms):

1. Publish while a second client verifies relayed bytes.
2. Delete the publish stream and play a new stream from the second client.
3. Delete playback and publish again while the second client verifies delivery.

Red5 2.0.40 reuses deleted wire stream IDs across these phases, proving that distinct local `StreamHandle`s prevent stale operations from colliding.

Rows 5–6 are characterization checks rather than strict relay assertions because Red5 has no Enhanced media path yet: they verify the connection survives Enhanced bytes and record what Red5 does.

## Default offline companions

These tests run on every `cargo test` and cover contracts that third-party live peers cannot exercise on their own:

| File | What it proves | Why it complements live suites |
| --- | --- | --- |
| `tests/amf3_loopback.rs` | AMF3 client publishing to `ServerSession` mirrors framing (`objectEncoding 3` without premature type-15/17) | FFmpeg and OBS are AMF0-only, and Red5 does not publish *to* us |
| `tests/enhanced_loopback.rs` | Byte-exact two-session relay of `hvc1` (AMF0) and `av01` (AMF3) alongside legacy media | Red5 lacks an Enhanced relay path |
| `tests/obs_ingest.rs` | OBS `connect -> releaseStream -> FCPublish -> createStream -> publish -> onMetaData -> A/V -> FCUnpublish -> deleteStream` | Verifies OBS-specific command quirks surface cleanly as `UnhandledCommand` |
| `tests/session_quirks.rs` | Extended timestamps past `0xFFFFFF`, GStreamer string `deleteStream`, and command-specific `_error` / `onStatus` refusal shapes | Avoids 4.6-hour live runs and tests edge-case error framing directly |

## Manual OBS probe

[`examples/obs_ingest_probe.rs`](../../examples/obs_ingest_probe.rs) listens on `127.0.0.1:19350` and logs everything a real OBS Studio build sends:

```sh
cargo run --example obs_ingest_probe
```

Point OBS (**Settings → Stream → Custom**) at server `rtmp://127.0.0.1:19350/live` with any stream key (for example `obs-probe`) to inspect `flashVer`, metadata, chunking, and reconnect behavior on new OBS releases.

## Configuration

| Variable | Default | Meaning |
| --- | --- | --- |
| `RED5_HOST` | `127.0.0.1` | Red5 host |
| `RED5_PORT` | `1935` | Red5 RTMP port |
| `RED5_APP` | `live` | Red5 application name (stock `live` needs no config) |
| `RED5_TIMEOUT_SECS` | `30` | Per-operation timeout (whole-test backstop is 180 s) |

Each test generates a unique stream key (`rtmpx-<tag>-<pid>-<n>`) so tests can run in parallel against one Red5 instance.

## Running locally

```sh
docker compose -f tests/red5/docker-compose.yml up -d --build
# Wait for port 1935 (e.g. `until nc -z 127.0.0.1 1935; do sleep 1; done`)
cargo test --test red5 --features red5-live
docker compose -f tests/red5/docker-compose.yml down
```

## CI workflows

[`.github/workflows/ci.yml`](../../.github/workflows/ci.yml) runs four jobs on PRs, pushes to `main`, manual dispatch, and a weekly schedule:

- `default`: `cargo test --locked` (offline, no external peers)
- `red5`: starts the pinned containerized Red5 and runs `cargo test --test red5 --features red5-live --locked`
- `ffmpeg`: installs `ffmpeg`/`ffprobe` and runs `cargo test --test ffmpeg --features ffmpeg-live --locked`
- `gstreamer`: installs GStreamer plugins and runs `cargo test --test gstreamer --features gstreamer-live --locked`
