# Red5 live-interop harness (`rtmpx`)

Optional end-to-end suite of `rtmpx` against a real Red5 server: our client
against an independent server implementation. For the other direction (our
server against an independent encoder and player), see `tests/ffmpeg/README.md`.

## Why Red5

Unit tests prove self-consistency; they cannot catch a framing rule both sides
of the same codebase get wrong together. Red5 is an independent Java RTMP
server (AMF0 + AMF3, plaintext handshake, `live` app relays publisher bytes
opaquely). This suite covers our **client** against that independent
**server**; the companion `tests/ffmpeg` suite covers our **server** against
an independent encoder and player.

AMF3 covers the command/data plane only; media packets are unaffected. So the
AMF3 legs validate exactly the layer the `objectEncoding` work changed.

## Gating: not run by default

The `red5` integration target is excluded from default runs via Cargo
`required-features` (no environment-variable gate, no `#[ignore]`):

```toml
[[test]]
name = "red5"
path = "tests/red5/main.rs"
required-features = ["red5-live"]
```

- `cargo test` runs only the default suite: the `red5` target is not even
  built, no network touched.
- `cargo test --test red5 --features red5-live` builds and runs
  the live suite. It expects Red5 on `RED5_HOST:RED5_PORT` (see below) and
  fails with a start hint otherwise.

CI runs the second command against a containerised Red5 (see Workflows).

## Layout

```text
rtmpx/
  tests/red5/
    README.md                 # this file
    red5.dockerfile           # pinned Red5 image (release tarball, no official image)
    docker-compose.yml        # local + CI entrypoint: `docker compose -f tests/red5/docker-compose.yml up -d --build`
  examples/obs_ingest_probe.rs # manual OBS probe (not CI): real OBS -> ServerSession logger
  tests/amf3_loopback.rs      # default suite: our AMF3 client -> our ServerSession
  tests/enhanced_loopback.rs  # default suite: Enhanced hvc1/av01 relay byte-exact
  tests/obs_ingest.rs         # default suite: OBS connect/releaseStream/FCPublish sequence
  tests/red5/
  tests/common/               # shared by the live suites (not a test target)
    mod.rs                    # Result, stream_key, run backstop, legacy_metadata
    driver.rs                 # async TCP + handshake + ClientSession driver
  tests/ffmpeg/               # ffmpeg interop suite (see its README)
  tests/red5/
    main.rs                   # live matrix
    lifecycle.rs              # publish/delete/play/delete/publish on one connection
    harness.rs                # Red5 endpoint (RED5_* env) + objectEncoding reader
    fixtures.rs               # legacy AVC/AAC, Enhanced hvc1/av01, AMF3 probe body
  .github/workflows/ci.yml  # CI: default + live Red5 + live ffmpeg jobs
```

## Matrix

`P` = publish leg (client publishes, Red5 accepts). `R` = round-trip leg
(second client plays while the first publishes; asserts byte-exact relay).
Red5's `live` app only relays media published **after** the player
subscribed, so every `R` test subscribes the player first.

| Row | Direction | AMF | Media | Test | Asserts |
| --- | --- | --- | --- | --- | --- |
| 1 | P | AMF0 | legacy AVC/AAC | `publish_amf0_legacy` | connect negotiates AMF0, `_result` carries `objectEncoding 0`, publish accepted, metadata + seq headers + 3 A/V frames flow |
| 2 | R | AMF0 | legacy AVC/AAC | `play_amf0_legacy_roundtrip` | 4 video + 4 audio relay byte-exact, metadata observed |
| 3 | P | AMF3 | legacy AVC/AAC | `publish_amf3_legacy` | connect negotiates AMF3, `_result` carries `objectEncoding 3`, publish accepted |
| 4 | R | AMF3 | legacy AVC/AAC | `play_amf3_legacy_roundtrip` | same byte-exact relay as row 2, over an AMF3 command plane |
| 5 | P | AMF0 | Enhanced hvc1 | `enhanced_hvc1_publish_characterization` | **characterization**: publish accepted, legacy media still flows afterwards; logs whether Red5 relayed the Enhanced bytes |
| 6 | (covered) | AMF0 | Enhanced av01 | (folded into row 5 shape) | same characterization shape; current suite pins hvc1 on AMF0 and av01 on AMF3 |
| 7 | P | AMF3 | Enhanced av01 | `enhanced_av1_publish_characterization` | **characterization**: same as row 5 over AMF3 |
| 8 | (covered) | AMF3 | Enhanced hvc1 | (folded into row 7 shape) | same characterization shape |
| 9 | P | AMF0 + AMF3 | caps advertisement | `connect_forwards_enhanced_capabilities_amf0/_amf3` | connect carrying full E-RTMP advertisement (`fourCcList`, `capsEx`, `videoFourCcInfoMap`, `audioFourCcInfoMap`, `videoFunction`) is accepted and publish proceeds |
| 10 | R | AMF3 | type-15 script data | `amf3_script_data_survives_red5` | `@setDataFrame`/`onMetaData` body with `rtmpxRed5Probe` marker relayed to player; connection stays usable afterwards |

The `one_connection_stream_lifecycle_amf0` and `one_connection_stream_lifecycle_amf3` tests exercise three phases on one connection:

1. Publish, with a second client verifying the relayed bytes
2. Delete that stream and play a new stream from the second client
3. Delete playback and publish again, with the second client verifying delivery

Each phase sends sequence headers and six audio/video frame pairs, paced at 40 ms intervals.
The tests assert exact media bytes, event handles, one connect acceptance, and an unchanged TCP endpoint.
They use the public pull API and vectored packet writes directly. No historical session adapter is involved.
Red5 2.0.40 reuses deleted wire IDs during this sequence. Distinct local handles prevent stale application operations.

Rows 5-8 are characterization, not assertion: Red5 has no Enhanced media
path, so there is no correct relay behaviour to pin. They prove the
connection survives Enhanced bytes and record what Red5 did, so a future
Red5 with Enhanced support (or a switch to another server) turns them into
real assertions without rewiring.

## Default-suite companions (default `cargo test`, no network/binaries)

The live suite above is `required-features = ["red5-live"]` and never builds by default. These files run on every `cargo test` and pin the same contracts without third parties:

| File | What it proves | Why it exists alongside Red5/ffmpeg |
| --- | --- | --- |
| `tests/amf3_loopback.rs` | our AMF3 client publishes into our `ServerSession`: server mirrors framing, so AMF3 `connect` is answered as AMF0 `_result` with `objectEncoding 3`, no early type-15/17 | No third-party encoder publishes AMF3 (ffmpeg/OBS are AMF0-only, Red5 never publishes *to* us), so this is the only AMF3-ingest coverage |
| `tests/enhanced_loopback.rs` | `hvc1` (AMF0) + `av01` (AMF3) publish and two-session relay byte-exact, legacy untouched | Red5 has no Enhanced media path (rows 5-8 are characterization); ffmpeg Enhanced ingest proves decode, but only this proves the relay contract |
| `tests/obs_ingest.rs` | OBS `connect -> releaseStream -> FCPublish -> createStream -> publish -> onMetaData -> A/V -> FCUnpublish -> deleteStream` ingests; quirks surface as `UnhandledCommand`, ingest unaffected | ffmpeg proves spec compliance; OBS proves quirk tolerance. AMF0-only, so no AMF3 signal |
| `tests/session_quirks.rs` | extended timestamps survive ingest + relay past 0xFFFFFF; GStreamer string `deleteStream` finishes the publish (garbage ignored); connect refusals are `_error` while publish/play refusals are `onStatus` errors, framed per-exchange (AMF0 mirror and genuine type 17) | no live leg runs 4.6h to cross the timestamp boundary, sends string stream ids, or asserts refusal shapes; also caught a real bug where a refused connect omitted the protocol preamble and was undecodable |

## Manual OBS probe (not CI)

`examples/obs_ingest_probe.rs` binds a loopback `ServerSession` on `127.0.0.1:19350` and logs what a real OBS build sends (connect props, `releaseStream`/`FCPublish` quirks, metadata shape, A/V flow):

```sh
cargo run --example obs_ingest_probe
# OBS -> Settings -> Stream -> Service: Custom
#   Server: rtmp://127.0.0.1:19350/live
#   Stream Key: obs-probe
```

Use it to eyeball a new OBS version by hand (flashVer, metadata fields, chunking, reconnects). The default-suite `tests/obs_ingest.rs` pins the same sequence in CI; this probe is for what no fake can reproduce. OBS is AMF0-only and heavy (GUI, Xvfb, version pinning), so it stays manual, not per-PR.

## Configuration

| Variable | Default | Meaning |
| --- | --- | --- |
| `RED5_HOST` | `127.0.0.1` | Red5 host |
| `RED5_PORT` | `1935` | Red5 RTMP port |
| `RED5_APP` | `live` | Red5 app (compose + workflow use stock `live`) |
| `RED5_TIMEOUT_SECS` | `30` | per-operation timeout; whole-file backstop is 180 s per test |

Stream keys are unique per test (`rtmpx-<tag>-<pid>-<n>`) so the tests can
run in parallel against one shared Red5.

## Running locally

```sh
docker compose -f tests/red5/docker-compose.yml up -d --build
# wait for 1935 (macOS: nc -z 127.0.0.1 1935 in a loop)
cargo test --test red5 --features red5-live
docker compose -f tests/red5/docker-compose.yml down
```

Red5's stock `live` app needs no config for this suite.

## Workflows

`.github/workflows/ci.yml` holds three jobs:

- `default` runs `cargo test --locked` - no Red5, no network.
- `red5` starts the pinned Red5 container, waits for port 1935, runs
  `cargo test --test red5 --features red5-live`, then dumps Red5 logs.
- `ffmpeg` installs ffmpeg (the Ubuntu `ffmpeg` package also provides
  `ffprobe`) and runs `cargo test --test ffmpeg --features ffmpeg-live` -
  no server container needed.

The workflow runs on PRs, pushes to main, manual dispatch, and a weekly schedule.
