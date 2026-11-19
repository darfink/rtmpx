# Enhanced RTMP v2 r2 compliance

This matrix tracks `rtmpx` against VSO Enhanced RTMP [`v2-2026-01-31-r2`](https://github.com/veovera/enhanced-rtmp/blob/9f371058b9c24e269cdadc34295333793b326719/docs/enhanced/enhanced-rtmp-v2.md) (commit `9f371058b9c24e269cdadc34295333793b326719`).

`rtmpx` uses an in-house FLV parser (with wire layouts ported by reference from `scuffle-flv` 0.2.2) and applies v2 r2 validation through owned or borrowed media views while keeping raw RTMP message bodies authoritative.

| v2 r2 area | Ingest & inspection | Lossless relay | Notes |
| --- | :---: | :---: | --- |
| Legacy audio/video FLV | Yes | Yes | Parsed by the in-house FLV parser; raw bytes retained. |
| Enhanced video packet families | Yes | Yes | Sequence start/end, coded frames, coded frames X, metadata, MPEG-2 TS, multitrack, and ModEx. |
| Enhanced audio packet families | Yes | Yes | Sequence start/end, coded frames, multichannel configuration, multitrack, and ModEx. |
| Video FourCCs | Yes | Yes | VP8, VP9, AV1, AVC, HEVC, and v2 r2 VVC (`vvc1`, accepted via the extensible raw FourCC type). |
| Audio FourCCs | Yes | Yes | AC-3, E-AC-3, Opus, MP3, FLAC, and AAC. |
| Multitrack IDs and per-track codecs | Yes | Yes | Track IDs are exposed with no implied priority; track metadata stays in raw script-data payloads. |
| Multichannel audio | Yes | Yes | Unspecified, native mask, and custom channel order are parsed. |
| ModEx timestamp nano offset | Yes | Yes | Known ModEx types are typed; unknown ModEx is rejected in strict mode and kept opaque in passthrough mode. |
| Video metadata frames | Yes | Yes | Accepted opaquely alongside the original bytes. |
| `onMetaData` and track maps | Yes | Yes | Additional audio/video track IDs, codec IDs, and arbitrary per-track fields have a typed view; raw AMF bytes remain authoritative for relay. |
| `fourCcList` | Yes | Yes | Strict mode requires an array of four-byte strings or the `*` wildcard. |
| `[audio|video]FourCcInfoMap` | Yes | Yes | Strict mode requires FourCC or wildcard keys and numeric capability masks. |
| `capsEx` | Yes | Yes | Numeric flags are preserved; servers advertise only capabilities they implement. |
| Server capability response | Yes | N/A | Server `_result` properties are configurable; clients receive complete command and status property maps. |
| Unknown connect properties | Yes | Selected | Retained for inspection; applications choose which properties to forward on new connections (the capability helper extracts Enhanced RTMP fields). |
| Unknown or malformed Enhanced packets | Policy-dependent | Yes (passthrough) | Strict mode rejects invalid structures and unknown enums/FourCCs; passthrough keeps them as opaque raw bytes. |
| AMF3 script data | Typed 15/17, opaque 16/19 | Raw relay | The first byte of type 15/17 is a format selector: `0x00` selects an AMF0 body with `avmplus` (`0x11`) AMF3 escapes, and `0x03` selects an AMF3 body. Both decode to `Amf3Value` while preserving `format` on `RtmpMessage::Amf3Command`/`Amf3Data` for faithful relay. Undefined selectors error strictly; shared objects (16/19) stay opaque and FLV TagType 15 is relay-only. |
| `objectEncoding` | Negotiated | N/A | Client requests are clamped to supported versions and advertised in `_result` (undefined defaults to `0`). Responses mirror the request's framing rather than switching unilaterally. |
| Reconnect request | Parsed capability only | N/A | Capability fields are exposed; applications own reconnect policy. |
| Typed outbound Enhanced FLV encoding | No | Raw only | Deliberately omitted; relays forward the original bytes. |
| Playback sequence-header caching | No | N/A | Deliberately omitted; applications own caching and media replay. |

Protocol validation is separate from application codec admission: an ingest pipeline can parse a structurally valid codec and still reject it under its own media policy.
