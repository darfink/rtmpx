//! Regression coverage for the EOF-handshake defect of the previous RTMP stack.
//!
//! The old vendored server read the handshake with
//! `while bytes_read < READ_SIZE { ...; bytes_read += n; }`, which never
//! advanced when `read` returned `n == 0` at EOF. Zero-byte reads complete
//! immediately, so per-read timeouts could not fire: the worker stayed stuck
//! on a dead connection and never accepted the next publisher (while the
//! kernel still completed TCP handshakes, which made the outage look like a
//! reconnect race).
//!
//! `rtmpx` handshakes are sans-I/O: `Handshake::process_bytes` only
//! inspects the slice the caller already holds, so an empty slice is simply
//! a "need more data" answer that returns promptly. Detecting a vanished
//! peer stays with the socket owner: `server_handshake` in
//! `gst-scuffle-rtmp` treats `n == 0` as a disconnect. These tests lock in
//! the protocol half of that contract: empty input never blocks, and a full
//! handshake still completes with trailing bytes preserved for the session.

use rtmpx::handshake::{Handshake, HandshakeProcessResult, PeerType};
use std::time::{Duration, Instant};

/// Feeding no bytes must answer promptly instead of spinning.
///
/// The old code spun here: EOF reads return Ok(0) immediately, so neither
/// progress nor a timeout could ever break the loop. The sans-I/O handshake
/// holds no socket and performs no I/O, so there is nothing to spin on.
#[test]
fn empty_input_returns_promptly_without_blocking() {
    let mut server = Handshake::new(PeerType::Server);

    let start = Instant::now();
    let result = server
        .process_bytes(&[])
        .expect("empty input must be a plain need-more-data answer");
    let elapsed = start.elapsed();

    match result {
        HandshakeProcessResult::InProgress { .. } => {}
        HandshakeProcessResult::Completed { .. } => {
            panic!("handshake cannot complete with no input")
        }
        _ => panic!("unexpected future protocol variant"),
    }
    assert!(
        elapsed < Duration::from_secs(1),
        "empty handshake input took too long; it must return immediately"
    );

    // A single version byte must behave the same way: wait for more, not hang.
    let start = Instant::now();
    let result = server
        .process_bytes(&[3])
        .expect("partial input must be a plain need-more-data answer");
    assert!(
        start.elapsed() < Duration::from_secs(1),
        "partial handshake input must also return immediately"
    );
    assert!(
        matches!(result, HandshakeProcessResult::InProgress { .. }),
        "one version byte cannot complete the handshake"
    );
}

/// A full client/server handshake still completes, and bytes pipelined after
/// the handshake are carried to the session instead of being swallowed.
#[test]
fn full_handshake_completes_and_preserves_trailing_bytes() {
    let mut client = Handshake::new(PeerType::Client);
    let mut server = Handshake::new(PeerType::Server);

    let c0_and_c1 = client
        .generate_outbound_p0_and_p1()
        .expect("client must emit C0+C1");
    let s0_s1_and_s2 = match server
        .process_bytes(&c0_and_c1)
        .expect("server must accept C0+C1")
    {
        HandshakeProcessResult::InProgress { response_bytes } => response_bytes,
        outcome => panic!("server must stay in progress after C0+C1: {outcome:?}"),
    };
    assert!(!s0_s1_and_s2.is_empty(), "server must answer with S0+S1+S2");

    let c2 = match client
        .process_bytes(&s0_s1_and_s2)
        .expect("client must accept S0+S1+S2")
    {
        HandshakeProcessResult::Completed { response_bytes, .. } => response_bytes,
        outcome => panic!("client must complete after S0+S1+S2: {outcome:?}"),
    };

    // A real publisher pipelines RTMP chunks right after C2; they must
    // survive the handshake as remaining bytes for the session to consume.
    let trailing: &[u8] = b"pipelined-rtmp-chunks";
    let mut c2_plus_trailing = c2.clone();
    c2_plus_trailing.extend_from_slice(trailing);
    match server
        .process_bytes(&c2_plus_trailing)
        .expect("server must accept C2")
    {
        HandshakeProcessResult::Completed {
            remaining_bytes, ..
        } => assert_eq!(
            remaining_bytes, trailing,
            "bytes after C2 must be preserved verbatim for the session"
        ),
        outcome => panic!("server must complete after C2: {outcome:?}"),
    }
}
