//! Red5-specific bits of the live suite: endpoint from RED5_* env vars, plus
//! reading objectEncoding out of Red5's connect _result. The generic client
//! driver lives in tests/common/driver.rs.

use std::time::Duration;

use rtmpx::amf0::{Amf0Object, Amf0Value};

use crate::common::driver::Endpoint;

/// How to reach the Red5 under test. Everything honours RED5_* env vars so CI
/// (service container) and local runs (compose file next to this crate) need
/// no code changes.
pub fn red5_endpoint() -> Endpoint {
    let host = std::env::var("RED5_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    let port: u16 = std::env::var("RED5_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(1935);
    let app = std::env::var("RED5_APP").unwrap_or_else(|_| "live".to_string());
    let secs: u64 = std::env::var("RED5_TIMEOUT_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(30);
    let addr = format!("{host}:{port}");
    let hint = format!(
        "Red5 is not reachable at {addr} (app '{app}'). Start it first:\n  docker compose -f tests/red5/docker-compose.yml up -d --build"
    );
    Endpoint {
        addr,
        app,
        op_timeout: Duration::from_secs(secs),
        unreachable_hint: Some(hint),
    }
}

/// Read a numeric objectEncoding out of a _result command object.
pub fn object_encoding_of(command_object: &Amf0Object) -> Option<f64> {
    command_object
        .get("objectEncoding")
        .and_then(Amf0Value::get_number)
}
