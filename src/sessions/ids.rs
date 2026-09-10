/// Identifier of an outstanding request within one server session.
/// It is not a message stream identifier and is not transferable between sessions.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct RequestId(pub(crate) u32);

impl RequestId {
    /// Numeric value for diagnostics. Requests are obtained from session events.
    pub fn get(self) -> u32 {
        self.0
    }
}

/// RTMP message stream identifier, scoped to one connection.
///
/// ```compile_fail
/// use rtmpx::sessions::{ServerSession, StreamId};
/// fn accept_stream_as_request(server: &mut ServerSession, stream: StreamId) {
///     server.accept_request(stream).unwrap();
/// }
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct StreamId(u32);

impl StreamId {
    pub const fn new(value: u32) -> Self {
        Self(value)
    }
    pub const fn get(self) -> u32 {
        self.0
    }
}

impl std::fmt::Display for StreamId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl std::fmt::Display for RequestId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
