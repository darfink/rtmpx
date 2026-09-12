/// Bounds for connection-level state, separate from chunk-decoder limits.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SessionLimits {
    /// Live handles, including streams whose creation is pending.
    pub max_streams: usize,
    /// Unanswered client transactions or server requests awaiting an application decision.
    /// Cancelled client creations still count until the peer answers.
    pub max_pending_requests: usize,
}
impl Default for SessionLimits {
    fn default() -> Self {
        Self {
            max_streams: 128,
            max_pending_requests: 128,
        }
    }
}
/// A session-state bound would be exceeded.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum SessionLimitError {
    #[error("live stream limit exceeded ({limit})")]
    Streams { limit: usize },
    #[error("pending request limit exceeded ({limit})")]
    PendingRequests { limit: usize },
}
impl SessionLimits {
    pub(crate) fn check_streams(self, count: usize) -> Result<(), SessionLimitError> {
        if count >= self.max_streams {
            Err(SessionLimitError::Streams {
                limit: self.max_streams,
            })
        } else {
            Ok(())
        }
    }
    pub(crate) fn check_requests(self, count: usize) -> Result<(), SessionLimitError> {
        if count >= self.max_pending_requests {
            Err(SessionLimitError::PendingRequests {
                limit: self.max_pending_requests,
            })
        } else {
            Ok(())
        }
    }
}
