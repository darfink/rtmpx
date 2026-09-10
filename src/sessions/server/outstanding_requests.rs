use super::PublishMode;
use crate::amf::AmfEncoding;
use std::sync::Arc;

pub enum OutstandingRequest {
    ConnectionRequest {
        app_name: Arc<str>,
        transaction_id: f64,
        /// Framing the request arrived under, so the deferred response mirrors it.
        encoding: AmfEncoding,
    },

    PublishRequested {
        stream_key: Arc<str>,
        mode: PublishMode,
        stream_id: u32,
        encoding: AmfEncoding,
    },

    PlayRequested {
        stream_key: Arc<str>,
        stream_id: u32,
        encoding: AmfEncoding,
    },
}
