use super::PublishMode;

pub enum TransactionPurpose {
    PlayRequest {
        stream_key: String,
    },

    PublishRequest {
        stream_key: String,
        request_type: PublishMode,
    },
}

pub enum OutstandingTransaction {
    ConnectionRequested {
        app_name: String,
    },

    CreateStream {
        stream: crate::sessions::StreamHandle,
        purpose: TransactionPurpose,
    },
}
