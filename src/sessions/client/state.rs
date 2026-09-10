#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ClientState {
    /// Client has not connected to an application on the server yet,
    Disconnected,
    /// Waiting for the connect result. Close the transport to cancel connection establishment.
    ConnectionRequested,
    /// Waiting for createStream before sending play.
    CreatingPlayStream,
    /// Waiting for createStream before sending publish.
    CreatingPublishStream,
    /// Cancellation waits for createStream so its returned stream can be deleted.
    CancellingPlay,
    /// Cancellation waits for createStream so its returned stream can be deleted.
    CancellingPublish,
    /// An input error terminated this session; close its transport.
    Failed,

    /// The client has connected to an application on the server
    Connected,

    /// Playback has been requested for a stream key and we are still waiting for a response
    PlayRequested,

    /// We are currently playing back a stream from the server
    Playing,

    /// Publish has been requested and we are waiting for a response
    PublishRequested,

    /// We are currently publishing to the server
    Publishing,
}
