use crate::chunk_io::Packet;
use crate::messages::RawMessage;
use crate::sessions::client::ClientEvent;

/// A single result that is returned when the client session performs an action
/// or receives messages from the server.
#[derive(PartialEq, Debug)]
#[non_exhaustive]
pub enum ClientOutput<D = crate::Payload> {
    /// A packet that is slated to be sent to the peer.  This packet should *ALWAYS* be sent
    /// in the order it produced and can only be dropped if it has explicitly been marked as
    /// able to be dropped.  Failing to do so may cause RTMP chunk deserialization errors on the
    /// other end due to RTMP chunk header compression.
    Packet(Packet),

    /// An event the client session is raising so consuming applications can perform custom logic
    Event(ClientEvent<D>),

    /// The client session received a message that it could not handle.  This result
    /// allows the consumer application to do something with it if it wants to (special logging)
    UnhandledMessage(RawMessage<D>),
}

impl<D> ClientOutput<D> {
    pub fn map_payload<T>(self, map: impl FnMut(D) -> T) -> ClientOutput<T> {
        let mut map = map;
        match self {
            Self::Packet(packet) => ClientOutput::Packet(packet),
            Self::Event(event) => ClientOutput::Event(event.map_payload(map)),
            Self::UnhandledMessage(payload) => ClientOutput::UnhandledMessage(RawMessage {
                timestamp: payload.timestamp,
                type_id: payload.type_id,
                message_stream_id: payload.message_stream_id,
                data: map(payload.data),
            }),
        }
    }
}
