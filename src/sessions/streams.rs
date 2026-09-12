use super::StreamId;
use std::sync::atomic::{AtomicU64, Ordering};

/// Local stream identity. Handles are scoped to one session and cannot be constructed externally.
/// Deletion invalidates a handle, even if the peer later reuses its wire stream ID.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct StreamHandle {
    session: u64,
    slot: u32,
    generation: u32,
}

/// Connection negotiation is independent of individual media streams.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectionState {
    Disconnected,
    Connecting,
    Connected,
    Failed,
}

/// State of one client stream. Deleted handles no longer have a state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClientStreamState {
    Creating,
    StartingPlayback,
    Playing,
    StartingPublish,
    Publishing,
}

/// State of one server-side message stream.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ServerStreamState {
    Created,
    Playing,
    Publishing,
    Completed,
}

pub(crate) struct Entry<T> {
    pub wire_id: Option<StreamId>,
    pub state: T,
}
struct Slot<T> {
    generation: u32,
    entry: Option<Entry<T>>,
}
pub(crate) struct Streams<T> {
    session: u64,
    slots: Vec<Slot<T>>,
    by_wire: std::collections::HashMap<u32, StreamHandle>,
}
impl<T> Streams<T> {
    pub fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        let session = NEXT
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_add(1))
            .expect("session identity exhausted");
        Self {
            session,
            slots: Vec::new(),
            by_wire: std::collections::HashMap::new(),
        }
    }
    pub fn insert(&mut self, state: T, wire_id: Option<StreamId>) -> StreamHandle {
        let slot = self
            .slots
            .iter()
            .position(|s| s.entry.is_none() && s.generation < u32::MAX)
            .unwrap_or_else(|| {
                self.slots.push(Slot {
                    generation: 0,
                    entry: None,
                });
                self.slots.len() - 1
            });
        let entry = &mut self.slots[slot];
        entry.generation += 1;
        entry.entry = Some(Entry { state, wire_id });
        let handle = StreamHandle {
            session: self.session,
            slot: u32::try_from(slot).expect("stream slot capacity exhausted"),
            generation: entry.generation,
        };
        if let Some(wire) = wire_id {
            self.by_wire.insert(wire.get(), handle);
        }
        handle
    }
    pub fn get(&self, h: StreamHandle) -> Option<&Entry<T>> {
        if h.session != self.session {
            return None;
        }
        let slot = self.slots.get(h.slot as usize)?;
        if slot.generation != h.generation {
            return None;
        }
        slot.entry.as_ref()
    }
    pub fn get_mut(&mut self, h: StreamHandle) -> Option<&mut Entry<T>> {
        if h.session != self.session {
            return None;
        }
        let slot = self.slots.get_mut(h.slot as usize)?;
        if slot.generation != h.generation {
            return None;
        }
        slot.entry.as_mut()
    }
    pub fn remove(&mut self, h: StreamHandle) -> Option<Entry<T>> {
        self.get(h)?;
        let entry = self.slots[h.slot as usize].entry.take()?;
        if let Some(wire) = entry.wire_id {
            self.by_wire.remove(&wire.get());
        }
        Some(entry)
    }
    pub fn iter(&self) -> impl Iterator<Item = (StreamHandle, &Entry<T>)> {
        self.slots.iter().enumerate().filter_map(|(slot, s)| {
            s.entry.as_ref().map(|e| {
                (
                    StreamHandle {
                        session: self.session,
                        slot: slot as u32,
                        generation: s.generation,
                    },
                    e,
                )
            })
        })
    }
    pub fn bind(&mut self, handle: StreamHandle, wire: StreamId) {
        self.get_mut(handle).expect("live handle").wire_id = Some(wire);
        self.by_wire.insert(wire.get(), handle);
    }
    pub fn find(&self, wire: u32) -> Option<StreamHandle> {
        self.by_wire.get(&wire).copied()
    }
}
