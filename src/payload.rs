//! Owned segmented storage, borrowed ranges, and reusable payload descriptors.
//!
//! [`Payload`] retains `Bytes` segments without copying their bodies.
//! Its first segment is inline; additional segments occupy a descriptor vector.
//! [`PayloadPool`] recycles that vector across payload lifetimes and threads.
//! Pool limits bound cached descriptors, not live payload bytes or application queues.
//!
//! [`PayloadView`] borrows ranges without copying bytes or descriptors.
//! [`Segments`] lets packet encoding accept other stable storage types.
//! Explicit conversion to contiguous bytes copies fragmented storage.
//! Cloning fragmented payloads can allocate descriptors and prolong receive-buffer lifetimes.
use bytes::Bytes;

/// Stable, nonempty segments of one logical byte string.
///
/// `len()` must equal the sum of segment lengths. Indices below `segment_count()`
/// must be valid, and every returned segment must be nonempty. Empty payloads
/// have no segments. Implementations must keep these values stable while borrowed.
pub trait Segments {
    fn len(&self) -> usize;
    fn segment_count(&self) -> usize;
    fn segment(&self, index: usize) -> &[u8];
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
impl Segments for [u8] {
    fn len(&self) -> usize {
        <[u8]>::len(self)
    }
    fn segment_count(&self) -> usize {
        usize::from(!self.is_empty())
    }
    fn segment(&self, index: usize) -> &[u8] {
        assert!(index == 0 && !self.is_empty());
        self
    }
}
impl Segments for Bytes {
    fn len(&self) -> usize {
        self.as_ref().len()
    }
    fn segment_count(&self) -> usize {
        usize::from(!self.is_empty())
    }
    fn segment(&self, index: usize) -> &[u8] {
        self.as_ref().segment(index)
    }
}
impl<T: Segments + ?Sized> Segments for &T {
    fn len(&self) -> usize {
        (**self).len()
    }
    fn segment_count(&self) -> usize {
        (**self).segment_count()
    }
    fn segment(&self, index: usize) -> &[u8] {
        (**self).segment(index)
    }
}

/// A message body with explicit, opt-in coalescing.
///
/// The first segment lives inline. Additional segments allocate descriptors, but
/// never copy payload bytes. `PayloadPool` can recycle these descriptors.
/// Cloning a fragmented payload clones its descriptors and can allocate.
/// Slices can retain a larger source allocation; use
/// bounded receive slabs and release consumed messages promptly.
#[derive(Clone, Debug, Default)]
pub struct Payload {
    first: Bytes,
    rest: Vec<Bytes>,
    len: usize,
    pool: Option<PayloadPool>,
}
impl Payload {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn push(&mut self, bytes: Bytes) {
        if bytes.is_empty() {
            return;
        }
        self.len = self
            .len
            .checked_add(bytes.len())
            .expect("payload length overflow");
        if self.first.is_empty() {
            self.first = bytes;
        } else {
            self.rest.push(bytes);
        }
    }
    pub fn len(&self) -> usize {
        self.len
    }
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
    pub fn segments(&self) -> impl Iterator<Item = &Bytes> + Clone {
        std::iter::once(&self.first)
            .filter(|b| !b.is_empty())
            .chain(self.rest.iter())
    }
    pub fn as_contiguous(&self) -> Option<&Bytes> {
        self.rest.is_empty().then_some(&self.first)
    }
    /// Return the original allocation for a contiguous payload; otherwise copy once.
    pub fn into_bytes(mut self) -> Bytes {
        if self.rest.is_empty() {
            return std::mem::take(&mut self.first);
        }
        let mut output = Vec::with_capacity(self.len);
        for bytes in self.segments() {
            output.extend_from_slice(bytes);
        }
        Bytes::from(output)
    }
    pub fn to_bytes(&self) -> Bytes {
        if let Some(bytes) = self.as_contiguous() {
            return bytes.clone();
        }
        let mut output = Vec::with_capacity(self.len);
        for bytes in self.segments() {
            output.extend_from_slice(bytes);
        }
        Bytes::from(output)
    }
    pub fn reader(&self) -> PayloadReader<'_> {
        PayloadReader {
            payload: self,
            segment: 0,
            offset: 0,
            remaining: self.len,
        }
    }
}
impl From<Bytes> for Payload {
    fn from(first: Bytes) -> Self {
        Self {
            len: first.len(),
            first,
            rest: Vec::new(),
            pool: None,
        }
    }
}
impl FromIterator<Bytes> for Payload {
    fn from_iter<T: IntoIterator<Item = Bytes>>(items: T) -> Self {
        let mut out = Self::new();
        for b in items {
            out.push(b);
        }
        out
    }
}
impl Segments for Payload {
    fn len(&self) -> usize {
        self.len
    }
    fn segment_count(&self) -> usize {
        usize::from(!self.first.is_empty()) + self.rest.len()
    }
    fn segment(&self, index: usize) -> &[u8] {
        if index == 0 {
            &self.first
        } else {
            &self.rest[index - 1]
        }
    }
}
impl PartialEq for Payload {
    fn eq(&self, other: &Self) -> bool {
        self.len == other.len
            && self
                .segments()
                .flat_map(|b| b.iter())
                .eq(other.segments().flat_map(|b| b.iter()))
    }
}
impl Eq for Payload {}

/// A reader over segments, suitable for AMF without first coalescing a message.
pub struct PayloadReader<'a> {
    payload: &'a dyn Segments,
    segment: usize,
    offset: usize,
    remaining: usize,
}
impl<'a> PayloadReader<'a> {
    pub fn new(payload: &'a impl Segments) -> Self {
        Self {
            payload,
            segment: 0,
            offset: 0,
            remaining: payload.len(),
        }
    }
}
impl std::io::Read for PayloadReader<'_> {
    fn read(&mut self, output: &mut [u8]) -> std::io::Result<usize> {
        if self.remaining == 0 || output.is_empty() {
            return Ok(0);
        }
        let segment = self.payload.segment(self.segment);
        let n = output.len().min(segment.len() - self.offset);
        output[..n].copy_from_slice(&segment[self.offset..self.offset + n]);
        self.offset += n;
        self.remaining -= n;
        if self.offset == segment.len() {
            self.segment += 1;
            self.offset = 0;
        }
        Ok(n)
    }
}
impl crate::amf::AmfRead for PayloadReader<'_> {
    fn remaining_hint(&self) -> Option<usize> {
        Some(self.remaining)
    }
}

impl Segments for Vec<u8> {
    fn len(&self) -> usize {
        Vec::len(self)
    }
    fn segment_count(&self) -> usize {
        usize::from(!self.is_empty())
    }
    fn segment(&self, index: usize) -> &[u8] {
        self.as_slice().segment(index)
    }
}
impl<const N: usize> Segments for [u8; N] {
    fn len(&self) -> usize {
        N
    }
    fn segment_count(&self) -> usize {
        usize::from(N != 0)
    }
    fn segment(&self, index: usize) -> &[u8] {
        self.as_slice().segment(index)
    }
}
impl From<Vec<u8>> for Payload {
    fn from(bytes: Vec<u8>) -> Self {
        Bytes::from(bytes).into()
    }
}
impl From<bytes::BytesMut> for Payload {
    fn from(bytes: bytes::BytesMut) -> Self {
        bytes.freeze().into()
    }
}

/// A bounded, thread-safe cache of empty payload descriptor vectors.
///
/// Clones share the cache. Dropping a pooled payload releases its receive buffers
/// before returning its descriptors. Cache limits do not restrict live messages;
/// use decoder limits and a bounded relay queue for that.
#[derive(Clone, Debug)]
pub struct PayloadPool(std::sync::Arc<PoolInner>);
#[derive(Debug)]
struct PoolInner {
    cached: std::sync::Mutex<Vec<Vec<Bytes>>>,
    maximum_cached_payloads: usize,
    maximum_segments_per_payload: usize,
}
/// Bounds for cached descriptor storage; live messages have separate decoder limits.
#[derive(Clone, Copy, Debug)]
pub struct PayloadPoolConfig {
    pub max_cached_payloads: usize,
    /// Maximum capacity of each cached descriptor vector, excluding the inline segment.
    pub max_descriptors_per_payload: usize,
}
impl Default for PayloadPoolConfig {
    fn default() -> Self {
        Self {
            max_cached_payloads: 16,
            max_descriptors_per_payload: 4096,
        }
    }
}
impl Default for PayloadPool {
    fn default() -> Self {
        Self::new(PayloadPoolConfig::default())
    }
}
impl PayloadPool {
    /// Reserve cache slots once. Oversized descriptor vectors are freed on return.
    /// The descriptor capacity limit excludes the inline first segment.
    pub fn new(config: PayloadPoolConfig) -> Self {
        let maximum_cached_payloads = config.max_cached_payloads;
        let maximum_segments_per_payload = config.max_descriptors_per_payload;
        Self(std::sync::Arc::new(PoolInner {
            cached: std::sync::Mutex::new(Vec::with_capacity(maximum_cached_payloads)),
            maximum_cached_payloads,
            maximum_segments_per_payload,
        }))
    }
    /// Acquire an empty payload. Descriptor storage grows only when necessary.
    pub fn acquire(&self) -> Payload {
        let rest = self
            .0
            .cached
            .lock()
            .ok()
            .and_then(|mut cache| cache.pop())
            .unwrap_or_default();
        Payload {
            first: Bytes::new(),
            rest,
            len: 0,
            pool: Some(self.clone()),
        }
    }
}
impl Drop for Payload {
    fn drop(&mut self) {
        self.first = Bytes::new();
        self.rest.clear();
        if self.rest.capacity() == 0 {
            return;
        }
        if let Some(pool) = &self.pool
            && self.rest.capacity() <= pool.0.maximum_segments_per_payload
            && let Ok(mut cache) = pool.0.cached.lock()
            && cache.len() < pool.0.maximum_cached_payloads
        {
            cache.push(std::mem::take(&mut self.rest));
        }
    }
}

/// A borrowed range of a segmented payload. Slicing copies no bytes or descriptors.
#[derive(Clone, Copy)]
pub struct PayloadView<'a> {
    source: &'a (dyn Segments + Sync),
    first: usize,
    offset: usize,
    last: usize,
    last_end: usize,
    len: usize,
}
impl<'a> PayloadView<'a> {
    pub fn new(source: &'a (impl Segments + Sync)) -> Self {
        let last = source.segment_count().saturating_sub(1);
        Self {
            source,
            first: 0,
            offset: 0,
            last,
            last_end: if source.is_empty() {
                0
            } else {
                source.segment(last).len()
            },
            len: source.len(),
        }
    }
    /// Borrow a subrange. Only boundaries are located; descriptors remain in the source.
    /// Panics if the range is reversed or extends beyond this view.
    pub fn slice(&self, range: std::ops::Range<usize>) -> Self {
        assert!(range.start <= range.end && range.end <= self.len);
        if range.is_empty() {
            return Self { len: 0, ..*self };
        }
        let mut out = *self;
        let mut skip = range.start;
        while skip > 0 {
            let available = out.source.segment(out.first).len() - out.offset;
            if skip < available {
                out.offset += skip;
                break;
            }
            skip -= available;
            out.first += 1;
            out.offset = 0;
        }
        out.len = range.end - range.start;
        if range.end != self.len {
            let mut remaining = out.len;
            out.last = out.first;
            let mut offset = out.offset;
            loop {
                let available = out.source.segment(out.last).len() - offset;
                if remaining <= available {
                    out.last_end = offset + remaining;
                    break;
                }
                remaining -= available;
                out.last += 1;
                offset = 0;
            }
        }
        out
    }
}
impl Segments for PayloadView<'_> {
    fn len(&self) -> usize {
        self.len
    }
    fn segment_count(&self) -> usize {
        if self.len == 0 {
            0
        } else {
            self.last - self.first + 1
        }
    }
    fn segment(&self, index: usize) -> &[u8] {
        assert!(index < self.segment_count());
        let absolute = self.first + index;
        let bytes = self.source.segment(absolute);
        let start = if index == 0 { self.offset } else { 0 };
        let end = if absolute == self.last {
            self.last_end
        } else {
            bytes.len()
        };
        &bytes[start..end]
    }
}
impl std::fmt::Debug for PayloadView<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PayloadView")
            .field("len", &self.len)
            .field("segments", &self.segment_count())
            .finish()
    }
}
impl PartialEq for PayloadView<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.len == other.len
            && (0..self.segment_count())
                .flat_map(|i| self.segment(i))
                .eq((0..other.segment_count()).flat_map(|i| other.segment(i)))
    }
}
impl Eq for PayloadView<'_> {}
impl Payload {
    /// Borrow a view for media parsing without coalescing or cloning descriptors.
    pub fn view(&self) -> PayloadView<'_> {
        PayloadView::new(self)
    }
}

#[cfg(test)]
mod pool_tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn recycling_across_threads_releases_sources_and_preserves_capacity() {
        struct Owner(Arc<Vec<u8>>);
        impl AsRef<[u8]> for Owner {
            fn as_ref(&self) -> &[u8] {
                &self.0
            }
        }
        let source = Arc::new(vec![42; 32]);
        let bytes = Bytes::from_owner(Owner(source.clone()));
        let pool = PayloadPool::new(crate::PayloadPoolConfig {
            max_cached_payloads: 1,
            max_descriptors_per_payload: 9,
        });
        let mut payload = pool.acquire();
        for i in 0..8 {
            payload.push(bytes.slice(i..i + 1));
        }
        let capacity = payload.rest.capacity();
        drop(bytes);
        assert_eq!(Arc::strong_count(&source), 2);
        std::thread::spawn(move || drop(payload)).join().unwrap();
        assert_eq!(
            Arc::strong_count(&source),
            1,
            "cache must not retain receive buffers"
        );
        let payload = pool.acquire();
        assert!(payload.is_empty());
        assert_eq!(payload.rest.capacity(), capacity);
    }

    #[test]
    fn cache_count_and_capacity_are_bounded() {
        let pool = PayloadPool::new(crate::PayloadPoolConfig {
            max_cached_payloads: 1,
            max_descriptors_per_payload: 9,
        });
        let fill = |n| {
            let mut p = pool.acquire();
            for _ in 0..n {
                p.push(Bytes::from_static(b"x"));
            }
            p
        };
        let a = fill(8);
        let b = fill(8);
        drop(a);
        drop(b);
        assert_eq!(pool.0.cached.lock().unwrap().len(), 1);
        let oversized = fill(17);
        drop(oversized);
        assert!(pool.0.cached.lock().unwrap().is_empty());
        let pool = PayloadPool::new(crate::PayloadPoolConfig {
            max_cached_payloads: 0,
            max_descriptors_per_payload: 9,
        });
        let mut p = pool.acquire();
        for _ in 0..8 {
            p.push(Bytes::from_static(b"x"));
        }
        drop(p);
        assert!(pool.0.cached.lock().unwrap().is_empty());
    }

    #[test]
    fn views_slice_empty_and_cross_segment_ranges() {
        let p: Payload = [
            Bytes::from_static(b"abc"),
            Bytes::new(),
            Bytes::from_static(b"def"),
            Bytes::from_static(b"ghi"),
        ]
        .into_iter()
        .collect();
        for start in 0..=p.len() {
            for end in start..=p.len() {
                let view = p.view().slice(start..end);
                let bytes: Vec<_> = (0..view.segment_count())
                    .flat_map(|i| view.segment(i))
                    .copied()
                    .collect();
                assert_eq!(bytes, &b"abcdefghi"[start..end]);
                assert_eq!(view.len(), end - start);
            }
        }
    }
}
