// Shared helpers for the in-house AMF0/AMF3 codecs.
// AMF0 and AMF3 share wire idioms: big-endian numbers, length-prefixed
// UTF-8, recursive values that need a depth cap, and collections that need
// a growth cap on a public ingest port. Codec-specific framing stays in
// the amf0 and amf3 modules.
use std::io::{self, Cursor, Read};

/// A reader that can optionally say how many bytes are still available.
///
/// Both codecs read length-prefixed blobs whose prefix is attacker controlled.
/// Checking the prefix against the bytes actually remaining lets a truncated
/// payload fail before it allocates, instead of reserving up to
/// `MAX_BYTEARRAY_LEN` from a handful of input bytes on a public ingest port.
/// Readers that cannot answer return `None` and fall back to the size caps
/// alone.
pub trait AmfRead: Read {
    fn remaining_hint(&self) -> Option<usize> {
        None
    }
}

impl<T: AsRef<[u8]>> AmfRead for Cursor<T> {
    fn remaining_hint(&self) -> Option<usize> {
        let len = self.get_ref().as_ref().len() as u64;
        Some(len.saturating_sub(self.position()) as usize)
    }
}

impl AmfRead for &[u8] {
    fn remaining_hint(&self) -> Option<usize> {
        Some(self.len())
    }
}

/// True when `len` could still be satisfied by `reader`.
#[inline]
pub fn length_is_plausible<R: AmfRead + ?Sized>(reader: &R, len: usize) -> bool {
    match reader.remaining_hint() {
        Some(remaining) => len <= remaining,
        None => true,
    }
}

// Maximum nesting depth shared by both codecs.
pub const MAX_DEPTH: usize = 64;
// Maximum number of elements or properties shared by both codecs.
pub const MAX_COLLECTION_LEN: usize = 100000;
// AMF0 short-string ceiling: property names and string values ride on a u16 length.
pub const AMF0_MAX_STRING_LEN: usize = 65535;

// Returns false once depth exceeds MAX_DEPTH.
#[inline]
pub fn check_depth(depth: usize) -> bool {
    depth <= MAX_DEPTH
}

// Read an exact byte vector.
pub fn read_exact_vec<R: Read>(reader: &mut R, len: usize) -> io::Result<Vec<u8>> {
    let mut buf = vec![0u8; len];
    reader.read_exact(&mut buf)?;
    Ok(buf)
}
#[inline]
pub fn read_u8<R: Read>(reader: &mut R) -> io::Result<u8> {
    let mut b = [0u8; 1];
    reader.read_exact(&mut b)?;
    Ok(b[0])
}

#[inline]
pub fn read_u16_be<R: Read>(reader: &mut R) -> io::Result<u16> {
    let mut b = [0u8; 2];
    reader.read_exact(&mut b)?;
    Ok(u16::from_be_bytes(b))
}

#[inline]
pub fn read_u32_be<R: Read>(reader: &mut R) -> io::Result<u32> {
    let mut b = [0u8; 4];
    reader.read_exact(&mut b)?;
    Ok(u32::from_be_bytes(b))
}

#[inline]
pub fn read_f64_be<R: Read>(reader: &mut R) -> io::Result<f64> {
    let mut b = [0u8; 8];
    reader.read_exact(&mut b)?;
    Ok(f64::from_be_bytes(b))
}

#[inline]
pub fn write_u16_be(buf: &mut Vec<u8>, value: u16) {
    buf.extend_from_slice(&value.to_be_bytes());
}

#[inline]
pub fn write_u32_be(buf: &mut Vec<u8>, value: u32) {
    buf.extend_from_slice(&value.to_be_bytes());
}

#[inline]
pub fn write_f64_be(buf: &mut Vec<u8>, value: f64) {
    buf.extend_from_slice(&value.to_be_bytes());
}

#[inline]
pub fn write_i32_be(buf: &mut Vec<u8>, value: i32) {
    buf.extend_from_slice(&value.to_be_bytes());
}

#[inline]
pub fn read_i32_be<R: Read>(reader: &mut R) -> io::Result<i32> {
    let mut b = [0u8; 4];
    reader.read_exact(&mut b)?;
    Ok(i32::from_be_bytes(b))
}

#[inline]
pub fn check_collection_len(len: usize) -> bool {
    len <= MAX_COLLECTION_LEN
}

impl<R: AmfRead> AmfRead for std::io::BufReader<R> {
    fn remaining_hint(&self) -> Option<usize> {
        self.get_ref()
            .remaining_hint()?
            .checked_add(self.buffer().len())
    }
}
