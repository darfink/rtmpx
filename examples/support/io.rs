#![allow(dead_code)]
//! Transport helpers for the examples. RTMPX itself performs no I/O.
use bytes::{Bytes, BytesMut};
use rtmpx::{
    Packet, Segments,
    handshake::{Handshake, HandshakeProgress, HandshakeRole},
};
use std::io::{self, IoSlice};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

pub async fn write_packet<P: Segments>(
    stream: &mut TcpStream,
    mut packet: Packet<P>,
) -> io::Result<()> {
    while !packet.is_complete() {
        let mut slices = [IoSlice::new(&[]); 32];
        let count = packet.io_slices(&mut slices);
        match stream.write_vectored(&slices[..count]).await {
            Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
            Ok(n) => packet.advance(n),
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

pub async fn read_input(
    stream: &mut TcpStream,
    buffer: &mut BytesMut,
) -> io::Result<Option<Bytes>> {
    buffer.reserve(16 * 1024);
    // Limit each read; the application owns receive-buffer allocation policy.
    let n = stream.take(16 * 1024).read_buf(buffer).await?;
    Ok((n != 0).then(|| buffer.split().freeze()))
}

pub async fn handshake(
    stream: &mut TcpStream,
    role: HandshakeRole,
) -> Result<Bytes, Box<dyn std::error::Error>> {
    let client = role == HandshakeRole::Client;
    let mut handshake = Handshake::new(role);
    if client {
        stream
            .write_all(&handshake.generate_outbound_p0_and_p1()?)
            .await?;
    }
    let mut buffer = [0; 4096];
    loop {
        let n = stream.read(&mut buffer).await?;
        if n == 0 {
            return Err("peer closed during handshake".into());
        }
        match handshake.process_bytes(&buffer[..n])? {
            HandshakeProgress::InProgress { response_bytes } => {
                stream.write_all(&response_bytes).await?
            }
            HandshakeProgress::Completed {
                response_bytes,
                remaining_bytes,
            } => {
                stream.write_all(&response_bytes).await?;
                return Ok(Bytes::from(remaining_bytes));
            }
        }
    }
}
