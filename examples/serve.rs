//! Minimal publisher ingest: cargo run --example serve -- 127.0.0.1:1935
#[path = "support/io.rs"]
mod transport;
use bytes::BytesMut;
use rtmpx::{
    EnhancedValidationMode, PayloadPool, ValidatedMedia,
    handshake::HandshakeRole,
    sessions::{ServerEvent, ServerOutput, ServerSession, ServerSessionConfig},
};
use tokio::net::{TcpListener, TcpStream};

async fn serve_one(mut stream: TcpStream) -> Result<(), Box<dyn std::error::Error>> {
    let mut input = transport::handshake(&mut stream, HandshakeRole::Server).await?;
    let config = ServerSessionConfig {
        payload_pool: Some(PayloadPool::default()),
        ..Default::default()
    };
    let mut session = ServerSession::new(config)?;
    let mut buffer = BytesMut::with_capacity(16 * 1024);
    let (mut audio, mut video) = (0, 0);
    loop {
        while let Some(output) = session.receive(&mut input)? {
            match output {
                ServerOutput::Packet(packet) => {
                    transport::write_packet(&mut stream, packet).await?
                }
                ServerOutput::Event(event) => match event {
                    ServerEvent::ConnectionRequested {
                        request_id,
                        app_name,
                        ..
                    } => {
                        println!("connect {app_name}");
                        session.accept_request(request_id)?;
                    }
                    ServerEvent::PublishStreamRequested {
                        request_id,
                        stream_key,
                        ..
                    } => {
                        println!("publish {stream_key}");
                        session.accept_request(request_id)?;
                    }
                    ServerEvent::PlayStreamRequested { request_id, .. } => session.reject_request(
                        request_id,
                        "NetStream.Play.StreamNotFound",
                        "This example only accepts publishers",
                    )?,
                    ServerEvent::AudioDataReceived { data, .. } => {
                        let media = ValidatedMedia::parse_audio(
                            data.view(),
                            EnhancedValidationMode::Strict,
                        )?;
                        audio += usize::from(media.classification().coded);
                    }
                    ServerEvent::VideoDataReceived { data, .. } => {
                        let media = ValidatedMedia::parse_video(
                            data.view(),
                            EnhancedValidationMode::Strict,
                        )?;
                        video += usize::from(media.classification().coded);
                    }
                    ServerEvent::StreamDataReceived { message, .. } => {
                        if let Some(properties) = message.metadata()? {
                            println!("metadata: {properties:?}");
                        }
                    }
                    _ => {}
                },
                _ => {}
            }
        }
        match transport::read_input(&mut stream, &mut buffer).await? {
            Some(bytes) => input = bytes,
            None => {
                println!("publisher left: {audio} audio / {video} video frames");
                return Ok(());
            }
        }
    }
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let address = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:1935".into());
    let listener = TcpListener::bind(&address).await?;
    println!("Publish to rtmp://{address}/live/demo");
    loop {
        let (stream, _) = listener.accept().await?;
        if let Err(error) = serve_one(stream).await {
            eprintln!("{error}");
        }
    }
}
