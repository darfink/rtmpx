#![cfg(feature = "red5-live")]
use rtmpx::amf::AmfEncoding;
use rtmpx::amf0::Amf0Object;
use rtmpx::chunk_io::ChunkDeserializer;
use rtmpx::handshake::{Handshake, HandshakeProcessResult, PeerType};
use rtmpx::sessions::{ClientSession, ClientSessionConfig, ClientSessionResult};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

async fn run_debug(encoding: AmfEncoding, tag: &str) {
    let addr = format!(
        "{}:{}",
        std::env::var("RED5_HOST").unwrap_or("127.0.0.1".into()),
        std::env::var("RED5_PORT").unwrap_or("1936".into())
    );
    eprintln!("[{tag}] connecting to {addr} with {encoding:?}");
    let mut stream = TcpStream::connect(&addr).await.unwrap();
    stream.set_nodelay(true).unwrap();
    let mut hs = Handshake::new(PeerType::Client);
    let c0c1 = hs.generate_outbound_p0_and_p1().unwrap();
    stream.write_all(&c0c1).await.unwrap();
    let mut buf = vec![0u8; 65536];
    let carry;
    loop {
        let n = stream.read(&mut buf).await.unwrap();
        match hs.process_bytes(&buf[..n]).unwrap() {
            HandshakeProcessResult::InProgress { response_bytes } => {
                stream.write_all(&response_bytes).await.unwrap();
            }
            HandshakeProcessResult::Completed {
                response_bytes,
                remaining_bytes,
            } => {
                if !response_bytes.is_empty() {
                    stream.write_all(&response_bytes).await.unwrap();
                }
                carry = remaining_bytes;
                break;
            }
            #[allow(unreachable_patterns)]
            _ => panic!("unexpected future protocol variant"),
        }
    }
    let mut config = ClientSessionConfig::new();
    config.object_encoding = encoding;
    config.tc_url = Some(format!("rtmp://{}/live", addr));
    let (mut session, _) = ClientSession::new(config).unwrap();
    let req = session
        .request_connection_with_properties("live".into(), Amf0Object::new())
        .unwrap();
    if let ClientSessionResult::OutboundResponse(p) = req {
        stream.write_all(&p.bytes).await.unwrap();
    }
    // Raw sniffer in parallel so we can see what kills the session.
    let mut de = ChunkDeserializer::new();
    // NOTE: ChunkDeserializer buffers internally: feed each flight once, then
    // drain with empty slices (re-feeding the same bytes corrupts the stream).
    let feed =
        |label: &str, bytes: &[u8], session: &mut ClientSession, de: &mut ChunkDeserializer| {
            eprintln!("[{tag}] {label}: {} bytes", bytes.len());
            let mut pending: Option<&[u8]> = Some(bytes);
            loop {
                let fed = pending.take().unwrap_or(&[]);
                match de.get_next_message(fed) {
                    Ok(Some(payload)) => {
                        let hex: String = payload
                            .data
                            .iter()
                            .take(48)
                            .map(|b| format!("{b:02x}"))
                            .collect::<Vec<_>>()
                            .join(" ");
                        eprintln!(
                            "[{tag}]   sniff type={} stream={} ts={:?} len={} head=[{hex}]",
                            payload.type_id,
                            payload.message_stream_id,
                            payload.timestamp,
                            payload.data.len()
                        );
                        if payload.type_id == 1 && payload.data.len() == 4 {
                            let sz = u32::from_be_bytes(payload.data[..4].try_into().unwrap());
                            eprintln!("[{tag}]   (peer set chunk size to {sz})");
                        }
                        match payload.to_rtmp_message() {
                            Ok(m) => eprintln!(
                                "[{tag}]     parsed: {:?}",
                                format!("{m:?}").chars().take(400).collect::<String>()
                            ),
                            Err(e) => eprintln!("[{tag}]     PARSE ERROR: {e:?}"),
                        }
                    }
                    Ok(None) => {
                        if fed.is_empty() {
                            break;
                        }
                    }
                    Err(e) => {
                        eprintln!("[{tag}]     CHUNK ERROR: {e:?}");
                        break;
                    }
                }
            }
            match session.handle_input(bytes) {
                Ok(results) => {
                    for r in &results {
                        match r {
                            ClientSessionResult::RaisedEvent(ev) => eprintln!(
                                "[{tag}]   session event: {:?}",
                                format!("{ev:?}").chars().take(400).collect::<String>()
                            ),
                            ClientSessionResult::OutboundResponse(p) => {
                                eprintln!("[{tag}]   session replies {} bytes", p.bytes.len())
                            }
                            ClientSessionResult::UnhandleableMessageReceived(m) => {
                                eprintln!("[{tag}]   unhandleable: {m:?}")
                            }
                            #[allow(unreachable_patterns)]
                            &_ => panic!("unexpected future protocol variant"),
                        }
                    }
                }
                Err(e) => eprintln!("[{tag}]   SESSION ERROR on {label}: {e:?}"),
            }
        };
    if !carry.is_empty() {
        feed(
            "post-handshake carry",
            &carry.clone(),
            &mut session,
            &mut de,
        );
    }
    for round in 0..4 {
        let n = tokio::time::timeout(std::time::Duration::from_secs(8), stream.read(&mut buf))
            .await
            .unwrap()
            .unwrap();
        if n == 0 {
            eprintln!("[{tag}] EOF");
            break;
        }
        let bytes = buf[..n].to_vec();
        feed(&format!("read {round}"), &bytes, &mut session, &mut de);
    }
}

#[tokio::test]
async fn debug_amf0_connect() {
    run_debug(AmfEncoding::Amf0, "amf0").await;
}

#[tokio::test]
async fn debug_amf3_connect() {
    run_debug(AmfEncoding::Amf3, "amf3").await;
}

async fn pump_flight(
    tag: &str,
    label: String,
    bytes: Vec<u8>,
    session: &mut ClientSession,
    de: &mut ChunkDeserializer,
    stream: &mut TcpStream,
) {
    eprintln!("[{tag}] {label}: {} bytes", bytes.len());
    let mut pending: Option<&[u8]> = Some(&bytes);
    loop {
        let fed = pending.take().unwrap_or(&[]);
        match de.get_next_message(fed) {
            Ok(Some(payload)) => {
                let hex: String = payload
                    .data
                    .iter()
                    .take(64)
                    .map(|b| format!("{b:02x}"))
                    .collect::<Vec<_>>()
                    .join(" ");
                eprintln!(
                    "[{tag}]   sniff type={} stream={} len={} head=[{hex}]",
                    payload.type_id,
                    payload.message_stream_id,
                    payload.data.len()
                );
                match payload.to_rtmp_message() {
                    Ok(m) => eprintln!(
                        "[{tag}]     parsed: {:?}",
                        format!("{m:?}").chars().take(600).collect::<String>()
                    ),
                    Err(e) => eprintln!("[{tag}]     PARSE ERROR: {e:?}"),
                }
            }
            Ok(None) => {
                if fed.is_empty() {
                    break;
                }
            }
            Err(e) => {
                eprintln!("[{tag}]     CHUNK ERROR: {e:?}");
                break;
            }
        }
    }
    match session.handle_input(&bytes) {
        Ok(results) => {
            for r in &results {
                match r {
                    ClientSessionResult::RaisedEvent(ev) => eprintln!(
                        "[{tag}]   session event: {:?}",
                        format!("{ev:?}").chars().take(400).collect::<String>()
                    ),
                    ClientSessionResult::OutboundResponse(p) => {
                        eprintln!("[{tag}]   session replies {} bytes", p.bytes.len());
                        stream.write_all(&p.bytes).await.unwrap();
                    }
                    ClientSessionResult::UnhandleableMessageReceived(m) => {
                        eprintln!("[{tag}]   unhandleable: {m:?}")
                    }
                    #[allow(unreachable_patterns)]
                    &_ => panic!("unexpected future protocol variant"),
                }
            }
        }
        Err(e) => eprintln!("[{tag}]   SESSION ERROR on {label}: {e:?}"),
    }
}

/// Connect (AMF3) -> publish -> send one video packet, logging every flight.
/// Reproduces the InvalidMessageFormat the suite hits past connect.
#[tokio::test]
async fn debug_amf3_publish_flow() {
    use rtmpx::sessions::PublishRequestType;
    let addr = format!(
        "{}:{}",
        std::env::var("RED5_HOST").unwrap_or("127.0.0.1".into()),
        std::env::var("RED5_PORT").unwrap_or("1936".into())
    );
    let mut stream = TcpStream::connect(&addr).await.unwrap();
    stream.set_nodelay(true).unwrap();
    let mut hs = Handshake::new(PeerType::Client);
    let c0c1 = hs.generate_outbound_p0_and_p1().unwrap();
    stream.write_all(&c0c1).await.unwrap();
    let mut buf = vec![0u8; 65536];
    let carry;
    loop {
        let n = stream.read(&mut buf).await.unwrap();
        match hs.process_bytes(&buf[..n]).unwrap() {
            HandshakeProcessResult::InProgress { response_bytes } => {
                stream.write_all(&response_bytes).await.unwrap();
            }
            HandshakeProcessResult::Completed {
                response_bytes,
                remaining_bytes,
            } => {
                if !response_bytes.is_empty() {
                    stream.write_all(&response_bytes).await.unwrap();
                }
                carry = remaining_bytes;
                break;
            }
            #[allow(unreachable_patterns)]
            _ => panic!("unexpected future protocol variant"),
        }
    }
    let mut config = ClientSessionConfig::new();
    config.object_encoding = AmfEncoding::Amf3;
    config.tc_url = Some(format!("rtmp://{}/live", addr));
    let (mut session, _) = ClientSession::new(config).unwrap();
    let mut de = ChunkDeserializer::new();
    let req = session
        .request_connection_with_properties("live".into(), Amf0Object::new())
        .unwrap();
    if let ClientSessionResult::OutboundResponse(p) = req {
        stream.write_all(&p.bytes).await.unwrap();
    }
    if !carry.is_empty() {
        let c = carry.clone();
        pump_flight(
            "pub3",
            "carry".to_string(),
            c,
            &mut session,
            &mut de,
            &mut stream,
        )
        .await;
    }
    for round in 0..4 {
        let n = tokio::time::timeout(std::time::Duration::from_secs(8), stream.read(&mut buf))
            .await
            .unwrap()
            .unwrap();
        if n == 0 {
            eprintln!("[pub3] EOF");
            break;
        }
        let bytes = buf[..n].to_vec();
        pump_flight(
            "pub3",
            format!("read {round}"),
            bytes,
            &mut session,
            &mut de,
            &mut stream,
        )
        .await;
    }
    eprintln!(
        "[pub3] negotiated={:?}; requesting publish",
        session.negotiated_encoding()
    );
    let key = format!("dbg-pub3-{}", std::process::id());
    let req = session
        .request_publishing(key.clone(), PublishRequestType::Live)
        .unwrap();
    if let ClientSessionResult::OutboundResponse(p) = req {
        stream.write_all(&p.bytes).await.unwrap();
    }
    for round in 0..8 {
        let n = tokio::time::timeout(std::time::Duration::from_secs(8), stream.read(&mut buf))
            .await
            .unwrap()
            .unwrap();
        if n == 0 {
            eprintln!("[pub3] EOF after publish");
            break;
        }
        let bytes = buf[..n].to_vec();
        pump_flight(
            "pub3",
            format!("pread {round}"),
            bytes,
            &mut session,
            &mut de,
            &mut stream,
        )
        .await;
    }
}
