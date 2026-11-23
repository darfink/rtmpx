use super::*;
use crate::amf0::{Amf0Object, Amf0Value};
use crate::api::sessions::{ServerSession, ServerSessionEvent, ServerSessionResult};
use crate::chunk_io::ContiguousDecoder;
use crate::messages::{RawMessage, RtmpMessage, UserControlEventType};
use bytes::BytesMut;

const DEFAULT_CHUNK_SIZE: u32 = 1111;
const DEFAULT_PEER_BANDWIDTH: u32 = 2222;
const DEFAULT_WINDOW_ACK_SIZE: u32 = 3333;

const TEST_APP_NAME: &str = "some_app";
const TEST_STREAM_KEY: &str = "stream_key";

#[test]
fn new_session_writes_nothing_before_connect() {
    let config = get_basic_config();
    let (_, results) = ServerSession::new(config).unwrap();
    assert!(results.is_empty());
}

#[test]
fn on_bw_done_not_sent_when_config_disables_it() {
    let mut config = get_basic_config();
    config.send_on_bw_done_message_on_start = false;
    let (mut deserializer, mut serializer, mut session) = common_setup(&config);

    let responses = connect_and_resolve(&mut session, &mut serializer, &mut deserializer, true);
    assert!(
        !responses.iter().any(|(_, m)| is_command(m, "onBWDone")),
        "onBWDone sent although disabled: {responses:?}"
    );
    assert!(
        is_command(&responses[responses.len() - 1].1, "_result"),
        "the connect result must be last: {responses:?}"
    );
}

#[test]
fn rejected_connection_does_not_send_on_bw_done() {
    let (mut deserializer, mut serializer, mut session) = common_basic_setup();

    let responses = connect_and_resolve(&mut session, &mut serializer, &mut deserializer, false);
    assert!(
        !responses.iter().any(|(_, m)| is_command(m, "onBWDone")),
        "onBWDone sent after a refused connect: {responses:?}"
    );
    let refusal = &responses[responses.len() - 1].1;
    assert!(
        is_command(refusal, "_error"),
        "the refusal must be last: {responses:?}"
    );
    // RTMP 1.0 levels are "warning", "status" or "error".
    assert_eq!(
        info_of(refusal).get("level"),
        Some(&Amf0Value::Utf8String("error".to_string()))
    );
}

#[test]
fn connect_flow_follows_the_spec_order() {
    let (mut deserializer, mut serializer, mut session) = common_basic_setup();

    let responses = connect_and_resolve(&mut session, &mut serializer, &mut deserializer, true);
    let kinds: Vec<&str> = responses
        .iter()
        .map(|(_, message)| match message {
            RtmpMessage::SetChunkSize { .. } => "SetChunkSize",
            RtmpMessage::WindowAcknowledgement { .. } => "WindowAcknowledgement",
            RtmpMessage::SetPeerBandwidth { .. } => "SetPeerBandwidth",
            RtmpMessage::UserControl {
                event_type: UserControlEventType::StreamBegin,
                ..
            } => "StreamBegin",
            RtmpMessage::Amf0Command { command_name, .. } => command_name.as_str(),
            _ => "other",
        })
        .collect();
    // RTMP 1.0 section 7.2.1.1, then `onBWDone` once the connection exists.
    assert_eq!(
        kinds,
        [
            "SetChunkSize",
            "WindowAcknowledgement",
            "SetPeerBandwidth",
            "StreamBegin",
            "_result",
            "onBWDone"
        ]
    );
}

#[test]
fn no_op_commands_are_answered_with_result() {
    let (mut deserializer, mut serializer, mut session) = common_basic_setup();
    perform_connection("some_app", &mut session, &mut serializer, &mut deserializer);

    for (name, transaction_id) in [
        ("releaseStream", 2.0),
        ("FCPublish", 3.0),
        ("FCUnpublish", 6.0),
    ] {
        let (responses, events) = send_command(
            name,
            transaction_id,
            &mut session,
            &mut serializer,
            &mut deserializer,
        );
        assert_eq!(
            responses.len(),
            1,
            "{name} needs exactly one reply: {responses:?}"
        );
        let (raw, message) = &responses[0];
        assert_eq!(
            raw.message_stream_id, 0,
            "{name} is answered on the stream it arrived on"
        );
        match message {
            RtmpMessage::Amf0Command {
                command_name,
                transaction_id: replied,
                command_object: Amf0Value::Null,
                additional_arguments,
            } if command_name == "_result" => {
                assert_eq!(
                    *replied, transaction_id,
                    "{name} reply must echo its transaction"
                );
                assert_eq!(additional_arguments, &vec![Amf0Value::Undefined]);
            }
            other => panic!("{name} must be answered with _result, got {other:?}"),
        }
        assert!(
            matches!(
                events.as_slice(),
                [ServerSessionEvent::UnhandledCommand { command_name, .. }] if command_name == name
            ),
            "{name} must still surface to the caller: {events:?}"
        );
    }
}

#[test]
fn unknown_command_is_refused_with_error() {
    let (mut deserializer, mut serializer, mut session) = common_basic_setup();
    perform_connection("some_app", &mut session, &mut serializer, &mut deserializer);

    let (responses, events) = send_command(
        "_checkbw",
        7.0,
        &mut session,
        &mut serializer,
        &mut deserializer,
    );
    assert_eq!(responses.len(), 1, "exactly one reply: {responses:?}");
    let (raw, message) = &responses[0];
    assert_eq!(raw.message_stream_id, 0);
    match message {
        RtmpMessage::Amf0Command {
            command_name,
            transaction_id,
            command_object: Amf0Value::Null,
            ..
        } if command_name == "_error" => assert_eq!(*transaction_id, 7.0),
        other => panic!("an unknown call must be refused with _error, got {other:?}"),
    }
    let info = info_of(message);
    assert_eq!(
        info.get("level"),
        Some(&Amf0Value::Utf8String("error".to_string()))
    );
    assert_eq!(
        info.get("code"),
        Some(&Amf0Value::Utf8String(
            "NetConnection.Call.Failed".to_string()
        ))
    );
    assert_eq!(
        info.get("description"),
        Some(&Amf0Value::Utf8String(
            "Method not found (_checkbw).".to_string()
        ))
    );
    assert!(
        matches!(
            events.as_slice(),
            [ServerSessionEvent::CommandRefused { command_name, transaction_id, .. }]
                if command_name == "_checkbw" && *transaction_id == 7.0
        ),
        "the refusal must be reported: {events:?}"
    );
}

#[test]
fn non_finite_transaction_ids_get_no_answer() {
    let (mut deserializer, mut serializer, mut session) = common_basic_setup();
    perform_connection("some_app", &mut session, &mut serializer, &mut deserializer);

    for transaction_id in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        let (responses, events) = send_command(
            "_checkbw",
            transaction_id,
            &mut session,
            &mut serializer,
            &mut deserializer,
        );
        assert!(
            responses.is_empty(),
            "{transaction_id} cannot be echoed: {responses:?}"
        );
        assert!(matches!(
            events.as_slice(),
            [ServerSessionEvent::UnhandledCommand { .. }]
        ));
    }
}

#[test]
fn nothing_is_answered_while_connect_awaits_acceptance() {
    let (mut deserializer, mut serializer, mut session) = common_basic_setup();
    let connect_payload = create_connect_message(TEST_APP_NAME.to_string(), 15, 0, 0.0);
    let connect_packet = serializer.serialize(&connect_payload, true, false).unwrap();
    consume_results(
        &mut deserializer,
        session.handle_input(&connect_packet.to_vec()[..]).unwrap(),
    );

    let (responses, _) = send_command(
        "_checkbw",
        2.0,
        &mut session,
        &mut serializer,
        &mut deserializer,
    );
    assert!(
        responses.is_empty(),
        "the connection does not exist until connect is accepted: {responses:?}"
    );
}

#[test]
fn command_on_a_media_stream_is_answered_on_that_stream() {
    let (mut deserializer, mut serializer, mut session) = common_basic_setup();
    perform_connection("some_app", &mut session, &mut serializer, &mut deserializer);
    let stream_id = create_active_stream(&mut session, &mut serializer, &mut deserializer);

    for (name, answer) in [("FCUnpublish", "_result"), ("_checkbw", "_error")] {
        let message = RtmpMessage::Amf0Command {
            command_name: name.to_string(),
            transaction_id: 8.0,
            command_object: Amf0Value::Null,
            additional_arguments: vec![],
        };
        let payload = message
            .into_raw_message(RtmpTimestamp::new(0), stream_id)
            .unwrap();
        let packet = serializer.serialize(&payload, false, false).unwrap();
        let (responses, _) = split_results(
            &mut deserializer,
            session.handle_input(&packet.to_vec()[..]).unwrap(),
        );
        assert_eq!(responses.len(), 1, "{name}: {responses:?}");
        assert_eq!(responses[0].0.message_stream_id, stream_id, "{name}");
        assert!(is_command(&responses[0].1, answer), "{name}: {responses:?}");
    }
}

#[test]
fn refusal_mirrors_amf3_framing() {
    let (mut deserializer, mut serializer, mut session) = common_basic_setup();
    // objectEncoding 3 negotiates AMF3.
    let connect_payload = create_connect_message(TEST_APP_NAME.to_string(), 15, 0, 3.0);
    let connect_packet = serializer.serialize(&connect_payload, true, false).unwrap();
    let (_, events) = split_results(
        &mut deserializer,
        session.handle_input(&connect_packet.to_vec()[..]).unwrap(),
    );
    let [ServerSessionEvent::ConnectionRequested { request_id, .. }] = events.as_slice() else {
        panic!("connect must be requested: {events:?}");
    };
    consume_results(
        &mut deserializer,
        session.accept_request(*request_id).unwrap(),
    );

    let message = RtmpMessage::Amf3Command {
        command_name: "_checkbw".to_string(),
        transaction_id: 2.0,
        command_object: crate::amf3::Amf3Value::Null,
        additional_arguments: vec![],
        format: crate::amf::AmfEncoding::Amf0,
    };
    let payload = message.into_raw_message(RtmpTimestamp::new(0), 0).unwrap();
    let packet = serializer.serialize(&payload, false, false).unwrap();
    let (responses, _) = split_results(
        &mut deserializer,
        session.handle_input(&packet.to_vec()[..]).unwrap(),
    );
    assert!(
        matches!(
            responses.as_slice(),
            [(_, RtmpMessage::Amf3Command { command_name, transaction_id, .. })]
                if command_name == "_error" && *transaction_id == 2.0
        ),
        "an AMF3 call is refused in AMF3 framing: {responses:?}"
    );
}

#[test]
fn delegated_command_is_answered_by_the_application() {
    let (mut deserializer, mut serializer, mut session) = common_basic_setup();
    perform_connection("some_app", &mut session, &mut serializer, &mut deserializer);
    session.delegate_command("_checkbw");

    let (responses, events) = send_command(
        "_checkbw",
        7.0,
        &mut session,
        &mut serializer,
        &mut deserializer,
    );
    assert!(
        responses.is_empty(),
        "the session must not answer: {responses:?}"
    );
    let request_id = match events.as_slice() {
        [
            ServerSessionEvent::CommandRequested {
                request_id,
                command_name,
                stream_id,
                additional_values,
                ..
            },
        ] if command_name == "_checkbw" && stream_id.get() == 0 => {
            assert_eq!(
                additional_values,
                &vec![Amf0Value::Utf8String(TEST_STREAM_KEY.to_string())]
            );
            *request_id
        }
        other => panic!("the application must be asked: {other:?}"),
    };

    let (responses, _) = split_results(
        &mut deserializer,
        session
            .accept_command(request_id, vec![Amf0Value::Number(42.0)])
            .unwrap(),
    );
    match responses.as_slice() {
        [
            (
                raw,
                RtmpMessage::Amf0Command {
                    command_name,
                    transaction_id,
                    command_object: Amf0Value::Null,
                    additional_arguments,
                },
            ),
        ] if command_name == "_result" => {
            assert_eq!(*transaction_id, 7.0);
            assert_eq!(raw.message_stream_id, 0);
            assert_eq!(additional_arguments, &vec![Amf0Value::Number(42.0)]);
        }
        other => panic!("the application's answer must be sent: {other:?}"),
    }
    assert!(matches!(
        session.accept_command(request_id, vec![]),
        Err(ServerSessionError::InvalidRequestId)
    ));
}

#[test]
fn delegated_command_can_be_refused_or_accepted_empty() {
    let (mut deserializer, mut serializer, mut session) = common_basic_setup();
    perform_connection("some_app", &mut session, &mut serializer, &mut deserializer);
    // Delegation also replaces the session's no-op answer.
    session.delegate_command("FCPublish");

    let request = |transaction_id: f64,
                   session: &mut ServerSession,
                   serializer: &mut ChunkEncoder,
                   deserializer: &mut ContiguousDecoder| {
        let (responses, events) = send_command(
            "FCPublish",
            transaction_id,
            session,
            serializer,
            deserializer,
        );
        assert!(responses.is_empty(), "{responses:?}");
        match events.as_slice() {
            [ServerSessionEvent::CommandRequested { request_id, .. }] => *request_id,
            other => panic!("expected CommandRequested, got {other:?}"),
        }
    };

    let refused = request(3.0, &mut session, &mut serializer, &mut deserializer);
    let (responses, _) = split_results(
        &mut deserializer,
        session
            .reject_request(refused, "NetStream.Publish.BadName", "unknown key")
            .unwrap(),
    );
    assert!(is_command(&responses[0].1, "_error"), "{responses:?}");
    let info = info_of(&responses[0].1);
    assert_eq!(
        info.get("code"),
        Some(&Amf0Value::Utf8String(
            "NetStream.Publish.BadName".to_string()
        ))
    );
    assert_eq!(
        info.get("level"),
        Some(&Amf0Value::Utf8String("error".to_string()))
    );

    let accepted = request(4.0, &mut session, &mut serializer, &mut deserializer);
    let (responses, _) =
        split_results(&mut deserializer, session.accept_request(accepted).unwrap());
    assert!(
        matches!(
            responses.as_slice(),
            [(_, RtmpMessage::Amf0Command { command_name, transaction_id, additional_arguments, .. })]
                if command_name == "_result"
                    && *transaction_id == 4.0
                    && additional_arguments == &vec![Amf0Value::Undefined]
        ),
        "{responses:?}"
    );

    // A delegated call that expects no answer is not the application's to answer.
    let (_, events) = send_command(
        "FCPublish",
        0.0,
        &mut session,
        &mut serializer,
        &mut deserializer,
    );
    assert!(matches!(
        events.as_slice(),
        [ServerSessionEvent::UnhandledCommand { .. }]
    ));
}

#[test]
fn delegating_an_implemented_command_has_no_effect() {
    let (mut deserializer, mut serializer, mut session) = common_basic_setup();
    perform_connection("some_app", &mut session, &mut serializer, &mut deserializer);
    session.delegate_command("createStream");
    // Still answered by the session with a stream id.
    create_active_stream(&mut session, &mut serializer, &mut deserializer);
}

#[test]
fn server_calls_are_matched_to_their_answers() {
    let (mut deserializer, mut serializer, mut session) = common_basic_setup();
    assert!(matches!(
        session.call("onBWCheck", vec![]),
        Err(ServerSessionError::NotConnected)
    ));
    perform_connection("some_app", &mut session, &mut serializer, &mut deserializer);

    let (first, results) = session
        .call("onBWCheck", vec![Amf0Value::Number(1.0)])
        .unwrap();
    let (responses, _) = split_results(&mut deserializer, results);
    let sent_id = match responses.as_slice() {
        [
            (
                raw,
                RtmpMessage::Amf0Command {
                    command_name,
                    transaction_id,
                    command_object: Amf0Value::Null,
                    additional_arguments,
                },
            ),
        ] if command_name == "onBWCheck" => {
            assert_eq!(raw.message_stream_id, 0);
            assert_eq!(additional_arguments, &vec![Amf0Value::Number(1.0)]);
            *transaction_id
        }
        other => panic!("the call must be sent: {other:?}"),
    };
    assert_eq!(sent_id, f64::from(first.get()));
    let (second, results) = session.call("onBWCheck", vec![]).unwrap();
    consume_results(&mut deserializer, results);
    assert_ne!(first, second);

    for (answer, id, accepted) in [("_error", second, false), ("_result", first, true)] {
        let (responses, events) = send_command(
            answer,
            f64::from(id.get()),
            &mut session,
            &mut serializer,
            &mut deserializer,
        );
        assert!(
            responses.is_empty(),
            "an answer is never answered: {responses:?}"
        );
        match events.as_slice() {
            [
                ServerSessionEvent::CallAnswered {
                    call_id,
                    accepted: was_accepted,
                    additional_values,
                    ..
                },
            ] => {
                assert_eq!(*call_id, id);
                assert_eq!(*was_accepted, accepted);
                assert_eq!(
                    additional_values,
                    &vec![Amf0Value::Utf8String(TEST_STREAM_KEY.to_string())]
                );
            }
            other => panic!("expected CallAnswered for {id}, got {other:?}"),
        }
    }

    // A second answer to the same call matches nothing.
    let (_, events) = send_command(
        "_result",
        f64::from(first.get()),
        &mut session,
        &mut serializer,
        &mut deserializer,
    );
    assert!(matches!(
        events.as_slice(),
        [ServerSessionEvent::UnhandledCommand { .. }]
    ));

    let (responses, _) = split_results(
        &mut deserializer,
        session.notify("onBWDone", vec![]).unwrap(),
    );
    assert!(
        matches!(
            responses.as_slice(),
            [(_, RtmpMessage::Amf0Command { command_name, transaction_id, .. })]
                if command_name == "onBWDone" && *transaction_id == 0.0
        ),
        "a notification carries transaction id 0: {responses:?}"
    );
}

#[test]
fn on_bw_done_is_off_by_default() {
    let (mut deserializer, mut serializer, mut session) = common_setup(&ServerSessionConfig::new());
    let responses = connect_and_resolve(&mut session, &mut serializer, &mut deserializer, true);
    assert!(
        !responses.iter().any(|(_, m)| is_command(m, "onBWDone")),
        "onBWDone is opt-in: {responses:?}"
    );
}

#[test]
fn a_reply_that_cannot_be_encoded_keeps_the_request() {
    let (mut deserializer, mut serializer, mut session) = common_basic_setup();
    perform_connection("some_app", &mut session, &mut serializer, &mut deserializer);
    session.delegate_command("_checkbw");
    let (_, events) = send_command(
        "_checkbw",
        3.0,
        &mut session,
        &mut serializer,
        &mut deserializer,
    );
    let [ServerSessionEvent::CommandRequested { request_id, .. }] = events.as_slice() else {
        panic!("expected CommandRequested, got {events:?}");
    };

    // AMF0 property names are limited to 65535 bytes.
    let mut unencodable = Amf0Object::new();
    unencodable.insert("x".repeat(70_000), Amf0Value::Null);
    assert!(
        session
            .accept_command(*request_id, vec![Amf0Value::Object(unencodable)])
            .is_err()
    );

    // The client is still waiting, and can still be answered.
    let (responses, _) = split_results(
        &mut deserializer,
        session
            .reject_request(*request_id, "NetConnection.Call.Failed", "failed")
            .unwrap(),
    );
    assert!(is_command(&responses[0].1, "_error"), "{responses:?}");
}

#[test]
fn deleting_a_stream_expires_its_delegated_commands() {
    let (mut deserializer, mut serializer, mut session) = common_basic_setup();
    perform_connection("some_app", &mut session, &mut serializer, &mut deserializer);
    let stream_id = create_active_stream(&mut session, &mut serializer, &mut deserializer);
    session.delegate_command("custom");

    let message = RtmpMessage::Amf0Command {
        command_name: "custom".to_string(),
        transaction_id: 3.0,
        command_object: Amf0Value::Null,
        additional_arguments: vec![],
    };
    let payload = message
        .into_raw_message(RtmpTimestamp::new(0), stream_id)
        .unwrap();
    let packet = serializer.serialize(&payload, false, false).unwrap();
    let (_, events) = split_results(
        &mut deserializer,
        session.handle_input(&packet.to_vec()[..]).unwrap(),
    );
    let [ServerSessionEvent::CommandRequested { request_id, .. }] = events.as_slice() else {
        panic!("expected CommandRequested, got {events:?}");
    };

    let message = RtmpMessage::Amf0Command {
        command_name: "deleteStream".to_string(),
        transaction_id: 0.0,
        command_object: Amf0Value::Null,
        additional_arguments: vec![Amf0Value::Number(stream_id as f64)],
    };
    let payload = message.into_raw_message(RtmpTimestamp::new(0), 0).unwrap();
    let packet = serializer.serialize(&payload, false, false).unwrap();
    consume_results(
        &mut deserializer,
        session.handle_input(&packet.to_vec()[..]).unwrap(),
    );

    assert!(matches!(
        session.accept_command(*request_id, vec![]),
        Err(ServerSessionError::InvalidRequestId)
    ));
}

#[test]
fn requests_and_calls_share_one_limit() {
    let mut config = get_basic_config();
    config.session_limits.max_pending_requests = 1;
    let (mut deserializer, mut serializer, mut session) = common_setup(&config);
    perform_connection("some_app", &mut session, &mut serializer, &mut deserializer);
    session.delegate_command("_checkbw");
    let (_, events) = send_command(
        "_checkbw",
        3.0,
        &mut session,
        &mut serializer,
        &mut deserializer,
    );
    assert!(matches!(
        events.as_slice(),
        [ServerSessionEvent::CommandRequested { .. }]
    ));

    assert!(matches!(
        session.call("onBWCheck", vec![]),
        Err(ServerSessionError::LimitExceeded(_))
    ));

    // And the other way round: an unanswered call leaves no room for a
    // request from the client.
    let (mut deserializer, mut serializer, mut session) = common_setup(&config);
    perform_connection("some_app", &mut session, &mut serializer, &mut deserializer);
    session.delegate_command("_checkbw");
    let (_, results) = session.call("onBWCheck", vec![]).unwrap();
    consume_results(&mut deserializer, results);
    let message = RtmpMessage::Amf0Command {
        command_name: "_checkbw".to_string(),
        transaction_id: 3.0,
        command_object: Amf0Value::Null,
        additional_arguments: vec![],
    };
    let payload = message.into_raw_message(RtmpTimestamp::new(0), 0).unwrap();
    let packet = serializer.serialize(&payload, false, false).unwrap();
    assert!(matches!(
        session.handle_input(&packet.to_vec()[..]),
        Err(ServerSessionError::LimitExceeded(_))
    ));
}

#[test]
fn only_a_net_connection_answer_completes_a_call() {
    let (mut deserializer, mut serializer, mut session) = common_basic_setup();
    perform_connection("some_app", &mut session, &mut serializer, &mut deserializer);
    let stream_id = create_active_stream(&mut session, &mut serializer, &mut deserializer);
    let (call_id, results) = session.call("onBWCheck", vec![]).unwrap();
    consume_results(&mut deserializer, results);

    let answer = |message_stream_id: u32,
                  session: &mut ServerSession,
                  serializer: &mut ChunkEncoder,
                  deserializer: &mut ContiguousDecoder| {
        let message = RtmpMessage::Amf0Command {
            command_name: "_result".to_string(),
            transaction_id: f64::from(call_id.get()),
            command_object: Amf0Value::Null,
            additional_arguments: vec![],
        };
        let payload = message
            .into_raw_message(RtmpTimestamp::new(0), message_stream_id)
            .unwrap();
        let packet = serializer.serialize(&payload, false, false).unwrap();
        split_results(
            deserializer,
            session.handle_input(&packet.to_vec()[..]).unwrap(),
        )
        .1
    };

    let events = answer(stream_id, &mut session, &mut serializer, &mut deserializer);
    assert!(
        matches!(
            events.as_slice(),
            [ServerSessionEvent::UnhandledCommand { .. }]
        ),
        "an answer on a media stream is not the call's: {events:?}"
    );
    let events = answer(0, &mut session, &mut serializer, &mut deserializer);
    assert!(
        matches!(events.as_slice(), [ServerSessionEvent::CallAnswered { call_id: id, .. }] if *id == call_id),
        "{events:?}"
    );
}

#[test]
fn delegated_reply_mirrors_the_framing_of_its_request() {
    let (mut deserializer, mut serializer, mut session) = common_basic_setup();
    let connect_payload = create_connect_message(TEST_APP_NAME.to_string(), 15, 0, 3.0);
    let connect_packet = serializer.serialize(&connect_payload, true, false).unwrap();
    let (_, events) = split_results(
        &mut deserializer,
        session.handle_input(&connect_packet.to_vec()[..]).unwrap(),
    );
    let [ServerSessionEvent::ConnectionRequested { request_id, .. }] = events.as_slice() else {
        panic!("connect must be requested: {events:?}");
    };
    consume_results(
        &mut deserializer,
        session.accept_request(*request_id).unwrap(),
    );
    session.delegate_command("custom");

    // An AMF3-framed request...
    let message = RtmpMessage::Amf3Command {
        command_name: "custom".to_string(),
        transaction_id: 2.0,
        command_object: crate::amf3::Amf3Value::Null,
        additional_arguments: vec![],
        format: crate::amf::AmfEncoding::Amf0,
    };
    let payload = message.into_raw_message(RtmpTimestamp::new(0), 0).unwrap();
    let packet = serializer.serialize(&payload, false, false).unwrap();
    let (_, events) = split_results(
        &mut deserializer,
        session.handle_input(&packet.to_vec()[..]).unwrap(),
    );
    let [ServerSessionEvent::CommandRequested { request_id, .. }] = events.as_slice() else {
        panic!("expected CommandRequested, got {events:?}");
    };
    // ...then an AMF0 command in between...
    send_command(
        "releaseStream",
        3.0,
        &mut session,
        &mut serializer,
        &mut deserializer,
    );

    // ...and the late answer still uses the request's AMF3 framing.
    let (responses, _) = split_results(
        &mut deserializer,
        session.accept_command(*request_id, vec![]).unwrap(),
    );
    assert!(
        matches!(
            responses.as_slice(),
            [(_, RtmpMessage::Amf3Command { command_name, transaction_id, .. })]
                if command_name == "_result" && *transaction_id == 2.0
        ),
        "{responses:?}"
    );
}

#[test]
fn control_operations_combine_in_one_event_handler() {
    // Drives the real pull loop, as an application does, rather than the
    // draining test adapter.
    use crate::sessions::{ServerEvent, ServerOutput, ServerSession as Session};

    fn pump(
        session: &mut Session,
        mut input: Bytes,
        decoder: &mut ContiguousDecoder,
    ) -> Vec<String> {
        let mut commands = Vec::new();
        while let Some(output) = session.receive(&mut input).unwrap() {
            match output {
                ServerOutput::Packet(packet) => {
                    let payload = decoder.get_next_message(&packet.to_vec()).unwrap().unwrap();
                    match payload.to_rtmp_message().unwrap() {
                        RtmpMessage::SetChunkSize { size } => {
                            decoder.set_chunk_size(size as usize).unwrap();
                        }
                        RtmpMessage::Amf0Command { command_name, .. } => {
                            commands.push(command_name)
                        }
                        _ => {}
                    }
                }
                // Each handler answers and then calls the client straight away.
                ServerOutput::Event(ServerEvent::ConnectionRequested { request_id, .. }) => {
                    session.accept_request(request_id).unwrap();
                    session.notify("onBWDone", vec![]).unwrap();
                }
                ServerOutput::Event(ServerEvent::CommandRequested { request_id, .. }) => {
                    session
                        .accept_command(request_id, vec![Amf0Value::Undefined])
                        .unwrap();
                    session.notify("onFCPublish", vec![]).unwrap();
                    session.call("onBWCheck", vec![]).unwrap();
                }
                _ => {}
            }
        }
        commands
    }

    let mut session = Session::new(get_basic_config()).unwrap();
    session.delegate_command("FCPublish");
    let mut serializer = ChunkEncoder::new();
    let mut decoder = ContiguousDecoder::new();

    let connect = create_connect_message(TEST_APP_NAME.to_string(), 15, 0, 0.0);
    let bytes = Bytes::from(
        serializer
            .serialize(&connect, true, false)
            .unwrap()
            .to_vec(),
    );
    // The test config enables `onBWDone`; the handler's own follows it.
    assert_eq!(
        pump(&mut session, bytes, &mut decoder),
        ["_result", "onBWDone", "onBWDone"]
    );

    let fc_publish = RtmpMessage::Amf0Command {
        command_name: "FCPublish".to_string(),
        transaction_id: 3.0,
        command_object: Amf0Value::Null,
        additional_arguments: vec![],
    }
    .into_raw_message(RtmpTimestamp::new(0), 0)
    .unwrap();
    let bytes = Bytes::from(
        serializer
            .serialize(&fc_publish, false, false)
            .unwrap()
            .to_vec(),
    );
    assert_eq!(
        pump(&mut session, bytes, &mut decoder),
        ["_result", "onFCPublish", "onBWCheck"]
    );
}

#[test]
fn control_sent_before_connect_is_readable_and_precedes_the_preamble() {
    // A fresh decoder, as the client has: nothing has been sent yet.
    let (_, mut serializer, mut session) = common_basic_setup();
    let mut deserializer = ContiguousDecoder::new();

    let ping = RtmpMessage::UserControl {
        event_type: UserControlEventType::PingRequest,
        timestamp: Some(RtmpTimestamp::new(7)),
        stream_id: None,
        buffer_length: None,
    }
    .into_raw_message(RtmpTimestamp::new(0), 0)
    .unwrap();
    let packet = serializer.serialize(&ping, true, false).unwrap();
    let (responses, _) = split_results(
        &mut deserializer,
        session.handle_input(&packet.to_vec()[..]).unwrap(),
    );
    assert!(
        matches!(
            responses.as_slice(),
            [(
                _,
                RtmpMessage::UserControl {
                    event_type: UserControlEventType::PingResponse,
                    ..
                }
            )]
        ),
        "{responses:?}"
    );

    // The preamble and connect result that follow still decode.
    let responses = connect_and_resolve(&mut session, &mut serializer, &mut deserializer, true);
    assert!(
        is_command(&responses.last().unwrap().1, "onBWDone"),
        "{responses:?}"
    );
}

#[test]
fn a_forgotten_call_frees_its_place() {
    let mut config = get_basic_config();
    config.session_limits.max_pending_requests = 1;
    let (mut deserializer, mut serializer, mut session) = common_setup(&config);
    perform_connection("some_app", &mut session, &mut serializer, &mut deserializer);

    let (ignored, results) = session.call("onBWCheck", vec![]).unwrap();
    consume_results(&mut deserializer, results);
    assert!(session.forget_call(ignored));
    assert!(!session.forget_call(ignored), "already forgotten");
    let (_, results) = session.call("onBWCheck", vec![]).unwrap();
    consume_results(&mut deserializer, results);

    // A late answer to the forgotten call matches nothing.
    let (_, events) = send_command(
        "_result",
        f64::from(ignored.get()),
        &mut session,
        &mut serializer,
        &mut deserializer,
    );
    assert!(matches!(
        events.as_slice(),
        [ServerSessionEvent::UnhandledCommand { .. }]
    ));
}

#[test]
fn answers_and_calls_follow_amf3_framing() {
    let (mut deserializer, mut serializer, mut session) = common_basic_setup();
    let connect_payload = create_connect_message(TEST_APP_NAME.to_string(), 15, 0, 3.0);
    let connect_packet = serializer.serialize(&connect_payload, true, false).unwrap();
    let (_, events) = split_results(
        &mut deserializer,
        session.handle_input(&connect_packet.to_vec()[..]).unwrap(),
    );
    let [ServerSessionEvent::ConnectionRequested { request_id, .. }] = events.as_slice() else {
        panic!("connect must be requested: {events:?}");
    };
    consume_results(
        &mut deserializer,
        session.accept_request(*request_id).unwrap(),
    );

    // An AMF3-framed no-op is answered in AMF3...
    let message = RtmpMessage::Amf3Command {
        command_name: "releaseStream".to_string(),
        transaction_id: 2.0,
        command_object: crate::amf3::Amf3Value::Null,
        additional_arguments: vec![],
        format: crate::amf::AmfEncoding::Amf0,
    };
    let payload = message.into_raw_message(RtmpTimestamp::new(0), 0).unwrap();
    let packet = serializer.serialize(&payload, false, false).unwrap();
    let (responses, _) = split_results(
        &mut deserializer,
        session.handle_input(&packet.to_vec()[..]).unwrap(),
    );
    assert!(
        matches!(
            responses.as_slice(),
            [(_, RtmpMessage::Amf3Command { command_name, transaction_id, .. })]
                if command_name == "_result" && *transaction_id == 2.0
        ),
        "{responses:?}"
    );

    // ...and once the peer frames in AMF3, the server's own calls do too.
    let (_, results) = session.call("onBWCheck", vec![]).unwrap();
    let (call, _) = split_results(&mut deserializer, results);
    let (notification, _) = split_results(
        &mut deserializer,
        session.notify("onBWDone", vec![]).unwrap(),
    );
    for (name, responses) in [("onBWCheck", call), ("onBWDone", notification)] {
        assert!(
            matches!(
                responses.as_slice(),
                [(_, RtmpMessage::Amf3Command { command_name, .. })] if command_name == name
            ),
            "{name}: {responses:?}"
        );
    }
}

#[test]
fn a_connect_answer_that_cannot_be_encoded_changes_nothing() {
    let (mut deserializer, mut serializer, mut session) = common_basic_setup();
    let connect_payload = create_connect_message(TEST_APP_NAME.to_string(), 15, 0, 0.0);
    let connect_packet = serializer.serialize(&connect_payload, true, false).unwrap();
    let (_, events) = split_results(
        &mut deserializer,
        session.handle_input(&connect_packet.to_vec()[..]).unwrap(),
    );
    let [ServerSessionEvent::ConnectionRequested { request_id, .. }] = events.as_slice() else {
        panic!("connect must be requested: {events:?}");
    };

    // AMF0 property names are limited to 65535 bytes.
    let mut unencodable = Amf0Object::new();
    unencodable.insert("x".repeat(70_000), Amf0Value::Null);
    assert!(
        session
            .accept_request_with_properties(*request_id, unencodable)
            .is_err()
    );
    assert!(
        matches!(
            session.call("onBWCheck", vec![]),
            Err(ServerSessionError::NotConnected)
        ),
        "a failed accept must not connect the session"
    );

    // The request is still outstanding, and its answer still decodes: the
    // preamble was not used up by the failed attempt.
    let (responses, _) = split_results(
        &mut deserializer,
        session.accept_request(*request_id).unwrap(),
    );
    let kinds: Vec<bool> = responses
        .iter()
        .map(|(_, message)| is_command(message, "_result"))
        .collect();
    assert_eq!(
        kinds,
        [false, false, false, false, true, false],
        "{responses:?}"
    );
}

#[test]
fn unanswered_server_calls_are_bounded() {
    let mut config = get_basic_config();
    config.session_limits.max_pending_requests = 2;
    let (mut deserializer, mut serializer, mut session) = common_setup(&config);
    perform_connection("some_app", &mut session, &mut serializer, &mut deserializer);

    for _ in 0..2 {
        let (_, results) = session.call("onBWCheck", vec![]).unwrap();
        consume_results(&mut deserializer, results);
    }
    assert!(matches!(
        session.call("onBWCheck", vec![]),
        Err(ServerSessionError::LimitExceeded(_))
    ));
}

#[test]
fn refusal_echoes_a_bounded_part_of_the_name() {
    let (mut deserializer, mut serializer, mut session) = common_basic_setup();
    perform_connection("some_app", &mut session, &mut serializer, &mut deserializer);

    let name = "x".repeat(10_000);
    let (responses, _) = send_command(&name, 2.0, &mut session, &mut serializer, &mut deserializer);
    // The long command crosses the acknowledgement window, so an
    // `Acknowledgement` can precede the refusal.
    let refusal = responses
        .iter()
        .find(|(_, m)| is_command(m, "_error"))
        .expect("the call must be refused");
    let Some(Amf0Value::Utf8String(description)) = info_of(&refusal.1).get("description") else {
        panic!("the refusal needs a description: {responses:?}");
    };
    assert_eq!(
        description,
        &format!("Method not found ({}).", "x".repeat(64))
    );
}

#[test]
fn commands_that_expect_no_answer_get_none() {
    let (mut deserializer, mut serializer, mut session) = common_basic_setup();
    perform_connection("some_app", &mut session, &mut serializer, &mut deserializer);

    // Transaction 0 asks for no answer, and `_result`/`_error` from the
    // client are answers, not requests.
    for (name, transaction_id) in [
        ("releaseStream", 0.0),
        ("_checkbw", 0.0),
        ("_result", 9.0),
        ("_error", 9.0),
    ] {
        let (responses, events) = send_command(
            name,
            transaction_id,
            &mut session,
            &mut serializer,
            &mut deserializer,
        );
        assert!(
            responses.is_empty(),
            "{name} ({transaction_id}) must not be answered: {responses:?}"
        );
        assert_eq!(events.len(), 1, "{name} must still surface: {events:?}");
    }
}

#[test]
fn nothing_is_answered_before_connect() {
    let (mut deserializer, mut serializer, mut session) = common_basic_setup();

    for name in ["FCPublish", "_checkbw"] {
        let (responses, events) =
            send_command(name, 3.0, &mut session, &mut serializer, &mut deserializer);
        assert!(
            responses.is_empty(),
            "{name} is not answered before connect: {responses:?}"
        );
        assert_eq!(events.len(), 1, "{name} must still surface: {events:?}");
    }
}

#[test]
fn can_accept_connection_request() {
    let config = get_basic_config();
    let (mut deserializer, mut serializer, mut session) = common_setup(&config);

    let connect_payload = create_connect_message("some_app".to_string(), 15, 0, 0.0);
    let connect_packet = serializer.serialize(&connect_payload, true, false).unwrap();
    let connect_results = session.handle_input(&connect_packet.to_vec()[..]).unwrap();
    assert_eq!(
        connect_results.len(),
        1,
        "Unexpected number of responses when handling connect request message"
    );

    let (_, events) = split_results(&mut deserializer, connect_results);
    assert_eq!(events.len(), 1, "Unexpected number of events returned");
    let request_id = match events[0] {
        ServerSessionEvent::ConnectionRequested {
            ref app_name,
            request_id,
            ..
        } if app_name.as_ref() == "some_app" => request_id,
        _ => panic!("First event was not as expected: {:?}", events[0]),
    };

    let accept_results = session.accept_request(request_id).unwrap();
    assert_eq!(
        accept_results.len(),
        6,
        "Connect acceptance should include control messages and _result"
    );

    let (responses, _) = split_results(&mut deserializer, accept_results);
    // `onBWDone` follows the connect result, once the connection exists.
    assert_is_on_bw_done(&responses[responses.len() - 1].1);
    match responses[responses.len() - 2] {
        (
            _,
            RtmpMessage::Amf0Command {
                ref command_name,
                transaction_id: _,
                command_object: Amf0Value::Object(ref properties),
                ref additional_arguments,
            },
        ) if command_name == "_result" => {
            assert_eq!(
                properties.get("fmsVer"),
                Some(&Amf0Value::Utf8String(config.fms_version)),
                "Unexpected fms version"
            );
            assert_eq!(
                properties.get("capabilities"),
                Some(&Amf0Value::Number(31.0)),
                "Unexpected capabilities value"
            );
            assert_eq!(
                additional_arguments.len(),
                1,
                "Unexpected number of additional arguments"
            );
            match additional_arguments[0] {
                Amf0Value::Object(ref properties) => {
                    assert_eq!(
                        properties.get("level"),
                        Some(&Amf0Value::Utf8String("status".to_string())),
                        "Unexpected level value"
                    );
                    assert_eq!(
                        properties.get("code"),
                        Some(&Amf0Value::Utf8String(
                            "NetConnection.Connect.Success".to_string()
                        )),
                        "Unexpected code value"
                    );
                    assert_eq!(
                        properties.get("objectEncoding"),
                        Some(&Amf0Value::Number(0.0)),
                        "Unexpected object encoding value"
                    );
                    assert!(
                        properties.contains_key("description"),
                        "No description provided"
                    );
                }

                _ => panic!(
                    "Additional arguments was not an Amf0 object: {:?}",
                    additional_arguments[0]
                ),
            }
        }

        _ => panic!("Unexpected first response message: {:?}", responses[0]),
    }
}

#[test]
fn connect_request_strips_trailing_slash() {
    let (mut deserializer, mut serializer, mut session) = common_basic_setup();

    let connect_payload = create_connect_message("some_app/".to_string(), 15, 0, 0.0);
    let connect_packet = serializer.serialize(&connect_payload, true, false).unwrap();
    let connect_results = session.handle_input(&connect_packet.to_vec()[..]).unwrap();
    assert_eq!(
        connect_results.len(),
        1,
        "Unexpected number of responses when handling connect request message"
    );

    let (_, events) = split_results(&mut deserializer, connect_results);
    assert_eq!(events.len(), 1, "Unexpected number of events returned");
    match events[0] {
        ServerSessionEvent::ConnectionRequested { ref app_name, .. } => {
            assert_eq!(app_name.as_ref(), "some_app", "Unexpected app name")
        }
        _ => panic!("First event was not as expected: {:?}", events[0]),
    };
}

// A proxy must be able to read the publisher's Enhanced RTMP
// capability advertisement off the connect object. Upstream discarded every
// property except `app` and `objectEncoding`.
#[test]
fn connection_requested_event_carries_additional_connect_properties() {
    let config = get_basic_config();
    let (mut deserializer, mut serializer, mut session) = common_setup(&config);

    let mut extra = Amf0Object::new();
    extra.insert(
        "fourCcList".to_string(),
        Amf0Value::Utf8String("hvc1".to_string()),
    );
    extra.insert("capsEx".to_string(), Amf0Value::Number(3.0));

    let connect_payload =
        create_connect_message_with_properties("some_app".to_string(), 15, 0, 0.0, extra);
    let connect_packet = serializer.serialize(&connect_payload, true, false).unwrap();
    let connect_results = session.handle_input(&connect_packet.to_vec()[..]).unwrap();

    let (_, events) = split_results(&mut deserializer, connect_results);
    assert_eq!(events.len(), 1, "Unexpected number of events returned");

    match events[0] {
        ServerSessionEvent::ConnectionRequested {
            ref additional_properties,
            ..
        } => {
            assert_eq!(
                additional_properties.get("fourCcList"),
                Some(&Amf0Value::Utf8String("hvc1".to_string())),
                "fourCcList was not surfaced to the caller"
            );
            assert_eq!(
                additional_properties.get("capsEx"),
                Some(&Amf0Value::Number(3.0)),
                "capsEx was not surfaced to the caller"
            );
            // Fields the session consumes itself must not be duplicated here.
            assert!(!additional_properties.contains_key("app"));
            assert!(!additional_properties.contains_key("objectEncoding"));
        }
        _ => panic!("First event was not as expected: {:?}", events[0]),
    };
}

#[test]
fn accepted_connection_responds_with_same_object_encoding_value_as_connection_request() {
    let config = get_basic_config();
    let (mut deserializer, mut serializer, mut session) = common_setup(&config);

    let connect_payload = create_connect_message("some_app".to_string(), 15, 0, 3.0);
    let connect_packet = serializer.serialize(&connect_payload, true, false).unwrap();
    let connect_results = session.handle_input(&connect_packet.to_vec()[..]).unwrap();
    assert_eq!(
        connect_results.len(),
        1,
        "Unexpected number of responses when handling connect request message"
    );

    let (_, events) = split_results(&mut deserializer, connect_results);
    assert_eq!(events.len(), 1, "Unexpected number of events returned");
    let request_id = match events[0] {
        ServerSessionEvent::ConnectionRequested {
            ref app_name,
            request_id,
            ..
        } if app_name.as_ref() == "some_app" => request_id,
        _ => panic!("First event was not as expected: {:?}", events[0]),
    };

    let accept_results = session.accept_request(request_id).unwrap();
    assert_eq!(
        accept_results.len(),
        6,
        "Connect acceptance should include control messages and _result"
    );

    let (responses, _) = split_results(&mut deserializer, accept_results);
    // `onBWDone` follows the connect result, once the connection exists.
    assert_is_on_bw_done(&responses[responses.len() - 1].1);
    match responses[responses.len() - 2] {
        (
            _,
            RtmpMessage::Amf0Command {
                ref command_name,
                transaction_id: _,
                command_object: Amf0Value::Object(ref properties),
                ref additional_arguments,
            },
        ) if command_name == "_result" => {
            assert_eq!(
                properties.get("fmsVer"),
                Some(&Amf0Value::Utf8String(config.fms_version)),
                "Unexpected fms version"
            );
            assert_eq!(
                properties.get("capabilities"),
                Some(&Amf0Value::Number(31.0)),
                "Unexpected capabilities value"
            );
            assert_eq!(
                additional_arguments.len(),
                1,
                "Unexpected number of additional arguments"
            );
            match additional_arguments[0] {
                Amf0Value::Object(ref properties) => {
                    assert_eq!(
                        properties.get("level"),
                        Some(&Amf0Value::Utf8String("status".to_string())),
                        "Unexpected level value"
                    );
                    assert_eq!(
                        properties.get("code"),
                        Some(&Amf0Value::Utf8String(
                            "NetConnection.Connect.Success".to_string()
                        )),
                        "Unexpected code value"
                    );
                    assert_eq!(
                        properties.get("objectEncoding"),
                        Some(&Amf0Value::Number(3.0)),
                        "Unexpected object encoding value"
                    );
                    assert!(
                        properties.contains_key("description"),
                        "No description provided"
                    );
                }

                _ => panic!(
                    "Additional arguments was not an Amf0 object: {:?}",
                    additional_arguments[0]
                ),
            }
        }

        _ => panic!("Unexpected first response message: {:?}", responses[0]),
    }
}

#[test]
fn can_create_stream_on_connected_session() {
    let (mut deserializer, mut serializer, mut session) = common_basic_setup();
    perform_connection("some_app", &mut session, &mut serializer, &mut deserializer);

    let message = RtmpMessage::Amf0Command {
        command_name: "createStream".to_string(),
        transaction_id: 4.0,
        command_object: Amf0Value::Null,
        additional_arguments: Vec::new(),
    };

    let payload = message.into_raw_message(RtmpTimestamp::new(0), 0).unwrap();
    let packet = serializer.serialize(&payload, true, false).unwrap();
    let results = session.handle_input(&packet.to_vec()[..]).unwrap();
    let (responses, _) = split_results(&mut deserializer, results);

    assert_eq!(
        responses.len(),
        1,
        "Unexpected number of responses returned"
    );
    match responses[0] {
        (
            ref payload,
            RtmpMessage::Amf0Command {
                ref command_name,
                transaction_id,
                command_object: Amf0Value::Null,
                ref additional_arguments,
            },
        ) if command_name == "_result" && transaction_id == 4.0 => {
            assert_eq!(
                additional_arguments.len(),
                1,
                "Unexpected number of additional arguments in response"
            );
            assert_vec_match!(additional_arguments, Amf0Value::Number(x) if x > 0.0);
            assert_eq!(payload.message_stream_id, 0, "Unexpected message stream id");
        }

        _ => panic!(
            "First response was not the expected value: {:?}",
            responses[0]
        ),
    }
}

#[test]
fn can_accept_live_publishing_to_requested_stream_key() {
    let (mut deserializer, mut serializer, mut session) = common_basic_setup();
    perform_connection("some_app", &mut session, &mut serializer, &mut deserializer);

    let stream_id = create_active_stream(&mut session, &mut serializer, &mut deserializer);
    let message = RtmpMessage::Amf0Command {
        command_name: "publish".to_string(),
        transaction_id: 5.0,
        command_object: Amf0Value::Null,
        additional_arguments: vec![
            Amf0Value::Utf8String("stream_key".to_string()),
            Amf0Value::Utf8String("live".to_string()),
        ],
    };

    let publish_payload = message
        .into_raw_message(RtmpTimestamp::new(0), stream_id)
        .unwrap();
    let publish_packet = serializer
        .serialize(&publish_payload, false, false)
        .unwrap();
    let publish_results = session.handle_input(&publish_packet.to_vec()[..]).unwrap();
    let (_, events) = split_results(&mut deserializer, publish_results);

    assert_eq!(events.len(), 1, "Unexpected number of events returned");
    let request_id = match events[0] {
        ServerSessionEvent::PublishStreamRequested {
            ref app_name,
            ref stream_key,
            request_id: returned_request_id,
            mode: PublishMode::Live,
            ..
        } if app_name.as_ref() == "some_app" && stream_key.as_ref() == "stream_key" => {
            returned_request_id
        }

        _ => panic!("Unexpected first event found: {:?}", events[0]),
    };

    let accept_results = session.accept_request(request_id).unwrap();
    let (mut responses, _) = split_results(&mut deserializer, accept_results);
    assert_eq!(
        responses.len(),
        2,
        "Unexpected number of responses received"
    );

    match responses.remove(0) {
        (
            raw,
            RtmpMessage::UserControl {
                event_type: UserControlEventType::StreamBegin,
                stream_id: Some(received_stream_id),
                buffer_length: None,
                timestamp: None,
            },
        ) => {
            assert_eq!(
                received_stream_id, stream_id,
                "Stream begin did not contain the expected stream id"
            );
            assert_eq!(
                raw.message_stream_id, 0,
                "User Control messages travel on message stream 0"
            );
        }

        x => panic!(
            "Expected stream begin for stream id {:?} but instead received: {:?}",
            stream_id, x
        ),
    }

    verify_is_onstatus(&responses.remove(0).1, "status", "NetStream.Publish.Start");
}

#[test]
fn can_receive_and_raise_event_for_metadata_from_obs() {
    let (mut deserializer, mut serializer, mut session) = common_basic_setup();
    perform_connection(
        TEST_APP_NAME,
        &mut session,
        &mut serializer,
        &mut deserializer,
    );
    let stream_id = create_active_stream(&mut session, &mut serializer, &mut deserializer);
    start_publishing(
        TEST_STREAM_KEY,
        stream_id,
        &mut session,
        &mut serializer,
        &mut deserializer,
    );

    let mut properties = Amf0Object::new();
    properties.insert("width".to_string(), Amf0Value::Number(1920_f64));
    properties.insert("height".to_string(), Amf0Value::Number(1080_f64));
    properties.insert("videocodecid".to_string(), Amf0Value::Number(10.0));
    properties.insert("videodatarate".to_string(), Amf0Value::Number(1200_f64));
    properties.insert("framerate".to_string(), Amf0Value::Number(30_f64));
    properties.insert("audiocodecid".to_string(), Amf0Value::Number(7.0));
    properties.insert("audiodatarate".to_string(), Amf0Value::Number(96_f64));
    properties.insert("audiosamplerate".to_string(), Amf0Value::Number(48000_f64));
    properties.insert("audiosamplesize".to_string(), Amf0Value::Number(16_f64));
    properties.insert("audiochannels".to_string(), Amf0Value::Number(2_f64));
    properties.insert("stereo".to_string(), Amf0Value::Boolean(true));
    properties.insert(
        "encoder".to_string(),
        Amf0Value::Utf8String("Test Encoder".to_string()),
    );
    // A key this crate does not model, to prove `raw_metadata`
    // preserves what the typed `StreamMetadata` view drops.
    properties.insert(
        "unknownVendorKey".to_string(),
        Amf0Value::Utf8String("keep-me".to_string()),
    );

    let message = RtmpMessage::Amf0Data {
        values: vec![
            Amf0Value::Utf8String("@setDataFrame".to_string()),
            Amf0Value::Utf8String("onMetaData".to_string()),
            Amf0Value::Object(properties),
        ],
    };

    let metadata_payload = message
        .into_raw_message(RtmpTimestamp::new(0), stream_id)
        .unwrap();
    let metadata_packet = serializer
        .serialize(&metadata_payload, false, false)
        .unwrap();
    let metadata_results = session.handle_input(&metadata_packet.to_vec()[..]).unwrap();
    let (_, mut events) = split_results(&mut deserializer, metadata_results);

    assert_eq!(events.len(), 1, "Unexpected number of metadata events");

    match events.remove(0) {
        ServerSessionEvent::StreamDataReceived {
            message,
            app_name,
            stream_key,
            ..
        } => {
            let metadata = crate::api::sessions::metadata(&message);
            let raw_metadata = message
                .metadata()
                .unwrap()
                .unwrap()
                .into_iter()
                .collect::<Vec<_>>();
            assert_eq!(
                app_name.as_ref(),
                TEST_APP_NAME,
                "Unexpected metadata app name"
            );
            assert_eq!(
                stream_key.as_ref(),
                TEST_STREAM_KEY,
                "Unexpected metadata stream key"
            );
            // The unparsed properties must survive intact, including
            // keys the typed view has no field for.
            let raw: std::collections::HashMap<_, _> = raw_metadata.into_iter().collect();
            assert_eq!(
                raw.get("unknownVendorKey"),
                Some(&Amf0Value::Utf8String("keep-me".to_string())),
                "raw metadata dropped an unmodelled key"
            );
            assert_eq!(
                raw.get("width"),
                Some(&Amf0Value::Number(1920_f64)),
                "raw metadata dropped a known key"
            );
            assert_eq!(metadata.video_width, Some(1920), "Unexpected video width");
            assert_eq!(metadata.video_height, Some(1080), "Unexepcted video height");
            assert_eq!(metadata.video_codec_id, Some(10), "Unexepcted video codec");
            assert_eq!(
                metadata.video_frame_rate,
                Some(30_f32),
                "Unexpected framerate"
            );
            assert_eq!(
                metadata.video_bitrate_kbps,
                Some(1200),
                "Unexpected video bitrate"
            );
            assert_eq!(metadata.audio_codec_id, Some(7), "Unexpected audio codec");
            assert_eq!(
                metadata.audio_bitrate_kbps,
                Some(96),
                "Unexpected audio bitrate"
            );
            assert_eq!(
                metadata.audio_sample_rate,
                Some(48000),
                "Unexpected audio sample rate"
            );
            assert_eq!(
                metadata.audio_channels,
                Some(2),
                "Unexpected audio channels"
            );
            assert_eq!(
                metadata.audio_is_stereo,
                Some(true),
                "Unexpected audio is stereo value"
            );
            assert_eq!(
                metadata.encoder,
                Some("Test Encoder".to_string()),
                "Unexpected encoder value"
            );
        }

        _ => panic!("Unexpected event received: {:?}", events[0]),
    }
}

#[test]
fn can_receive_audio_data_on_published_stream() {
    let (mut deserializer, mut serializer, mut session) = common_basic_setup();
    perform_connection(
        TEST_APP_NAME,
        &mut session,
        &mut serializer,
        &mut deserializer,
    );
    let stream_id = create_active_stream(&mut session, &mut serializer, &mut deserializer);
    start_publishing(
        TEST_STREAM_KEY,
        stream_id,
        &mut session,
        &mut serializer,
        &mut deserializer,
    );

    let message = RtmpMessage::AudioData {
        data: Bytes::from(vec![1_u8, 2_u8, 3_u8]),
    };
    let payload = message
        .into_raw_message(RtmpTimestamp::new(1234), stream_id)
        .unwrap();
    let packet = serializer.serialize(&payload, false, false).unwrap();
    let results = session.handle_input(&packet.to_vec()[..]).unwrap();
    let (_, mut events) = split_results(&mut deserializer, results);

    assert_eq!(events.len(), 1, "Unexpected number of events returned");

    match events.remove(0) {
        ServerSessionEvent::AudioDataReceived {
            app_name,
            stream_key,
            data,
            timestamp,
            ..
        } => {
            assert_eq!(app_name.as_ref(), TEST_APP_NAME, "Unexpected app name");
            assert_eq!(
                stream_key.as_ref(),
                TEST_STREAM_KEY,
                "Unexpected stream key"
            );
            assert_eq!(timestamp, RtmpTimestamp::new(1234), "Unexepcted timestamp");
            assert_eq!(&data[..], &[1_u8, 2_u8, 3_u8], "Unexpected data");
        }

        event => panic!("Expected AudioDataReceived event, instead got: {:?}", event),
    }
}

#[test]
fn can_receive_video_data_on_published_stream() {
    let (mut deserializer, mut serializer, mut session) = common_basic_setup();
    perform_connection(
        TEST_APP_NAME,
        &mut session,
        &mut serializer,
        &mut deserializer,
    );
    let stream_id = create_active_stream(&mut session, &mut serializer, &mut deserializer);
    start_publishing(
        TEST_STREAM_KEY,
        stream_id,
        &mut session,
        &mut serializer,
        &mut deserializer,
    );

    let message = RtmpMessage::VideoData {
        data: Bytes::from(vec![1_u8, 2_u8, 3_u8]),
    };
    let payload = message
        .into_raw_message(RtmpTimestamp::new(1234), stream_id)
        .unwrap();
    let packet = serializer.serialize(&payload, false, false).unwrap();
    let results = session.handle_input(&packet.to_vec()[..]).unwrap();
    let (_, mut events) = split_results(&mut deserializer, results);

    assert_eq!(events.len(), 1, "Unexpected number of events returned");

    match events.remove(0) {
        ServerSessionEvent::VideoDataReceived {
            app_name,
            stream_key,
            data,
            timestamp,
            ..
        } => {
            assert_eq!(app_name.as_ref(), TEST_APP_NAME, "Unexpected app name");
            assert_eq!(
                stream_key.as_ref(),
                TEST_STREAM_KEY,
                "Unexpected stream key"
            );
            assert_eq!(timestamp, RtmpTimestamp::new(1234), "Unexpected timestamp");
            assert_eq!(&data[..], &[1_u8, 2_u8, 3_u8], "Unexpected data");
        }

        event => panic!("Expected AudioDataReceived event, instead got: {:?}", event),
    }
}

#[test]
fn publish_finished_event_raised_when_delete_stream_invoked_on_publishing_stream() {
    let (mut deserializer, mut serializer, mut session) = common_basic_setup();
    perform_connection(
        TEST_APP_NAME,
        &mut session,
        &mut serializer,
        &mut deserializer,
    );
    let stream_id = create_active_stream(&mut session, &mut serializer, &mut deserializer);
    start_publishing(
        TEST_STREAM_KEY,
        stream_id,
        &mut session,
        &mut serializer,
        &mut deserializer,
    );

    let message = RtmpMessage::Amf0Command {
        command_name: "deleteStream".to_string(),
        transaction_id: 4_f64,
        command_object: Amf0Value::Null,
        additional_arguments: vec![Amf0Value::Number(stream_id as f64)],
    };

    let payload = message
        .into_raw_message(RtmpTimestamp::new(1234), stream_id)
        .unwrap();
    let packet = serializer.serialize(&payload, false, false).unwrap();
    let results = session.handle_input(&packet.to_vec()[..]).unwrap();
    let (_, mut events) = split_results(&mut deserializer, results);

    assert_eq!(events.len(), 1, "Unexpected number of events returned");

    match events.remove(0) {
        ServerSessionEvent::PublishStreamFinished {
            app_name,
            stream_key,
            ..
        } => {
            assert_eq!(app_name.as_ref(), TEST_APP_NAME, "Unexpected app name");
            assert_eq!(
                stream_key.as_ref(),
                TEST_STREAM_KEY,
                "Unexpected stream key"
            );
        }

        event => panic!(
            "Expected PublishStreamFinished event, instead got: {:?}",
            event
        ),
    }
}

#[test]
fn gstreamer_string_delete_stream_id_finishes_publishing() {
    let (mut deserializer, mut serializer, mut session) = common_basic_setup();
    perform_connection(
        TEST_APP_NAME,
        &mut session,
        &mut serializer,
        &mut deserializer,
    );
    let stream_id = create_active_stream(&mut session, &mut serializer, &mut deserializer);
    start_publishing(
        TEST_STREAM_KEY,
        stream_id,
        &mut session,
        &mut serializer,
        &mut deserializer,
    );

    let message = RtmpMessage::Amf0Command {
        command_name: "deleteStream".to_string(),
        transaction_id: 4_f64,
        command_object: Amf0Value::Null,
        additional_arguments: vec![Amf0Value::Utf8String(stream_id.to_string())],
    };
    let payload = message
        .into_raw_message(RtmpTimestamp::new(1234), stream_id)
        .unwrap();
    let packet = serializer.serialize(&payload, false, false).unwrap();
    let results = session.handle_input(&packet.to_vec()[..]).unwrap();
    let (_, events) = split_results(&mut deserializer, results);

    assert!(matches!(
        events.as_slice(),
        [ServerSessionEvent::PublishStreamFinished { app_name, stream_key, .. }]
            if app_name.as_ref() == TEST_APP_NAME && stream_key.as_ref() == TEST_STREAM_KEY
    ));
}

#[test]
fn publish_finished_event_raised_when_close_stream_invoked_on_publishing_stream() {
    let (mut deserializer, mut serializer, mut session) = common_basic_setup();
    perform_connection(
        TEST_APP_NAME,
        &mut session,
        &mut serializer,
        &mut deserializer,
    );
    let stream_id = create_active_stream(&mut session, &mut serializer, &mut deserializer);
    start_publishing(
        TEST_STREAM_KEY,
        stream_id,
        &mut session,
        &mut serializer,
        &mut deserializer,
    );

    let message = RtmpMessage::Amf0Command {
        command_name: "closeStream".to_string(),
        transaction_id: 4_f64,
        command_object: Amf0Value::Null,
        additional_arguments: vec![Amf0Value::Number(stream_id as f64)],
    };

    let payload = message
        .into_raw_message(RtmpTimestamp::new(1234), stream_id)
        .unwrap();
    let packet = serializer.serialize(&payload, false, false).unwrap();
    let results = session.handle_input(&packet.to_vec()[..]).unwrap();
    let (_, mut events) = split_results(&mut deserializer, results);

    assert_eq!(events.len(), 1, "Unexpected number of events returned");

    match events.remove(0) {
        ServerSessionEvent::PublishStreamFinished {
            app_name,
            stream_key,
            ..
        } => {
            assert_eq!(app_name.as_ref(), TEST_APP_NAME, "Unexpected app name");
            assert_eq!(
                stream_key.as_ref(),
                TEST_STREAM_KEY,
                "Unexpected stream key"
            );
        }

        event => panic!(
            "Expected PublishStreamFinished event, instead got: {:?}",
            event
        ),
    }
}

#[test]
fn can_request_publishing_on_closed_stream() {
    let (mut deserializer, mut serializer, mut session) = common_basic_setup();
    perform_connection(
        TEST_APP_NAME,
        &mut session,
        &mut serializer,
        &mut deserializer,
    );
    let stream_id = create_active_stream(&mut session, &mut serializer, &mut deserializer);
    start_publishing(
        TEST_STREAM_KEY,
        stream_id,
        &mut session,
        &mut serializer,
        &mut deserializer,
    );
    close_stream(stream_id, &mut session, &mut serializer, &mut deserializer);

    let message = RtmpMessage::Amf0Command {
        command_name: "publish".to_string(),
        transaction_id: 5.0,
        command_object: Amf0Value::Null,
        additional_arguments: vec![
            Amf0Value::Utf8String(TEST_STREAM_KEY.to_string()),
            Amf0Value::Utf8String("live".to_string()),
        ],
    };

    let publish_payload = message
        .into_raw_message(RtmpTimestamp::new(2000), stream_id)
        .unwrap();
    let publish_packet = serializer
        .serialize(&publish_payload, false, false)
        .unwrap();
    let publish_results = session.handle_input(&publish_packet.to_vec()[..]).unwrap();
    let (_, events) = split_results(&mut deserializer, publish_results);

    assert_eq!(events.len(), 1, "Unexpected number of events returned");
    match events[0] {
        ServerSessionEvent::PublishStreamRequested {
            ref app_name,
            ref stream_key,
            request_id: _,
            mode: PublishMode::Live,
            ..
        } => {
            assert_eq!(app_name.as_ref(), TEST_APP_NAME, "Unexpected app name");
            assert_eq!(
                stream_key.as_ref(),
                TEST_STREAM_KEY,
                "Unexpected stream key"
            );
        }

        _ => panic!("Unexpected first event found: {:?}", events[0]),
    }
}

#[test]
fn can_accept_play_command_with_no_optional_parameters_to_requested_stream_key() {
    let (mut deserializer, mut serializer, mut session) = common_basic_setup();
    perform_connection(
        TEST_APP_NAME,
        &mut session,
        &mut serializer,
        &mut deserializer,
    );
    let stream_id = create_active_stream(&mut session, &mut serializer, &mut deserializer);

    let message = RtmpMessage::Amf0Command {
        command_name: "play".to_string(),
        transaction_id: 4.0,
        command_object: Amf0Value::Null,
        additional_arguments: vec![Amf0Value::Utf8String(TEST_STREAM_KEY.to_string())],
    };

    let play_payload = message
        .into_raw_message(RtmpTimestamp::new(0), stream_id)
        .unwrap();
    let play_packet = serializer.serialize(&play_payload, false, false).unwrap();
    let play_results = session.handle_input(&play_packet.to_vec()[..]).unwrap();
    let (_, mut events) = split_results(&mut deserializer, play_results);

    assert_eq!(events.len(), 1, "Unexpected number of events returned");
    let request_id = match events.remove(0) {
        ServerSessionEvent::PlayStreamRequested {
            app_name,
            stream_key,
            start_at,
            duration,
            reset,
            request_id,
            stream_id: sid,
            ..
        } => {
            assert_eq!(app_name.as_ref(), TEST_APP_NAME, "Unexpected app name");
            assert_eq!(
                stream_key.as_ref(),
                TEST_STREAM_KEY,
                "Unexpected stream key"
            );
            assert_eq!(
                start_at,
                PlayStartValue::LiveOrRecorded,
                "Unexpected start at"
            );
            assert_eq!(duration, None, "Unexpected duration");
            assert_eq!(reset, false, "Unexpected reset value");
            assert_eq!(sid.get(), stream_id, "Unexpected stream id");
            request_id
        }

        x => panic!("Expected play event but instead received: {:?}", x),
    };

    let accept_results = session.accept_request(request_id).unwrap();
    let (mut responses, _) = split_results(&mut deserializer, accept_results);
    assert_eq!(responses.len(), 5, "Unexpected number of messages received");

    let (raw, reset) = responses.remove(0);
    // The reset is serialized first because it is sent first; out of order,
    // its compressed header would inherit the wrong message stream.
    assert_eq!(
        raw.message_stream_id, stream_id,
        "the reset belongs to the stream"
    );
    verify_is_onstatus(&reset, "status", "NetStream.Play.Reset");

    match responses.remove(0) {
        (
            raw,
            RtmpMessage::UserControl {
                event_type,
                stream_id: sid,
                buffer_length,
                timestamp,
            },
        ) => {
            assert_eq!(
                event_type,
                UserControlEventType::StreamBegin,
                "Unexpected user control event type received"
            );
            assert_eq!(sid, Some(stream_id), "Unexpected user control stream id");
            assert_eq!(buffer_length, None, "Unexpected user control buffer length");
            assert_eq!(timestamp, None, "Unexpected user control timestamp");
            assert_eq!(
                raw.message_stream_id, 0,
                "User Control messages travel on message stream 0"
            );
        }

        x => panic!("Expected stream begin message, instead received: {:?}", x),
    }

    verify_is_onstatus(&responses.remove(0).1, "status", "NetStream.Play.Start");

    match responses.remove(0) {
        (_, RtmpMessage::Amf0Data { values }) => {
            assert_eq!(values.len(), 3, "Unexpected number of values received");
            assert_eq!(
                values[0],
                Amf0Value::Utf8String("|RtmpSampleAccess".to_string()),
                "Incorrect first data argument"
            );
            assert_eq!(
                values[1],
                Amf0Value::Boolean(false),
                "Incorrect second data argument"
            );
            assert_eq!(
                values[2],
                Amf0Value::Boolean(false),
                "Incorrect third data argument"
            );
        }

        x => println!(
            "Expected RtmpSampleAccess data argument, instead received: {:?}",
            x
        ),
    }

    match responses.remove(0) {
        (_, RtmpMessage::Amf0Data { mut values }) => {
            assert_eq!(values.len(), 2, "Unexpected number of values received");
            assert_eq!(
                values[0],
                Amf0Value::Utf8String("onStatus".to_string()),
                "Unexpected first data argument"
            );
            match values.remove(1) {
                Amf0Value::Object(ref properties) => {
                    assert_eq!(
                        properties.get("code"),
                        Some(&Amf0Value::Utf8String("NetStream.Data.Start".to_string())),
                        "Unexpected code value"
                    );
                }

                x => panic!(
                    "Expected object for 2nd data argument, instead received: {:?}",
                    x
                ),
            }
        }

        x => panic!("Expected onStatus data argument, instead received: {:?}", x),
    }
}

#[test]
fn can_accept_play_command_with_all_optional_parameters_to_requested_stream_key() {
    let (mut deserializer, mut serializer, mut session) = common_basic_setup();
    perform_connection(
        TEST_APP_NAME,
        &mut session,
        &mut serializer,
        &mut deserializer,
    );
    let stream_id = create_active_stream(&mut session, &mut serializer, &mut deserializer);

    let message = RtmpMessage::Amf0Command {
        command_name: "play".to_string(),
        transaction_id: 4.0,
        command_object: Amf0Value::Null,
        additional_arguments: vec![
            Amf0Value::Utf8String(TEST_STREAM_KEY.to_string()),
            Amf0Value::Number(5.0),   // Start argument
            Amf0Value::Number(25.0),  // Duration,
            Amf0Value::Boolean(true), // reset
        ],
    };

    let play_payload = message
        .into_raw_message(RtmpTimestamp::new(0), stream_id)
        .unwrap();
    let play_packet = serializer.serialize(&play_payload, false, false).unwrap();
    let play_results = session.handle_input(&play_packet.to_vec()[..]).unwrap();
    let (_, mut events) = split_results(&mut deserializer, play_results);

    assert_eq!(events.len(), 1, "Unexpected number of events returned");
    let request_id = match events.remove(0) {
        ServerSessionEvent::PlayStreamRequested {
            app_name,
            stream_key,
            start_at,
            duration,
            reset,
            request_id,
            stream_id: sid,
            ..
        } => {
            assert_eq!(app_name.as_ref(), TEST_APP_NAME, "Unexpected app name");
            assert_eq!(
                stream_key.as_ref(),
                TEST_STREAM_KEY,
                "Unexpected stream key"
            );
            assert_eq!(
                start_at,
                PlayStartValue::StartTimeInSeconds(5),
                "Unexpected start at"
            );
            assert_eq!(duration, Some(25), "Unexpected duration");
            assert_eq!(reset, true, "Unexpected reset value");
            assert_eq!(sid.get(), stream_id, "Unexpected stream id");
            request_id
        }

        x => panic!("Expected play event but instead received: {:?}", x),
    };

    let accept_results = session.accept_request(request_id).unwrap();
    consume_results(&mut deserializer, accept_results);
}

#[test]
fn play_finished_event_when_close_stream_invoked() {
    let (mut deserializer, mut serializer, mut session) = common_basic_setup();
    perform_connection(
        TEST_APP_NAME,
        &mut session,
        &mut serializer,
        &mut deserializer,
    );
    let stream_id = create_active_stream(&mut session, &mut serializer, &mut deserializer);

    start_playing(
        TEST_STREAM_KEY,
        stream_id,
        &mut session,
        &mut serializer,
        &mut deserializer,
    );

    let message = RtmpMessage::Amf0Command {
        command_name: "closeStream".to_string(),
        transaction_id: 4_f64,
        command_object: Amf0Value::Null,
        additional_arguments: vec![Amf0Value::Number(stream_id as f64)],
    };

    let payload = message
        .into_raw_message(RtmpTimestamp::new(1234), stream_id)
        .unwrap();
    let packet = serializer.serialize(&payload, false, false).unwrap();
    let results = session.handle_input(&packet.to_vec()[..]).unwrap();
    let (_, mut events) = split_results(&mut deserializer, results);

    assert_eq!(events.len(), 1, "Unexpected number of events returned");

    match events.remove(0) {
        ServerSessionEvent::PlayStreamFinished {
            app_name,
            stream_key,
            ..
        } => {
            assert_eq!(app_name.as_ref(), TEST_APP_NAME, "Unexpected app name");
            assert_eq!(
                stream_key.as_ref(),
                TEST_STREAM_KEY,
                "Unexpected stream key"
            );
        }

        event => panic!(
            "Expected PublishStreamFinished event, instead got: {:?}",
            event
        ),
    }
}

#[test]
fn play_finished_event_when_delete_stream_invoked_on_playing_stream() {
    let (mut deserializer, mut serializer, mut session) = common_basic_setup();
    perform_connection(
        TEST_APP_NAME,
        &mut session,
        &mut serializer,
        &mut deserializer,
    );
    let stream_id = create_active_stream(&mut session, &mut serializer, &mut deserializer);

    start_playing(
        TEST_STREAM_KEY,
        stream_id,
        &mut session,
        &mut serializer,
        &mut deserializer,
    );

    let message = RtmpMessage::Amf0Command {
        command_name: "deleteStream".to_string(),
        transaction_id: 4_f64,
        command_object: Amf0Value::Null,
        additional_arguments: vec![Amf0Value::Number(stream_id as f64)],
    };

    let payload = message
        .into_raw_message(RtmpTimestamp::new(1234), stream_id)
        .unwrap();
    let packet = serializer.serialize(&payload, false, false).unwrap();
    let results = session.handle_input(&packet.to_vec()[..]).unwrap();
    let (_, mut events) = split_results(&mut deserializer, results);

    assert_eq!(events.len(), 1, "Unexpected number of events returned");

    match events.remove(0) {
        ServerSessionEvent::PlayStreamFinished {
            app_name,
            stream_key,
            ..
        } => {
            assert_eq!(app_name.as_ref(), TEST_APP_NAME, "Unexpected app name");
            assert_eq!(
                stream_key.as_ref(),
                TEST_STREAM_KEY,
                "Unexpected stream key"
            );
        }

        event => panic!(
            "Expected PublishStreamFinished event, instead got: {:?}",
            event
        ),
    }
}

#[test]
fn can_send_metadata_to_playing_stream() {
    let (mut deserializer, mut serializer, mut session) = common_basic_setup();
    perform_connection(
        TEST_APP_NAME,
        &mut session,
        &mut serializer,
        &mut deserializer,
    );
    let stream_id = create_active_stream(&mut session, &mut serializer, &mut deserializer);
    start_playing(
        TEST_STREAM_KEY,
        stream_id,
        &mut session,
        &mut serializer,
        &mut deserializer,
    );

    let metadata = StreamMetadata {
        audio_bitrate_kbps: Some(100),
        audio_channels: Some(101),
        audio_codec_id: Some(7),
        audio_is_stereo: Some(true),
        audio_sample_rate: Some(103),
        encoder: Some("104".to_string()),
        video_bitrate_kbps: Some(105),
        video_codec_id: Some(10),
        video_frame_rate: Some(107.0),
        video_height: Some(108),
        video_width: Some(109),
    };

    let packet = session
        .send_metadata(crate::sessions::StreamId::new(stream_id), &metadata)
        .unwrap();
    let payload = deserializer
        .get_next_message(&packet.to_vec()[..])
        .unwrap()
        .unwrap();
    let message = payload.to_rtmp_message().unwrap();

    match message {
        RtmpMessage::Amf0Data { mut values } => {
            assert_eq!(values.len(), 2, "2 amf0 data values expected");

            match values.remove(0) {
                Amf0Value::Utf8String(string) => {
                    assert_eq!(string, "onMetaData");
                }
                x => panic!("Expected 'onMetaData' received: {:?}", x),
            }

            match values.remove(0) {
                Amf0Value::Object(properties) => {
                    assert_eq!(
                        properties.get("width"),
                        Some(&Amf0Value::Number(109.0)),
                        "Unexpected width"
                    );
                    assert_eq!(
                        properties.get("height"),
                        Some(&Amf0Value::Number(108.0)),
                        "Unexpected height"
                    );
                    assert_eq!(
                        properties.get("videocodecid"),
                        Some(&Amf0Value::Number(10.0)),
                        "Unexpected videocodecid"
                    );
                    assert_eq!(
                        properties.get("videodatarate"),
                        Some(&Amf0Value::Number(105.0)),
                        "Unexpected videodatarate"
                    );
                    assert_eq!(
                        properties.get("framerate"),
                        Some(&Amf0Value::Number(107.0)),
                        "Unexpected framerate"
                    );
                    assert_eq!(
                        properties.get("audiocodecid"),
                        Some(&Amf0Value::Number(7.0)),
                        "Unexpected audiocodecid"
                    );
                    assert_eq!(
                        properties.get("audiodatarate"),
                        Some(&Amf0Value::Number(100.0)),
                        "Unexpected audiodatarate"
                    );
                    assert_eq!(
                        properties.get("audiosamplerate"),
                        Some(&Amf0Value::Number(103.0)),
                        "Unexpected audiosamplerate"
                    );
                    assert_eq!(
                        properties.get("audiochannels"),
                        Some(&Amf0Value::Number(101.0)),
                        "Unexpected audiochannels"
                    );
                    assert_eq!(
                        properties.get("stereo"),
                        Some(&Amf0Value::Boolean(true)),
                        "Unexpected stereo"
                    );
                    assert_eq!(
                        properties.get("encoder"),
                        Some(&Amf0Value::Utf8String("104".to_string())),
                        "Unexpected encoder"
                    );
                }

                x => panic!(
                    "Expected Amf0 object with metadata, instead received: {:?}",
                    x
                ),
            }
        }

        x => panic!("Expected Amf0 data, instead received: {:?}", x),
    }
}

#[test]
fn can_send_video_data_to_playing_stream() {
    let (mut deserializer, mut serializer, mut session) = common_basic_setup();
    perform_connection(
        TEST_APP_NAME,
        &mut session,
        &mut serializer,
        &mut deserializer,
    );
    let stream_id = create_active_stream(&mut session, &mut serializer, &mut deserializer);
    start_playing(
        TEST_STREAM_KEY,
        stream_id,
        &mut session,
        &mut serializer,
        &mut deserializer,
    );

    let original_data = Bytes::from(vec![1_u8, 2_u8, 3_u8]);
    let timestamp = RtmpTimestamp::new(500);
    let packet = session
        .send_video_data(
            crate::sessions::StreamId::new(stream_id),
            original_data.clone(),
            timestamp.clone(),
            false,
        )
        .unwrap();
    let payload = deserializer
        .get_next_message(&packet.to_vec()[..])
        .unwrap()
        .unwrap();
    let message = payload.to_rtmp_message().unwrap();

    match message {
        RtmpMessage::VideoData { data: message_data } => {
            assert_eq!(
                payload.timestamp, timestamp,
                "Serialized timestamp did not match original timestamp"
            );
            assert_eq!(
                &message_data[..],
                &original_data[..],
                "Packetized data did not match original data"
            );
        }

        x => panic!("Expected video data message, received: {:?}", x),
    }
}

#[test]
fn can_send_audio_data_to_playing_stream() {
    let (mut deserializer, mut serializer, mut session) = common_basic_setup();
    perform_connection(
        TEST_APP_NAME,
        &mut session,
        &mut serializer,
        &mut deserializer,
    );
    let stream_id = create_active_stream(&mut session, &mut serializer, &mut deserializer);
    start_playing(
        TEST_STREAM_KEY,
        stream_id,
        &mut session,
        &mut serializer,
        &mut deserializer,
    );

    let original_data = Bytes::from(vec![1_u8, 2_u8, 3_u8]);
    let timestamp = RtmpTimestamp::new(500);
    let packet = session
        .send_audio_data(
            crate::sessions::StreamId::new(stream_id),
            original_data.clone(),
            timestamp.clone(),
            false,
        )
        .unwrap();
    let payload = deserializer
        .get_next_message(&packet.to_vec()[..])
        .unwrap()
        .unwrap();
    let message = payload.to_rtmp_message().unwrap();

    match message {
        RtmpMessage::AudioData { data: message_data } => {
            assert_eq!(
                payload.timestamp, timestamp,
                "Serialized timestamp did not match original timestamp"
            );
            assert_eq!(
                &message_data[..],
                &original_data[..],
                "Packetized data did not match original data"
            );
        }

        x => panic!("Expected video data message, received: {:?}", x),
    }
}

#[test]
fn automatically_responds_to_ping_requests() {
    let (mut deserializer, mut serializer, mut session) = common_basic_setup();
    perform_connection(
        TEST_APP_NAME,
        &mut session,
        &mut serializer,
        &mut deserializer,
    );

    let message = RtmpMessage::UserControl {
        event_type: UserControlEventType::PingRequest,
        timestamp: Some(RtmpTimestamp::new(5230)),
        stream_id: None,
        buffer_length: None,
    };

    let payload = message
        .into_raw_message(RtmpTimestamp::new(6000), 0)
        .unwrap();
    let packet = serializer.serialize(&payload, false, false).unwrap();
    let results = session.handle_input(&packet.to_vec()[..]).unwrap();
    let (mut responses, _) = split_results(&mut deserializer, results);

    assert_eq!(
        responses.len(),
        1,
        "Expected one response for handling ping request"
    );
    match responses.remove(0) {
        (
            _,
            RtmpMessage::UserControl {
                event_type,
                timestamp: Some(timestamp),
                stream_id: None,
                buffer_length: None,
            },
        ) => {
            assert_eq!(
                event_type,
                UserControlEventType::PingResponse,
                "Unexpected event type"
            );
            assert_eq!(timestamp, RtmpTimestamp::new(5230), "Unexpected timestamp");
        }

        x => panic!("Expected PingResponse, found {:?}", x),
    }
}

#[test]
fn event_raised_when_ping_response_received() {
    let (mut deserializer, mut serializer, mut session) = common_basic_setup();
    perform_connection(
        TEST_APP_NAME,
        &mut session,
        &mut serializer,
        &mut deserializer,
    );

    let message = RtmpMessage::UserControl {
        event_type: UserControlEventType::PingResponse,
        timestamp: Some(RtmpTimestamp::new(5230)),
        stream_id: None,
        buffer_length: None,
    };

    let payload = message
        .into_raw_message(RtmpTimestamp::new(6000), 0)
        .unwrap();
    let packet = serializer.serialize(&payload, false, false).unwrap();
    let results = session.handle_input(&packet.to_vec()[..]).unwrap();
    let (_, mut events) = split_results(&mut deserializer, results);

    assert_eq!(events.len(), 1, "One event expected");
    match events.remove(0) {
        ServerSessionEvent::PingResponseReceived { timestamp, .. } => {
            assert_eq!(
                timestamp,
                RtmpTimestamp::new(5230),
                "Unexpected timestamp received"
            );
        }

        x => panic!("Expected PingResponse event, instead received {:?}", x),
    }
}

#[test]
fn can_send_ping_request() {
    let (mut deserializer, mut serializer, mut session) = common_basic_setup();
    perform_connection(
        TEST_APP_NAME,
        &mut session,
        &mut serializer,
        &mut deserializer,
    );

    let (packet, sent_timestamp) = session.send_ping_request().unwrap();
    let payload = deserializer
        .get_next_message(&packet.to_vec()[..])
        .unwrap()
        .unwrap();
    let message = payload.to_rtmp_message().unwrap();

    match message {
        RtmpMessage::UserControl {
            event_type,
            timestamp: Some(timestamp),
            buffer_length: None,
            stream_id: None,
        } => {
            assert_eq!(
                event_type,
                UserControlEventType::PingRequest,
                "Unexpected user control event type"
            );
            assert_eq!(
                timestamp, sent_timestamp,
                "Unexpected timestamp in outbound message"
            );
        }

        x => panic!("Expected PingRequest being sent, instead found {:?}", x),
    }
}

#[test]
fn can_complete_playback_stream() {
    let (mut deserializer, mut serializer, mut session) = common_basic_setup();
    perform_connection(
        TEST_APP_NAME,
        &mut session,
        &mut serializer,
        &mut deserializer,
    );
    let stream_id = create_active_stream(&mut session, &mut serializer, &mut deserializer);
    start_playing(
        TEST_STREAM_KEY,
        stream_id,
        &mut session,
        &mut serializer,
        &mut deserializer,
    );

    let packet = session
        .complete_playback(crate::sessions::StreamId::new(stream_id))
        .unwrap();
    let payload = deserializer
        .get_next_message(&packet.to_vec()[..])
        .unwrap()
        .unwrap();
    let message = payload.to_rtmp_message().unwrap();

    verify_is_onstatus(&message, "status", "NetStream.Play.Complete");
}

// ffmpeg publishes without ever sending a `WindowAcknowledgement`
// (verified against ffmpeg 9.0.1, which sends only `SetChunkSize`). Upstream
// acknowledged only after receiving one, so such a publisher was never
// acknowledged at all. We fall back to the window we advertised.
#[test]
fn acknowledges_a_peer_that_never_advertises_its_own_window() {
    let config = ServerSessionConfig {
        chunk_size: DEFAULT_CHUNK_SIZE,
        fms_version: "fms_version".to_string(),
        peer_bandwidth: DEFAULT_PEER_BANDWIDTH,
        window_ack_size: 200,
        send_on_bw_done_message_on_start: true,
        max_object_encoding: crate::amf::AmfEncoding::Amf3,
        payload_pool: None,
        decoder_limits: crate::chunk_io::DecoderLimits::default(),
        session_limits: crate::sessions::SessionLimits::default(),
    };
    let (mut deserializer, mut serializer, mut session) = common_setup(&config);
    perform_connection(
        TEST_APP_NAME,
        &mut session,
        &mut serializer,
        &mut deserializer,
    );

    // Note: no WindowAcknowledgement is ever sent by this peer.
    let mut bytes = BytesMut::new();
    bytes.extend_from_slice(&[1; 300]);
    let video_message = RtmpMessage::VideoData {
        data: bytes.freeze(),
    };
    let video_payload = video_message
        .into_raw_message(RtmpTimestamp::new(0), 0)
        .unwrap();
    let video_packet = serializer.serialize(&video_payload, false, false).unwrap();
    let results = session.handle_input(&video_packet.to_vec()[..]).unwrap();
    let (mut responses, _) = split_results(&mut deserializer, results);

    assert_eq!(
        responses.len(),
        1,
        "expected an acknowledgement without the peer advertising a window"
    );
    match responses.remove(0) {
        (_, RtmpMessage::Acknowledgement { sequence_number }) => {
            assert!(sequence_number > 0, "acknowledgement must report progress")
        }
        x => panic!("Expected Acknowledgement, instead received: {:?}", x),
    }
}

#[test]
fn sends_ack_after_receiving_window_ack_bytes() {
    let (mut deserializer, mut serializer, mut session) = common_basic_setup();
    perform_connection(
        TEST_APP_NAME,
        &mut session,
        &mut serializer,
        &mut deserializer,
    );

    let window_ack_message = RtmpMessage::WindowAcknowledgement { size: 100 };
    let window_ack_payload = window_ack_message
        .into_raw_message(RtmpTimestamp::new(0), 0)
        .unwrap();
    let window_ack_packet = serializer
        .serialize(&window_ack_payload, false, false)
        .unwrap();
    // Track cumulative bytes fed to the session so the
    // acknowledgement sequence number can be asserted exactly. It is the total
    // received on the connection, which includes the connect handshake traffic
    // and this window-ack packet, not just the video that triggered the ack.
    let mut total_input_bytes = session.bytes_received_for_test();
    total_input_bytes += window_ack_packet.to_vec().len() as u64;
    let results = session
        .handle_input(&window_ack_packet.to_vec()[..])
        .unwrap();
    consume_results(&mut deserializer, results);

    let mut bytes = BytesMut::new();
    bytes.extend_from_slice(&[1; 101]);
    let video_message = RtmpMessage::VideoData {
        data: bytes.freeze(),
    };
    let video_payload = video_message
        .into_raw_message(RtmpTimestamp::new(0), 0)
        .unwrap();
    let video_packet = serializer.serialize(&video_payload, false, false).unwrap();
    total_input_bytes += video_packet.to_vec().len() as u64;
    let results = session.handle_input(&video_packet.to_vec()[..]).unwrap();
    let (mut responses, _) = split_results(&mut deserializer, results);

    assert_eq!(responses.len(), 1, "Unexpected number of responses");
    match responses.remove(0) {
        // The spec defines this as cumulative bytes received on the
        // connection. Upstream sent bytes-since-last-ack, which reports a
        // stalled counter to the peer.
        (_, RtmpMessage::Acknowledgement { sequence_number }) => assert_eq!(
            sequence_number, total_input_bytes as u32,
            "Acknowledgement must report cumulative bytes received"
        ),
        x => panic!("Expected Acknowledgement, instead received: {:?}", x),
    }

    let mut bytes = BytesMut::new();
    bytes.extend_from_slice(&[1; 1]);
    let video_message = RtmpMessage::VideoData {
        data: bytes.freeze(),
    };
    let video_payload = video_message
        .into_raw_message(RtmpTimestamp::new(0), 0)
        .unwrap();
    let video_packet = serializer.serialize(&video_payload, false, false).unwrap();
    total_input_bytes += video_packet.to_vec().len() as u64;
    let results = session.handle_input(&video_packet.to_vec()[..]).unwrap();
    let (responses, _) = split_results(&mut deserializer, results);
    assert_eq!(responses.len(), 0, "Expected no responses");

    let mut bytes = BytesMut::new();
    bytes.extend_from_slice(&[1; 100]);
    let video_message = RtmpMessage::VideoData {
        data: bytes.freeze(),
    };
    let video_payload = video_message
        .into_raw_message(RtmpTimestamp::new(0), 0)
        .unwrap();
    let video_packet = serializer.serialize(&video_payload, false, false).unwrap();
    total_input_bytes += video_packet.to_vec().len() as u64;
    let results = session.handle_input(&video_packet.to_vec()[..]).unwrap();
    let (mut responses, _) = split_results(&mut deserializer, results);
    assert_eq!(responses.len(), 1, "Unexpected number of responses");
    match responses.remove(0) {
        // Still cumulative, and strictly greater than the first ack.
        (_, RtmpMessage::Acknowledgement { sequence_number }) => assert_eq!(
            sequence_number, total_input_bytes as u32,
            "Second acknowledgement must continue the cumulative count"
        ),
        x => panic!("Expected Acknowledgement, instead received: {:?}", x),
    }
}

#[test]
fn event_raised_when_client_sends_an_acknowledgement() {
    let (mut deserializer, mut serializer, mut session) = common_basic_setup();
    perform_connection(
        TEST_APP_NAME,
        &mut session,
        &mut serializer,
        &mut deserializer,
    );

    let message = RtmpMessage::Acknowledgement {
        sequence_number: 1234,
    };
    let payload = message.into_raw_message(RtmpTimestamp::new(0), 0).unwrap();
    let packet = serializer.serialize(&payload, false, false).unwrap();
    let results = session.handle_input(&packet.to_vec()[..]).unwrap();
    let (_, mut events) = split_results(&mut deserializer, results);

    assert_eq!(events.len(), 1, "Unexpected number of events");
    match events.remove(0) {
        ServerSessionEvent::AcknowledgementReceived { bytes_received, .. } => {
            assert_eq!(
                bytes_received, 1234,
                "Incorrect number of bytes received in event"
            );
        }

        x => panic!(
            "Expected acknowledgement received event, instead got: {:?}",
            x
        ),
    }
}

fn get_basic_config() -> ServerSessionConfig {
    ServerSessionConfig {
        chunk_size: DEFAULT_CHUNK_SIZE,
        fms_version: "fms_version".to_string(),
        peer_bandwidth: DEFAULT_PEER_BANDWIDTH,
        window_ack_size: DEFAULT_WINDOW_ACK_SIZE,
        send_on_bw_done_message_on_start: true,
        max_object_encoding: crate::amf::AmfEncoding::Amf3,
        payload_pool: None,
        decoder_limits: crate::chunk_io::DecoderLimits::default(),
        session_limits: crate::sessions::SessionLimits::default(),
    }
}

fn common_setup(config: &ServerSessionConfig) -> (ContiguousDecoder, ChunkEncoder, ServerSession) {
    let mut deserializer = ContiguousDecoder::new();
    let serializer = ChunkEncoder::new();
    let (session, initial_results) = ServerSession::new(config.clone()).unwrap();
    consume_results(&mut deserializer, initial_results);
    (deserializer, serializer, session)
}

fn common_basic_setup() -> (ContiguousDecoder, ChunkEncoder, ServerSession) {
    common_setup(&get_basic_config())
}

fn split_results(
    deserializer: &mut ContiguousDecoder,
    mut results: Vec<ServerSessionResult>,
) -> (Vec<(RawMessage, RtmpMessage)>, Vec<ServerSessionEvent>) {
    let mut responses = Vec::new();
    let mut events = Vec::new();

    for result in results.drain(..) {
        match result {
            ServerSessionResult::Packet(packet) => {
                let payload = deserializer
                    .get_next_message(&packet.to_vec()[..])
                    .unwrap()
                    .unwrap();
                let message = payload.to_rtmp_message().unwrap();
                match message {
                    RtmpMessage::SetChunkSize { size } => {
                        deserializer.set_chunk_size(size as usize).unwrap()
                    }
                    _ => (),
                }

                println!("response received from server: {:?}", message);
                responses.push((payload, message));
            }

            ServerSessionResult::Event(event) => {
                println!("event received from server: {:?}", event);
                events.push(event);
            }

            _ => (),
        }
    }

    (responses, events)
}

fn consume_results(deserializer: &mut ContiguousDecoder, results: Vec<ServerSessionResult>) {
    // Needed to keep the deserializer up to date
    split_results(deserializer, results);
}

fn create_connect_message(
    app_name: String,
    timestamp: u32,
    stream_id: u32,
    object_encoding: f64,
) -> RawMessage {
    create_connect_message_with_properties(
        app_name,
        timestamp,
        stream_id,
        object_encoding,
        Amf0Object::new(),
    )
}

// Connect message builder that can carry extra command object
// properties, used to cover Enhanced RTMP capability passthrough.
fn create_connect_message_with_properties(
    app_name: String,
    timestamp: u32,
    stream_id: u32,
    object_encoding: f64,
    extra: Amf0Object,
) -> RawMessage {
    let mut properties = extra;
    properties.insert("app".to_string(), Amf0Value::Utf8String(app_name));
    properties.insert(
        "objectEncoding".to_string(),
        Amf0Value::Number(object_encoding),
    );

    let message = RtmpMessage::Amf0Command {
        command_name: "connect".to_string(),
        transaction_id: 1.0,
        command_object: Amf0Value::Object(properties),
        additional_arguments: vec![],
    };

    let timestamp = RtmpTimestamp::new(timestamp);
    let payload = message.into_raw_message(timestamp, stream_id).unwrap();
    payload
}

fn perform_connection(
    app_name: &str,
    session: &mut ServerSession,
    serializer: &mut ChunkEncoder,
    deserializer: &mut ContiguousDecoder,
) {
    let connect_payload = create_connect_message(app_name.to_string(), 15, 0, 0.0);
    let connect_packet = serializer.serialize(&connect_payload, true, false).unwrap();
    let connect_results = session.handle_input(&connect_packet.to_vec()[..]).unwrap();
    assert_eq!(
        connect_results.len(),
        1,
        "Unexpected number of responses when handling connect request message"
    );

    let (_, events) = split_results(deserializer, connect_results);
    assert_eq!(events.len(), 1, "Unexpected number of events returned");
    let request_id = match events[0] {
        ServerSessionEvent::ConnectionRequested {
            ref app_name,
            request_id,
            ..
        } if app_name.as_ref() == "some_app" => request_id,
        _ => panic!("First event was not as expected: {:?}", events[0]),
    };

    let results = session.accept_request(request_id).unwrap();
    consume_results(deserializer, results);

    // Assume it was successful
}

fn create_active_stream(
    session: &mut ServerSession,
    serializer: &mut ChunkEncoder,
    deserializer: &mut ContiguousDecoder,
) -> u32 {
    let message = RtmpMessage::Amf0Command {
        command_name: "createStream".to_string(),
        transaction_id: 4.0,
        command_object: Amf0Value::Null,
        additional_arguments: Vec::new(),
    };

    let payload = message.into_raw_message(RtmpTimestamp::new(0), 0).unwrap();
    let packet = serializer.serialize(&payload, true, false).unwrap();
    let results = session.handle_input(&packet.to_vec()[..]).unwrap();
    let (responses, _) = split_results(deserializer, results);

    assert_eq!(
        responses.len(),
        1,
        "Unexpected number of responses returned"
    );
    match responses[0] {
        (
            _,
            RtmpMessage::Amf0Command {
                ref command_name,
                transaction_id,
                command_object: Amf0Value::Null,
                ref additional_arguments,
            },
        ) if command_name == "_result" && transaction_id == 4.0 => {
            assert_eq!(
                additional_arguments.len(),
                1,
                "Unexpected number of additional arguments in response"
            );
            match additional_arguments[0] {
                Amf0Value::Number(x) => return x as u32,
                _ => panic!("First additional argument was not an Amf0Value::Number"),
            }
        }

        _ => panic!(
            "First response was not the expected value: {:?}",
            responses[0]
        ),
    }
}

fn close_stream(
    stream_id: u32,
    session: &mut ServerSession,
    serializer: &mut ChunkEncoder,
    deserializer: &mut ContiguousDecoder,
) {
    let message = RtmpMessage::Amf0Command {
        command_name: "closeStream".to_string(),
        transaction_id: 4_f64,
        command_object: Amf0Value::Null,
        additional_arguments: vec![Amf0Value::Number(stream_id as f64)],
    };

    let payload = message
        .into_raw_message(RtmpTimestamp::new(0), stream_id)
        .unwrap();
    let packet = serializer.serialize(&payload, false, false).unwrap();
    let results = session.handle_input(&packet.to_vec()[..]).unwrap();
    consume_results(deserializer, results);
}

fn start_publishing(
    stream_key: &str,
    stream_id: u32,
    session: &mut ServerSession,
    serializer: &mut ChunkEncoder,
    deserializer: &mut ContiguousDecoder,
) {
    let message = RtmpMessage::Amf0Command {
        command_name: "publish".to_string(),
        transaction_id: 5.0,
        command_object: Amf0Value::Null,
        additional_arguments: vec![
            Amf0Value::Utf8String(stream_key.to_string()),
            Amf0Value::Utf8String("live".to_string()),
        ],
    };

    let publish_payload = message
        .into_raw_message(RtmpTimestamp::new(0), stream_id)
        .unwrap();
    let publish_packet = serializer
        .serialize(&publish_payload, false, false)
        .unwrap();
    let publish_results = session.handle_input(&publish_packet.to_vec()[..]).unwrap();
    let (_, events) = split_results(deserializer, publish_results);

    assert_eq!(events.len(), 1, "Unexpected number of events returned");
    let request_id = match events[0] {
        ServerSessionEvent::PublishStreamRequested {
            ref app_name,
            ref stream_key,
            request_id: returned_request_id,
            mode: PublishMode::Live,
            ..
        } if app_name.as_ref() == "some_app" && stream_key.as_ref() == "stream_key" => {
            returned_request_id
        }

        _ => panic!("Unexpected first event found: {:?}", events[0]),
    };

    let accept_results = session.accept_request(request_id).unwrap();
    consume_results(deserializer, accept_results);
}

fn start_playing(
    stream_key: &str,
    stream_id: u32,
    session: &mut ServerSession,
    serializer: &mut ChunkEncoder,
    deserializer: &mut ContiguousDecoder,
) {
    let message = RtmpMessage::Amf0Command {
        command_name: "play".to_string(),
        transaction_id: 4.0,
        command_object: Amf0Value::Null,
        additional_arguments: vec![
            Amf0Value::Utf8String(stream_key.to_string()),
            Amf0Value::Number(5.0),   // Start argument
            Amf0Value::Number(25.0),  // Duration,
            Amf0Value::Boolean(true), // reset
        ],
    };

    let play_payload = message
        .into_raw_message(RtmpTimestamp::new(0), stream_id)
        .unwrap();
    let play_packet = serializer.serialize(&play_payload, false, false).unwrap();
    let play_results = session.handle_input(&play_packet.to_vec()[..]).unwrap();
    let (_, mut events) = split_results(deserializer, play_results);

    assert_eq!(events.len(), 1, "Unexpected number of events returned");
    let request_id = match events.remove(0) {
        ServerSessionEvent::PlayStreamRequested {
            app_name: _,
            stream_key: _,
            start_at: _,
            duration: _,
            reset: _,
            request_id,
            stream_id: _,
            ..
        } => request_id,

        x => panic!("Expected play event but instead received: {:?}", x),
    };

    let accept_results = session.accept_request(request_id).unwrap();
    consume_results(deserializer, accept_results);
}

/// The info object of a command reply.
fn info_of(message: &RtmpMessage) -> &Amf0Object {
    match message {
        RtmpMessage::Amf0Command {
            additional_arguments,
            ..
        } => match additional_arguments.as_slice() {
            [Amf0Value::Object(info)] => info,
            other => panic!("expected one info object, got {other:?}"),
        },
        other => panic!("expected an AMF0 command, got {other:?}"),
    }
}

fn is_command(message: &RtmpMessage, name: &str) -> bool {
    matches!(message, RtmpMessage::Amf0Command { command_name, .. } if command_name == name)
}

fn assert_is_on_bw_done(message: &RtmpMessage) {
    assert!(
        is_command(message, "onBWDone"),
        "expected onBWDone, got {message:?}"
    );
}

/// Send `connect`, then accept or reject it, and return what the server sent.
fn connect_and_resolve(
    session: &mut ServerSession,
    serializer: &mut ChunkEncoder,
    deserializer: &mut ContiguousDecoder,
    accept: bool,
) -> Vec<(RawMessage, RtmpMessage)> {
    let connect_payload = create_connect_message(TEST_APP_NAME.to_string(), 15, 0, 0.0);
    let connect_packet = serializer.serialize(&connect_payload, true, false).unwrap();
    let results = session.handle_input(&connect_packet.to_vec()[..]).unwrap();
    let (_, events) = split_results(deserializer, results);
    let request_id = match events.as_slice() {
        [ServerSessionEvent::ConnectionRequested { request_id, .. }] => *request_id,
        other => panic!("connect must be requested, got {other:?}"),
    };
    let results = if accept {
        session.accept_request(request_id).unwrap()
    } else {
        session
            .reject_request(request_id, "NetConnection.Connect.Rejected", "refused")
            .unwrap()
    };
    split_results(deserializer, results).0
}

/// Send a command with the stream key as its argument on stream 0.
fn send_command(
    name: &str,
    transaction_id: f64,
    session: &mut ServerSession,
    serializer: &mut ChunkEncoder,
    deserializer: &mut ContiguousDecoder,
) -> (Vec<(RawMessage, RtmpMessage)>, Vec<ServerSessionEvent>) {
    let message = RtmpMessage::Amf0Command {
        command_name: name.to_string(),
        transaction_id,
        command_object: Amf0Value::Null,
        additional_arguments: vec![Amf0Value::Utf8String(TEST_STREAM_KEY.to_string())],
    };
    let payload = message.into_raw_message(RtmpTimestamp::new(0), 0).unwrap();
    let packet = serializer.serialize(&payload, false, false).unwrap();
    let results = session.handle_input(&packet.to_vec()[..]).unwrap();
    split_results(deserializer, results)
}

fn verify_is_onstatus(subject: &RtmpMessage, expected_status: &str, expected_code: &str) {
    match subject {
        RtmpMessage::Amf0Command {
            command_name,
            transaction_id,
            command_object,
            additional_arguments,
        } => {
            assert_eq!(command_name.as_str(), "onStatus", "Unexpected command name");
            assert_eq!(transaction_id, &0.0, "Unexpected transaction id");
            assert_eq!(
                command_object,
                &Amf0Value::Null,
                "Unexpected command object"
            );
            assert_eq!(
                additional_arguments.len(),
                1,
                "Unexpected number of additional arguments"
            );

            match additional_arguments.first().unwrap() {
                Amf0Value::Object(properties) => {
                    assert_eq!(
                        properties.get("level"),
                        Some(&Amf0Value::Utf8String(expected_status.to_string())),
                        "Unexpected level value"
                    );
                    assert_eq!(
                        properties.get("code"),
                        Some(&Amf0Value::Utf8String(expected_code.to_string())),
                        "Unexpected code value"
                    );
                    assert!(
                        properties.contains_key("description"),
                        "Expected description"
                    );
                }

                x => panic!("Expected amf0 object, but instead argument was: {:?}", x),
            }
        }

        x => panic!("Expected Amf0Command command, instead received: {:?}", x),
    }
}

#[test]
fn same_key_streams_keep_distinct_identity_for_data_media_and_finish() {
    let (mut session, initial) = ServerSession::new(get_basic_config()).unwrap();
    let mut serializer = ChunkEncoder::new();
    let mut deserializer = ContiguousDecoder::new();
    consume_results(&mut deserializer, initial);
    perform_connection(
        TEST_APP_NAME,
        &mut session,
        &mut serializer,
        &mut deserializer,
    );
    let first = create_active_stream(&mut session, &mut serializer, &mut deserializer);
    let second = create_active_stream(&mut session, &mut serializer, &mut deserializer);
    for id in [first, second] {
        start_publishing(
            TEST_STREAM_KEY,
            id,
            &mut session,
            &mut serializer,
            &mut deserializer,
        );
    }
    assert_ne!(first, second);
    for id in [second, first] {
        let messages = vec![
            RtmpMessage::AudioData {
                data: Bytes::from_static(b"audio"),
            },
            RtmpMessage::VideoData {
                data: Bytes::from_static(b"video"),
            },
            RtmpMessage::Amf0Data {
                values: vec![
                    Amf0Value::Utf8String("onMetaData".into()),
                    Amf0Value::Object(Amf0Object::new()),
                ],
            },
            RtmpMessage::Amf0Data {
                values: vec![Amf0Value::Utf8String("onCaption".into())],
            },
            RtmpMessage::Amf0Command {
                command_name: "deleteStream".into(),
                transaction_id: 0.0,
                command_object: Amf0Value::Null,
                additional_arguments: vec![Amf0Value::Number(id as f64)],
            },
        ];
        for message in messages {
            let payload = message
                .into_raw_message(RtmpTimestamp::new(100), id)
                .unwrap();
            let packet = serializer.serialize(&payload, false, false).unwrap();
            let results = session.handle_input(&packet.to_vec()).unwrap();
            let (_, events) = split_results(&mut deserializer, results);
            assert_eq!(events.len(), 1);
            let event_id = match &events[0] {
                ServerSessionEvent::AudioDataReceived { stream_id, .. }
                | ServerSessionEvent::VideoDataReceived { stream_id, .. }
                | ServerSessionEvent::StreamDataReceived { stream_id, .. }
                | ServerSessionEvent::PublishStreamFinished { stream_id, .. } => *stream_id,
                e => panic!("unexpected event: {e:?}"),
            };
            assert_eq!(event_id.get(), id);
        }
    }
}
