use redis_protocol::resp2::types::OwnedFrame as Frame;
use respd::cmd::{Command, CommandError};

#[test]
fn test_select_command_parsing() {
    // Test SELECT with just namespace (2 arguments)
    let frame = Frame::Array(vec![
        Frame::BulkString(b"SELECT".to_vec()),
        Frame::BulkString(b"test_namespace".to_vec()),
    ]);

    let cmd = Command::from_frame(frame).unwrap();
    match cmd {
        Command::Select {
            namespace,
            password,
        } => {
            assert_eq!(namespace, "test_namespace");
            assert_eq!(password, None);
        }
        _ => panic!("Expected SELECT command"),
    }

    // Test SELECT with namespace and password (3 arguments)
    let frame = Frame::Array(vec![
        Frame::BulkString(b"SELECT".to_vec()),
        Frame::BulkString(b"test_namespace".to_vec()),
        Frame::BulkString(b"secret_password".to_vec()),
    ]);

    let cmd = Command::from_frame(frame).unwrap();
    match cmd {
        Command::Select {
            namespace,
            password,
        } => {
            assert_eq!(namespace, "test_namespace");
            assert_eq!(password, Some("secret_password".to_string()));
        }
        _ => panic!("Expected SELECT command"),
    }

    // Test SELECT with too few arguments
    let frame = Frame::Array(vec![Frame::BulkString(b"SELECT".to_vec())]);

    let result = Command::from_frame(frame);
    assert!(matches!(
        result,
        Err(CommandError::WrongNumberOfArguments(_))
    ));

    // Test SELECT with too many arguments
    let frame = Frame::Array(vec![
        Frame::BulkString(b"SELECT".to_vec()),
        Frame::BulkString(b"test_namespace".to_vec()),
        Frame::BulkString(b"secret_password".to_vec()),
        Frame::BulkString(b"extra_arg".to_vec()),
    ]);

    let result = Command::from_frame(frame);
    assert!(matches!(
        result,
        Err(CommandError::WrongNumberOfArguments(_))
    ));
}

#[test]
fn test_echo_command_parsing() {
    // Test ECHO with its one argument
    let frame = Frame::Array(vec![
        Frame::BulkString(b"ECHO".to_vec()),
        Frame::BulkString(b"hello".to_vec()),
    ]);

    let cmd = Command::from_frame(frame).unwrap();
    match cmd {
        Command::Echo { message } => assert_eq!(message.as_ref(), b"hello"),
        _ => panic!("Expected ECHO command"),
    }

    // Test ECHO with no message
    let frame = Frame::Array(vec![Frame::BulkString(b"ECHO".to_vec())]);

    let result = Command::from_frame(frame);
    assert!(matches!(
        result,
        Err(CommandError::WrongNumberOfArguments(_))
    ));

    // Test ECHO with too many arguments
    let frame = Frame::Array(vec![
        Frame::BulkString(b"ECHO".to_vec()),
        Frame::BulkString(b"hello".to_vec()),
        Frame::BulkString(b"extra_arg".to_vec()),
    ]);

    let result = Command::from_frame(frame);
    assert!(matches!(
        result,
        Err(CommandError::WrongNumberOfArguments(_))
    ));
}

#[test]
fn test_echo_keeps_a_payload_that_is_not_utf8() {
    // The twenty random bytes valkey-cli --pipe ends its stream with are not
    // text: parsing must hand them through untouched, not lossily decoded.
    let payload = vec![0x00u8, 0xff, 0xfe, b'a', 0x80, b'\n'];
    let frame = Frame::Array(vec![
        Frame::BulkString(b"ECHO".to_vec()),
        Frame::BulkString(payload.clone()),
    ]);

    let cmd = Command::from_frame(frame).unwrap();
    match cmd {
        Command::Echo { message } => assert_eq!(message.as_ref(), payload.as_slice()),
        _ => panic!("Expected ECHO command"),
    }
}

#[test]
fn test_flush_command_parsing() {
    // Test FLUSH command with correct number of arguments (1 argument - just the command name)
    let frame = Frame::Array(vec![Frame::BulkString(b"FLUSH".to_vec())]);

    let cmd = Command::from_frame(frame).unwrap();
    match cmd {
        Command::Flush => {
            // Command parsed correctly
        }
        _ => panic!("Expected FLUSH command"),
    }

    // Test FLUSH with too many arguments
    let frame = Frame::Array(vec![
        Frame::BulkString(b"FLUSH".to_vec()),
        Frame::BulkString(b"extra_arg".to_vec()),
    ]);

    let result = Command::from_frame(frame);
    assert!(matches!(
        result,
        Err(CommandError::WrongNumberOfArguments(_))
    ));
}
