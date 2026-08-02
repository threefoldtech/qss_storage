use redis_protocol::resp2::types::OwnedFrame as Frame;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum RespError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Protocol error: {0}")]
    Protocol(String),
}

/// The largest inline command accepted, matching Redis's 64 KiB
/// `PROTO_INLINE_MAX_SIZE`. A client that opens an inline command and never
/// sends its newline would otherwise grow the read buffer without bound.
const INLINE_MAX_SIZE: usize = 64 * 1024;

/// The bytes RESP2 uses to introduce a frame. Anything else at the head of a
/// command is an inline command.
const FRAME_MARKERS: &[u8] = b"+-:$*";

/// One inline command: its arguments and the bytes the line occupied.
type InlineCommand = (Vec<Vec<u8>>, usize);

/// Split an inline command line into its arguments.
///
/// These are the rules Redis's `sdssplitargs` applies, and clients written
/// against Redis rely on them: words separate on whitespace; `"..."` quotes a
/// word and honours `\n`, `\r`, `\t`, `\b`, `\a`, `\xHH` and `\<any>`
/// escapes; `'...'` quotes a word and honours only `\'`. A quote that closes
/// against anything but whitespace, or one that never closes, is a protocol
/// error rather than a silently different command.
fn split_args(line: &[u8]) -> Result<Vec<Vec<u8>>, RespError> {
    fn is_space(b: u8) -> bool {
        matches!(b, b' ' | b'\n' | b'\r' | b'\t' | 0x0b | 0x0c)
    }

    fn hex_value(b: u8) -> Option<u8> {
        match b {
            b'0'..=b'9' => Some(b - b'0'),
            b'a'..=b'f' => Some(b - b'a' + 10),
            b'A'..=b'F' => Some(b - b'A' + 10),
            _ => None,
        }
    }

    let unbalanced = || RespError::Protocol("unbalanced quotes in request".to_string());

    let mut args = Vec::new();
    let mut pos = 0;

    loop {
        while pos < line.len() && is_space(line[pos]) {
            pos += 1;
        }
        if pos == line.len() {
            return Ok(args);
        }

        let mut current = Vec::new();
        let quote = match line[pos] {
            q @ (b'"' | b'\'') => {
                pos += 1;
                Some(q)
            }
            _ => None,
        };

        loop {
            match quote {
                // Inside quotes the word ends at the closing quote, and the
                // line ending before it is the client's mistake.
                Some(q) => {
                    let Some(&byte) = line.get(pos) else {
                        return Err(unbalanced());
                    };
                    if byte == q {
                        pos += 1;
                        // A closing quote has to end the word.
                        if line.get(pos).is_some_and(|&b| !is_space(b)) {
                            return Err(unbalanced());
                        }
                        break;
                    }
                    if byte == b'\\' && q == b'"' {
                        if let (Some(b'x'), Some(hi), Some(lo)) = (
                            line.get(pos + 1),
                            line.get(pos + 2).copied().and_then(hex_value),
                            line.get(pos + 3).copied().and_then(hex_value),
                        ) {
                            current.push(hi * 16 + lo);
                            pos += 4;
                            continue;
                        }
                        if let Some(&escaped) = line.get(pos + 1) {
                            current.push(match escaped {
                                b'n' => b'\n',
                                b'r' => b'\r',
                                b't' => b'\t',
                                b'b' => 0x08,
                                b'a' => 0x07,
                                other => other,
                            });
                            pos += 2;
                            continue;
                        }
                        return Err(unbalanced());
                    }
                    if byte == b'\\' && q == b'\'' && line.get(pos + 1) == Some(&b'\'') {
                        current.push(b'\'');
                        pos += 2;
                        continue;
                    }
                    current.push(byte);
                    pos += 1;
                }
                // Outside quotes the word ends at whitespace or the line end.
                None => match line.get(pos) {
                    None => break,
                    Some(&byte) if is_space(byte) => break,
                    Some(&byte) => {
                        current.push(byte);
                        pos += 1;
                    }
                },
            }
        }

        args.push(current);
    }
}

/// Helper functions for Redis RESP protocol
pub struct RespHelper;

impl RespHelper {
    /// Parse a frame from a byte buffer
    ///
    /// A command arrives in one of two forms. The usual one is a RESP array,
    /// which the codec decodes. The other is an *inline command*: a plain
    /// line of space-separated words, the form Redis accepts from
    /// telnet-style clients. `valkey-cli --pipe` depends on inline being
    /// understood -- it prefixes its terminating `ECHO` with a bare CRLF,
    /// which is an empty inline command, and a server that rejects it never
    /// answers the ECHO and leaves the client waiting out its timeout.
    ///
    /// Empty inline lines carry no command, so they are consumed and parsing
    /// continues with whatever follows.
    ///
    /// The returned count is what the caller should drop, and it is non-zero
    /// even when no frame completed: blank lines are gone for good rather
    /// than held against a command that may never arrive, so a client sending
    /// nothing but newlines cannot grow the read buffer.
    pub fn parse_frame(buffer: &[u8]) -> Result<(Option<Frame>, usize), RespError> {
        let mut consumed = 0;

        loop {
            let rest = &buffer[consumed..];
            let Some(&first) = rest.first() else {
                return Ok((None, consumed));
            };

            if !FRAME_MARKERS.contains(&first) {
                match Self::parse_inline(rest)? {
                    // No newline yet: the line is still arriving.
                    None => return Ok((None, consumed)),
                    // A blank line is not a command. Skip it and look at
                    // what follows in the same buffer.
                    Some((args, len)) if args.is_empty() => consumed += len,
                    Some((args, len)) => {
                        let frame = Frame::Array(args.into_iter().map(Frame::BulkString).collect());
                        return Ok((Some(frame), consumed + len));
                    }
                }
                continue;
            }

            // For redis-protocol 6.0.0, we'll use the regular decode function
            // since we're working with a byte slice
            return match redis_protocol::resp2::decode::decode(rest) {
                Ok(Some((frame, len))) => {
                    // Return the frame and how many bytes were consumed
                    Ok((Some(frame), consumed + len))
                }
                Ok(None) => {
                    // Need more data
                    Ok((None, consumed))
                }
                Err(e) => {
                    if e.to_string().contains("incomplete") {
                        // Need more data
                        Ok((None, consumed))
                    } else {
                        Err(RespError::Protocol(e.to_string()))
                    }
                }
            };
        }
    }

    /// Read one inline command from the head of `buffer`.
    ///
    /// Returns the arguments and how many bytes the line occupied, or `None`
    /// when the line has not arrived in full yet. An empty argument list means
    /// the line was blank.
    fn parse_inline(buffer: &[u8]) -> Result<Option<InlineCommand>, RespError> {
        let Some(newline) = buffer.iter().position(|&b| b == b'\n') else {
            // Redis draws the line at the same place: a client is allowed to
            // send a long inline command, but not an endless one.
            if buffer.len() > INLINE_MAX_SIZE {
                return Err(RespError::Protocol("too big inline request".to_string()));
            }
            return Ok(None);
        };

        if newline > INLINE_MAX_SIZE {
            return Err(RespError::Protocol("too big inline request".to_string()));
        }

        let mut line = &buffer[..newline];
        if line.last() == Some(&b'\r') {
            line = &line[..line.len() - 1];
        }

        Ok(Some((split_args(line)?, newline + 1)))
    }

    /// Encode a frame to bytes
    pub fn encode_frame(frame: &Frame) -> Result<Vec<u8>, RespError> {
        // Estimate the frame size - for COMMAND responses, we need a much larger buffer
        let estimated_size = match frame {
            Frame::Array(items) if items.len() > 10 => 4096, // Large arrays like COMMAND response
            _ => 512,                                        // Default size for most responses
        };

        // Use Vec<u8> for encoding with zeros already in place
        let mut buffer = vec![0; estimated_size];

        // Try to encode with the current buffer size
        match redis_protocol::resp2::encode::encode(&mut buffer, frame, false) {
            Ok(len) => {
                buffer.truncate(len);
                Ok(buffer)
            }
            Err(e) => {
                if e.to_string().contains("Buffer too small") {
                    // If buffer is too small, try with a much larger buffer
                    let mut larger_buffer = vec![0; 16384]; // 16KB should be enough for most responses
                    let len =
                        redis_protocol::resp2::encode::encode(&mut larger_buffer, frame, false)
                            .map_err(|e| RespError::Protocol(e.to_string()))?;
                    larger_buffer.truncate(len);
                    Ok(larger_buffer)
                } else {
                    Err(RespError::Protocol(e.to_string()))
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The command name and arguments of a parsed frame, for terse assertions.
    fn parse(buffer: &[u8]) -> (Vec<Vec<u8>>, usize) {
        let (frame, len) = RespHelper::parse_frame(buffer).expect("must parse");
        let frame = frame.expect("must be a complete frame");
        let Frame::Array(items) = frame else {
            panic!("inline commands parse to arrays, got {:?}", frame);
        };
        let args = items
            .into_iter()
            .map(|item| match item {
                Frame::BulkString(bytes) => bytes,
                other => panic!("arguments are bulk strings, got {:?}", other),
            })
            .collect();
        (args, len)
    }

    #[test]
    fn an_inline_command_parses_to_an_array() {
        let (args, len) = parse(b"SET key value\r\n");
        assert_eq!(
            args,
            vec![b"SET".to_vec(), b"key".to_vec(), b"value".to_vec()]
        );
        assert_eq!(len, 15);
    }

    #[test]
    fn an_inline_command_may_end_with_a_bare_newline() {
        let (args, len) = parse(b"PING\n");
        assert_eq!(args, vec![b"PING".to_vec()]);
        assert_eq!(len, 5);
    }

    #[test]
    fn a_blank_line_is_skipped_and_the_next_command_answers() {
        // This is valkey-cli --pipe's terminator: a bare CRLF, then the ECHO
        // it waits for. Answering it means consuming both.
        let buffer = b"\r\n*2\r\n$4\r\nECHO\r\n$3\r\nabc\r\n";
        let (args, len) = parse(buffer);
        assert_eq!(args, vec![b"ECHO".to_vec(), b"abc".to_vec()]);
        assert_eq!(
            len,
            buffer.len(),
            "the blank line is consumed with the frame"
        );
    }

    #[test]
    fn a_line_that_has_not_arrived_yet_asks_for_more() {
        let (frame, consumed) =
            RespHelper::parse_frame(b"SET key val").expect("a partial line is not an error");
        assert!(frame.is_none());
        assert_eq!(consumed, 0, "an unfinished line is not consumed");
    }

    #[test]
    fn blank_lines_are_consumed_even_with_no_command_behind_them() {
        // Otherwise a client sending nothing but newlines would grow the
        // read buffer for as long as it cared to keep sending them.
        let (frame, consumed) = RespHelper::parse_frame(b"\r\n\r\n\n").expect("must parse");
        assert!(frame.is_none());
        assert_eq!(consumed, 5);
    }

    #[test]
    fn quotes_group_words_and_honour_escapes() {
        let (args, _) = parse(b"SET key \"a b\\tc\\x41\"\r\n");
        assert_eq!(
            args,
            vec![b"SET".to_vec(), b"key".to_vec(), b"a b\tcA".to_vec()]
        );

        let (args, _) = parse(b"SET key 'it\\'s'\r\n");
        assert_eq!(
            args,
            vec![b"SET".to_vec(), b"key".to_vec(), b"it's".to_vec()]
        );
    }

    #[test]
    fn an_unbalanced_quote_is_a_protocol_error() {
        assert!(RespHelper::parse_frame(b"SET key \"unterminated\r\n").is_err());
        // A closing quote has to end the word.
        assert!(RespHelper::parse_frame(b"SET key \"ab\"cd\r\n").is_err());
    }

    #[test]
    fn an_endless_inline_command_is_refused_rather_than_buffered() {
        let mut buffer = vec![b'x'; INLINE_MAX_SIZE + 1];
        assert!(RespHelper::parse_frame(&buffer).is_err());

        // Same bytes, but terminated: still refused, on its length.
        buffer.push(b'\n');
        assert!(RespHelper::parse_frame(&buffer).is_err());
    }

    #[test]
    fn a_resp_array_still_parses() {
        let (args, len) = parse(b"*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n");
        assert_eq!(args, vec![b"ECHO".to_vec(), b"hello".to_vec()]);
        assert_eq!(len, 25);
    }
}
