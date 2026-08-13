//! RESP2 on a socket, with nothing between the caller and the wire.
//!
//! The campaign already has a client for respcas (`tests/real/tools/respcli.py`)
//! and this is deliberately not a replacement for it: valkey-cli stays the
//! client that proves operator-visible behaviour. What a Python client cannot
//! do is saturate a server -- the GIL puts one interpreter thread between every
//! connection and its socket, so past a few tens of thousands of operations per
//! second the number being reported is the driver, not the server.
//!
//! Two properties are load-bearing here and both are about not lying:
//!
//! - **Bytes, never strings.** Arguments and replies are `Vec<u8>` the whole
//!   way down. A key with an embedded NUL (ADR 0014 mode B puts a raw BLAKE3
//!   hash on the wire as the key) and a value with high bytes in it must
//!   arrive as they were meant, or the load is measuring a different command
//!   than the one asked for.
//! - **An error reply is data.** `-ERR` comes back as `Reply::Error`, never as
//!   a transport failure. A command that is supposed to fail is a measurement,
//!   not an accident, and a driver that panicked on one would hide exactly the
//!   result worth having.
//!
//! TCP and Unix are the same code path above `connect`, because hero_db serves
//! RESP on both (`127.0.0.1:6378` and `$PATH_SOCKETS/hero_db/resp.sock`) and a
//! number from one is not a number from the other.

use std::fmt;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
#[cfg(test)]
use std::time::Duration;

use anyhow::{Context, Result, bail};
use tokio::io::{
    AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader, BufWriter,
};
use tokio::net::{TcpStream, UnixStream};

/// A bulk string longer than this is a protocol desync, not a value: redis's
/// own proto-max-bulk-len is 512 MiB and both servers here cap well below it.
/// Without the guard a garbled length allocates whatever the four bytes said.
const MAX_BULK: i64 = 512 * 1024 * 1024;

/// Likewise for arrays: KEYS on a large namespace is big, hostile framing is
/// unbounded.
const MAX_ARRAY: i64 = 16 * 1024 * 1024;

/// Where the server is. TCP for respcas and hero_db's port, Unix for
/// hero_db's `resp.sock`.
#[derive(Clone, Debug)]
pub enum Target {
    Tcp { host: String, port: u16 },
    Unix(PathBuf),
}

impl fmt::Display for Target {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Target::Tcp { host, port } => write!(f, "{host}:{port}"),
            Target::Unix(p) => write!(f, "unix:{}", p.display()),
        }
    }
}

/// One RESP2 reply. `Nil` covers both null bulk (`$-1`) and null array
/// (`*-1`); no caller here needs to tell them apart.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Reply {
    Simple(Vec<u8>),
    Error(String),
    Int(i64),
    Bulk(Vec<u8>),
    Nil,
    Array(Vec<Reply>),
}

impl Reply {
    /// The error text, if this is one.
    pub fn as_error(&self) -> Option<&str> {
        match self {
            Reply::Error(e) => Some(e),
            _ => None,
        }
    }

    /// The payload of a reply that carries bytes.
    pub fn bytes(&self) -> Option<&[u8]> {
        match self {
            Reply::Bulk(b) | Reply::Simple(b) => Some(b),
            _ => None,
        }
    }
}

/// One connection. Reads are buffered; writes are buffered and flushed by the
/// caller, which is what makes pipelining a pipeline rather than a syscall per
/// command.
pub struct Conn {
    r: BufReader<Box<dyn AsyncRead + Send + Unpin>>,
    w: BufWriter<Box<dyn AsyncWrite + Send + Unpin>>,
}

impl Conn {
    pub async fn connect(target: &Target) -> Result<Conn> {
        match target {
            Target::Tcp { host, port } => {
                let s = TcpStream::connect((host.as_str(), *port))
                    .await
                    .with_context(|| format!("connect {host}:{port}"))?;
                // Without this the kernel holds a small write back waiting for
                // the previous reply, and a pipeline depth of 1 measures
                // Nagle's timer. It is the driver's half of the same bug
                // hero_db had on its accept path.
                s.set_nodelay(true)?;
                let (r, w) = s.into_split();
                Ok(Conn::new(Box::new(r), Box::new(w)))
            }
            Target::Unix(path) => {
                let s = UnixStream::connect(path)
                    .await
                    .with_context(|| format!("connect {}", path.display()))?;
                let (r, w) = s.into_split();
                Ok(Conn::new(Box::new(r), Box::new(w)))
            }
        }
    }

    fn new(r: Box<dyn AsyncRead + Send + Unpin>, w: Box<dyn AsyncWrite + Send + Unpin>) -> Conn {
        Conn {
            r: BufReader::with_capacity(256 * 1024, r),
            w: BufWriter::with_capacity(256 * 1024, w),
        }
    }

    /// Queue one command. Nothing reaches the socket until `flush`.
    pub async fn send(&mut self, args: &[&[u8]]) -> Result<()> {
        self.w
            .write_all(format!("*{}\r\n", args.len()).as_bytes())
            .await?;
        for a in args {
            self.w
                .write_all(format!("${}\r\n", a.len()).as_bytes())
                .await?;
            self.w.write_all(a).await?;
            self.w.write_all(b"\r\n").await?;
        }
        Ok(())
    }

    pub async fn flush(&mut self) -> Result<()> {
        self.w.flush().await?;
        Ok(())
    }

    /// One command, flushed, and its reply.
    pub async fn call(&mut self, args: &[&[u8]]) -> Result<Reply> {
        self.send(args).await?;
        self.flush().await?;
        self.reply().await
    }

    /// SELECT, with the namespace password when there is one. respcas takes a
    /// namespace name, hero_db a numeric index; the wire shape is the same and
    /// the caller decides which it means.
    pub async fn select(&mut self, ns: &str, password: &str) -> Result<Reply> {
        let mut args: Vec<&[u8]> = vec![b"SELECT", ns.as_bytes()];
        if !password.is_empty() {
            args.push(password.as_bytes());
        }
        self.call(&args).await
    }

    /// One reply. Boxed because RESP arrays nest and an `async fn` cannot
    /// call itself.
    pub fn reply(&mut self) -> Pin<Box<dyn Future<Output = Result<Reply>> + Send + '_>> {
        Box::pin(async move {
            let line = self.line().await?;
            let Some((&kind, rest)) = line.split_first() else {
                bail!("empty reply line");
            };
            match kind {
                b'+' => Ok(Reply::Simple(rest.to_vec())),
                b'-' => Ok(Reply::Error(String::from_utf8_lossy(rest).into_owned())),
                b':' => Ok(Reply::Int(parse_int(rest)?)),
                b'$' => {
                    let n = parse_int(rest)?;
                    if n < 0 {
                        return Ok(Reply::Nil);
                    }
                    if n > MAX_BULK {
                        bail!("bulk length {n} over the {MAX_BULK} byte cap");
                    }
                    let mut buf = vec![0u8; n as usize];
                    self.r.read_exact(&mut buf).await?;
                    let mut crlf = [0u8; 2];
                    self.r.read_exact(&mut crlf).await?;
                    Ok(Reply::Bulk(buf))
                }
                b'*' => {
                    let n = parse_int(rest)?;
                    if n < 0 {
                        return Ok(Reply::Nil);
                    }
                    if n > MAX_ARRAY {
                        bail!("array length {n} over the {MAX_ARRAY} element cap");
                    }
                    let mut out = Vec::new();
                    for _ in 0..n {
                        out.push(self.reply().await?);
                    }
                    Ok(Reply::Array(out))
                }
                _ => bail!("unparseable reply: {}", String::from_utf8_lossy(&line)),
            }
        })
    }

    /// One CRLF-terminated protocol line, terminator stripped.
    async fn line(&mut self) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        let n = self.r.read_until(b'\n', &mut buf).await?;
        if n == 0 {
            bail!("the server closed the connection");
        }
        if buf.last() == Some(&b'\n') {
            buf.pop();
        }
        if buf.last() == Some(&b'\r') {
            buf.pop();
        }
        Ok(buf)
    }
}

fn parse_int(b: &[u8]) -> Result<i64> {
    std::str::from_utf8(b)
        .ok()
        .and_then(|s| s.parse::<i64>().ok())
        .with_context(|| format!("not an integer: {}", String::from_utf8_lossy(b)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncWriteExt, duplex, split};

    /// A `Conn` whose peer is a byte buffer under the test's control, and the
    /// handle it writes and reads through.
    fn pair() -> (Conn, tokio::io::DuplexStream) {
        let (client, server) = duplex(1 << 20);
        let (r, w) = split(client);
        (Conn::new(Box::new(r), Box::new(w)), server)
    }

    async fn parse(bytes: &[u8]) -> Result<Reply> {
        let (mut c, mut server) = pair();
        server.write_all(bytes).await.unwrap();
        c.reply().await
    }

    #[tokio::test]
    async fn parses_every_resp2_type() {
        assert_eq!(
            parse(b"+OK\r\n").await.unwrap(),
            Reply::Simple(b"OK".to_vec())
        );
        assert_eq!(
            parse(b"-ERR nope\r\n").await.unwrap(),
            Reply::Error("ERR nope".into())
        );
        assert_eq!(parse(b":-3\r\n").await.unwrap(), Reply::Int(-3));
        assert_eq!(
            parse(b"$3\r\nabc\r\n").await.unwrap(),
            Reply::Bulk(b"abc".to_vec())
        );
        assert_eq!(parse(b"$0\r\n\r\n").await.unwrap(), Reply::Bulk(Vec::new()));
        assert_eq!(parse(b"$-1\r\n").await.unwrap(), Reply::Nil);
        assert_eq!(parse(b"*-1\r\n").await.unwrap(), Reply::Nil);
    }

    /// The property the whole tool exists for: a value is bytes. A bulk string
    /// holding a NUL, a CRLF and a high byte must come back byte for byte,
    /// because a driver that truncated it would be testing a shorter value
    /// than the one it claims to have written.
    #[tokio::test]
    async fn bulk_strings_are_bytes_not_text() {
        let payload = [0u8, 0xff, b'\r', b'\n', 0x41, 0x80];
        let mut wire = format!("${}\r\n", payload.len()).into_bytes();
        wire.extend_from_slice(&payload);
        wire.extend_from_slice(b"\r\n");
        assert_eq!(parse(&wire).await.unwrap(), Reply::Bulk(payload.to_vec()));
    }

    #[tokio::test]
    async fn parses_nested_arrays() {
        let r = parse(b"*2\r\n$1\r\na\r\n*2\r\n:1\r\n$-1\r\n")
            .await
            .unwrap();
        assert_eq!(
            r,
            Reply::Array(vec![
                Reply::Bulk(b"a".to_vec()),
                Reply::Array(vec![Reply::Int(1), Reply::Nil]),
            ])
        );
    }

    #[tokio::test]
    async fn a_closed_connection_is_an_error_not_a_hang() {
        let (mut c, server) = pair();
        drop(server);
        assert!(c.reply().await.is_err());
    }

    #[tokio::test]
    async fn a_length_over_the_cap_is_refused_before_it_allocates() {
        let over = MAX_BULK + 1;
        assert!(parse(format!("${over}\r\n").as_bytes()).await.is_err());
    }

    /// Commands go out as RESP arrays of bulk strings, with byte lengths --
    /// the only framing that can carry a key with a NUL in it (ADR 0014 mode
    /// B puts a raw BLAKE3 hash on the wire as the key).
    #[tokio::test]
    async fn encodes_commands_with_byte_lengths() {
        let (mut c, mut server) = pair();
        c.send(&[b"GET", &[0x00, 0x41][..]]).await.unwrap();
        c.flush().await.unwrap();

        let mut got = vec![0u8; 21];
        tokio::io::AsyncReadExt::read_exact(&mut server, &mut got)
            .await
            .unwrap();
        assert_eq!(got, b"*2\r\n$3\r\nGET\r\n$2\r\n\x00A\r\n".to_vec());
    }

    /// Nothing reaches the socket until the caller flushes: that is what makes
    /// a pipeline one write instead of one per command.
    #[tokio::test]
    async fn send_does_not_write_until_flush() {
        let (mut c, mut server) = pair();
        c.send(&[b"PING"]).await.unwrap();

        let mut buf = [0u8; 16];
        let peek = tokio::time::timeout(
            Duration::from_millis(50),
            tokio::io::AsyncReadExt::read(&mut server, &mut buf),
        )
        .await;
        assert!(peek.is_err(), "bytes reached the socket before the flush");

        c.flush().await.unwrap();
        let n = tokio::io::AsyncReadExt::read(&mut server, &mut buf)
            .await
            .unwrap();
        assert_eq!(&buf[..n], b"*1\r\n$4\r\nPING\r\n");
    }
}
