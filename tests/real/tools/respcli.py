#!/usr/bin/env python3
"""A binary-safe RESP client for the campaign, and its throughput driver.

valkey-cli is the campaign's RESP client everywhere it can be, because
testing against the client operators actually use is the point. It cannot
be used for two things, and this tool exists for exactly those:

  - ADR 0014's mode B puts the BLAKE3 hash on the wire AS THE KEY: 32 raw
    bytes, NUL and all. valkey-cli builds its argv from C strings, so a key
    with an embedded NUL is truncated at the first one and the command that
    arrives is not the command that was meant. Every cas-mode assertion
    would be testing a 4-byte key.
  - a throughput number needs many in-flight commands over a stable set of
    connections. One valkey-cli process per operation measures process
    spawn, not respcas.

Nothing here grades anything. It moves bytes, reports what it moved, and
lets the phase script decide what that means.

Sub-commands
    cmd    one command; the reply goes to stdout in a chosen encoding
    bench  a fixed-duration load, TSV on stdout

Argument encodings, so a shell can express bytes it cannot hold:
    plain        utf-8 as written
    hex:a1b2..   raw bytes from hex
    @path        the file's bytes
    rand:N       N random bytes (incompressible, fresh every call)
"""
import argparse
import hashlib
import os
import socket
import sys
import threading
import time

CRLF = b"\r\n"


# --- wire ---------------------------------------------------------------

class Resp:
    """One connection. Speaks RESP2, buffers its own reads."""

    def __init__(self, host, port, timeout=30.0):
        self.sock = socket.create_connection((host, port), timeout=timeout)
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.buf = b""

    def close(self):
        try:
            self.sock.close()
        except OSError:
            pass

    def encode(self, args):
        out = [b"*%d\r\n" % len(args)]
        for a in args:
            out.append(b"$%d\r\n" % len(a))
            out.append(a)
            out.append(CRLF)
        return b"".join(out)

    def send(self, args):
        self.sock.sendall(self.encode(args))

    def _fill(self):
        chunk = self.sock.recv(1 << 16)
        if not chunk:
            raise ConnectionError("respcas closed the connection")
        self.buf += chunk

    def _line(self):
        while True:
            i = self.buf.find(CRLF)
            if i >= 0:
                line, self.buf = self.buf[:i], self.buf[i + 2:]
                return line
            self._fill()

    def _take(self, n):
        while len(self.buf) < n:
            self._fill()
        out, self.buf = self.buf[:n], self.buf[n:]
        return out

    def reply(self):
        """One reply. Errors come back as RespError, never raised here:
        a command that is SUPPOSED to fail is a test, not an accident."""
        line = self._line()
        kind, rest = line[:1], line[1:]
        if kind == b"+":
            return rest
        if kind == b"-":
            return RespError(rest)
        if kind == b":":
            return int(rest)
        if kind == b"$":
            n = int(rest)
            if n < 0:
                return None
            out = self._take(n)
            self._take(2)
            return out
        if kind == b"*":
            n = int(rest)
            if n < 0:
                return None
            return [self.reply() for _ in range(n)]
        raise ValueError("unparseable reply: %r" % line)

    def call(self, *args):
        self.send(list(args))
        return self.reply()

    def select(self, ns, password=None):
        if not ns:
            return None
        args = [b"SELECT", tob(ns)]
        if password:
            args.append(tob(password))
        return self.call(*args)


class RespError(bytes):
    """An -ERR reply. A bytes subclass so it prints and compares like one."""


# --- argument decoding ---------------------------------------------------

def tob(v):
    return v if isinstance(v, bytes) else str(v).encode()


def decode_arg(a):
    if a.startswith("hex:"):
        return bytes.fromhex(a[4:])
    if a.startswith("@"):
        with open(a[1:], "rb") as f:
            return f.read()
    if a.startswith("rand:"):
        return os.urandom(int(a[5:]))
    return a.encode()


def render(reply, how):
    if isinstance(reply, list):
        return b"\n".join(render(r, how) for r in reply)
    if reply is None:
        return b"" if how == "raw" else b"(nil)"
    if isinstance(reply, int):
        return str(reply).encode()
    if isinstance(reply, RespError):
        return b"ERR-REPLY " + bytes(reply) if how != "raw" else bytes(reply)
    if how == "hex":
        return reply.hex().encode()
    if how == "len":
        return str(len(reply)).encode()
    return reply


# --- sub-command: cmd ----------------------------------------------------

def do_cmd(args):
    c = Resp(args.host, args.port)
    try:
        if args.select:
            r = c.select(args.select, args.password)
            if isinstance(r, RespError):
                sys.stderr.write("SELECT failed: %s\n" % r.decode("utf-8", "replace"))
                return 3
        reply = c.call(*[decode_arg(a) for a in args.args])
        out = render(reply, args.reply)
        sys.stdout.buffer.write(out)
        if args.reply != "raw":
            sys.stdout.buffer.write(b"\n")
        sys.stdout.buffer.flush()
        return 1 if isinstance(reply, RespError) else 0
    finally:
        c.close()


# --- sub-command: bench --------------------------------------------------

class Worker(threading.Thread):
    """One connection driving one operation with a fixed pipeline depth.

    Content is random per worker and stamped with a counter, so every value
    is unique and incompressible unless --dedup says otherwise. That
    distinction is the whole measurement in a cas namespace: unique content
    measures the ingest path, repeated content measures the dedup path, and
    a bench that did not choose would report an average of the two.
    """

    def __init__(self, opts, wid, deadline, stop):
        super().__init__(daemon=True)
        self.o, self.wid, self.deadline, self.stop = opts, wid, deadline, stop
        self.ops = 0
        self.bytes = 0
        self.errors = 0
        self.misses = 0
        self.first_error = None
        self.latencies = []
        self.keys = []

    def _value(self, n):
        buf = bytearray(self.base)
        if not self.o.dedup:
            buf[0:16] = ("%08d%08d" % (self.wid, n)).encode()
        return bytes(buf)

    def run(self):
        try:
            self._run()
        except Exception as e:  # a dead worker must not hang the bench
            self.errors += 1
            if self.first_error is None:
                self.first_error = "%s: %s" % (type(e).__name__, e)

    def _run(self):
        o = self.o
        self.base = os.urandom(o.size)
        c = Resp(o.host, o.port, timeout=o.timeout)
        try:
            r = c.select(o.select, o.password)
            if isinstance(r, RespError):
                self.errors += 1
                self.first_error = "SELECT: " + r.decode("utf-8", "replace")
                return

            n = 0
            depth = o.pipeline
            while not self.stop.is_set() and time.monotonic() < self.deadline:
                batch = []
                t0 = time.monotonic()
                for _ in range(depth):
                    args, nbytes = self._command(n)
                    batch.append(nbytes)
                    c.send(args)
                    n += 1
                for nbytes in batch:
                    rep = c.reply()
                    if isinstance(rep, RespError):
                        self.errors += 1
                        if self.first_error is None:
                            self.first_error = rep.decode("utf-8", "replace")
                    else:
                        self.ops += 1
                        # For reads the bytes moved are the bytes that came
                        # back, not the bytes that were asked for: counting
                        # the request size would credit a miss with a full
                        # value and report a read bandwidth nothing read.
                        if o.op in ("get",):
                            self.bytes += len(rep) if isinstance(rep, bytes) else 0
                            if rep is None:
                                self.misses += 1
                        else:
                            self.bytes += nbytes
                        if o.op == "cset" and isinstance(rep, bytes) and len(self.keys) < 64:
                            self.keys.append(rep.hex())
                self.latencies.append((time.monotonic() - t0) / max(1, depth))
        finally:
            c.close()

    def _command(self, n):
        o = self.o
        if o.op == "set":
            key = ("%s:%d:%d" % (o.prefix, self.wid, n)).encode()
            val = self._value(n)
            return [b"SET", key, val], len(val)
        if o.op == "cset":
            val = self._value(n)
            return [b"CSET", val], len(val)
        if o.op == "cset-empty":
            val = self._value(n)
            return [b"SET", b"", val], len(val)
        if o.op == "get":
            key = ("%s:%d:%d" % (o.prefix, self.wid, n % max(1, o.keyspace))).encode()
            return [b"GET", key], o.size
        if o.op == "exists":
            key = ("%s:%d:%d" % (o.prefix, self.wid, n % max(1, o.keyspace))).encode()
            return [b"EXISTS", key], 0
        raise ValueError("unknown op %s" % o.op)


def do_bench(args):
    stop = threading.Event()
    deadline = time.monotonic() + args.seconds
    workers = [Worker(args, i, deadline, stop) for i in range(args.connections)]

    t0 = time.monotonic()
    for w in workers:
        w.start()
    for w in workers:
        w.join(timeout=args.seconds + args.timeout + 10)
    stop.set()
    elapsed = time.monotonic() - t0

    ops = sum(w.ops for w in workers)
    nbytes = sum(w.bytes for w in workers)
    errors = sum(w.errors for w in workers)
    misses = sum(w.misses for w in workers)
    lat = [x for w in workers for x in w.latencies]
    lat.sort()
    first_error = next((w.first_error for w in workers if w.first_error), "")

    def pct(p):
        if not lat:
            return 0.0
        return lat[min(len(lat) - 1, int(len(lat) * p))] * 1000.0

    # TSV, one header and one row: the phase script reads it with awk and
    # the run keeps it verbatim as evidence.
    print("op\tseconds\tops\tbytes\tops_per_s\tmib_per_s\terrors\t"
          "p50_ms\tp95_ms\tp99_ms\tconnections\tpipeline\tvalue_bytes\tmisses\t"
          "first_error")
    print("%s\t%.3f\t%d\t%d\t%.1f\t%.2f\t%d\t%.3f\t%.3f\t%.3f\t%d\t%d\t%d\t%d\t%s" % (
        args.op, elapsed, ops, nbytes,
        ops / elapsed if elapsed > 0 else 0,
        nbytes / elapsed / 1048576 if elapsed > 0 else 0,
        errors, pct(0.50), pct(0.95), pct(0.99),
        args.connections, args.pipeline, args.size, misses, first_error))

    if args.keys_out:
        with open(args.keys_out, "w") as f:
            for w in workers:
                for k in w.keys:
                    f.write(k + "\n")
    return 2 if errors and args.fail_on_error else 0


# --- cli ------------------------------------------------------------------

def main(argv):
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("-H", "--host", default="127.0.0.1")
    p.add_argument("-p", "--port", type=int, default=16379)
    p.add_argument("-s", "--select", default="", help="namespace to SELECT first")
    p.add_argument("-w", "--password", default="", help="namespace password")
    sub = p.add_subparsers(dest="sub", required=True)

    c = sub.add_parser("cmd", help="one command")
    c.add_argument("--reply", default="text",
                   choices=["text", "raw", "hex", "len"])
    c.add_argument("args", nargs="+")
    c.set_defaults(fn=do_cmd)

    b = sub.add_parser("bench", help="fixed-duration load")
    b.add_argument("--op", default="set",
                   choices=["set", "get", "cset", "cset-empty", "exists"])
    b.add_argument("--seconds", type=float, default=10.0)
    b.add_argument("--connections", type=int, default=8)
    b.add_argument("--pipeline", type=int, default=1)
    b.add_argument("--size", type=int, default=4096)
    b.add_argument("--prefix", default="bench")
    b.add_argument("--keyspace", type=int, default=1000,
                   help="for read ops: how many distinct keys to cycle")
    b.add_argument("--dedup", action="store_true",
                   help="write identical content, to measure the dedup path")
    b.add_argument("--timeout", type=float, default=60.0)
    b.add_argument("--keys-out", default="",
                   help="write the first server-returned keys here")
    b.add_argument("--fail-on-error", action="store_true")
    b.set_defaults(fn=do_bench)

    args = p.parse_args(argv)
    return args.fn(args)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
