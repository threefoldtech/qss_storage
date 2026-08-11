# The S3 access log: one line per request, off the request path

**Status**: Proposed
**Date**: 2026-08-11

---

## Context

On 2026-08-10 the server logged this four times, hours apart:

```
ERROR s3s::ops: error=S3Error { code: AccessDenied, message: "Signature is required", .. }
ERROR s3s::ops: failed to prepare err=S3Error(Inner { code: AccessDenied, ... })
```

Nothing in the deployment could answer the only question worth asking:
who sent it, and what did they ask for. Three separate places drop the
answer on the floor:

1. **The peer address never leaves the accept loop.** `s3cas/src/main.rs:463`
   accepts with `Ok((socket,_))`. That `_` is the `SocketAddr`. No layer
   below it can name the client because no layer below it was told.
2. **The request is logged only at DEBUG.** s3s does `debug!(?req)` at the
   top of `S3Service::call` (service.rs:614). At the `info` default that
   is off, and turning it on dumps entire request structs for every call
   -- a developer's tool, not an operator's.
3. **The error line has no request context.** `error!(?err, "failed to
   prepare")` (ops/mod.rs:235) carries the S3 error and nothing else: no
   method, no URI, no status, no timing.

The message itself is the one piece of signal available. "Signature is
required" is raised at `route.rs:19` and `access/mod.rs:12` only when
there is neither an `Authorization` header nor a presigned query string.
A client holding the wrong keys fails later and differently, with
`SignatureDoesNotMatch`. So these were anonymous HTTP requests: a
scanner, a health check, or a browser. Which one, we cannot say.

Raising `RUST_LOG` is not the fix. It cannot be aimed at its own sink, it
cannot be shaped, its volume is set by a dependency's idea of debug
output, and it interleaves with the service log. What the deployment
wants is the ordinary thing every storage endpoint ships: an access log,
on or off by configuration, in a format existing tools already read.

Related: ADR 0010 and ADR 0013 established that nothing off the data path
may be allowed to slow or endanger it. That constraint governs the design
here more than any other.

---

## Decision

s3cas gains an access log: **one record per HTTP request on the S3
listener**, in AWS S3 server access log field order, written to a file
(or to stdout when the path is `-`), **disabled by default**, enabled by
config file or CLI flag, decided once at startup.

Four properties are load-bearing:

- **It sits outside s3s, not inside it.** The record is produced by a
  wrapper around the built `S3Service`, at the `let hyper_service =
  service;` seam in `main.rs:404`. That position sees the peer address,
  sees requests s3s rejects before dispatch (exactly the case that
  prompted this), and sees the final HTTP status, which the s3s error
  log does not carry.
- **Formatting never runs on the request task.** The wrapper builds a
  small owned `Record` and sends it on a bounded channel. Rendering to
  text and writing to the file happen in a dedicated writer task.
- **It cannot block a request, ever.** The channel send is non-blocking.
  If the queue is full, the record is dropped and a counter is
  incremented. A stalled disk degrades logging, never the endpoint.
- **It is not durable, and does not claim to be.** Records are buffered
  and flushed periodically, never fsynced. This is an operational log,
  not an audit-of-record. See "What an Expert Would Ask".

---

## Architecture Overview

### Component Breakdown

1. **`AccessLogConfig`** (`cas-storage/src/config.rs`)
   - The `[s3.access_log]` sub-table, sibling of `[s3.metrics]`:
     `enabled: Option<bool>`, `path: Option<String>`,
     `queue_capacity: Option<usize>`.
   - Defaults live beside the existing `DEFAULT_*` consts:
     `DEFAULT_ACCESS_LOG_ENABLED = false`,
     `DEFAULT_ACCESS_LOG_QUEUE = 8192`.
   - Merged by `resolve_server` under the established rule (flag over
     file over default). `--access-log <PATH>` sets path and implies
     enabled; `--no-access-log` forces off and outranks the file.
   - `enabled = true` with no path is a refusal to start, phrased like
     the existing credential refusal, not a silent fallback.

2. **`Record`** (`s3cas/src/access_log/record.rs`)
   - One owned struct per request, built from the request head plus the
     response head. Cheap to construct, `Send`, no borrows.
   - Holds the derivations that need request context: bucket/key split,
     operation name, requester, error code.

3. **`format`** (`s3cas/src/access_log/format.rs`)
   - `Record -> String` in S3 field order. Absent fields are `-`.
     Quoted fields are quoted; embedded quotes and control characters
     are escaped so one request is always exactly one line.

4. **`sink`** (`s3cas/src/access_log/sink.rs`)
   - `AccessLogSink` handle (a bounded `mpsc::Sender<Record>`) plus the
     writer task: `BufWriter` over the file or stdout, drained in
     batches, flushed when the queue empties or every 1s, whichever
     comes first. Shuts down with the same signal that ends the accept
     loop, flushing what it holds.

5. **`AccessLogged<S>`** (`s3cas/src/access_log/service.rs`)
   - The hyper `Service` wrapper. Per call: stamp start, capture peer
     addr, mint a request id, call inner, stamp turn-around, wrap the
     response body in a counting body, inject `x-amz-request-id`, and
     emit the record when the body completes or is dropped.

6. **Wiring** (`s3cas/src/main.rs`)
   - Peer address kept at accept (`Ok((socket, peer))`), sink built when
     enabled, `hyper_service` wrapped. The metrics listener is not
     wrapped: it is a separate listener with a separate purpose.

7. **Counters** (`s3cas/src/metrics.rs`)
   - `access_log_records_total`, `access_log_records_dropped_total`.
     The dropped counter is the only honest way to know the log is
     incomplete.

### Data Flow

```
accept() -> (socket, peer)
   |
   v
AccessLogged<S3Service>            request task
   |  t0, request_id, peer
   +--> inner.call(req) ---------> s3s -> S3Cas
   |        |
   |        +-- response head (status, content-length)
   |        |   t_turnaround
   |        v
   +--> CountingBody wraps resp body
            |  (bytes counted as they stream)
            v
        body end or drop -> t_total
            |
            v
        Record -> try_send ------> [bounded mpsc, cap 8192]
            |                            |
       (full: drop + counter)            v
                                  writer task: format -> BufWriter -> file | stdout
```

### Field mapping

The AWS field order, and what s3cas can honestly put in each:

| # | Field | Source |
|---|-------|--------|
| 1 | Bucket Owner | `-` (no such concept here) |
| 2 | Bucket | path-style first segment, else `-` |
| 3 | Time | `[11/Aug/2026:12:36:12 +0000]` |
| 4 | Remote IP | `peer.ip()` |
| 5 | Requester | access key from `Credential=`, else `-` |
| 6 | Request ID | minted per request, also returned as a header |
| 7 | Operation | synthesized, e.g. `REST.GET.OBJECT` |
| 8 | Key | remainder of the path, else `-` |
| 9 | Request-URI | `"GET /bucket/key HTTP/1.1"`, signature redacted |
| 10 | HTTP status | response status |
| 11 | Error Code | `<Code>` peeked from the in-memory error body |
| 12 | Bytes Sent | counted by the body wrapper |
| 13 | Object Size | `Content-Length` when present, else `-` |
| 14 | Total Time | t0 to body end, ms |
| 15 | Turn-Around Time | t0 to response head, ms |
| 16 | Referer | header, else `-` |
| 17 | User-Agent | header, else `-` |
| 18 | Version Id | `-` (no versioning) |
| 19 | Host Id | `-` |
| 20 | Signature Version | `SigV4` when the header says so, else `-` |
| 21 | Cipher Suite | `-` (s3cas terminates no TLS) |
| 22 | Authentication Type | `AuthHeader` / `QueryString` / `-` |
| 23 | Host Header | `Host` header |
| 24 | TLS version | `-` |
| 25 | Access Point ARN | `-` |
| 26 | aclRequired | `-` |

Bucket and key parsing is exact rather than heuristic because s3cas never
calls `S3ServiceBuilder::set_host`: virtual-hosted-style is not enabled,
so every request is path-style. **If `set_host` is ever configured, this
parser must follow it in the same commit.**

The 2026-08-10 probe would have rendered as:

```
- - [10/Aug/2026:12:36:12 +0000] 45.83.64.7 - 3B2A9C1D4E5F6071 REST.GET.SERVICE -
"GET / HTTP/1.1" 403 AccessDenied 226 - 0 0 "-" "zgrab/0.x" - - - - - - - - -
```

Peer, status, error code, operation, agent. Four fields that were not
available on the night, in one grep-able line.

---

## Alternatives Considered

### A dedicated tracing target with its own layer

- **The idea**: emit `info!(target: "access", ...)` and give that target
  its own `tracing_subscriber` layer pointed at a file.
- **Optimizes for**: no new machinery; reuses what is already wired.
- **Sharpest tradeoff**: the on/off switch becomes an `EnvFilter`
  directive, which is precisely what was rejected. Field escaping and
  line shape become the fmt layer's business, so the format is not ours
  to guarantee.
- **Bets on**: operators being willing to express "log accesses" as a
  filter directive.

### Combined Log Format instead of the S3 format

- **The idea**: the Apache/nginx line, parseable by goaccess, lnav, and
  every Loki example on the internet.
- **Optimizes for**: zero-configuration analysis with generic web tools.
- **Sharpest tradeoff**: it has no field for bucket, key, operation, or
  S3 error code. The 2026-08-10 line would say `403` and stop, losing
  `AccessDenied` and the fact that no signature was offered at all.
- **Bets on**: analysis being about traffic shape rather than S3
  semantics. Revive this if the log's main consumer turns out to be a
  generic dashboard; the renderer is one function, so a `format =
  "combined"` knob is cheap to add later.

### Log from inside `S3Cas` (the `S3` trait impl)

- **The idea**: record each operation where the handlers already are.
- **Optimizes for**: full access to bucket, key, and the real operation
  name, with no synthesis.
- **Sharpest tradeoff**: it cannot see anything s3s rejects before
  dispatch. Unsigned requests, unroutable URIs, and unknown operations
  are invisible -- the entire class of event that motivated this ADR.
- **Bets on**: the interesting traffic being well-formed. The incident
  says otherwise.

### Push the hook upstream into s3s

- **The idea**: ask Nugine for a per-request completion hook carrying the
  resolved operation name, error code, and peer address.
- **Optimizes for**: correctness of exactly the two fields this design
  has to synthesize (operation, error code).
- **Sharpest tradeoff**: it is a round trip through someone else's
  review queue for a feature we need now, and s3s does not currently
  carry the peer address either, so the upstream change is larger than
  it sounds.
- **Bets on**: upstream wanting this shape. Worth proposing regardless
  (the house rule prefers upstream over local workarounds), but the
  wrapper is needed either way for the peer address and the timing, so
  this is an improvement to two fields, not an alternative design.

---

## Consequences

### Positive

- The question "who is calling this endpoint" becomes answerable from a
  file, with no restart, no debug build, and no packet capture.
- The response status and S3 error code land in the same record as the
  request line, which the s3s error log has never done.
- `x-amz-request-id` starts being returned, so a client-side complaint
  can be matched to a server-side line.
- Standard tooling written against real S3 access logs applies unchanged.

### Negative

- Two fields are synthesized rather than sourced: the operation name and
  the error code. They can drift from what s3s actually resolved.
- Without a SIGHUP reopen (deliberately out of scope), rotation requires
  `copytruncate` in the logrotate stanza. A plain `create` rotation
  leaves s3cas writing to the renamed inode until restart. **This must be
  in the operator docs, not just here.**
- One more startup knob, one more merge path in `resolve_server`, one
  more thing to get wrong in a config file.

### Risks

- **A slow or full disk.** Mitigated structurally: bounded queue,
  non-blocking send, drop-and-count. The endpoint's latency is
  independent of the log device by construction.
- **Capability leak through the URI field.** A presigned URL carries
  `X-Amz-Signature` in its query string; anyone who can read the log
  could replay that URL until it expires. Mitigation: the value of
  `X-Amz-Signature` is replaced with `REDACTED` before the record is
  built. `X-Amz-Credential` is kept -- it names the key, it does not
  authorize anything.
- **Silent incompleteness.** A log with holes that looks complete is
  worse than no log. Mitigated by the dropped counter plus a single
  `warn!` the first time a drop occurs.
- **Unbounded growth.** No size cap is implemented; disk usage is the
  operator's problem via logrotate. Called out rather than solved.

---

## What an Expert Would Ask

**Q: The operation name is s3s's business. How can a wrapper outside s3s
produce `REST.GET.OBJECT` correctly?**
A: It cannot, exactly. s3s resolves the operation from method, path,
query keys, and headers, and only the resolved op knows its own name.
The wrapper re-derives it from method plus path shape plus the handful of
query markers that matter (`uploads`, `uploadId`, `delete`, `list-type`,
`acl`). For the operations s3cas implements this is a small table and it
will agree; for anything it does not recognise it emits
`REST.<METHOD>.UNKNOWN` rather than guessing. The honest fix is the
upstream hook described above. Until then, the log's operation field is
best-effort and the request line beside it is authoritative -- which is
why field 9 is never synthesized.

**Q: What does the record say when a client disconnects halfway through a
GET?**
A: The status is the one that was sent in the head (200), bytes sent is
what actually reached the socket, and total time runs to the drop. The
record is emitted from the body wrapper's `Drop`, not only on clean end,
so an aborted transfer produces a line rather than vanishing. A 200 with
bytes sent far below object size is exactly how a truncated download
looks, and that is a diagnosis, not a defect.

**Q: What does this cost per request?**
A: On the request task: one `Instant::now()` pair, a request-id mint, a
handful of header lookups, one small allocation for the `Record`, and a
non-blocking channel send. No formatting, no I/O, no lock held across an
await. Rendering and writing happen on the writer task. The intended
answer is "unmeasurable next to a block write", and the acceptance bar
below says how that gets checked rather than asserted.

**Q: Can records be lost, and does that make the log useless for audit?**
A: Yes, two ways. Under sustained overload the bounded queue drops
records (counted, never silent). On an unclean stop the unflushed tail of
the `BufWriter` is lost, because the access log is explicitly not
fsynced. That is the right trade for an operational log and the wrong one
for an audit log. Making it an audit log would mean an fsync per record
or per batch, which reintroduces exactly the convoy ADR 0013 spent a
campaign removing from the ack path. If audit is ever the requirement, it
should be a separate sink with its own ADR, not a flag on this one.

**Q: Why is the metrics listener excluded?**
A: It serves one route to a scraper, typically every 15s, and its
requests carry no bucket, key, or requester. Logging them would add a
constant background hum to a file whose value is that unusual lines stand
out. If a scraper's identity ever matters, that is a firewall question.

---

## Implementation Plan

### Decisions you will probably want to tweak

- **Field format: AWS S3 server access log order.**
  - *Alternative*: Combined Log Format, or JSON lines.
  - *Cost to change later*: one function plus its golden tests, if and
    only if the renderer stays the only place that knows the field
    order. Keep `Record` format-agnostic and this stays a `format =`
    knob; leak field order into the wrapper and it becomes a refactor.

- **`Record` field set.**
  - *Alternative*: carry the whole request head and response head and
    derive at render time.
  - *Cost to change later*: cheap while it is one struct in one module;
    the reason not to carry the heads is that it moves work onto the
    request task and keeps borrowed data alive across the queue.

- **Config surface: `enabled` + `path` + `queue_capacity`, restart to
  change.**
  - *Alternative*: SIGHUP reopen, or a control endpoint on the metrics
    port.
  - *Cost to change later*: additive. A SIGHUP reopen can be bolted onto
    the writer task without touching the record or format layers, which
    is the reason the writer owns the file handle rather than main.

- **Drop policy: drop the newest, count it.**
  - *Alternative*: block the request task, or drop the oldest.
  - *Cost to change later*: one branch. Stated as a decision because
    "block" is the default people reach for and it is the one option
    this ADR forbids.

### Known unknowns and how the plan absorbs them

- **Whether the synthesized operation name is good enough.** Default:
  ship the table. Pivot signal: the first time a real diagnosis is
  wrong because the field lied -- then upstream the hook.
- **Whether 8192 is the right queue depth.** Default: 8192 records,
  roughly a second of a very busy endpoint. Pivot signal: a non-zero
  drop counter on a healthy run; the depth is configurable precisely so
  that pivot costs a config line, not a release.
- **Whether operators want more than one format.** Default: one format,
  no knob. Pivot signal: the first request for goaccess output.

### The mechanical work

Six components, listed above: config struct plus defaults plus the
`resolve_server` merge and two flags; `Record`; `format`; `sink`;
`AccessLogged<S>` with its counting body; the `main.rs` wiring and two
counters. `main.rs` is 666 lines today and takes only the wiring, so the
under-800 rule holds without a split.

Tests:
- Golden line tests over the renderer: signed GET, unsigned probe,
  multipart part upload, aborted GET.
- Escaping: a key containing a quote, a newline, and a control byte
  still produces exactly one line.
- Redaction: a presigned URI logs `X-Amz-Signature=REDACTED`.
- Drop accounting: a sink with capacity 1 and a stalled writer
  increments the counter and does not block the sender.
- End-to-end: an unsigned request against a live server produces a line
  whose fields 10 and 11 are `403` and `AccessDenied`.
- Acceptance for the cost claim: the existing A/B rig at cap 64, access
  log off vs on to `/dev/null`, expected within run-to-run noise.

Review asks:

1. `[s3.access_log]` with `enabled` + `path` + `queue_capacity`, and
   flags `--access-log <PATH>` / `--no-access-log`: right surface, or do
   you want the bare `--access-log` path to be the only switch?
2. Emitting `x-amz-request-id` on every response is a visible protocol
   change (a new header clients did not see before). In or out?
3. Redacting `X-Amz-Signature` in field 9: agreed, or do you want the
   URI byte-exact?
4. Off by default, or on-to-stdout by default so a fresh deployment logs
   something without being asked?

---

## Open Questions

**Architecture-changers**

- [ ] Should the same sink serve respcas eventually? If yes, `Record`
      and `sink` belong in `cas-storage`, not `s3cas`, and that is
      decided now or paid for twice.

**Behavior definers**

- [ ] Are health-check style requests (`GET /` with no bucket) worth a
      suppression rule, or does the noise argue for filtering at
      analysis time?
- [ ] On writer-task failure (disk full, file removed), does the server
      keep serving with logging dead and a loud warning, or refuse to
      continue? This ADR assumes the former.
