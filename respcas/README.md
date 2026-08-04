# respcas

`respcas` is a Redis-compatible server over the cas-storage engine. It works
with any Redis client, supports a subset of Redis commands, and adds
namespace management.

A namespace stores values under keys the client chooses, or -- since ADR 0014
-- under the BLAKE3-256 of the value itself. See
[Content-addressed namespaces](#content-addressed-namespaces).

## Store layout

A respcas data directory is the same meta-plus-blocks pair every other store
in this workspace is, which is what lets `qss-storage-fsck` and the s3cas
`inspect` subcommands walk it:

```text
<data_dir>/store_header.bin   the sidecar copy of the QSST header
<data_dir>/db/                the namespace metadata database
<data_dir>/blocks/            block data files, and the blocks database
```

A store created before that layout keeps its database in `<data_dir>` itself
and is opened there; it gains `blocks/` on first open by a build that has it.
Nothing is moved and no migration runs.

## Running respcas

```console
respcas --data-dir=/tmp/respcas/data
```

With admin authentication:

```console
respcas --data-dir=/tmp/respcas/data --admin=mypassword
```

By default, respcas listens on `127.0.0.1:6379` and can be accessed using any Redis client.

## Supported Commands

| Command           | Description                              | Example Usage                      |
|-------------------|------------------------------------------|------------------------------------|
| GET <key>         | Get the value of a key                   | `GET mykey`                        |
| MGET <key>...     | Get the values of multiple keys          | `MGET key1 key2`                   |
| SET <key> <value> | Set the value of a key. In a `cas` namespace the key is the value's address: send an empty key to have the server compute it (the reply is the 32-byte key), or the 32-byte key you computed yourself | `SET mykey somevalue` |
| CSET <value>      | Store a value by its content and reply with its 32-byte key. `cas` namespaces only; the same operation as `SET "" <value>` | `CSET somevalue`      |
| DEL <key>         | Delete a key                             | `DEL mykey`                        |
| EXISTS <key>      | Check if a key exists                    | `EXISTS mykey`                     |
| PING [message]    | Ping the server (optionally with message)| `PING` or `PING hello`             |
| ECHO <message>    | Return the message unchanged, byte for byte | `ECHO hello`                    |
| CHECK <key>       | Verify data integrity for a key          | `CHECK mykey`                      |
| LENGTH <key>      | Get the size (in bytes) of a key's value, returns nil if key doesn't exist | `LENGTH mykey`                     |
| KEYTIME <key>     | Get the last-update timestamp of a key (Unix time), returns nil if key doesn't exist | `KEYTIME mykey`                    |
| AUTH <password>   | Authenticate as admin                    | `AUTH mypassword`                  |
| SELECT <namespace> [password]| Switch to a different namespace (with optional password for protected namespaces) | `SELECT mynamespace` or `SELECT mynamespace mypassword` |
| NSNEW <n>      | Create a new namespace (admin only)      | `NSNEW mynamespace`                |
| NSINFO <n>     | Show info about a namespace              | `NSINFO mynamespace`               |
| NSLIST           | List all available namespaces            | `NSLIST`                           |
| NSSET <n> <prop> <val> | Set a property for a namespace (admin only) | `NSSET mynamespace worm 1`         |
| DBSIZE           | Get the number of keys in the current namespace (approximate) | `DBSIZE`                          |
| SCAN [cursor]     | Incrementally iterate over keys in the current namespace | `SCAN 0` or `SCAN mycursor`        |
| RSCAN [cursor]    | Incrementally iterate over keys in backward direction | `RSCAN 0` or `RSCAN mycursor`      |

- All commands are case-insensitive.
- Commands may be sent as RESP arrays or as inline text (`SET key value`
  followed by a newline), the plain form a telnet session or
  `valkey-cli --pipe` sends. Inline arguments split on whitespace, and
  `"..."` or `'...'` group a value that contains spaces.
- Namespace commands (`SELECT`, `NSNEW`, `NSINFO`, `NSLIST`) allow multi-tenant data separation.
- Authentication with `AUTH` is only applicable if the server was started with the `--admin` parameter.
- Some commands (like `NSNEW`) require admin privileges.

## Features
- Redis protocol compatibility (subset)
- Namespace support with configurable properties
- Data integrity checking
- Simple to run and integrate

## Namespace Properties

Namespaces can be configured with various properties using the `NSSET` command. These properties control the behavior and security of the namespace.

| Property | Values | Description |
|----------|--------|-------------|
| `password` | string | Sets a password for the namespace. When set, users must provide the password when using the `SELECT` command to switch to that namespace. Without authentication, write operations are denied and read operations may be restricted based on the `public` property. |
| `worm` | 0 or 1 | Write Once Read Many mode. When enabled (1), keys cannot be modified or deleted once written. |
| `lock` | 0 or 1 | Temporarily locks the namespace. When enabled (1), write operations are not allowed. |
| `public` | 0 or 1 | Controls read access. When disabled (0), users must authenticate to perform read operations like GET and MGET. Default is enabled (1). |
| `key_mode` | `userkey` or `cas` | What a key means here. `userkey` (the default) stores values under the key the client chose. `cas` makes the key the BLAKE3-256 of the value -- see below. Only settable while the namespace holds no keys. |

Example usage:
```
NSSET mynamespace password mysecretpassword  # Set namespace password
NSSET mynamespace worm 1                    # Enable WORM mode
NSSET mynamespace lock 1                    # Lock namespace (read-only)
NSSET mynamespace public 0                  # Require authentication for read operations
NSSET mynamespace key_mode cas              # Address records by content (empty namespaces only)
```

## Content-addressed namespaces

A namespace with `key_mode = cas` keys every record by the BLAKE3-256 of its
value: 32 raw bytes on the wire, which any client can compute for itself
(`b3sum`). Two ways in, one storage path:

- **The server hashes it.** `SET "" <value>`, or `CSET <value>`. The reply is
  the 32-byte key.
- **You hashed it.** `SET <32-byte key> <value>`. Anything but 32 bytes is an
  error, and on a key nothing has stored yet the value is verified against it
  before it is written.

That gives the dedup-upload workflow: `EXISTS <key>`, and transfer only on a
miss. Storing content that is already in the store writes nothing -- the same
namespace acknowledges and drops the bytes, and another content-addressed
namespace's copy is referenced rather than copied.

A few consequences worth stating:

- **`EXISTS` is advisory outside `worm`.** Another client may `DEL` the key
  between your probe and the upload you skipped because of it. Nothing
  mechanically prevents that; a namespace with `worm` set refuses `DEL`, and
  there an `EXISTS` answer is permanent. This is the same trade git makes
  with a repo that can be force-pruned concurrently.
- **A record is immutable per key.** Re-`SET`ting a key that is present is an
  idempotent success: the content cannot differ from what the key names, so
  there is nothing to overwrite. `DEL` is the only real mutation.
- **`CHECK` re-hashes the value against its key**, which is a stronger check
  than the MD5 comparison it makes in a user-keyed namespace, and works for
  values too big to live in their record.
- **Values above the inline threshold are stored as blocks**, deduplicated
  across the whole store, and read back whole by `GET`.
- **`SCAN`/`RSCAN` enumerate hash keys** in tree order, and their cursor is
  the last key of a page -- binary, like the keys.
- **Ingest is buffer-then-write**, bounded by `resp.max_value_size` (64 MiB by
  default). A command declaring a longer value is refused before its bytes
  are read. Objects larger than that belong on the S3 face, which has
  multipart.

## Known Limitations
- The `NSLIST` command currently collects all namespace names before sending the response. Future improvements will implement streaming responses to handle large numbers of namespaces more efficiently.

---

For project overview and build instructions, see the [main README](../README.md).
