# respcas

`respcas` is a Redis-compatible server using metastore as its backend. It is designed to work seamlessly with Redis clients and supports a subset of Redis commands, along with namespace management features.

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
| SET <key> <value> | Set the value of a key                   | `SET mykey somevalue`              |
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

Example usage:
```
NSSET mynamespace password mysecretpassword  # Set namespace password
NSSET mynamespace worm 1                    # Enable WORM mode
NSSET mynamespace lock 1                    # Lock namespace (read-only)
NSSET mynamespace public 0                  # Require authentication for read operations
```

## Known Limitations
- The `NSLIST` command currently collects all namespace names before sending the response. Future improvements will implement streaming responses to handle large numbers of namespaces more efficiently.

---

For project overview and build instructions, see the [main README](../README.md).
