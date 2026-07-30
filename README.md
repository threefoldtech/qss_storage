# QSS Storage

Distributed and resilient object and block storage services. QSS Storage aggregates storage capacity from nodes into a unified pool accessible through standard protocols, ensuring data redundancy and availability across a decentralized node network.

## What this is

QSS Storage is a storage subsystem that provides content-addressed storage backends exposed through familiar APIs. It is designed to run on decentralized infrastructure where storage is contributed by individual nodes and made available as a coherent service layer. The system focuses on efficiency, deduplication, and protocol compatibility.

## What this repository contains

1. **[s3cas](./s3cas/README.md)** — An S3-compatible API server using content-addressed storage. [Read more about s3cas →](./s3cas/README.md)
2. **[respd](./respd/README.md)** — A Redis-compatible server using metastore as backend. [Read more about respd →](./respd/README.md)

## Role in the stack

QSS Storage operates as the storage layer, sitting below the application and virtualization stacks. It provides persistent object and key-value storage to workloads running on grid nodes. The S3-compatible server allows existing applications to use standard S3 clients, while the Redis-compatible server offers a fast key-value interface for caching and session storage. QSS Storage is designed to work alongside the node operating system and network layers to provide reliable storage without centralized dependencies.

## ZOS / Zero-OS

ZOS, also known as Zero-OS, is the operating system layer used to run and manage nodes. It provides the low-level runtime environment for workloads, networking, storage, and automation. QSS Storage storage services are deployed on nodes running ZOS and integrate with its resource management and isolation mechanisms.

## Relation to ThreeFold

This technology is used within the ThreeFold ecosystem and was first deployed on the ThreeFold Grid. The component itself is designed as reusable infrastructure technology and should be understood by its technical function first, independent of any specific deployment.

## Ownership

This repository is owned and maintained by TF-Tech NV, a Belgian company responsible for the development and maintenance of this technology.

## Building

To build the project, use the standard Rust tools:

```bash
git clone https://github.com/threefoldtech/qss_storage
cd qss_storage

# Build all components
cargo build --release

# Or build individual components
cargo build --release -p s3cas
cargo build --release -p respd
```

With this feature, the data blocks will be deleted when they aren't used anymore.

## Configuration

Both servers read a single TOML file, `qss_storage.toml`. Start from
[`qss_storage.toml.example`](./qss_storage.toml.example), which documents every
key and its default:

```bash
cp qss_storage.toml.example qss_storage.toml
$EDITOR qss_storage.toml
cargo run -p s3cas -- server          # no flags needed
cargo run -p respd
```

Settings are resolved per field, highest precedence first:

1. **CLI flags** -- `--port 9000` beats whatever the file says about the port,
   and nothing else.
2. **The config file** -- the first of: the path given to `--config`, then
   `./qss_storage.toml`, then `/etc/qss_storage/qss_storage.toml`. A `--config`
   path that does not exist is an error rather than a fall-through, and each
   binary logs at startup which file it loaded (or that it found none).
3. **Built-in defaults** -- listed in the example file next to each key.

An unknown key anywhere in the file is a startup error: a misspelled setting
should stop the daemon, not be silently ignored.

The `[store]` table is shared by both binaries; `[s3]` is read by s3cas only
and `[resp]` by respd only. `[store.hash]` applies when a store is *created*:
an existing store is addressed by the hash recorded in its immutable header, so
a config that disagrees with the store it opened gets a warning at startup, and
changing the block hash of a deployment means creating a new store.

## respd Features

The respd server implements some basic Redis commands and also provides additional commands for namespace and data management that are not part of the standard Redis protocol.

## Known issues

### s3cas
- Only the basic S3 API is implemented (no copy between servers)
- Single key only, no support to add multiple keys with different permissions

### respd
- Limited subset of Redis commands implemented

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.
