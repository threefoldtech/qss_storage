# respload

A RESP2 load generator and binary-safe client. It drives respcas, and it drives
anything else that speaks RESP2 -- hero_db on its TCP port or its `resp.sock`, a
stock redis or valkey for a baseline. That portability is the point: a
throughput number is only worth something next to another one taken the same
way, by the same driver, over the same wire.

It grades nothing. It moves bytes and reports what it moved.

## Why it exists next to respcli.py

`tests/real/tools/respcli.py` stays where it is. It is the campaign's client,
and valkey-cli stays the client for everything valkey-cli can express, because
testing against the client operators actually use is the point.

What a Python driver cannot do is saturate a server: the GIL puts one
interpreter thread between every connection and its socket, so past a few tens
of thousands of operations per second the number being reported is the driver.
This tool exists for loads above that line, and for pointing the same load at a
server that is not respcas.

## Two sub-commands

    respload [-H host] [-p port] [--unix PATH] [-s select] [-w password] <cmd|bench>

`cmd` sends one command and renders the reply as `--reply text|raw|hex|len`.
`bench` drives a fixed-duration load and prints one TSV row -- the same fifteen
columns `respcli.py bench` prints, so the campaign's `awk` reads either.

Arguments accept encodings a shell cannot hold in a variable:

    plain        utf-8 as written
    hex:a1b2..   raw bytes from hex
    @path        the file's bytes
    rand:N       N pseudorandom bytes

`--select` names a respcas namespace, or a hero_db database index. The wire
shape is the same and the server decides what the argument means.

## Bench options that change what is being measured

    --op set|get|cset|cset-empty|exists|del
    --connections N --pipeline D     N connections, D commands in flight on each
    --size BYTES                     value size
    --dedup                          identical content, to measure the dedup path
    --verify                         read every written value back and compare it
    --seed N                         repeat a run's exact content (0 = from the clock)
    --keyspace N                     for reads: how many keys per connection to cycle
    --keys-out PATH                  where CSET's server-chosen keys go

Content is unique per command by default -- a stamp in the first sixteen bytes
of otherwise incompressible content -- so a content-addressed store ingests
every value. `--dedup` removes the stamp and every value becomes the same
bytes, which measures the dedup path instead. A driver that did not choose
between those would report the average of two different systems.

The seed defaults to the clock, and that is not cosmetic: a fixed seed means
the second run of `--op set` offers content the store already holds, so a
dedup-aware store answers from its index and the row reports an ingest rate
nothing achieved.

`--verify` reads each written value back on the same connection and compares it
byte for byte. It is not a throughput mode -- it doubles the round trips -- it
is how a server that mangles high bytes or truncates a value at a NUL gets
caught while it is busy rather than while it is idle. Mismatches are counted as
errors and named in `first_error`.

## Examples

    # respcas: 8 connections, 16 in flight each, 4 KiB values, 30 seconds
    respload -p 16379 bench --op set --seconds 30 --connections 8 --pipeline 16

    # the same load against hero_db, over TCP and then over its unix socket
    respload -p 6378 bench --op set --seconds 30 --connections 8 --pipeline 16
    respload --unix ~/hero/var/sockets/hero_db/resp.sock bench --op set --seconds 30

    # ADR 0014 mode B: CSET returns a raw BLAKE3 key, NULs and all
    respload -p 16379 -s casns bench --op cset --size 1024 --keys-out keys.txt
    respload -p 16379 -s casns cmd --reply len GET "hex:$(head -1 keys.txt)"

    # a value a shell variable would mangle, round-tripped
    respload -p 16379 cmd SET k hex:00ff0d0a41
    respload -p 16379 cmd --reply hex GET k

## Exit codes

    0  ran; for bench, or a command the server answered
    1  cmd only: the server answered with an error reply
    2  bench with --fail-on-error, and at least one command errored
    3  could not ask: connect failed, SELECT failed, bad arguments

## A note on pipeline depth

`--pipeline D` puts D commands on the wire before reading any reply. It is the
setting most likely to expose something about the server rather than the store:
a server that does not set `TCP_NODELAY` on its accepted sockets pays one
delayed-ACK stall per batch, and the whole batch takes ~41 ms no matter how
deep it is. If throughput falls as depth rises, that is the shape to check for.
