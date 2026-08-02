#!/usr/bin/env python3
# Dump a fjall 3.1.8 journal (.jnl): batches of Start/Item*/End+magic.
# Wire format from src/journal/entry.rs at tag 3.1.8 (6debe70).
# usage: dump-journal.py <file.jnl> [--grep SUBSTR] [--tail N]
import struct
import sys

MAGIC = b"FJL\x03"


def parse(path):
    data = open(path, "rb").read()
    pos = 0
    batches = []
    err = None
    while pos < len(data):
        start = pos
        tag = data[pos]
        if tag != 1:
            err = (start, f"expected Start tag 1, got {tag}")
            break
        if pos + 13 > len(data):
            err = (start, "truncated Start")
            break
        item_count, seqno = struct.unpack_from("<IQ", data, pos + 1)
        pos += 13
        items = []
        broken = False
        for _ in range(item_count):
            if pos >= len(data) or data[pos] not in (2, 4):
                err = (start, f"batch seqno={seqno}: bad item tag at {pos}")
                broken = True
                break
            if data[pos] == 4:  # Clear
                (ks,) = struct.unpack_from("<Q", data, pos + 1)
                items.append((ks, b"<CLEAR>", 0))
                pos += 9
                continue
            # Item: tag u8, value_type u8, compression u8, ks u64,
            # key_len u16, value_len u32, disk_len u32, key, value
            if pos + 21 > len(data):
                err = (start, f"batch seqno={seqno}: truncated item hdr")
                broken = True
                break
            vt = data[pos + 1]
            comp = data[pos + 2]
            ks, klen, vlen, dlen = struct.unpack_from("<QHII", data, pos + 3)
            pos += 21
            if pos + klen + dlen > len(data):
                err = (start, f"batch seqno={seqno}: truncated item body")
                broken = True
                break
            key = data[pos : pos + klen]
            pos += klen + dlen
            items.append((ks, key, vt))
        if broken:
            break
        # End: tag u8, checksum u64, magic
        if pos + 9 + len(MAGIC) > len(data):
            err = (start, f"batch seqno={seqno}: truncated End")
            break
        if data[pos] != 3:
            err = (start, f"batch seqno={seqno}: expected End tag, got {data[pos]}")
            break
        if data[pos + 9 : pos + 9 + len(MAGIC)] != MAGIC:
            err = (start, f"batch seqno={seqno}: bad trailer magic")
            break
        pos += 9 + len(MAGIC)
        batches.append((start, seqno, items))
    tail = data[pos:]
    return batches, pos, err, tail


def fmt_key(k):
    # Render every byte: printable ASCII as-is, the rest as \xNN. Full length.
    out = []
    for b in k:
        if 31 < b < 127:
            out.append(chr(b))
        else:
            out.append(f"\\x{b:02x}")
    return "".join(out)


def main():
    path = sys.argv[1]
    grep = None
    tail_n = None
    if "--grep" in sys.argv:
        grep = sys.argv[sys.argv.index("--grep") + 1].encode()
    if "--tail" in sys.argv:
        tail_n = int(sys.argv[sys.argv.index("--tail") + 1])

    batches, endpos, err, tail = parse(path)
    print(f"{path}: {len(batches)} batches, parsed to offset {endpos}")
    if err:
        print(f"PARSE STOP at offset {err[0]}: {err[1]}")
    nz = tail.lstrip(b"\x00")
    print(f"unparsed tail: {len(tail)} bytes, {len(nz)} non-zero-prefix bytes")

    seqnos = [b[1] for b in batches]
    gaps = []
    for a, b in zip(seqnos, seqnos[1:]):
        if b != a + 1:
            gaps.append((a, b))
    print(f"seqno range: {seqnos[0]}..{seqnos[-1]}" if seqnos else "no batches")
    print(f"seqno gaps (prev,next): {gaps[:20]}{' ...' if len(gaps) > 20 else ''}")

    show = []
    if grep:
        show = [b for b in batches if any(grep in k for _, k, _ in b[2])]
        print(f"batches matching {grep!r}: {len(show)}")
    elif tail_n:
        show = batches[-tail_n:]
    for off, seqno, items in show:
        keys = ", ".join(
            f"ks{ks}:{fmt_key(k)}{'(del)' if vt != 0 else ''}" for ks, k, vt in items[:8]
        )
        more = f" +{len(items) - 8} more" if len(items) > 8 else ""
        print(f"  off={off} seqno={seqno} items={len(items)}: {keys}{more}")


if __name__ == "__main__":
    main()
