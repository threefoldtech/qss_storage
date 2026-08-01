#!/usr/bin/env python3
"""10s read/write bandwidth samples for one block device, TSV to stdout.

Reads /proc/diskstats (sectors are 512 bytes) and emits
    epoch  read_mib_s  write_mib_s  util_pct
until killed. Companion to the sysstat 10-min sar record: sar is the
persistent coarse log, this is the fine curve for one campaign run.
"""
import sys
import time

DEV = sys.argv[1] if len(sys.argv) > 1 else "nvme1n1"
STEP = 10


def snap():
    with open("/proc/diskstats") as f:
        for line in f:
            parts = line.split()
            if parts[2] == DEV:
                # fields: 5=sectors read, 9=sectors written, 12=io ticks ms
                return int(parts[5]), int(parts[9]), int(parts[12])
    raise SystemExit(f"no device {DEV} in /proc/diskstats")


print("epoch\tread_mib_s\twrite_mib_s\tutil_pct", flush=True)
r0, w0, t0 = snap()
while True:
    time.sleep(STEP)
    r1, w1, t1 = snap()
    rd = (r1 - r0) * 512 / STEP / 1048576
    wr = (w1 - w0) * 512 / STEP / 1048576
    util = min(100.0, (t1 - t0) / (STEP * 1000) * 100)
    print(f"{int(time.time())}\t{rd:.1f}\t{wr:.1f}\t{util:.1f}", flush=True)
    r0, w0, t0 = r1, w1, t1
