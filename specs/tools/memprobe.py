#!/usr/bin/env python3
"""What the server holds for a subscriber that never reads.

One connection subscribes and stops reading; another publishes. The reference
drops that subscriber at its 100 % pending mark (`max_pending`, 64 MiB by
default), so the honest memory figure for this shape is ~64 MiB plus frames in
flight — and the honest failure mode is a server holding *far* more, because a
delivered payload pins the buffer it was read into.

Deliveries in this server copy the payload once (routing.rs, `route`) instead of
handing out a view of the publisher's read buffer, and this is the measurement
that says so. Both binaries run the same way:

    python3 specs/tools/memprobe.py /home/vlad/apps/nats-server 3000000
    python3 specs/tools/memprobe.py target/release/nats-server-rs 3000000

Recorded 2026-09-22 (3 M messages of 128 B, aarch64): Go 127.8 MiB peak RSS, ours
124.5 MiB — 1.95x `max_pending` each, no chunk amplification in either.
"""
import os, signal, socket, subprocess, sys, time

BIN = sys.argv[1]
MSGS = int(sys.argv[2]) if len(sys.argv) > 2 else 3_000_000
PORT = int(os.environ.get("MEMPROBE_PORT", "4226"))
SIZE = int(os.environ.get("SIZE", "128"))
MAX_PENDING = 64 * 1024 * 1024      # config::MAX_PENDING, the reference's default


def conn():
    s = socket.create_connection(("127.0.0.1", PORT))
    s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    s.recv(65536)                                    # INFO
    s.sendall(b'CONNECT {"verbose":false,"pedantic":false,"headers":true}\r\n')
    time.sleep(0.05)
    s.recv(65536)
    return s


def rss_kb(pid):
    for line in open(f"/proc/{pid}/status"):
        if line.startswith("VmRSS"):
            return int(line.split()[1])
    return 0


p = subprocess.Popen([BIN, "-a", "127.0.0.1", "-p", str(PORT)],
                     stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
try:
    time.sleep(0.6)
    sub, pub = conn(), conn()
    sub.sendall(b"SUB slow 1\r\n")
    time.sleep(0.1)
    frame = b"PUB slow %d\r\n" % SIZE + b"x" * SIZE + b"\r\n"
    chunk = frame * (262144 // len(frame))
    peak = sent = 0
    start = time.time()
    while sent < MSGS:
        pub.sendall(chunk)                           # blocks as the victim's socket fills
        sent += len(chunk) // len(frame)
        peak = max(peak, rss_kb(p.pid))
    dur = time.time() - start
    print(f"bench name=memprobe bin={os.path.basename(BIN)} msgs={min(sent, MSGS)} "
          f"size={SIZE} peak_rss_mib={peak / 1024:.1f} "
          f"ratio_to_max_pending={peak * 1024 / MAX_PENDING:.2f} seconds={dur:.1f} "
          f"alive={p.poll() is None}")
finally:
    p.send_signal(signal.SIGTERM)
    try:
        p.wait(timeout=5)
    except subprocess.TimeoutExpired:
        p.kill()
