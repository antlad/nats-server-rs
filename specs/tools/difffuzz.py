#!/usr/bin/env python3
"""Differential corpus fuzz: the same 200+ control lines to both binaries, then a
byte-for-byte diff of what came back and whether the connection survived.

An empty diff is the evidence behind `specs/parity-log.md`; a non-empty one is a
bug in the Rust server until proven otherwise in writing.

    python3 specs/tools/difffuzz.py [GO_BIN] [RUST_BIN]

Needs the raw-socket helper from Part 1's probing session (`Conn`/`Server`);
`PROBE_DIR` overrides where it lives.
"""
import sys, os, time, importlib
sys.path.insert(0, os.environ.get("PROBE_DIR", "/tmp/natsprobe"))
import probe, importlib
GO = sys.argv[1] if len(sys.argv) > 1 else "/home/vlad/apps/nats-server"
RS = sys.argv[2] if len(sys.argv) > 2 else os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "target/release/nats-server-rs")

def run(binary, lines, cfg=None):
    import probe as _p
    _p.BIN = binary
    Server, Conn = _p.Server, _p.Conn
    srv=Server(); out=[]
    for pre, line in lines:
        c=Conn(srv)
        c.send(pre.encode() if isinstance(pre,str) else pre)
        time.sleep(0.02)
        c.send(line.encode() if isinstance(line,str) else line)
        time.sleep(0.15)
        data=c.rest(0.25); closed=c.closed(0.4)
        out.append((data, closed))
        c.close()
    srv.stop()
    return out

CAP='CONNECT {"verbose":false}\r\n'
verbs=["PING","PONG","SUB","UNSUB","PUB","HPUB","CONNECT","INFO","+OK","-ERR","PSUB","BOGUS","ping","sub","PuB","hPuB"]
cases=[]
for v in verbs:
    for tail in ["", " ", " a", " a b", " a b c", " a b c d", " 1", " a 1", " a b 1", " a 1 2", " a 1 2 3", " x y z 1 2 3"]:
        line = v + tail + "\r\n"
        if v in ("PUB",) and tail.strip():
            line += "z\r\n"
        if v in ("HPUB",) and tail.strip():
            line += "NATS/1.0\r\n\r\nab\r\n"
        cases.append((CAP, line))
# payloads and framing
cases += [
 (CAP, "PUB a 3\r\nabc\r\n"), (CAP, "PUB a 3\r\nab\r\n"), (CAP, "PUB a 3\r\nabcd\r\n"),
 (CAP, "PUB a 0\r\n\r\n"), (CAP, "SUB a 1\r\nUNSUB 1\r\n"), (CAP, "SUB a 1\r\nUNSUB 1 1\r\n"),
 (CAP, 'CONNECT {"verbose":true}\r\nSUB a 1\r\n'), (CAP, 'CONNECT {"verbose":true}\r\nPING extra\r\n'),
 ('', "SUB pre 1\r\nPING\r\n"), ('', "PING\r\n"), ('', "PUB a 1\r\nx\r\nPING\r\n"),
 (CAP, "\r\n"), (CAP, "\n"), (CAP, "SUB "+("x"*5000)+" 1\r\n"),
]
res_go = run(GO, cases)
res_rs = run(RS, cases)
diffs=0
for (pre,line), g, r in zip(cases, res_go, res_rs):
    if g != r:
        diffs+=1
        print(f"DIFF {line!r}\n   go  : {g}\n   rust: {r}")
print(f"{len(cases)} cases, {diffs} diffs")
