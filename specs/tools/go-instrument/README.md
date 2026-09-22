# Counting the reference's allocations

`specs/perf-notes.md` has always had to say "allocations per message: ours 11.08,
the reference's — not obtainable". It is obtainable. The reference never mounts
pprof on its monitoring mux (`/debug/pprof/heap` is **404** on `-m 8222`, with or
without `NATS_MONITOR_PPROF=true`; the `_ "net/http/pprof"` import in
`server/server.go` is there for `pprof.WithLabels`, not for the endpoints), so the
way in is a build of the source we already have, plus this one file.

```sh
cp -r /home/vlad/go_path/src/github.com/nats-io/nats-server /tmp/nats-instr
cp specs/tools/go-instrument/zz_allocstats.go /tmp/nats-instr/server/
cd /tmp/nats-instr && go build -o /tmp/nats-instr/nats-server .
```

Two env knobs, both inert when unset:

| Knob | Gives |
|---|---|
| `NATS_GO_STATS_FILE=/tmp/g.txt` | `runtime.MemStats` every 100 ms, in the same `allocs=… bytes=… live=… gc=…` shape as our `--features allocstats` build, so a per-message count is read off both binaries the same way |
| `NATS_GO_PPROF=127.0.0.1:6060` | `/debug/pprof/heap`, i.e. **where** the reference allocates: `go tool pprof -top -sample_index=alloc_objects <bin> heap.prof` |

Measured with it on 2026-09-22 (this host, aarch64; the deltas are
`last − first` around a run, divided by messages):

| Path | allocations | bytes | where |
|---|---:|---:|---|
| publish only, one publisher, no subscribers, 20 M × 128 B | **1.00 / msg** | ~4 / msg | `(*client).processInboundClientMsg` — 96.6 % of every object the process ever allocated. It is the `string(c.pa.subject)` argument to `acc.sl.Match(...)`: a byte→string conversion used as a *map key* is free in Go, one passed as a function argument is not. The reference's L1 result cache does not save it here, because it only caches a result that has subscribers (`len(r.psubs)+len(r.qsubs) > 0`) — with none, `Match` runs, and allocates, every message. |
| delivery, one publisher, one subscriber, 4 M × 256 B | **1.06 / msg** | ~171 / msg | `(*client).processMsgResults` (96.6 %), plus `queueOutbound` (3.3 %) and the `netBuffers` pool's `nbPoolGet` (1.3 %) |
| a connection opened and closed | **73.2 / conn** | ~13 KiB / conn | goroutine stacks, the two buffers, the client struct, the sid map |

So the reference is not allocation-free, and on the publish path it is not even
close to optimal: **one malloc'd string per message, forever.** "Zero allocations
per message" is a target that beats it, not one that catches it up to it.
