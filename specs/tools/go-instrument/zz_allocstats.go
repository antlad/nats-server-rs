// Measurement-only addition to the reference build. Two knobs, both off by
// default, so a normal run is untouched:
//
//   NATS_GO_STATS_FILE=/tmp/g.txt   dump runtime.MemStats every 100 ms, in the
//                                   same line format as crates/server's
//                                   allocstats feature, so "allocations per
//                                   message" is read the same way off both.
//   NATS_GO_PPROF=127.0.0.1:6060    serve net/http/pprof on a private port. The
//                                   reference's own monitoring mux does not
//                                   mount pprof (/debug/pprof/heap is 404 on
//                                   -m 8222), so this is how you find out WHERE
//                                   the reference allocates: `go tool pprof -top
//                                   -sample_index=alloc_objects`.
package server

import (
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"time"
)

func init() {
	if addr := os.Getenv("NATS_GO_PPROF"); addr != "" {
		go func() { _ = http.ListenAndServe(addr, nil) }()
	}
	path := os.Getenv("NATS_GO_STATS_FILE")
	if path == "" {
		return
	}
	go func() {
		for {
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
			if err == nil {
				fmt.Fprintf(f, "goallocs stage=t allocs=%d bytes=%d live=%d gc=%d t=%.3f\n",
					m.Mallocs, m.TotalAlloc, m.HeapAlloc, m.NumGC, float64(time.Now().UnixNano())/1e9)
				f.Close()
			}
			time.Sleep(100 * time.Millisecond)
		}
	}()
}
