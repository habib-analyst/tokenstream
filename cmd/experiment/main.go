// Command experiment runs the tokenstream workload harness and writes
// results.json with every measured number used in PAPER.md / README.md.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"tokenstream/internal/harness"
)

func main() {
	out := flag.String("out", "results.json", "where to write results JSON")
	flag.Parse()

	res := harness.RunAll()
	data, err := json.MarshalIndent(res, "", "  ")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := os.WriteFile(*out, data, 0o644); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	for _, sc := range res.Scenarios {
		fmt.Printf("== %s (publisher %.1f ms)\n", sc.Scenario, sc.PublisherMs)
		for _, c := range sc.Clients {
			fmt.Printf("   %-8s %-6s ttft %7.2f ms  token p50 %6.2f  p95 %7.2f  done %8.1f ms  frames %4d  audit ok=%v errors=%v\n",
				c.Name, c.Class, c.TTFTms, c.TokenP50ms, c.TokenP95ms, c.CompletionMs, c.Frames, c.Audit.OK, c.Audit.Errors)
			if c.Pump != nil {
				fmt.Printf("            pump: coalesced=%d resyncs=%d subsumed=%d lag_end=%d\n",
					c.Pump.Coalesced, c.Pump.Resyncs, c.Pump.DroppedState, c.Pump.LagEvents)
			}
		}
		if sc.PeakPendingBytes > 0 {
			fmt.Printf("   peak pending bytes (all clients): %d, peak lag: %d events\n", sc.PeakPendingBytes, sc.PeakLagEvents)
		}
	}
	fmt.Println("wrote", *out)
}
