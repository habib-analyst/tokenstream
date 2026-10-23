// Command tokenstream serves the dual-source streaming server.
package main

import (
	"flag"
	"log"
	"net/http"

	"tokenstream/internal/server"
	"tokenstream/internal/stream"
)

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	budget := flag.Int("budget", 64, "per-client lag budget in pending state events before a resync")
	flag.Parse()

	hub := stream.NewHub(*budget)
	srv := server.New(hub)
	log.Printf("tokenstream listening on %s (lag budget %d state events)", *addr, *budget)
	log.Fatal(http.ListenAndServe(*addr, srv.Handler()))
}
