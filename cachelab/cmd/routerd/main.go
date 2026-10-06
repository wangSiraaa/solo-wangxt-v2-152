// routerd 启动路由服务。
package main

import (
	"context"
	"flag"
	"log"
	"net"
	"os"

	"google.golang.org/grpc"

	cachelabv1 "cachelab/gen/cachelab/v1"
	"cachelab/internal/router"
	"cachelab/internal/store"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:7000", "listen address")
	dsn := flag.String("pg", os.Getenv("CACHELAB_PG_DSN"), "PostgreSQL DSN")
	flag.Parse()
	if *dsn == "" {
		log.Fatal("missing -pg or CACHELAB_PG_DSN")
	}
	ctx := context.Background()
	st, err := store.Open(ctx, *dsn)
	if err != nil {
		log.Fatalf("open store: %v", err)
	}
	defer st.Close()
	if err := st.Migrate(ctx); err != nil {
		log.Fatalf("migrate: %v", err)
	}
	srv := router.New(st)
	if err := srv.Bootstrap(ctx); err != nil {
		log.Fatalf("bootstrap: %v", err)
	}
	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatal(err)
	}
	gs := grpc.NewServer()
	cachelabv1.RegisterRouterServer(gs, srv)
	log.Printf("routerd listening on %s (active ring v%d)", *addr, srv.ActiveVersion())
	log.Fatal(gs.Serve(lis))
}
