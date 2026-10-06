// kvnode 启动一个本地键值节点并向 Router 报到。
package main

import (
	"context"
	"flag"
	"log"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	cachelabv1 "cachelab/gen/cachelab/v1"
	"cachelab/internal/kvnode"
)

func main() {
	id := flag.String("id", "", "node id (must be pre-registered)")
	token := flag.String("token", "", "join token")
	addr := flag.String("addr", "", "listen address, e.g. 127.0.0.1:7101")
	routerAddr := flag.String("router", "127.0.0.1:7000", "router address")
	flag.Parse()
	if *id == "" || *token == "" || *addr == "" {
		log.Fatal("need -id, -token, -addr")
	}

	srv := kvnode.New(*id, *token)
	gs := grpc.NewServer(
		grpc.UnaryInterceptor(srv.UnaryInterceptor()),
		grpc.StreamInterceptor(srv.StreamInterceptor()),
	)
	cachelabv1.RegisterKVNodeServer(gs, srv)
	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatal(err)
	}
	go func() {
		if err := gs.Serve(lis); err != nil {
			log.Fatal(err)
		}
	}()

	// 向 Router 报到(带重试);未知节点/错误令牌会被拒绝。
	conn, err := grpc.NewClient(*routerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal(err)
	}
	rc := cachelabv1.NewRouterClient(conn)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		resp, err := rc.Join(ctx, &cachelabv1.JoinRequest{NodeId: *id, Token: *token})
		cancel()
		if err != nil {
			log.Printf("join failed: %v; retrying in 2s", err)
			time.Sleep(2 * time.Second)
			continue
		}
		srv.SetActive(resp.ActiveRingVersion)
		log.Printf("kvnode %s joined on %s (active ring v%d, weight %d)",
			*id, *addr, resp.ActiveRingVersion, resp.Weight)
		break
	}
	select {}
}
