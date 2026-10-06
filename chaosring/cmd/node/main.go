// 命令 node 启动一个本地键值节点(gRPC)。
//
// 节点本身不知道集群中还有谁: 它只校验 node_token, 等待路由器调用。
// 令牌来源: 节点进程启动时由环境变量/参数注入(管理员注册后下发),
// 或由演示脚本通过 RegisterNode 返回值写入。
package main

import (
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/grpc"

	"chaosring/internal/nodesvc"
	"chaosring/internal/pb"
)

func main() {
	listen := flag.String("listen", "127.0.0.1:9101", "gRPC listen address")
	id := flag.String("id", "", "node id (e.g. nA)")
	dataDir := flag.String("data", "./data/nA", "WAL data directory")
	token := flag.String("token", "", "node token issued at registration")
	admin := flag.String("admin-token", envOr("CHAOS_ADMIN_TOKEN", "admin-secret"), "admin token for Fail injection")
	flag.Parse()

	if *id == "" {
		log.Fatal("--id is required")
	}
	tok := *token
	if tok == "" {
		tok = os.Getenv("CHAOS_NODE_TOKEN")
	}
	if tok == "" {
		log.Fatal("node token required via --token or CHAOS_NODE_TOKEN")
	}

	srv, err := nodesvc.New(nodesvc.Config{
		NodeID: *id, Token: tok, AdminToken: *admin, DataDir: *dataDir,
	})
	if err != nil {
		log.Fatalf("node init: %v", err)
	}
	lis, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	g := grpc.NewServer()
	pb.RegisterKVNodeServer(g, srv)

	go func() {
		log.Printf("node %s listening on %s (data=%s)", *id, *listen, *dataDir)
		if err := g.Serve(lis); err != nil {
			log.Fatalf("serve: %v", err)
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Printf("node %s shutting down", *id)
	g.GracefulStop()
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
