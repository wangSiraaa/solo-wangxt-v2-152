// 命令 router 启动路由服务(gRPC)。
//
// 启动即从 PostgreSQL 恢复: 环版本、虚拟节点都在库中, 内存环缓存按需重建;
// 随后调用 Resume 对中断的迁移做断点对账(但绝不自动切换)。
package main

import (
	"context"
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"chaosring/internal/pb"
	"chaosring/internal/routersvc"
	"chaosring/internal/store"
)

func main() {
	listen := flag.String("listen", "127.0.0.1:9001", "router gRPC listen address")
	dsn := flag.String("dsn", envOr("CHAOS_PG_DSN",
		"host=/tmp port=5432 user=chaos dbname=chaosring sslmode=disable"),
		"PostgreSQL connection string")
	adminToken := flag.String("admin-token", envOr("CHAOS_ADMIN_TOKEN", "admin-secret"), "admin API token")
	enroll := flag.String("enrollment-secret", envOr("CHAOS_ENROLL_SECRET", "enroll-secret"),
		"shared secret a node must present to be registered")
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	st, err := store.Open(ctx, *dsn)
	cancel()
	if err != nil {
		log.Fatalf("connect postgres: %v", err)
	}
	if err := st.Ping(context.Background()); err != nil {
		log.Fatalf("ping postgres: %v", err)
	}

	svc := routersvc.New(routersvc.Config{
		Store: st, AdminToken: *adminToken, EnrollmentSecret: *enroll,
	})

	// 启动恢复: 断点续传复制, 但不提交切换。
	rctx, rcancel := context.WithTimeout(context.Background(), 30*time.Second)
	res, err := svc.Resume(rctx, &pb.ResumeRequest{AdminToken: *adminToken})
	rcancel()
	if err != nil {
		log.Printf("resume warning: %v", err)
	} else {
		log.Printf("resume: active_ring=%s recovered=%v failed=%v",
			res.ActiveRing, res.RecoveredMigrations, res.FailedMigrations)
	}

	lis, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	g := grpc.NewServer()
	pb.RegisterRouterServer(g, svc)

	go func() {
		log.Printf("router listening on %s", *listen)
		if err := g.Serve(lis); err != nil {
			log.Fatalf("serve: %v", err)
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Println("router shutting down")
	g.GracefulStop()
	svc.Close()
	st.Close()
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
