// demo 自包含地演示完整教学流程:
// 启动 Router + 4 个 KV 节点 → 写入数据 → 新增节点 →
// 观察迁移计划(路由变化量)→ 复制 → 强调"复制完成≠已切换" →
// 切换 → 清理 → 对照键分布与热点。
// 需要 PostgreSQL(CACHELAB_PG_DSN)。
package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"time"

	"google.golang.org/grpc"

	cachelabv1 "cachelab/gen/cachelab/v1"
	"cachelab/internal/client"
	"cachelab/internal/kvnode"
	"cachelab/internal/router"
	"cachelab/internal/store"
)

func mustListen() (net.Listener, string) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Fatal(err)
	}
	return lis, lis.Addr().String()
}

func startNode(id, token string) string {
	lis, addr := mustListen()
	srv := kvnode.New(id, token)
	gs := grpc.NewServer(
		grpc.UnaryInterceptor(srv.UnaryInterceptor()),
		grpc.StreamInterceptor(srv.StreamInterceptor()),
	)
	cachelabv1.RegisterKVNodeServer(gs, srv)
	go gs.Serve(lis)
	return addr
}

func printDistribution(c *client.Client, title string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	d, err := c.Router.KeyDistribution(ctx, &cachelabv1.KeyDistributionRequest{})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("\n== %s (环 v%d) ==\n", title, d.RingVersion)
	fmt.Printf("%-8s %-10s %-10s %-10s\n", "节点", "预测占比", "实际键数", "虚拟节点")
	for _, n := range d.Nodes {
		fmt.Printf("%-8s %-10.4f %-10d %-10d\n", n.NodeId, n.PredictedFraction, n.ActualKeys, n.VnodeCount)
	}
}

func printPlan(c *client.Client, planID int64, title string) *cachelabv1.MigrationStatusResponse {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	st, err := c.Router.MigrationStatus(ctx, &cachelabv1.MigrationStatusRequest{PlanId: planID})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("\n== %s ==\n", title)
	fmt.Printf("计划 #%d: 环 v%d → v%d, 状态 %s\n", planID, st.FromVersion, st.ToVersion, st.State)
	fmt.Printf("迁移区间 %d 个, 哈希空间易主比例 %.2f%%\n", len(st.Ranges), st.ChangedFraction*100)
	fmt.Printf("路由变化量(预计迁移键数): %d, 实际迁移量(已复制键数): %d\n",
		st.TotalExpectedKeys, st.TotalCopiedKeys)
	return st
}

func main() {
	dsn := os.Getenv("CACHELAB_PG_DSN")
	if dsn == "" {
		dsn = "postgres://postgres@localhost:5432/cachelab?sslmode=disable"
	}
	ctx := context.Background()
	st, err := store.Open(ctx, dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer st.Close()
	if err := st.Migrate(ctx); err != nil {
		log.Fatal(err)
	}
	if err := st.ResetForTest(ctx); err != nil { // 演示从干净状态开始
		log.Fatal(err)
	}

	// 1. 预注册 4 个节点(n4 权重 0,暂不上环),再启动 Router。
	tokens := map[string]string{"n1": "tok-n1", "n2": "tok-n2", "n3": "tok-n3", "n4": "tok-n4"}
	addrs := map[string]string{}
	for id, tok := range tokens {
		addrs[id] = startNode(id, tok)
		weight := 1
		if id == "n4" {
			weight = 0
		}
		if err := st.RegisterNode(ctx, store.Node{NodeID: id, Addr: addrs[id], Token: tok, Weight: weight}); err != nil {
			log.Fatal(err)
		}
	}
	rLis, rAddr := mustListen()
	rSrv := router.New(st)
	if err := rSrv.Bootstrap(ctx); err != nil {
		log.Fatal(err)
	}
	gs := grpc.NewServer()
	cachelabv1.RegisterRouterServer(gs, rSrv)
	go gs.Serve(rLis)

	c, err := client.Dial(rAddr)
	if err != nil {
		log.Fatal(err)
	}
	// 节点报到
	for id, tok := range tokens {
		jctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		if _, err := c.Router.Join(jctx, &cachelabv1.JoinRequest{NodeId: id, Token: tok}); err != nil {
			log.Fatalf("join %s: %v", id, err)
		}
		cancel()
	}
	fmt.Println("集群启动: n1,n2,n3 在环(权重 1),n4 已注册但权重 0")

	// 2. 写入 3000 个键
	const N = 3000
	for i := 0; i < N; i++ {
		if err := c.Put(fmt.Sprintf("key-%05d", i), []byte(fmt.Sprintf("value-%d", i))); err != nil {
			log.Fatal(err)
		}
	}
	fmt.Printf("\n已写入 %d 个键", N)
	printDistribution(c, "初始键分布")

	// 制造热点
	for i := 0; i < 500; i++ {
		if _, _, err := c.Get("key-00001", 0); err != nil {
			log.Fatal(err)
		}
		if _, _, err := c.Get("key-00002", 0); err != nil {
			log.Fatal(err)
		}
	}

	// 3. 新增节点 n4:权重 0 → 1,生成新环与迁移计划
	sw, err := c.Router.SetWeight(ctx, &cachelabv1.SetWeightRequest{NodeId: "n4", Weight: 1})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("\n>>> SetWeight(n4, 1): 新环 v%d, 迁移计划 #%d(%d 个区间)",
		sw.NewRingVersion, sw.PlanId, sw.RangeCount)
	printPlan(c, sw.PlanId, "迁移计划(尚未复制)")

	// 4. 复制
	if _, err := c.Router.StartMigration(ctx, &cachelabv1.StartMigrationRequest{PlanId: sw.PlanId, FailAfterRanges: -1}); err != nil {
		log.Fatal(err)
	}
	stAfterCopy := printPlan(c, sw.PlanId, "复制完成")
	fmt.Println("\n注意: 状态已是 COPIED,但路由仍走旧环 —— 复制完成 ≠ 已安全切换")
	rt, err := c.Router.Route(ctx, &cachelabv1.RouteRequest{Key: "key-00000"})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("例如 key-00000 当前仍路由到 %s (环 v%d)\n", rt.NodeId, rt.RingVersion)

	// 5. 切换(切换前会逐区间校验源/目标键数一致)
	swResp, err := c.Router.SwitchRing(ctx, &cachelabv1.SwitchRingRequest{PlanId: sw.PlanId})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("\n>>> SwitchRing: 活跃环切换到 v%d\n", swResp.ActiveRingVersion)

	// 6. 清理源数据
	cl, err := c.Router.Cleanup(ctx, &cachelabv1.CleanupRequest{PlanId: sw.PlanId})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf(">>> Cleanup: 从源节点删除已迁出的 %d 个键\n", cl.DeletedKeys)
	printDistribution(c, "迁移后键分布")

	// 7. 数据完整性抽查
	missing := 0
	for i := 0; i < N; i++ {
		_, found, err := c.Get(fmt.Sprintf("key-%05d", i), 0)
		if err != nil || !found {
			missing++
		}
	}
	fmt.Printf("\n完整性校验: %d/%d 键可读, 丢失 %d\n", N-missing, N, missing)

	// 8. 热点统计
	hk, err := c.Router.HotKeys(ctx, &cachelabv1.HotKeysRequest{TopN: 3})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("\n== 全局热点键 Top3 ==")
	for _, k := range hk.Global {
		fmt.Printf("  %s: %d 次访问\n", k.Key, k.Accesses)
	}

	_ = stAfterCopy
	fmt.Println("\n演示结束。更多场景(权重归零、迁移中断恢复)见: go test ./internal/integration -v")
}
