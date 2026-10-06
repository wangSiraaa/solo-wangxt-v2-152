// Package integration 端到端验证缓存集群教学场景:
//  1. 新增节点:路由变化量与实际迁移量对照,数据零丢失
//  2. 权重归零:节点完全退出,数据完整迁移
//  3. 迁移途中失败:中断恢复;复制完成 ≠ 已安全切换
//  4. 未知节点/错误令牌不能加入集群
//
// 需要 PostgreSQL:环境变量 CACHELAB_PG_DSN。
package integration

import (
	"context"
	"fmt"
	"net"
	"os"
	"sort"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	cachelabv1 "cachelab/gen/cachelab/v1"
	"cachelab/internal/client"
	"cachelab/internal/kvnode"
	"cachelab/internal/router"
	"cachelab/internal/store"
)

// nodeHandle 是测试内运行的节点实例。
type nodeHandle struct {
	id   string
	addr string
	srv  *kvnode.Server
	gs   *grpc.Server
	lis  net.Listener
}

type cluster struct {
	t      *testing.T
	st     *store.Store
	router *router.Server
	rgs    *grpc.Server
	rAddr  string
	nodes  map[string]*nodeHandle
	cli    *client.Client
}

func dsn(t *testing.T) string {
	d := os.Getenv("CACHELAB_PG_DSN")
	if d == "" {
		d = "postgres://postgres@localhost:5432/cachelab?sslmode=disable"
	}
	return d
}

// newCluster 启动 Router;nodes 为 nodeID → 初始权重(权重 0 表示已注册但不上环)。
func newCluster(t *testing.T, weights map[string]int) *cluster {
	t.Helper()
	ctx := context.Background()
	st, err := store.Open(ctx, dsn(t))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if err := st.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if err := st.ResetForTest(ctx); err != nil {
		t.Fatalf("reset: %v", err)
	}

	c := &cluster{t: t, st: st, nodes: map[string]*nodeHandle{}}

	// 先注册节点(带令牌),再启动 Router —— 未知节点无法加入。
	ids := make([]string, 0, len(weights))
	for id := range weights {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		h := c.startNode(id)
		if err := st.RegisterNode(ctx, store.Node{
			NodeID: id, Addr: h.addr, Token: "tok-" + id, Weight: weights[id],
		}); err != nil {
			t.Fatalf("register %s: %v", id, err)
		}
	}

	rSrv := router.New(st)
	if err := rSrv.Bootstrap(ctx); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	c.router = rSrv
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	c.rAddr = lis.Addr().String()
	c.rgs = grpc.NewServer()
	cachelabv1.RegisterRouterServer(c.rgs, rSrv)
	go c.rgs.Serve(lis)

	cli, err := client.Dial(c.rAddr)
	if err != nil {
		t.Fatal(err)
	}
	c.cli = cli

	// 全部节点 Join(令牌正确)
	for _, id := range ids {
		c.joinNode(id, "tok-"+id)
	}
	t.Cleanup(func() {
		c.rgs.Stop()
		for _, h := range c.nodes {
			h.gs.Stop()
			h.lis.Close()
		}
		st.Close()
	})
	return c
}

func (c *cluster) startNode(id string) *nodeHandle {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		c.t.Fatal(err)
	}
	srv := kvnode.New(id, "tok-"+id)
	gs := grpc.NewServer(
		grpc.UnaryInterceptor(srv.UnaryInterceptor()),
		grpc.StreamInterceptor(srv.StreamInterceptor()),
	)
	cachelabv1.RegisterKVNodeServer(gs, srv)
	go gs.Serve(lis)
	h := &nodeHandle{id: id, addr: lis.Addr().String(), srv: srv, gs: gs, lis: lis}
	c.nodes[id] = h
	return h
}

func (c *cluster) joinNode(id, token string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := c.cli.Router.Join(ctx, &cachelabv1.JoinRequest{NodeId: id, Token: token})
	if err == nil {
		c.nodes[id].srv.SetActive(resp.ActiveRingVersion)
	}
	return err
}

// putKeys 写入 n 个键并返回键列表;值由键名派生,便于校验。
func (c *cluster) putKeys(prefix string, n int) []string {
	c.t.Helper()
	keys := make([]string, n)
	for i := 0; i < n; i++ {
		keys[i] = fmt.Sprintf("%s-%05d", prefix, i)
		if err := c.cli.Put(keys[i], []byte("val-of-"+keys[i])); err != nil {
			c.t.Fatalf("put %s: %v", keys[i], err)
		}
	}
	return keys
}

// assertAllReadable 断言所有键按当前路由可读且值正确。
func (c *cluster) assertAllReadable(keys []string, ringVersion int64) {
	c.t.Helper()
	for _, k := range keys {
		v, found, err := c.cli.Get(k, ringVersion)
		if err != nil {
			c.t.Fatalf("get %s: %v", k, err)
		}
		if !found {
			c.t.Fatalf("key %s lost", k)
		}
		if string(v) != "val-of-"+k {
			c.t.Fatalf("key %s value corrupted: %q", k, v)
		}
	}
}

// totalKeys 返回所有节点键数总和。
func (c *cluster) totalKeys() int {
	total := 0
	for _, h := range c.nodes {
		total += h.srv.KeyCount()
	}
	return total
}

// routesOf 记录一组键当前的路由结果。
func (c *cluster) routesOf(keys []string) map[string]string {
	c.t.Helper()
	out := map[string]string{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, k := range keys {
		r, err := c.cli.Router.Route(ctx, &cachelabv1.RouteRequest{Key: k})
		if err != nil {
			c.t.Fatalf("route %s: %v", k, err)
		}
		out[k] = r.NodeId
	}
	return out
}

func (c *cluster) mustStatus(planID int64) *cachelabv1.MigrationStatusResponse {
	c.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	st, err := c.cli.Router.MigrationStatus(ctx, &cachelabv1.MigrationStatusRequest{PlanId: planID})
	if err != nil {
		c.t.Fatalf("status: %v", err)
	}
	return st
}

func (c *cluster) setWeight(id string, w int) *cachelabv1.SetWeightResponse {
	c.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	resp, err := c.cli.Router.SetWeight(ctx, &cachelabv1.SetWeightRequest{NodeId: id, Weight: int32(w)})
	if err != nil {
		c.t.Fatalf("SetWeight(%s,%d): %v", id, w, err)
	}
	return resp
}

func (c *cluster) startMigration(planID int64, failAfter int32) error {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	_, err := c.cli.Router.StartMigration(ctx, &cachelabv1.StartMigrationRequest{
		PlanId: planID, FailAfterRanges: failAfter,
	})
	return err
}

func (c *cluster) resumeMigration(planID int64) {
	c.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if _, err := c.cli.Router.ResumeMigration(ctx, &cachelabv1.ResumeMigrationRequest{PlanId: planID}); err != nil {
		c.t.Fatalf("resume: %v", err)
	}
}

func (c *cluster) switchRing(planID int64) error {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	_, err := c.cli.Router.SwitchRing(ctx, &cachelabv1.SwitchRingRequest{PlanId: planID})
	return err
}

func (c *cluster) cleanup(planID int64) int64 {
	c.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	resp, err := c.cli.Router.Cleanup(ctx, &cachelabv1.CleanupRequest{PlanId: planID})
	if err != nil {
		c.t.Fatalf("cleanup: %v", err)
	}
	return resp.DeletedKeys
}

// ---------------------------------------------------------------------------
// 场景 1:新增节点 —— 路由变化量与实际迁移量对照,全程数据可读
// ---------------------------------------------------------------------------

func TestAddNode_MigrationMatchesRouteChange(t *testing.T) {
	c := newCluster(t, map[string]int{"n1": 1, "n2": 1, "n3": 1, "n4": 0})
	keys := c.putKeys("add", 2000)
	oldRoutes := c.routesOf(keys)
	oldVersion := c.router.ActiveVersion()

	// 新增节点 n4(权重 0 → 1):生成新环 + 迁移计划
	sw := c.setWeight("n4", 1)
	if sw.RangeCount == 0 {
		t.Fatal("新增节点应产生迁移区间")
	}

	// 路由变化量:实际路由结果改变的键数
	changed := 0
	ctx := context.Background()
	for _, k := range keys {
		r, err := c.cli.Router.Route(ctx, &cachelabv1.RouteRequest{Key: k, RingVersion: sw.NewRingVersion})
		if err != nil {
			t.Fatal(err)
		}
		if r.NodeId != oldRoutes[k] {
			changed++
			if r.NodeId != "n4" {
				t.Fatalf("键 %s 迁往了非新节点 %s", k, r.NodeId)
			}
		}
	}

	// 复制前:旧环仍是活跃版本,数据全部可读
	c.assertAllReadable(keys, 0)

	// 执行复制
	if err := c.startMigration(sw.PlanId, -1); err != nil {
		t.Fatalf("start migration: %v", err)
	}
	st := c.mustStatus(sw.PlanId)
	if st.State != "COPIED" {
		t.Fatalf("计划状态 = %s, 应为 COPIED", st.State)
	}

	// 对照:路由变化量(计划时源区间键数)== 实际迁移量(复制键数)== 路由改变的键数
	if st.TotalExpectedKeys != st.TotalCopiedKeys {
		t.Fatalf("路由变化量 %d ≠ 实际迁移量 %d", st.TotalExpectedKeys, st.TotalCopiedKeys)
	}
	if int(st.TotalCopiedKeys) != changed {
		t.Fatalf("实际迁移量 %d ≠ 路由改变键数 %d", st.TotalCopiedKeys, changed)
	}
	t.Logf("路由变化量 = 实际迁移量 = %d 键 (%.1f%% 哈希空间)", changed, st.ChangedFraction*100)

	// 复制完成但尚未切换:活跃环必须仍是旧版本
	if got := c.router.ActiveVersion(); got != oldVersion {
		t.Fatalf("复制完成后活跃环变为 v%d —— 复制完成不应等于已切换", got)
	}
	r, err := c.cli.Router.Route(ctx, &cachelabv1.RouteRequest{Key: keys[0]})
	if err != nil {
		t.Fatal(err)
	}
	if r.RingVersion != oldVersion {
		t.Fatalf("COPIED 后路由仍应走旧环 v%d, 实际 v%d", oldVersion, r.RingVersion)
	}

	// 切换
	if err := c.switchRing(sw.PlanId); err != nil {
		t.Fatalf("switch: %v", err)
	}
	if got := c.router.ActiveVersion(); got != sw.NewRingVersion {
		t.Fatalf("切换后活跃环 = v%d, 应为 v%d", got, sw.NewRingVersion)
	}

	// 切换后:新路由全部可读;旧环仍可为在途请求提供一致视图
	c.assertAllReadable(keys, 0)
	r, err = c.cli.Router.Route(ctx, &cachelabv1.RouteRequest{Key: keys[0], RingVersion: oldVersion})
	if err != nil {
		t.Fatalf("旧环应仍可查询(在途请求): %v", err)
	}
	if r.NodeId != oldRoutes[keys[0]] {
		t.Fatalf("旧环路由结果应不变: %s → %s, 原为 %s", keys[0], r.NodeId, oldRoutes[keys[0]])
	}

	// 清理:源节点删除迁出数据,总键数回到 N(无重复、无丢失)
	deleted := c.cleanup(sw.PlanId)
	if int(deleted) != changed {
		t.Fatalf("清理删除 %d 键, 应等于迁移量 %d", deleted, changed)
	}
	if total := c.totalKeys(); total != len(keys) {
		t.Fatalf("清理后总键数 = %d, 应为 %d(出现重复或丢失)", total, len(keys))
	}
	c.assertAllReadable(keys, 0)
}

// ---------------------------------------------------------------------------
// 场景 2:权重归零 —— 节点完全退出环,数据完整迁出
// ---------------------------------------------------------------------------

func TestWeightZero_DrainsNode(t *testing.T) {
	c := newCluster(t, map[string]int{"n1": 1, "n2": 1, "n3": 1})
	keys := c.putKeys("drain", 2000)

	n2KeysBefore := c.nodes["n2"].srv.KeyCount()
	if n2KeysBefore == 0 {
		t.Fatal("前置条件: n2 应持有部分键")
	}

	sw := c.setWeight("n2", 0)
	st := c.mustStatus(sw.PlanId)
	// 所有迁出区间的源都必须是 n2
	for _, rg := range st.Ranges {
		if rg.FromNode != "n2" {
			t.Fatalf("权重归零后仍有区间从 %s 迁出", rg.FromNode)
		}
	}

	if err := c.startMigration(sw.PlanId, -1); err != nil {
		t.Fatal(err)
	}
	st = c.mustStatus(sw.PlanId)
	if st.TotalCopiedKeys != int64(n2KeysBefore) {
		t.Fatalf("迁移量 %d ≠ n2 原键数 %d", st.TotalCopiedKeys, n2KeysBefore)
	}

	if err := c.switchRing(sw.PlanId); err != nil {
		t.Fatal(err)
	}
	deleted := c.cleanup(sw.PlanId)
	if int(deleted) != n2KeysBefore {
		t.Fatalf("清理数 %d ≠ n2 原键数 %d", deleted, n2KeysBefore)
	}

	// n2 已清空但仍在集群中;数据零丢失、零重复
	if got := c.nodes["n2"].srv.KeyCount(); got != 0 {
		t.Fatalf("n2 应已清空, 仍有 %d 键", got)
	}
	if total := c.totalKeys(); total != len(keys) {
		t.Fatalf("总键数 = %d, 应为 %d", total, len(keys))
	}
	c.assertAllReadable(keys, 0)

	// 权重恢复后 n2 重新进环
	sw2 := c.setWeight("n2", 1)
	if err := c.startMigration(sw2.PlanId, -1); err != nil {
		t.Fatal(err)
	}
	if err := c.switchRing(sw2.PlanId); err != nil {
		t.Fatal(err)
	}
	c.cleanup(sw2.PlanId)
	if got := c.nodes["n2"].srv.KeyCount(); got == 0 {
		t.Fatal("权重恢复后 n2 应重新分到键")
	}
	c.assertAllReadable(keys, 0)
}

// ---------------------------------------------------------------------------
// 场景 3:迁移途中失败 —— 中断恢复;复制完成 ≠ 已安全切换
// ---------------------------------------------------------------------------

func TestMigrationInterrupted_ResumeAndVerify(t *testing.T) {
	c := newCluster(t, map[string]int{"n1": 1, "n2": 1, "n3": 1, "n4": 0})
	keys := c.putKeys("fail", 2000)

	sw := c.setWeight("n4", 1)
	// 故障注入:复制 2 个区间后"崩溃"
	err := c.startMigration(sw.PlanId, 2)
	if err == nil {
		t.Fatal("应注入故障")
	}
	if code := status.Code(err); code != codes.Aborted {
		t.Fatalf("故障应表现为 Aborted, 实际 %v", code)
	}
	st := c.mustStatus(sw.PlanId)
	if st.State != "COPYING" {
		t.Fatalf("中断后计划应保持 COPYING, 实际 %s", st.State)
	}
	copied := 0
	for _, rg := range st.Ranges {
		if rg.State == "COPIED" {
			copied++
		}
	}
	if copied != 2 {
		t.Fatalf("中断时应恰好完成 2 个区间, 实际 %d", copied)
	}

	// 中断期间:路由仍走旧环,数据完整可读
	c.assertAllReadable(keys, 0)

	// 复制未完成时禁止切换
	if err := c.switchRing(sw.PlanId); err == nil {
		t.Fatal("COPYING 状态不应允许切换")
	}

	// 断点续跑
	c.resumeMigration(sw.PlanId)
	st = c.mustStatus(sw.PlanId)
	if st.State != "COPIED" {
		t.Fatalf("续跑后应为 COPIED, 实际 %s", st.State)
	}
	if st.TotalExpectedKeys != st.TotalCopiedKeys {
		t.Fatalf("路由变化量 %d ≠ 实际迁移量 %d", st.TotalExpectedKeys, st.TotalCopiedKeys)
	}

	// 复制完成 ≠ 已切换:活跃环不变
	oldVersion := c.router.ActiveVersion()
	r, _ := c.cli.Router.Route(context.Background(), &cachelabv1.RouteRequest{Key: keys[0]})
	if r.RingVersion != oldVersion {
		t.Fatal("COPIED 后路由不应自动切换")
	}

	// 模拟目标节点在复制后、切换前宕机丢数据(n4 内存存储重启清空)
	c.restartNode("n4")

	// 切换前校验必须发现 n4 数据丢失并拒绝切换
	err = c.switchRing(sw.PlanId)
	if err == nil {
		t.Fatal("目标数据丢失时切换必须被拒绝")
	}
	if code := status.Code(err); code != codes.FailedPrecondition {
		t.Fatalf("校验失败应返回 FailedPrecondition, 实际 %v", code)
	}
	t.Logf("切换被正确拒绝: %v", err)

	// 被打回的区间重新复制后,切换成功
	c.resumeMigration(sw.PlanId)
	if err := c.switchRing(sw.PlanId); err != nil {
		t.Fatalf("重拷后切换应成功: %v", err)
	}
	c.cleanup(sw.PlanId)
	if total := c.totalKeys(); total != len(keys) {
		t.Fatalf("总键数 = %d, 应为 %d", total, len(keys))
	}
	c.assertAllReadable(keys, 0)
}

// restartNode 模拟节点宕机重启:内存数据丢失,以相同身份重新加入。
func (c *cluster) restartNode(id string) {
	c.t.Helper()
	h := c.nodes[id]
	h.gs.Stop()
	h.lis.Close()
	delete(c.nodes, id)

	lis, err := net.Listen("tcp", h.addr) // 同地址重启
	if err != nil {
		c.t.Fatal(err)
	}
	srv := kvnode.New(id, "tok-"+id)
	gs := grpc.NewServer(
		grpc.UnaryInterceptor(srv.UnaryInterceptor()),
		grpc.StreamInterceptor(srv.StreamInterceptor()),
	)
	cachelabv1.RegisterKVNodeServer(gs, srv)
	go gs.Serve(lis)
	c.nodes[id] = &nodeHandle{id: id, addr: h.addr, srv: srv, gs: gs, lis: lis}
	if err := c.joinNode(id, "tok-"+id); err != nil {
		c.t.Fatalf("rejoin %s: %v", id, err)
	}
}

// ---------------------------------------------------------------------------
// 场景 4:未知节点与错误令牌不能加入集群
// ---------------------------------------------------------------------------

func TestUnknownNode_CannotJoin(t *testing.T) {
	c := newCluster(t, map[string]int{"n1": 1, "n2": 1})

	// 完全未注册的节点
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := c.cli.Router.Join(ctx, &cachelabv1.JoinRequest{NodeId: "rogue", Token: "whatever"})
	if code := status.Code(err); code != codes.PermissionDenied {
		t.Fatalf("未知节点 Join 应 PermissionDenied, 实际 %v", code)
	}

	// 已注册但令牌错误
	_, err = c.cli.Router.Join(ctx, &cachelabv1.JoinRequest{NodeId: "n1", Token: "wrong-token"})
	if code := status.Code(err); code != codes.Unauthenticated {
		t.Fatalf("错误令牌 Join 应 Unauthenticated, 实际 %v", code)
	}

	// 未注册节点不能上环
	_, err = c.cli.Router.SetWeight(ctx, &cachelabv1.SetWeightRequest{NodeId: "rogue", Weight: 1})
	if code := status.Code(err); code != codes.NotFound {
		t.Fatalf("未知节点 SetWeight 应 NotFound, 实际 %v", code)
	}

	// 无令牌直接调用节点控制面被拒绝
	conn, err := grpc.NewClient(c.nodes["n1"].addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	nc := cachelabv1.NewKVNodeClient(conn)
	_, err = nc.Stats(ctx, &cachelabv1.StatsRequest{})
	if code := status.Code(err); code != codes.Unauthenticated {
		t.Fatalf("无令牌调用控制面应 Unauthenticated, 实际 %v", code)
	}
}

// ---------------------------------------------------------------------------
// 观测 API:键分布与热点统计
// ---------------------------------------------------------------------------

func TestDistributionAndHotKeys(t *testing.T) {
	c := newCluster(t, map[string]int{"n1": 1, "n2": 1, "n3": 1})
	keys := c.putKeys("dist", 3000)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	d, err := c.cli.Router.KeyDistribution(ctx, &cachelabv1.KeyDistributionRequest{})
	if err != nil {
		t.Fatal(err)
	}
	var actualSum int64
	var fracSum float64
	for _, n := range d.Nodes {
		actualSum += n.ActualKeys
		fracSum += n.PredictedFraction
		// 3 节点等权:预测占比应在 1/3 附近(虚拟节点随机性给宽界)
		if n.PredictedFraction < 0.15 || n.PredictedFraction > 0.55 {
			t.Errorf("节点 %s 预测占比 %.3f 异常", n.NodeId, n.PredictedFraction)
		}
		if n.ActualKeys == 0 {
			t.Errorf("节点 %s 没有键", n.NodeId)
		}
	}
	if actualSum != int64(len(keys)) {
		t.Fatalf("各节点键数之和 %d ≠ 写入数 %d", actualSum, len(keys))
	}
	if fracSum < 0.99 || fracSum > 1.01 {
		t.Fatalf("预测占比之和 %.3f 应为 1", fracSum)
	}

	// 热点:狂读两个键
	for i := 0; i < 300; i++ {
		if _, _, err := c.cli.Get("dist-00001", 0); err != nil {
			t.Fatal(err)
		}
		if _, _, err := c.cli.Get("dist-00002", 0); err != nil {
			t.Fatal(err)
		}
	}
	hk, err := c.cli.Router.HotKeys(ctx, &cachelabv1.HotKeysRequest{TopN: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(hk.Global) < 2 {
		t.Fatalf("热点键不足: %+v", hk.Global)
	}
	got := map[string]int64{}
	for _, k := range hk.Global {
		got[k.Key] = k.Accesses
	}
	if got["dist-00001"] < 300 || got["dist-00002"] < 300 {
		t.Fatalf("热点统计不准: %v", got)
	}
}

// ---------------------------------------------------------------------------
// 写路径:迁移窗口内双写保证不丢写
// ---------------------------------------------------------------------------

func TestWritesDuringMigration_AreMirrored(t *testing.T) {
	c := newCluster(t, map[string]int{"n1": 1, "n2": 1, "n3": 1, "n4": 0})
	keys := c.putKeys("dual", 1000)

	sw := c.setWeight("n4", 1)
	if err := c.startMigration(sw.PlanId, -1); err != nil {
		t.Fatal(err)
	}
	// 计划已 COPIED 但未切换:此时写入的键若落在迁移区间,必须双写到 n4
	newKeys := c.putKeys("dual-new", 500)
	if err := c.switchRing(sw.PlanId); err != nil {
		t.Fatalf("静默写入后切换应通过校验: %v", err)
	}
	c.cleanup(sw.PlanId)
	c.assertAllReadable(append(keys, newKeys...), 0)
	if total := c.totalKeys(); total != len(keys)+len(newKeys) {
		t.Fatalf("总键数 = %d, 应为 %d", total, len(keys)+len(newKeys))
	}
}
