package routersvc_test

import (
	"context"
	"fmt"
	"net"
	"sort"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"chaosring/internal/hashring"
	"chaosring/internal/nodesvc"
	"chaosring/internal/pb"
	"chaosring/internal/routersvc"
	"chaosring/internal/store"
)

// 这是一组端到端集成测试:
//
//	真实 PostgreSQL(保存环/虚拟节点/迁移进度) + 真实 gRPC KV 节点(带 WAL)。
//
// 覆盖教学案例:
//  1. 路由金向量(哈希固定)
//  2. 增加节点: 路由变化量 == 计划迁移量 == 实际提升量; 复制完成不等于切换
//  3. 迁移窗口双写(更新与删除)在切换后不丢
//  4. 权重为零摘流: 零 vnode, 无键路由到它
//  5. 迁移途中失败: 拒绝提交 -> 节点重启(WAL) -> 断点续传 -> 安全切换
//  6. 路由器"重启" + Resume 恢复
//  7. force 提交演示"复制完成被误当安全切换"产生的数据缺口
//  8. 旧环在途请求: 提交后仍可按旧版本路由, 退役后明确拒绝
//  9. 未知节点: 错 enrollment_secret / 错 node_token / 未登记权重 全部拒绝
//  10. 键分布与热点统计 API
const (
	adminToken = "admin-secret"
	enroll     = "enroll-secret"
)

// testKeys 是本轮演示使用的高熵键集(确定性, 不依赖随机源)。
var testKeys []string

// sampleKey 生成在 FNV-1a 环上分布均匀的确定性键。
// 用线性同余(固定常量)打散整数, 再以十六进制混入多个分隔符,
// 避免 "key:0001" 这类连续后缀造成的哈希聚集。
func sampleKey(i int) string {
	// 数值配方里的数字固定, 保证整套测试可重复
	x := uint32(i*2654435761 + 2463534242)
	x ^= x << 13
	x *= 1664525
	x ^= x >> 16
	y := uint32(i*40503 + 12345)
	y ^= y << 7
	return fmt.Sprintf("obj/%08x/%08x/k", x, y)
}

type harness struct {
	t       *testing.T
	ctx     context.Context
	st      *store.Store
	router  pb.RouterClient
	svc     *routersvc.Service
	nodes   map[string]*nodeHandle
	baseDir string
}

type nodeHandle struct {
	id     string
	token  string
	addr   string
	dir    string
	server *grpc.Server
	lis    net.Listener
	svc    *nodesvc.Server
}

func dsn() string {
	return "host=/tmp port=5432 user=chaos dbname=chaosring sslmode=disable"
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	ctx := context.Background()
	st, err := store.Open(ctx, dsn())
	if err != nil {
		t.Skipf("postgres not available, skip integration: %v", err)
	}
	if err := st.TruncateForTests(ctx); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	h := &harness{
		t: t, ctx: ctx, st: st,
		nodes: map[string]*nodeHandle{},
	}
	h.svc = routersvc.New(routersvc.Config{
		Store: st, AdminToken: adminToken, EnrollmentSecret: enroll,
	})
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	g := grpc.NewServer()
	pb.RegisterRouterServer(g, h.svc)
	go func() { _ = g.Serve(lis) }()
	t.Cleanup(func() {
		g.GracefulStop()
		for _, n := range h.nodes {
			n.server.GracefulStop()
			_ = n.lis.Close()
		}
		h.svc.Close()
		st.Close()
	})
	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	h.router = pb.NewRouterClient(conn)
	return h
}

// startNode 在随机端口启动节点, 令牌固定可预测。
func (h *harness) startNode(id string) *nodeHandle {
	token := "tok-" + id + "-secret"
	dir := h.t.TempDir()
	ns, err := nodesvc.New(nodesvc.Config{
		NodeID: id, Token: token, AdminToken: adminToken, DataDir: dir,
	})
	if err != nil {
		h.t.Fatalf("node %s new: %v", id, err)
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		h.t.Fatal(err)
	}
	g := grpc.NewServer()
	pb.RegisterKVNodeServer(g, ns)
	go func() { _ = g.Serve(lis) }()
	n := &nodeHandle{id: id, token: token, addr: lis.Addr().String(), dir: dir, server: g, lis: lis, svc: ns}
	h.nodes[id] = n
	return n
}

// restartNode 模拟节点进程崩溃后重启: 新进程重放同一 WAL 目录。
func (h *harness) restartNode(n *nodeHandle) {
	n.server.GracefulStop()
	_ = n.lis.Close()
	addr := n.addr
	// 重新监听同一地址
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		h.t.Fatalf("relisten %s: %v", addr, err)
	}
	ns, err := nodesvc.New(nodesvc.Config{
		NodeID: n.id, Token: n.token, AdminToken: adminToken, DataDir: n.dir,
	})
	if err != nil {
		h.t.Fatalf("node %s reload: %v", n.id, err)
	}
	g := grpc.NewServer()
	pb.RegisterKVNodeServer(g, ns)
	go func() { _ = g.Serve(lis) }()
	n.server, n.lis, n.svc = g, lis, ns
}

func (h *harness) register(id string, weight uint32) {
	n := h.nodes[id]
	if n == nil {
		n = h.startNode(id)
	}
	_, err := h.router.RegisterNode(h.ctx, &pb.RegisterNodeRequest{
		AdminToken: adminToken, NodeId: id, Address: n.addr,
		Weight: weight, EnrollmentSecret: enroll, NodeToken: n.token,
	})
	if err != nil {
		h.t.Fatalf("register %s: %v", id, err)
	}
}

func (h *harness) rebalance(weights map[string]uint32, reason string) *pb.MigrationInfo {
	info, err := h.router.Rebalance(h.ctx, &pb.RebalanceRequest{
		AdminToken: adminToken, Weights: weights, Reason: reason,
	})
	if err != nil {
		h.t.Fatalf("rebalance: %v", err)
	}
	return info
}

func (h *harness) put(key, val string) *pb.PutResponse {
	r, err := h.router.Put(h.ctx, &pb.PutRequest{Key: key, Value: []byte(val)})
	if err != nil {
		h.t.Fatalf("put %s: %v", key, err)
	}
	return r
}

func (h *harness) get(key string) *pb.GetResponse {
	r, err := h.router.Get(h.ctx, &pb.GetRequest{Key: key})
	if err != nil {
		h.t.Fatalf("get %s: %v", key, err)
	}
	return r
}

func (h *harness) nodeClient(id string) (pb.KVNodeClient, string) {
	n := h.nodes[id]
	conn, err := grpc.NewClient(n.addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		h.t.Fatal(err)
	}
	return pb.NewKVNodeClient(conn), n.token
}

func (h *harness) waitMigration(mid string, want string) *pb.MigrationInfo {
	for i := 0; i < 50; i++ {
		info, err := h.router.MigrationProgress(h.ctx, &pb.MigrationProgressRequest{
			AdminToken: adminToken, MigrationId: mid,
		})
		if err != nil {
			h.t.Fatal(err)
		}
		if info.State == want {
			return info
		}
		time.Sleep(20 * time.Millisecond)
	}
	info, _ := h.router.MigrationProgress(h.ctx, &pb.MigrationProgressRequest{
		AdminToken: adminToken, MigrationId: mid,
	})
	h.t.Fatalf("migration %s never reached %s, got %s", mid, want, info.State)
	return nil
}

// TestEndToEnd 顺序执行完整教学脚本。
func TestEndToEnd(t *testing.T) {
	h := newHarness(t)

	// ---- 0. 未知节点防护 ----------------------------------------------------
	t.Run("unknown_node_rejected", func(t *testing.T) {
		n := h.startNode("rogue")
		// 错误的 enrollment_secret
		if _, err := h.router.RegisterNode(h.ctx, &pb.RegisterNodeRequest{
			AdminToken: adminToken, NodeId: "rogue", Address: n.addr,
			EnrollmentSecret: "wrong", NodeToken: n.token,
		}); err == nil {
			t.Fatal("register with wrong enrollment secret must fail")
		}
		// 错误的 admin token
		if _, err := h.router.RegisterNode(h.ctx, &pb.RegisterNodeRequest{
			AdminToken: "nope", NodeId: "rogue", Address: n.addr,
			EnrollmentSecret: enroll, NodeToken: n.token,
		}); err == nil {
			t.Fatal("register with wrong admin token must fail")
		}
		// 未注册节点, 直接拿假 token 调用节点数据面
		badConn, _ := grpc.NewClient(n.addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		badCli := pb.NewKVNodeClient(badConn)
		if _, err := badCli.Put(h.ctx, &pb.NodePutRequest{
			Key: "x", Value: []byte("y"), NodeToken: "forged",
		}); err == nil {
			t.Fatal("node must reject forged token")
		}
		// Rebalance 引用未登记节点
		h.register("nA", 1)
		if _, err := h.router.Rebalance(h.ctx, &pb.RebalanceRequest{
			AdminToken: adminToken,
			Weights:    map[string]uint32{"nA": 1, "ghost": 1},
		}); err == nil {
			t.Fatal("rebalance referencing unregistered node must fail")
		}
	})

	// ---- 1. 三节点初始环 + 金向量 -------------------------------------------
	h.register("nB", 1)
	h.register("nC", 1)
	seed := h.rebalance(map[string]uint32{"nA": 1, "nB": 1, "nC": 1}, "initial")
	if seed.ToRing != 1 {
		t.Fatalf("seed ring = %d, want 1", seed.ToRing)
	}
	golden := map[string]string{
		"user:1": "nC", "user:2": "nC", "user:3": "nC",
		"user:100": "nA", "order:42": "nC", "hotkey:🔥": "nA",
	}
	for k, want := range golden {
		rr, err := h.router.Route(h.ctx, &pb.RouteRequest{Key: k})
		if err != nil {
			t.Fatal(err)
		}
		if rr.NodeId != want {
			t.Fatalf("golden route %q = %s, want %s", k, rr.NodeId, want)
		}
		if rr.RingVersion != 1 {
			t.Fatalf("route ring = %d, want 1", rr.RingVersion)
		}
	}

	// 写入 200 个"高熵"键。
	// 注意: 顺序键(如 key:000..key:199)在 FNV-1a 下会严重聚集,
	// 这本身是教学要展示的现象; 这里用分散键集以保证新增节点能拿到样本。
	testKeys = make([]string, 200)
	for i := 0; i < 200; i++ {
		k := sampleKey(i)
		testKeys[i] = k
		h.put(k, "v"+fmt.Sprint(i))
	}
	// 热点: 反复读两个键
	for i := 0; i < 30; i++ {
		h.get(testKeys[7])
	}

	// ---- 2. 增加节点 nD -----------------------------------------------------
	h.register("nD", 0) // 注册时权重 0, 不承载
	ring1, _ := hashRingOf(t, h.st, 1)
	info2 := h.rebalance(map[string]uint32{"nA": 1, "nB": 1, "nC": 1, "nD": 1}, "add nD")
	if info2.ToRing != 2 || info2.State != store.MigPlanned {
		t.Fatalf("m-2 unexpected: %+v", info2)
	}
	// 路由变化量必须等于环 Diff(应用到已存在键)
	ring2, _ := hashRingOf(t, h.st, 2)
	var allKeys []string
	recs, _ := h.st.AllKeys(h.ctx)
	for _, r := range recs {
		allKeys = append(allKeys, r.Key)
	}
	diffMoves := hashring.Diff(ring1, ring2, allKeys)
	if int(info2.Total) != len(diffMoves) {
		t.Fatalf("planned moves %d != ring diff %d", info2.Total, len(diffMoves))
	}
	movingKey := ""
	for _, m := range diffMoves {
		if m.ToNode == "nD" {
			movingKey = m.Key
			break
		}
	}
	if movingKey == "" {
		t.Fatal("expected at least one key moving to nD")
	}

	// 复制阶段: 先在"迁移窗口内"更新一个会迁移的键, 并删除另一个
	oldOwnerBefore := ring1.OwnerOfKey(movingKey)
	h.put(movingKey, "VALUE-AFTER-REBALANCE") // 应双写
	delKey := ""
	for _, m := range diffMoves {
		if m.ToNode == "nD" && m.Key != movingKey {
			delKey = m.Key
			break
		}
	}
	if delKey == "" {
		t.Fatal("need second key moving to nD for delete test; enlarge key set")
	}
	if _, err := h.router.Delete(h.ctx, &pb.DeleteRequest{Key: delKey}); err != nil {
		t.Fatal(err)
	}

	if _, err := h.router.StartMigration(h.ctx, &pb.StartMigrationRequest{
		AdminToken: adminToken, MigrationId: "m-2",
	}); err != nil {
		t.Fatal(err)
	}
	rep := h.waitMigration("m-2", store.MigReplicated)
	if int(rep.Replicated) != int(rep.Total) {
		t.Fatalf("replicated %d != total %d", rep.Replicated, rep.Total)
	}

	// 关键检查点: 复制完成 != 已切换。active 环仍是 1。
	if rr, _ := h.router.Route(h.ctx, &pb.RouteRequest{Key: movingKey}); rr.NodeId != oldOwnerBefore {
		t.Fatalf("after replication only, route already changed to %s", rr.NodeId)
	}
	if g := h.get(movingKey); g.NodeId != oldOwnerBefore {
		t.Fatalf("read after replication must still serve old owner, got %s", g.NodeId)
	}

	// 显式提交切换
	committed, err := h.router.CommitSwitch(h.ctx, &pb.CommitSwitchRequest{
		AdminToken: adminToken, MigrationId: "m-2",
	})
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	if committed.State != store.MigCommitted {
		t.Fatalf("state = %s", committed.State)
	}
	// 实际迁移量(提升条目) == 路由变化量
	if int(committed.Promoted) != len(diffMoves) {
		t.Fatalf("promoted %d != diff %d", committed.Promoted, len(diffMoves))
	}
	// 切换后新值在新主可见(双写生效), 删除也是墓碑
	if g := h.get(movingKey); string(g.Value) != "VALUE-AFTER-REBALANCE" || g.NodeId != "nD" {
		t.Fatalf("post-switch get = %q @%s", string(g.Value), g.NodeId)
	}
	if g := h.get(delKey); g.Found && !g.Deleted {
		t.Fatal("tombstone must propagate to new owner")
	}
	// 全局完整性: 每个仍存活的键, 路由属主与注册表属主一致
	h.assertOwnersMatchRing(2)

	// 旧环在途请求: 按 ring_hint=1 仍解析到旧主
	rr, err := h.router.Route(h.ctx, &pb.RouteRequest{Key: movingKey, RingHint: 1})
	if err != nil {
		t.Fatal(err)
	}
	if rr.NodeId != oldOwnerBefore {
		t.Fatalf("in-flight old-ring route = %s, want %s", rr.NodeId, oldOwnerBefore)
	}

	// ---- 3. 权重为零摘流 nD -------------------------------------------------
	info3 := h.rebalance(map[string]uint32{"nA": 1, "nB": 1, "nC": 1, "nD": 0}, "drain nD")
	if info3.ToRing != 3 {
		t.Fatalf("ring = %d", info3.ToRing)
	}
	if info3.Total == 0 {
		t.Fatal("draining nD must move its keys")
	}
	for _, it := range info3.Items {
		if it.FromNode != "nD" {
			t.Fatalf("drain move from %s, want nD", it.FromNode)
		}
		if it.ToNode == "nD" {
			t.Fatal("zero-weight node cannot receive keys")
		}
	}
	h.startAndCommit(t, "m-3")
	h.assertOwnersMatchRing(3)
	dist, err := h.router.KeyDistribution(h.ctx, &pb.DistributionRequest{AdminToken: adminToken})
	if err != nil {
		t.Fatal(err)
	}
	for _, sh := range dist.Shares {
		if sh.NodeId == "nD" && (sh.Vnodes != 0 || sh.OwnedKeys != 0) {
			t.Fatalf("nD after drain: vnodes=%d owned=%d, want 0/0", sh.Vnodes, sh.OwnedKeys)
		}
	}

	// 热点统计冒烟
	hot, err := h.router.HotStats(h.ctx, &pb.HotStatsRequest{AdminToken: adminToken})
	if err != nil {
		t.Fatal(err)
	}
	if len(hot.GlobalTop) == 0 {
		t.Fatal("hot stats empty")
	}
	foundHot := false
	for _, k := range hot.GlobalTop {
		if k.Key == testKeys[7] && k.Gets >= 30 {
			foundHot = true
		}
	}
	if !foundHot {
		t.Fatalf("hotkey testKeys[7] missing/undercounted: %+v", hot.GlobalTop)
	}

	// ---- 4. 迁移途中失败 + 节点重启 + 断点续传 -------------------------------
	h.register("nE", 0)
	info4 := h.rebalance(map[string]uint32{"nA": 1, "nB": 1, "nC": 1, "nD": 0, "nE": 1}, "add nE")
	if info4.Total == 0 {
		t.Fatal("adding nE should move keys")
	}
	// 让一个将落到 nE 的键带上特殊值, 便于事后校验
	var toE []string
	for _, it := range info4.Items {
		if it.ToNode == "nE" {
			toE = append(toE, it.Key)
		}
	}
	sort.Strings(toE)
	h.put(toE[0], "SURVIVE-RESTART")

	// 故障注入: nE 宕机, 开始复制 -> 部分失败
	ec, _ := h.nodeClient("nE")
	if _, err := ec.Fail(h.ctx, &pb.FailRequest{AdminToken: adminToken}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.router.StartMigration(h.ctx, &pb.StartMigrationRequest{
		AdminToken: adminToken, MigrationId: "m-4",
	}); err != nil {
		t.Fatal(err)
	}
	failed := h.waitMigration("m-4", store.MigFailed)
	if failed.Failed == 0 {
		t.Fatal("expected failed items while nE down")
	}
	// 检查点: 迁移失败时禁止切换(复制进度 != 安全切换)
	if _, err := h.router.CommitSwitch(h.ctx, &pb.CommitSwitchRequest{
		AdminToken: adminToken, MigrationId: "m-4",
	}); err == nil {
		t.Fatal("commit must be refused while items not replicated")
	}
	// active 环仍是 3, 数据继续可用
	if g := h.get(toE[0]); g.NodeId == "nE" {
		t.Fatal("active routing must not switch to failed migration target")
	}

	// 节点进程重启(重放 WAL, 恢复服务), 路由器侧再"重启"并 Resume
	// 先 Recover gRPC 故障位不做——直接重启进程, 故障位天然消失。
	h.restartNode(h.nodes["nE"])
	newSvc := routersvc.New(routersvc.Config{
		Store: h.st, AdminToken: adminToken, EnrollmentSecret: enroll,
	})
	res, err := newSvc.Resume(h.ctx, &pb.ResumeRequest{AdminToken: adminToken})
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if len(res.FailedMigrations) != 0 {
		t.Fatalf("after node restart resume should finish, failed=%v", res.FailedMigrations)
	}
	if !contains(res.RecoveredMigrations, "m-4") {
		t.Fatalf("m-4 not recovered: %+v", res)
	}
	// 再次显式 Retry 也应是幂等无操作
	retry, err := h.router.RetryMigration(h.ctx, &pb.RetryMigrationRequest{
		AdminToken: adminToken, MigrationId: "m-4",
	})
	if err != nil {
		t.Fatal(err)
	}
	if retry.State != store.MigReplicated || retry.Failed != 0 {
		t.Fatalf("retry state=%s failed=%d", retry.State, retry.Failed)
	}
	// 提交并验证完整性
	if _, err := h.router.CommitSwitch(h.ctx, &pb.CommitSwitchRequest{
		AdminToken: adminToken, MigrationId: "m-4",
	}); err != nil {
		t.Fatalf("commit after recovery: %v", err)
	}
	h.assertOwnersMatchRing(4)
	if g := h.get(toE[0]); g.NodeId != "nE" || string(g.Value) != "SURVIVE-RESTART" {
		t.Fatalf("key did not survive migration safely: @%s=%q", g.NodeId, string(g.Value))
	}
	// 对账: 环 3->4 的 Diff 量与迁移提升量完全相等
	ring3, _ := hashRingOf(t, h.st, 3)
	ring4, _ := hashRingOf(t, h.st, 4)
	diff34 := hashring.Diff(ring3, ring4, allKeys)
	final4 := h.waitMigration("m-4", store.MigCommitted)
	if int(final4.Promoted) != len(diff34) {
		t.Fatalf("m-4 promoted %d != diff %d", final4.Promoted, len(diff34))
	}

	// ---- 5. 旧环退役 --------------------------------------------------------
	// m-2 之后环 1 是 old(被环 2 取代), 但又经历了 3、4。环 1 仍是 old。
	if _, err := h.router.RetireOldRing(h.ctx, &pb.RetireRingRequest{
		AdminToken: adminToken, RingVersion: 1,
	}); err != nil {
		t.Fatalf("retire ring 1: %v", err)
	}
	if rr, err := h.router.Route(h.ctx, &pb.RouteRequest{Key: movingKey, RingHint: 1}); err != nil {
		// 路由服务选择返回错误
		if rr != nil {
			t.Fatal("retired ring should be indicated")
		}
	} else if !rr.Retired {
		t.Fatal("retired ring must set retired=true")
	}

	// ---- 6. force 提交: 演示误把"复制完成"当安全切换的后果 -------------------
	h.register("nF", 0)
	info5 := h.rebalance(map[string]uint32{"nA": 1, "nB": 1, "nC": 1, "nD": 0, "nE": 1, "nF": 1}, "add nF")
	var toF []string
	for _, it := range info5.Items {
		if it.ToNode == "nF" {
			toF = append(toF, it.Key)
		}
	}
	if len(toF) == 0 {
		t.Fatal("expected keys moving to nF")
	}
	fc, ftok := h.nodeClient("nF")
	_, _ = fc.Fail(h.ctx, &pb.FailRequest{AdminToken: adminToken})
	if _, err := h.router.StartMigration(h.ctx, &pb.StartMigrationRequest{
		AdminToken: adminToken, MigrationId: "m-5",
	}); err != nil {
		t.Fatal(err)
	}
	bad := h.waitMigration("m-5", store.MigFailed)
	if bad.Failed == 0 {
		t.Fatal("m-5 expected failures with nF down")
	}
	// 正常路径拒绝; force=true 强行切换 -> 产生数据缺口
	if _, err := h.router.CommitSwitch(h.ctx, &pb.CommitSwitchRequest{
		AdminToken: adminToken, MigrationId: "m-5",
	}); err == nil {
		t.Fatal("non-forced commit must be rejected")
	}
	forced, err := h.router.CommitSwitch(h.ctx, &pb.CommitSwitchRequest{
		AdminToken: adminToken, MigrationId: "m-5", Force: true,
	})
	if err != nil {
		t.Fatalf("force commit: %v", err)
	}
	if forced.Failed == 0 {
		t.Fatal("force commit should retain failed count to expose the gap")
	}
	// 恢复 nF: 路由已经指向它, 但未复制的键读不到——这就是不安全切换的缺口实证
	h.restartNode(h.nodes["nF"])
	missing := 0
	for _, k := range toF {
		g := h.get(k)
		if g.NodeId == "nF" && !g.Found {
			missing++
		}
	}
	if missing == 0 {
		t.Fatal("force-switch expected at least one unreadable key at nF (data gap)")
	}
	t.Logf("unsafe switch demonstration: %d/%d keys routed to nF are missing", missing, len(toF))
	_ = ftok
}

// ---- helpers ----

func (h *harness) startAndCommit(t *testing.T, mid string) {
	t.Helper()
	if _, err := h.router.StartMigration(h.ctx, &pb.StartMigrationRequest{
		AdminToken: adminToken, MigrationId: mid,
	}); err != nil {
		t.Fatal(err)
	}
	h.waitMigration(mid, store.MigReplicated)
	if _, err := h.router.CommitSwitch(h.ctx, &pb.CommitSwitchRequest{
		AdminToken: adminToken, MigrationId: mid,
	}); err != nil {
		t.Fatalf("commit %s: %v", mid, err)
	}
	h.waitMigration(mid, store.MigCommitted)
}

// assertOwnersMatchRing 校验数据完整性: 注册表每个存活键的属主 == 该环路由属主,
// 且该属主节点上确实能读到值(键没有在迁移中凭空消失)。
func (h *harness) assertOwnersMatchRing(ringVersion int64) {
	h.t.Helper()
	r, err := func() (*hashring.Ring, error) {
		rec, err := h.st.GetRing(h.ctx, ringVersion)
		if err != nil {
			return nil, err
		}
		mw := make([]hashring.NodeWeight, 0, len(rec.Weights))
		for id, w := range rec.Weights {
			mw = append(mw, hashring.NodeWeight{NodeID: id, Weight: uint32(w)})
		}
		return hashring.New(rec.Version, mw)
	}()
	if err != nil {
		h.t.Fatal(err)
	}
	recs, err := h.st.AllKeys(h.ctx)
	if err != nil {
		h.t.Fatal(err)
	}
	conns := map[string]pb.KVNodeClient{}
	for _, rec := range recs {
		want := r.Owner(rec.Hash)
		if rec.CurrentOwner != want {
			h.t.Fatalf("registry owner of %q = %s, ring %d routes to %s",
				rec.Key, rec.CurrentOwner, ringVersion, want)
		}
		if rec.Deleted {
			continue
		}
		c := conns[want]
		if c == nil {
			cli, _ := h.nodeClient(want)
			c = cli
			conns[want] = c
		}
		tok := h.nodes[want].token
		gr, err := c.Get(h.ctx, &pb.NodeGetRequest{Key: rec.Key, NodeToken: tok})
		if err != nil {
			h.t.Fatalf("read %q at owner %s: %v", rec.Key, want, err)
		}
		if !gr.Found || gr.Deleted {
			h.t.Fatalf("live key %q missing at routed owner %s", rec.Key, want)
		}
	}
}

func hashRingOf(t *testing.T, st *store.Store, version int64) (*hashring.Ring, []hashring.NodeWeight) {
	t.Helper()
	rec, err := st.GetRing(context.Background(), version)
	if err != nil {
		t.Fatal(err)
	}
	mw := make([]hashring.NodeWeight, 0, len(rec.Weights))
	for id, w := range rec.Weights {
		mw = append(mw, hashring.NodeWeight{NodeID: id, Weight: uint32(w)})
	}
	sort.Slice(mw, func(i, j int) bool { return mw[i].NodeID < mw[j].NodeID })
	r, err := hashring.New(rec.Version, mw)
	if err != nil {
		t.Fatal(err)
	}
	return r, mw
}

func contains(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}
