// 命令 demo 是 chaosring 路由器的教学命令行客户端, 同时提供带解说的
// walkthrough, 用真实 gRPC 调用把"新增节点 / 权重为零 / 迁移途中失败"
// 三个案例一步步演给学生看。
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"chaosring/internal/pb"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	addr := envOr("CHAOS_ROUTER", "127.0.0.1:9001")
	admin := envOr("CHAOS_ADMIN_TOKEN", "admin-secret")
	enroll := envOr("CHAOS_ENROLL_SECRET", "enroll-secret")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	must(err)
	r := pb.NewRouterClient(conn)

	args := os.Args[2:]
	cmd := &cui{ctx: ctx, r: r, admin: admin, enroll: enroll}
	switch os.Args[1] {
	case "register":
		cmd.register(args)
	case "nodes":
		cmd.nodes()
	case "put":
		cmd.put(args)
	case "get":
		cmd.get(args)
	case "del":
		cmd.del(args)
	case "route":
		cmd.route(args)
	case "rebalance":
		cmd.rebalance(args)
	case "migrate":
		cmd.migrate(args)
	case "distribution", "dist":
		cmd.distribution(args)
	case "hot":
		cmd.hot()
	case "node-fail":
		cmd.nodeFail(args)
	case "node-recover":
		cmd.nodeRecover(args)
	case "retire":
		cmd.retire(args)
	case "resume":
		cmd.resume()
	case "walkthrough":
		cmd.walkthrough(contains(args, "--include-unsafe"))
	default:
		usage()
		os.Exit(2)
	}
}

type cui struct {
	ctx    context.Context
	r      pb.RouterClient
	admin  string
	enroll string
}

func (c *cui) register(args []string) {
	// register <id> <address> [weight] [token]
	if len(args) < 2 {
		fatal("usage: register <id> <address> [weight] [token]")
	}
	w := uint32(0)
	token := ""
	if len(args) >= 3 {
		w = uint32(atoi(args[2]))
	}
	if len(args) >= 4 {
		token = args[3]
	}
	resp, err := c.r.RegisterNode(c.ctx, &pb.RegisterNodeRequest{
		AdminToken: c.admin, NodeId: args[0], Address: args[1],
		Weight: w, EnrollmentSecret: c.enroll, NodeToken: token,
	})
	must(err)
	printJSON(resp)
}

func (c *cui) nodes() {
	resp, err := c.r.ListNodes(c.ctx, &pb.ListNodesRequest{AdminToken: c.admin})
	must(err)
	printJSON(resp)
}

func (c *cui) put(args []string) {
	if len(args) < 2 {
		fatal("usage: put <key> <value>")
	}
	resp, err := c.r.Put(c.ctx, &pb.PutRequest{Key: args[0], Value: []byte(args[1])})
	must(err)
	printJSON(resp)
}

func (c *cui) get(args []string) {
	if len(args) < 1 {
		fatal("usage: get <key> [ring-hint]")
	}
	hint := uint64(0)
	if len(args) == 2 {
		hint = uint64(atoi(args[1]))
	}
	resp, err := c.r.Get(c.ctx, &pb.GetRequest{Key: args[0], RingHint: hint})
	must(err)
	printJSON(resp)
}

func (c *cui) del(args []string) {
	if len(args) < 1 {
		fatal("usage: del <key>")
	}
	resp, err := c.r.Delete(c.ctx, &pb.DeleteRequest{Key: args[0]})
	must(err)
	printJSON(resp)
}

func (c *cui) route(args []string) {
	if len(args) < 1 {
		fatal("usage: route <key> [ring-hint]")
	}
	hint := uint64(0)
	if len(args) == 2 {
		hint = uint64(atoi(args[1]))
	}
	resp, err := c.r.Route(c.ctx, &pb.RouteRequest{Key: args[0], RingHint: hint})
	must(err)
	printJSON(resp)
}

func (c *cui) rebalance(args []string) {
	// rebalance '<json weights>' [reason]
	if len(args) < 1 {
		fatal(`usage: rebalance '{"nA":1,"nB":1}' [reason]`)
	}
	var weights map[string]uint32
	must(json.Unmarshal([]byte(args[0]), &weights))
	reason := ""
	if len(args) == 2 {
		reason = args[1]
	}
	resp, err := c.r.Rebalance(c.ctx, &pb.RebalanceRequest{
		AdminToken: c.admin, Weights: weights, Reason: reason,
	})
	must(err)
	printMigration(resp)
}

func (c *cui) migrate(args []string) {
	if len(args) < 2 {
		fatal("usage: migrate <start|progress|commit|abort|retry> <migration-id> [--force]")
	}
	op, mid := args[0], args[1]
	force := contains(args[2:], "--force")
	switch op {
	case "start":
		resp, err := c.r.StartMigration(c.ctx, &pb.StartMigrationRequest{AdminToken: c.admin, MigrationId: mid})
		must(err)
		printMigration(resp)
	case "progress":
		resp, err := c.r.MigrationProgress(c.ctx, &pb.MigrationProgressRequest{AdminToken: c.admin, MigrationId: mid})
		must(err)
		printMigration(resp)
	case "commit":
		resp, err := c.r.CommitSwitch(c.ctx, &pb.CommitSwitchRequest{AdminToken: c.admin, MigrationId: mid, Force: force})
		must(err)
		printMigration(resp)
	case "abort":
		resp, err := c.r.AbortMigration(c.ctx, &pb.AbortMigrationRequest{AdminToken: c.admin, MigrationId: mid})
		must(err)
		printMigration(resp)
	case "retry":
		resp, err := c.r.RetryMigration(c.ctx, &pb.RetryMigrationRequest{AdminToken: c.admin, MigrationId: mid})
		must(err)
		printMigration(resp)
	default:
		fatal("unknown migrate op: " + op)
	}
}

func (c *cui) distribution(args []string) {
	req := &pb.DistributionRequest{AdminToken: c.admin}
	if len(args) >= 1 {
		req.RingVersion = uint64(atoi(args[0]))
	}
	resp, err := c.r.KeyDistribution(c.ctx, req)
	must(err)
	printJSON(resp)
}

func (c *cui) hot() {
	resp, err := c.r.HotStats(c.ctx, &pb.HotStatsRequest{AdminToken: c.admin})
	must(err)
	printJSON(resp)
}

func (c *cui) retire(args []string) {
	if len(args) < 1 {
		fatal("usage: retire <ring-version>")
	}
	resp, err := c.r.RetireOldRing(c.ctx, &pb.RetireRingRequest{
		AdminToken: c.admin, RingVersion: uint64(atoi(args[0]))})
	must(err)
	printJSON(resp)
}

func (c *cui) resume() {
	resp, err := c.r.Resume(c.ctx, &pb.ResumeRequest{AdminToken: c.admin})
	must(err)
	printJSON(resp)
}

// nodeDial 拿到某节点的直连地址, 返回节点 gRPC 客户端与令牌。
func (c *cui) nodeDial(id string) (pb.KVNodeClient, string) {
	nl, err := c.r.ListNodes(c.ctx, &pb.ListNodesRequest{AdminToken: c.admin})
	must(err)
	for _, n := range nl.Nodes {
		if n.NodeId == id {
			conn, derr := grpc.NewClient(n.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
			must(derr)
			return pb.NewKVNodeClient(conn), nodeTokenOf(id)
		}
	}
	fatal("node not registered: " + id)
	return nil, ""
}

// nodeTokenOf 与脚本约定的固定令牌保持一致(dev-up.sh 使用相同规则)。
func nodeTokenOf(id string) string { return "tok-" + id + "-secret" }

func (c *cui) nodeFail(args []string) {
	if len(args) < 1 {
		fatal("usage: node-fail <node-id>")
	}
	cli, _ := c.nodeDial(args[0])
	_, err := cli.Fail(c.ctx, &pb.FailRequest{AdminToken: c.admin})
	must(err)
	fmt.Printf("node %s is now FAILED (all RPCs return Unavailable)\n", args[0])
}

func (c *cui) nodeRecover(args []string) {
	if len(args) < 1 {
		fatal("usage: node-recover <node-id>")
	}
	cli, tok := c.nodeDial(args[0])
	_, err := cli.Recover(c.ctx, &pb.RecoverRequest{NodeToken: tok})
	must(err)
	fmt.Printf("node %s recovered\n", args[0])
}

func printMigration(m *pb.MigrationInfo) {
	fmt.Printf("migration %s: ring %d -> %d, state=%s\n",
		orDash(m.MigrationId), m.FromRing, m.ToRing, m.State)
	fmt.Printf("  total=%d replicated=%d promoted=%d pending=%d failed=%d\n",
		m.Total, m.Replicated, m.Promoted, m.Pending, m.Failed)
	for _, it := range m.Items {
		flag := ""
		if it.LastError != "" {
			flag = "  err=" + truncate(it.LastError, 60)
		}
		fmt.Printf("    %-28s %s -> %s  [%s]%s\n", it.Key, it.FromNode, it.ToNode, it.State, flag)
	}
}

// ---- 带解说的教学 walkthrough ----

// walkNode 描述演示集群里的固定节点(dev-up.sh 按相同端口/令牌启动)。
type walkNode struct {
	id     string
	addr   string
	weight uint32
}

func walkNodes() []walkNode {
	return []walkNode{
		{"nA", "127.0.0.1:9101", 1},
		{"nB", "127.0.0.1:9102", 1},
		{"nC", "127.0.0.1:9103", 1},
		{"nD", "127.0.0.1:9104", 0},
		{"nE", "127.0.0.1:9105", 0},
		{"nF", "127.0.0.1:9106", 0},
	}
}

// seedKey 与集成测试使用同一确定性高熵生成器(固定常量, 可重复)。
func seedKey(i int) string {
	x := uint32(i*2654435761 + 2463534242)
	x ^= x << 13
	x *= 1664525
	x ^= x >> 16
	y := uint32(i*40503 + 12345)
	y ^= y << 7
	return fmt.Sprintf("obj/%08x/%08x/k", x, y)
}

// bootstrap 幂等注册全部节点, 在空集群上建首环并灌入种子键。
// 返回当前 active 环版本。
func (c *cui) bootstrap() uint64 {
	existing := map[string]bool{}
	nl, err := c.r.ListNodes(c.ctx, &pb.ListNodesRequest{AdminToken: c.admin})
	must(err)
	for _, n := range nl.Nodes {
		existing[n.NodeId] = true
	}
	weights := map[string]uint32{}
	for _, n := range walkNodes() {
		weights[n.id] = n.weight
		if !existing[n.id] {
			tok := nodeTokenOf(n.id)
			_, err := c.r.RegisterNode(c.ctx, &pb.RegisterNodeRequest{
				AdminToken: c.admin, NodeId: n.id, Address: n.addr,
				Weight: n.weight, EnrollmentSecret: c.enroll, NodeToken: tok,
			})
			must(err)
			fmt.Printf("  registered %s @ %s (weight=%d)\n", n.id, n.addr, n.weight)
		}
	}

	// 空集群 -> 建首个 active 环。
	if d, derr := c.r.KeyDistribution(c.ctx, &pb.DistributionRequest{AdminToken: c.admin}); derr == nil {
		return d.RingVersion
	}
	seed := c.rebalanceRaw(weights, "initial ring")
	fmt.Printf("  seeded active ring = %d\n", seed.ToRing)

	// 灌入 120 个高熵键 + 2 个命名键。
	for i := 0; i < 120; i++ {
		c.putRaw(seedKey(i), fmt.Sprintf("v%d", i))
	}
	c.putRaw("named:alpha", "alpha-v1")
	c.putRaw("named:beta", "beta-v1")
	return seed.ToRing
}

func (c *cui) walkthrough(includeUnsafe bool) {
	banner("chaosring 教学演示: 一致性哈希 + 分阶段迁移")
	fmt.Println("哈希固定为 FNV-1a 64-bit(大端), 金向量见 hashring 包公开测试。")
	fmt.Println("原则: 复制(replication)与切换(switch)是两个独立阶段。")
	fmt.Println("提示: 建议对空集群运行(dev-reset.sh 可重置)。")
	fmt.Println()

	step(0, "自举: 注册 nA..nF(nA/B/C 权重1, nD/E/F 权重0), 建首环并灌种子键")
	r1 := c.bootstrap()
	printShare(c.distributionRaw(r1))

	// ---------------- 案例 1: 增加节点 ----------------
	step(1, "新增节点 nD(权重1): 只生成新环和迁移计划, 路由暂不改变")
	plan := c.rebalanceRaw(
		map[string]uint32{"nA": 1, "nB": 1, "nC": 1, "nD": 1, "nE": 0, "nF": 0}, "add nD")
	r2 := plan.ToRing
	var sampleKey string
	for _, it := range plan.Items {
		if it.ToNode == "nD" {
			sampleKey = it.Key
			break
		}
	}
	if sampleKey == "" {
		fatal("没有键迁向 nD: 请用 dev-reset.sh 重置空集群后重跑")
	}
	fmt.Printf("路由变化量 = %d 个键(由环%d/环%d 对已登记键 Diff 得到)。\n", plan.Total, plan.FromRing, r2)
	fmt.Println("此时路由仍按旧环解析, 下面这个键的属主不变:")
	before := c.routeRaw(sampleKey, 0)
	fmt.Printf("  %s -> %s (ring %d)\n", sampleKey, before.NodeId, before.RingVersion)

	step(2, "开始复制(StageCopy 到 nD 暂存区)")
	c.mustStart(plan.MigrationId)
	rep := c.wait(plan.MigrationId, "replicated")
	fmt.Printf("复制完成: replicated=%d/%d。注意: 这只是字节落到暂存区！\n", rep.Replicated, rep.Total)
	afterRep := c.routeRaw(sampleKey, 0)
	fmt.Printf("复制完成后再查路由: %s -> %s (ring %d)\n", sampleKey, afterRep.NodeId, afterRep.RingVersion)
	if afterRep.NodeId == "nD" {
		fatal("!!! 复制阶段路由不应改变, 这违反了复制/切换分离")
	}
	fmt.Println("=> 复制完成 ≠ 已安全切换。旧环仍是 active。")

	// 迁移窗口内写
	step(3, "迁移窗口内更新该键(双写: 旧主正式区 + 新主暂存区)")
	pr := c.putRaw(sampleKey, "WALKWINDOW-VALUE")
	fmt.Printf("dual_written=%v version=%d\n", pr.DualWritten, pr.Version)

	step(4, "显式提交切换 CommitSwitch: Promote 暂存区 + 提升新环为 active")
	committed := c.commitRaw(plan.MigrationId, false)
	fmt.Printf("实际迁移量 promoted=%d, 与路由变化量 %d 必须相等: %v\n",
		committed.Promoted, plan.Total, committed.Promoted == plan.Total)
	g := c.getRaw(sampleKey)
	fmt.Printf("切换后读取: %s -> %s (ring %d) = %q\n", sampleKey, g.NodeId, g.RingVersion, string(g.Value))
	if g.NodeId != "nD" || string(g.Value) != "WALKWINDOW-VALUE" {
		fatal("迁移窗口内的写在切换后丢失/路由未更新")
	}

	step(5, "在途请求: 持旧环版本仍可解析到旧主; 旧环宽限期内有效")
	old := c.routeRaw(sampleKey, plan.FromRing)
	fmt.Printf("  ring_hint=%d: %s -> %s\n", plan.FromRing, sampleKey, old.NodeId)
	// 独立对账: 路由变化量 == 迁移 promoted
	rd := c.ringDiffRaw(plan.FromRing, r2)
	fmt.Printf("独立 RingDiff 复算变化量=%d, 迁移 promoted=%d\n", rd.Changed, committed.Promoted)
	if rd.Changed != committed.Promoted {
		fatal("路由变化量与实际迁移量不一致")
	}

	// ---------------- 案例 2: 权重为零 ----------------
	step(6, "把 nD 权重调为 0: 节点保留成员身份但摘流, 0 个虚拟节点")
	p2 := c.rebalanceRaw(
		map[string]uint32{"nA": 1, "nB": 1, "nC": 1, "nD": 0, "nE": 0, "nF": 0}, "drain nD")
	r3 := p2.ToRing
	fmt.Printf("需要迁离 nD 的键 = %d\n", p2.Total)
	if p2.Total == 0 {
		fatal("摘流 nD 应当产生迁移键")
	}
	for _, it := range p2.Items {
		if it.ToNode == "nD" {
			fatal("零权重节点不应该接收任何键")
		}
	}
	c.mustStart(p2.MigrationId)
	c.wait(p2.MigrationId, "replicated")
	c.commitRaw(p2.MigrationId, false)
	d3 := c.distributionRaw(r3)
	printShare(d3)
	for _, s := range d3.Shares {
		if s.NodeId == "nD" && (s.Vnodes != 0 || s.OwnedKeys != 0) {
			fatal("摘流后 nD 仍持有 vnode/键")
		}
	}

	// ---------------- 案例 3: 迁移途中失败 ----------------
	step(7, "新增 nE, 然后在复制途中让 nE 宕机(故障注入)")
	p3 := c.rebalanceRaw(
		map[string]uint32{"nA": 1, "nB": 1, "nC": 1, "nD": 0, "nE": 1, "nF": 0}, "add nE")
	r4 := p3.ToRing
	var toE string
	for _, it := range p3.Items {
		if it.ToNode == "nE" {
			toE = it.Key
			break
		}
	}
	if toE == "" {
		fatal("没有键迁向 nE, 无法演示失败恢复")
	}
	c.putRaw(toE, "SURVIVE")
	c.nodeFailRaw("nE")
	c.mustStart(p3.MigrationId)
	failed := c.wait(p3.MigrationId, "failed")
	fmt.Printf("迁移状态 failed=%d/%d, 示例原因: %s\n",
		failed.Failed, failed.Total, firstError(failed))

	step(8, "数据仍安全: active 环未变; 此时提交必须被拒绝")
	safeGet := c.getRaw(toE)
	fmt.Printf("  读取 %s 仍来自旧环节点 %s(不是 nE)\n", toE, safeGet.NodeId)
	if _, err := c.r.CommitSwitch(c.ctx, &pb.CommitSwitchRequest{
		AdminToken: c.admin, MigrationId: p3.MigrationId}); err == nil {
		fatal("存在未复制条目时提交必须被拒绝")
	} else {
		fmt.Printf("  拒绝切换: %v\n", err)
	}

	step(9, "恢复 nE, 断点续传(只重试未完成条目), 再安全切换")
	c.nodeRecoverRaw("nE")
	retried, err := c.r.RetryMigration(c.ctx, &pb.RetryMigrationRequest{
		AdminToken: c.admin, MigrationId: p3.MigrationId})
	must(err)
	fmt.Printf("续传后: state=%s failed=%d replicated=%d\n",
		retried.State, retried.Failed, retried.Replicated)
	c.commitRaw(p3.MigrationId, false)
	g4 := c.getRaw(toE)
	fmt.Printf("切换后: %s -> %s = %q\n", toE, g4.NodeId, string(g4.Value))
	if g4.NodeId != "nE" || string(g4.Value) != "SURVIVE" {
		fatal("断点续传后数据不完整")
	}
	diff := c.ringDiffRaw(p3.FromRing, r4)
	final := c.progressRaw(p3.MigrationId)
	fmt.Printf("对账: 环%d->环%d 路由变化量=%d, 迁移 promoted=%d\n",
		p3.FromRing, r4, diff.Changed, final.Promoted)
	if diff.Changed != final.Promoted {
		fatal("路由变化量与实际迁移量不一致")
	}

	step(10, "Resume 中断恢复 API: 重启路由器后调用, 只续传不切换")
	res, err := c.r.Resume(c.ctx, &pb.ResumeRequest{AdminToken: c.admin})
	must(err)
	printJSON(res)

	if includeUnsafe {
		c.unsafeScenario()
	}
	banner("全部教学检查点通过 ✔")
}

// unsafeScenario 显式演示反面教材: 未复制完强行切换 -> 数据缺口。
func (c *cui) unsafeScenario() {
	step(100, "反面演示: nF 宕机时 force=true 强行切换(请勿在生产模仿)")
	p := c.rebalanceRaw(map[string]uint32{
		"nA": 1, "nB": 1, "nC": 1, "nD": 0, "nE": 1, "nF": 1}, "add nF")
	c.nodeFailRaw("nF")
	c.mustStart(p.MigrationId)
	c.wait(p.MigrationId, "failed")
	forced, err := c.r.CommitSwitch(c.ctx, &pb.CommitSwitchRequest{
		AdminToken: c.admin, MigrationId: p.MigrationId, Force: true})
	must(err)
	fmt.Printf("force 切换完成, failed 条目仍有 %d 个。恢复 nF 后这些键读不到:\n", forced.Failed)
	c.nodeRecoverRaw("nF")
	missing := 0
	for _, it := range forced.Items {
		if it.State == "failed" {
			g := c.getRaw(it.Key)
			if g.NodeId == "nF" && !g.Found {
				missing++
			}
		}
	}
	if missing == 0 {
		fatal("force 切换应当产生至少一个缺失键")
	}
	fmt.Printf("=> 数据缺口实证: %d 个键被路由到 nF 但那里没有值。\n", missing)
	fmt.Println("   这就是'把复制完成等同已安全切换'的后果。")
}

// ---- 原始调用小工具 ----

func (c *cui) rebalanceRaw(w map[string]uint32, reason string) *pb.MigrationInfo {
	m, err := c.r.Rebalance(c.ctx, &pb.RebalanceRequest{
		AdminToken: c.admin, Weights: w, Reason: reason})
	must(err)
	return m
}
func (c *cui) mustStart(mid string) {
	_, err := c.r.StartMigration(c.ctx, &pb.StartMigrationRequest{AdminToken: c.admin, MigrationId: mid})
	must(err)
}
func (c *cui) wait(mid, want string) *pb.MigrationInfo {
	for i := 0; i < 100; i++ {
		m, err := c.r.MigrationProgress(c.ctx, &pb.MigrationProgressRequest{
			AdminToken: c.admin, MigrationId: mid})
		must(err)
		if m.State == want {
			return m
		}
		time.Sleep(100 * time.Millisecond)
	}
	fatal("migration " + mid + " did not reach " + want)
	return nil
}
func (c *cui) commitRaw(mid string, force bool) *pb.MigrationInfo {
	m, err := c.r.CommitSwitch(c.ctx, &pb.CommitSwitchRequest{
		AdminToken: c.admin, MigrationId: mid, Force: force})
	must(err)
	return m
}
func (c *cui) progressRaw(mid string) *pb.MigrationInfo {
	m, err := c.r.MigrationProgress(c.ctx, &pb.MigrationProgressRequest{
		AdminToken: c.admin, MigrationId: mid})
	must(err)
	return m
}
func (c *cui) routeRaw(key string, hint uint64) *pb.RouteResponse {
	r, err := c.r.Route(c.ctx, &pb.RouteRequest{Key: key, RingHint: hint})
	must(err)
	return r
}
func (c *cui) putRaw(key, val string) *pb.PutResponse {
	r, err := c.r.Put(c.ctx, &pb.PutRequest{Key: key, Value: []byte(val)})
	must(err)
	return r
}
func (c *cui) getRaw(key string) *pb.GetResponse {
	r, err := c.r.Get(c.ctx, &pb.GetRequest{Key: key})
	must(err)
	return r
}
func (c *cui) distributionRaw(ring uint64) *pb.DistributionResponse {
	d, err := c.r.KeyDistribution(c.ctx, &pb.DistributionRequest{
		AdminToken: c.admin, RingVersion: ring})
	must(err)
	return d
}
func (c *cui) nodeFailRaw(id string) {
	cli, _ := c.nodeDial(id)
	_, e := cli.Fail(c.ctx, &pb.FailRequest{AdminToken: c.admin})
	must(e)
}
func (c *cui) nodeRecoverRaw(id string) {
	cli, tok := c.nodeDial(id)
	_, e := cli.Recover(c.ctx, &pb.RecoverRequest{NodeToken: tok})
	must(e)
}

func firstError(m *pb.MigrationInfo) string {
	for _, it := range m.Items {
		if it.LastError != "" {
			return truncate(it.LastError, 70)
		}
	}
	return "-"
}

// ringDiffRaw 调用路由器复算两环间的路由变化量。
func (c *cui) ringDiffRaw(from, to uint64) *pb.RingDiffResponse {
	d, err := c.r.RingDiff(c.ctx, &pb.RingDiffRequest{
		AdminToken: c.admin, FromRing: from, ToRing: to})
	must(err)
	return d
}

// ---- 输出工具 ----

func banner(s string)      { fmt.Println("\n================ " + s + " ================") }
func step(i int, s string) { fmt.Printf("\n--- 步骤 %d: %s ---\n", i, s) }

func printShare(d *pb.DistributionResponse) {
	fmt.Printf("环%d 键分布(共%d个登记键):\n", d.RingVersion, d.TotalRegisteredKeys)
	for _, s := range d.Shares {
		fmt.Printf("  %-4s vnodes=%-4d keys=%-4d share=%6.2f%%\n",
			s.NodeId, s.Vnodes, s.OwnedKeys, s.SharePct)
	}
}

func printJSON(v any) {
	b, _ := json.MarshalIndent(v, "", "  ")
	fmt.Println(string(b))
}

func usage() {
	fmt.Fprintln(os.Stderr, `commands:
  register <id> <address> [weight] [token]
  nodes
  put <key> <value> | get <key> [ring-hint] | del <key>
  route <key> [ring-hint]
  rebalance '<{"nA":1,...}>' [reason]
  migrate <start|progress|commit|abort|retry> <id> [--force]
  distribution [ring] | hot
  node-fail <id> | node-recover <id>
  retire <ring> | resume
  walkthrough [--include-unsafe]`)
}

func must(err error) {
	if err != nil {
		fatal(err.Error())
	}
}
func fatal(s string) {
	fmt.Fprintln(os.Stderr, "ERROR: "+s)
	os.Exit(1)
}
func atoi(s string) int {
	n, err := strconv.Atoi(s)
	must(err)
	return n
}
func contains(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}
func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}
func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
