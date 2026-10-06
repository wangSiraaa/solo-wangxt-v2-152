// Package router 实现路由服务:环版本管理、路由查询、迁移编排。
//
// 核心不变式:
//  1. 任何时刻只有一个活跃环版本;旧版本保留,供在途请求查询。
//  2. 权重变化只生成新环与迁移计划,绝不影响当前路由。
//  3. 复制完成(COPIED)≠ 可以切换:SwitchRing 逐区间校验源/目标键数一致。
//  4. 只有切换完成(SWITCHED)后才允许 Cleanup 删除源数据。
package router

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	cachelabv1 "cachelab/gen/cachelab/v1"
	"cachelab/internal/hash"
	"cachelab/internal/ring"
	"cachelab/internal/store"
)

type Server struct {
	cachelabv1.UnimplementedRouterServer
	st *store.Store

	mu     sync.RWMutex
	rings  map[int64]*ring.Ring
	active int64

	nodesMu sync.RWMutex
	nodes   map[string]store.Node // 节点注册表缓存

	connsMu sync.Mutex
	conns   map[string]cachelabv1.KVNodeClient

	migratingMu sync.Mutex // 同一时刻只允许一个迁移执行器
	migrating   bool
}

func New(st *store.Store) *Server {
	return &Server{
		st:    st,
		rings: map[int64]*ring.Ring{},
		nodes: map[string]store.Node{},
		conns: map[string]cachelabv1.KVNodeClient{},
	}
}

// Bootstrap 启动恢复:加载活跃环;若集群为空则以当前注册节点初始化首个环。
// 同时把处于 COPYING 状态的计划留给 ResumeMigration 恢复(崩溃恢复入口)。
func (s *Server) Bootstrap(ctx context.Context) error {
	if err := s.refreshNodes(ctx); err != nil {
		return err
	}
	active, err := s.st.ActiveRing(ctx)
	if err != nil {
		return err
	}
	if active == 0 {
		weights, err := s.st.Weights(ctx)
		if err != nil {
			return err
		}
		r := ring.Build(weights)
		v, err := s.st.BootstrapRing(ctx, r)
		if err != nil {
			return err
		}
		s.mu.Lock()
		s.rings[v] = r
		s.active = v
		s.mu.Unlock()
		return nil
	}
	r, err := s.st.LoadRing(ctx, active)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.rings[active] = r
	s.active = active
	s.mu.Unlock()
	return nil
}

func (s *Server) refreshNodes(ctx context.Context) error {
	nodes, err := s.st.ListNodes(ctx)
	if err != nil {
		return err
	}
	s.nodesMu.Lock()
	defer s.nodesMu.Unlock()
	s.nodes = map[string]store.Node{}
	for _, n := range nodes {
		s.nodes[n.NodeID] = n
	}
	return nil
}

func (s *Server) nodeInfo(nodeID string) (store.Node, bool) {
	s.nodesMu.RLock()
	defer s.nodesMu.RUnlock()
	n, ok := s.nodes[nodeID]
	return n, ok
}

// nodeConn 缓存到各节点的 gRPC 连接。
func (s *Server) nodeConn(ctx context.Context, nodeID string) (cachelabv1.KVNodeClient, error) {
	s.connsMu.Lock()
	defer s.connsMu.Unlock()
	if c, ok := s.conns[nodeID]; ok {
		return c, nil
	}
	n, ok := s.nodeInfo(nodeID)
	if !ok {
		return nil, status.Errorf(codes.NotFound, "node %q not registered", nodeID)
	}
	conn, err := grpc.NewClient(n.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	c := cachelabv1.NewKVNodeClient(conn)
	s.conns[nodeID] = c
	return c, nil
}

// nodeCtx 构造携带节点令牌的控制面调用上下文。
func (s *Server) nodeCtx(ctx context.Context, nodeID string) (context.Context, error) {
	n, ok := s.nodeInfo(nodeID)
	if !ok {
		return nil, status.Errorf(codes.NotFound, "node %q not registered", nodeID)
	}
	return metadata.AppendToOutgoingContext(ctx, "x-node-token", n.Token), nil
}

// ---------------- 节点管理 ----------------

func (s *Server) RegisterNode(ctx context.Context, req *cachelabv1.RegisterNodeRequest) (*cachelabv1.RegisterNodeResponse, error) {
	if req.NodeId == "" || req.Addr == "" || req.Token == "" {
		return nil, status.Error(codes.InvalidArgument, "node_id, addr, token are required")
	}
	if err := s.st.RegisterNode(ctx, store.Node{
		NodeID: req.NodeId, Addr: req.Addr, Token: req.Token, Weight: int(req.Weight),
	}); err != nil {
		return nil, err
	}
	return &cachelabv1.RegisterNodeResponse{}, s.refreshNodes(ctx)
}

// Join 节点加入:必须已预注册且令牌匹配,未知节点被拒绝。
func (s *Server) Join(ctx context.Context, req *cachelabv1.JoinRequest) (*cachelabv1.JoinResponse, error) {
	n, err := s.st.CheckJoin(ctx, req.NodeId, req.Token)
	if errors.Is(err, store.ErrNodeNotFound) {
		return nil, status.Errorf(codes.PermissionDenied, "node %q is not registered", req.NodeId)
	}
	if err != nil {
		return nil, status.Error(codes.Unauthenticated, "invalid token")
	}
	if err := s.st.MarkOnline(ctx, req.NodeId); err != nil {
		return nil, err
	}
	if err := s.refreshNodes(ctx); err != nil {
		return nil, err
	}
	s.mu.RLock()
	active := s.active
	s.mu.RUnlock()
	_ = n
	return &cachelabv1.JoinResponse{ActiveRingVersion: active, Weight: int32(n.Weight)}, nil
}

func (s *Server) ListNodes(ctx context.Context, _ *cachelabv1.ListNodesRequest) (*cachelabv1.ListNodesResponse, error) {
	nodes, err := s.st.ListNodes(ctx)
	if err != nil {
		return nil, err
	}
	resp := &cachelabv1.ListNodesResponse{}
	for _, n := range nodes {
		resp.Nodes = append(resp.Nodes, &cachelabv1.NodeInfo{
			NodeId: n.NodeID, Addr: n.Addr, Weight: int32(n.Weight), State: n.State,
		})
	}
	return resp, nil
}

// ---------------- 环版本 ----------------

// SetWeight 变更权重:生成新环(STAGING)与迁移计划(PREPARED),活跃环不变。
func (s *Server) SetWeight(ctx context.Context, req *cachelabv1.SetWeightRequest) (*cachelabv1.SetWeightResponse, error) {
	if req.Weight < 0 {
		return nil, status.Error(codes.InvalidArgument, "weight must be >= 0")
	}
	unfinished, err := s.st.UnfinishedPlan(ctx)
	if err != nil {
		return nil, err
	}
	if unfinished != nil {
		return nil, status.Errorf(codes.FailedPrecondition,
			"plan %d still in state %s; finish or clean it up first", unfinished.PlanID, unfinished.State)
	}
	weights, err := s.st.SetWeight(ctx, req.NodeId, int(req.Weight))
	if errors.Is(err, store.ErrNodeNotFound) {
		return nil, status.Errorf(codes.NotFound, "node %q not registered", req.NodeId)
	}
	if err != nil {
		return nil, err
	}
	newRing := ring.Build(weights)
	if newRing.Empty() {
		return nil, status.Error(codes.InvalidArgument, "resulting ring would be empty")
	}

	s.mu.RLock()
	active := s.active
	oldRing := s.rings[active]
	s.mu.RUnlock()

	newVersion, err := s.st.CreateRing(ctx, newRing, store.RingStaging)
	if err != nil {
		return nil, err
	}
	ranges := ring.Diff(oldRing, newRing)
	planID, err := s.st.CreatePlan(ctx, active, newVersion, ranges, hash.Hex)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.rings[newVersion] = newRing
	s.mu.Unlock()
	return &cachelabv1.SetWeightResponse{
		NewRingVersion: newVersion, PlanId: planID, RangeCount: int32(len(ranges)),
	}, nil
}

func (s *Server) ListRings(ctx context.Context, _ *cachelabv1.ListRingsRequest) (*cachelabv1.ListRingsResponse, error) {
	infos, err := s.st.ListRings(ctx)
	if err != nil {
		return nil, err
	}
	s.mu.RLock()
	active := s.active
	s.mu.RUnlock()
	resp := &cachelabv1.ListRingsResponse{}
	for _, ri := range infos {
		resp.Rings = append(resp.Rings, &cachelabv1.RingInfo{
			Version: ri.Version, State: ri.State, VnodeCount: int32(ri.VNodeCount),
			Active: ri.Version == active,
		})
	}
	return resp, nil
}

// ---------------- 路由 ----------------

func (s *Server) ringFor(version int64) (*ring.Ring, int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if version == 0 {
		version = s.active
	}
	r, ok := s.rings[version]
	if !ok {
		// 内存未缓存(如旧版本),从库中加载
		var err error
		r, err = s.st.LoadRing(context.Background(), version)
		if err != nil {
			return nil, 0, status.Errorf(codes.NotFound, "ring version %d not found", version)
		}
	}
	return r, version, nil
}

// Route 读路径路由。显式指定旧版本可为在途请求提供一致视图。
func (s *Server) Route(ctx context.Context, req *cachelabv1.RouteRequest) (*cachelabv1.RouteResponse, error) {
	r, version, err := s.ringFor(req.RingVersion)
	if err != nil {
		return nil, err
	}
	owner := r.OwnerOfKey(req.Key)
	if owner == "" {
		return nil, status.Error(codes.Unavailable, "ring is empty")
	}
	n, ok := s.nodeInfo(owner)
	if !ok {
		return nil, status.Errorf(codes.Internal, "owner %q not in registry", owner)
	}
	return &cachelabv1.RouteResponse{NodeId: owner, Addr: n.Addr, RingVersion: version}, nil
}

// RouteWrite 写路径路由:主副本按活跃环;若键落在进行中的迁移区间内,
// 额外返回镜像写目标(新属主),保证复制窗口内的写不丢失。
func (s *Server) RouteWrite(ctx context.Context, req *cachelabv1.RouteWriteRequest) (*cachelabv1.RouteWriteResponse, error) {
	r, version, err := s.ringFor(0)
	if err != nil {
		return nil, err
	}
	owner := r.OwnerOfKey(req.Key)
	if owner == "" {
		return nil, status.Error(codes.Unavailable, "ring is empty")
	}
	n, ok := s.nodeInfo(owner)
	if !ok {
		return nil, status.Errorf(codes.Internal, "owner %q not in registry", owner)
	}
	resp := &cachelabv1.RouteWriteResponse{
		Primary: &cachelabv1.WriteTarget{NodeId: owner, Addr: n.Addr, RingVersion: version},
	}
	// 迁移窗口:计划处于 COPYING/COPIED(未切换)时,区间内写需双写。
	plan, err := s.st.UnfinishedPlan(ctx)
	if err != nil {
		return nil, err
	}
	if plan != nil && (plan.State == store.PlanCopying || plan.State == store.PlanCopied) {
		p := hash.KeyPoint(req.Key)
		for _, rg := range plan.Ranges {
			start, err1 := hash.ParseHex(rg.StartHex)
			end, err2 := hash.ParseHex(rg.EndHex)
			if err1 != nil || err2 != nil {
				continue
			}
			if hash.InRange(p, start, end) && rg.ToNode != owner {
				if tn, ok := s.nodeInfo(rg.ToNode); ok {
					resp.Mirrors = append(resp.Mirrors, &cachelabv1.WriteTarget{
						NodeId: rg.ToNode, Addr: tn.Addr, RingVersion: plan.ToVersion,
					})
				}
			}
		}
	}
	return resp, nil
}

// ---------------- 观测 ----------------

func (s *Server) KeyDistribution(ctx context.Context, _ *cachelabv1.KeyDistributionRequest) (*cachelabv1.KeyDistributionResponse, error) {
	s.mu.RLock()
	r, active := s.rings[s.active], s.active
	s.mu.RUnlock()
	if r == nil {
		return nil, status.Error(codes.Unavailable, "no active ring")
	}
	fractions := r.Fractions()
	vnodeCount := map[string]int{}
	for _, vn := range r.VNodes() {
		vnodeCount[vn.NodeID]++
	}
	resp := &cachelabv1.KeyDistributionResponse{RingVersion: active}
	s.nodesMu.RLock()
	nodes := make([]store.Node, 0, len(s.nodes))
	for _, n := range s.nodes {
		nodes = append(nodes, n)
	}
	s.nodesMu.RUnlock()
	for _, n := range nodes {
		nd := &cachelabv1.NodeDistribution{
			NodeId: n.NodeID, PredictedFraction: fractions[n.NodeID],
			VnodeCount: int32(vnodeCount[n.NodeID]),
		}
		if n.State == store.NodeOnline {
			if c, err := s.nodeConn(ctx, n.NodeID); err == nil {
				nctx, _ := s.nodeCtx(ctx, n.NodeID)
				sctx, cancel := context.WithTimeout(nctx, 5*time.Second)
				if st, err := c.Stats(sctx, &cachelabv1.StatsRequest{}); err == nil {
					nd.ActualKeys = st.KeyCount
				}
				cancel()
			}
		}
		resp.Nodes = append(resp.Nodes, nd)
	}
	return resp, nil
}

func (s *Server) HotKeys(ctx context.Context, req *cachelabv1.HotKeysRequest) (*cachelabv1.HotKeysResponse, error) {
	topN := int(req.TopN)
	if topN <= 0 {
		topN = 10
	}
	resp := &cachelabv1.HotKeysResponse{}
	global := map[string]int64{}
	s.nodesMu.RLock()
	nodes := make([]store.Node, 0, len(s.nodes))
	for _, n := range s.nodes {
		nodes = append(nodes, n)
	}
	s.nodesMu.RUnlock()
	for _, n := range nodes {
		if n.State != store.NodeOnline {
			continue
		}
		c, err := s.nodeConn(ctx, n.NodeID)
		if err != nil {
			continue
		}
		nctx, _ := s.nodeCtx(ctx, n.NodeID)
		sctx, cancel := context.WithTimeout(nctx, 5*time.Second)
		st, err := c.Stats(sctx, &cachelabv1.StatsRequest{})
		cancel()
		if err != nil {
			continue
		}
		nh := &cachelabv1.NodeHotKeys{NodeId: n.NodeID}
		for i, hk := range st.HotKeys {
			if i >= topN {
				break
			}
			nh.HotKeys = append(nh.HotKeys, hk)
			global[hk.Key] += hk.Accesses
		}
		resp.PerNode = append(resp.PerNode, nh)
	}
	for k, v := range global {
		resp.Global = append(resp.Global, &cachelabv1.HotKey{Key: k, Accesses: v})
	}
	for i := range resp.Global {
		for j := i + 1; j < len(resp.Global); j++ {
			if resp.Global[j].Accesses > resp.Global[i].Accesses {
				resp.Global[i], resp.Global[j] = resp.Global[j], resp.Global[i]
			}
		}
	}
	if len(resp.Global) > topN {
		resp.Global = resp.Global[:topN]
	}
	return resp, nil
}

// ActiveVersion 返回当前活跃环版本(测试与调试用)。
func (s *Server) ActiveVersion() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.active
}

var _ = fmt.Sprintf // keep fmt imported if unused in future edits
