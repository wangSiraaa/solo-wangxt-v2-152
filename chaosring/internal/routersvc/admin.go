package routersvc

import (
	"context"
	"time"

	"chaosring/internal/hashring"
	"chaosring/internal/pb"
	"chaosring/internal/store"
)

// RegisterNode 登记新节点。双重门:
//  1. 管理员令牌(谁可以扩容集群);
//  2. enrollment_secret(节点必须事先与带外共享秘密一致)。
//
// 新节点初始权重为 0: 已登记但暂不上环, 必须再走一次 Rebalance 才承载流量,
// 避免"一连上就自动被分配键"。
func (s *Service) RegisterNode(ctx context.Context, req *pb.RegisterNodeRequest) (*pb.RegisterNodeResponse, error) {
	if err := s.checkAdmin(req.AdminToken); err != nil {
		return nil, err
	}
	if err := s.checkEnrollment(req.EnrollmentSecret); err != nil {
		return nil, err
	}
	if req.NodeId == "" || req.Address == "" {
		return nil, errInvalidArg("node_id and address are required")
	}
	if _, err := s.st.GetNode(ctx, req.NodeId); err == nil {
		return nil, errInvalidArg("node %q already registered", req.NodeId)
	}
	token := req.NodeToken
	if token == "" {
		t, err := newToken()
		if err != nil {
			return nil, errInternal("token: %v", err)
		}
		token = t
	} else if len(token) < 8 {
		return nil, errInvalidArg("provided node_token must be at least 8 chars")
	}
	ringAt := int64(0)
	if v, err := s.st.ActiveRingVersion(ctx); err == nil {
		ringAt = v
	}
	weight := req.Weight
	if err := s.st.RegisterNode(ctx, store.Node{
		ID: req.NodeId, Address: req.Address, Weight: int(weight),
		Enabled: true, Token: token, RegisteredRing: ringAt,
	}); err != nil {
		return nil, errInternal("register: %v", err)
	}
	return &pb.RegisterNodeResponse{NodeToken: token, RegisteredAtRing: uint64(ringAt)}, nil
}

// ListNodes 列出集群成员。
func (s *Service) ListNodes(ctx context.Context, req *pb.ListNodesRequest) (*pb.NodeList, error) {
	if err := s.checkAdmin(req.AdminToken); err != nil {
		return nil, err
	}
	nodes, err := s.st.ListNodes(ctx)
	if err != nil {
		return nil, errInternal("list nodes: %v", err)
	}
	out := &pb.NodeList{}
	for _, n := range nodes {
		hint := n.Token
		if len(hint) > 6 {
			hint = hint[:6] + "…" // 只回显前缀, 完整令牌仅在注册时给出一次
		}
		out.Nodes = append(out.Nodes, &pb.NodeInfo{
			NodeId: n.ID, Address: n.Address, Weight: uint32(n.Weight),
			Enabled: n.Enabled, TokenHint: hint,
		})
	}
	return out, nil
}

// Rebalance 权重变化 -> 生成 staging 新环 + 迁移计划(不复制、不切换)。
//
// 校验:
//   - 必须指定全部已登记节点的权重(防止漏写把某节点意外摘流);
//   - 至少一个正权重, 否则无法成环;
//   - 权重只允许非负, 0 表示摘流保留成员身份;
//   - 不允许引用任何未登记节点(未知节点不能借权重表混入)。
func (s *Service) Rebalance(ctx context.Context, req *pb.RebalanceRequest) (*pb.MigrationInfo, error) {
	if err := s.checkAdmin(req.AdminToken); err != nil {
		return nil, err
	}
	if inflight, err := s.st.ActiveUncommittedMigration(ctx); err != nil {
		return nil, errInternal("check inflight: %v", err)
	} else if inflight != nil {
		return nil, errFailedPrecondition(
			"migration %s is still in flight (state=%s); commit or abort it before rebalancing",
			inflight.ID, inflight.State)
	}

	registered, err := s.st.ListNodes(ctx)
	if err != nil {
		return nil, errInternal("load nodes: %v", err)
	}
	byID := map[string]store.Node{}
	ids := make(map[string]bool, len(registered))
	positive := 0
	for _, n := range registered {
		byID[n.ID] = n
		ids[n.ID] = true
	}
	for id, w := range req.Weights {
		if !ids[id] {
			return nil, errPermission("node %q is not registered; unknown node cannot be placed on the ring", id)
		}
		if w > 0 {
			positive++
		}
	}
	for _, id := range sortedKeys(ids) {
		if _, ok := req.Weights[id]; !ok {
			return nil, errInvalidArg("weights must cover every registered node; %q missing", id)
		}
	}
	if positive == 0 {
		return nil, errInvalidArg("at least one positive-weight node is required to form a ring")
	}

	// 当前 active 环(首次扩容前可能不存在)。
	oldVer := int64(0)
	var oldRing *hashring.Ring
	if v, err := s.st.ActiveRingVersion(ctx); err == nil {
		oldVer = v
		oldRing, err = s.getRing(ctx, v)
		if err != nil {
			return nil, err
		}
	}

	members := make([]hashring.NodeWeight, 0, len(req.Weights))
	weights := make(map[string]int, len(req.Weights))
	for _, id := range sortedMapUint32(req.Weights) {
		w := req.Weights[id]
		weights[id] = int(w)
		members = append(members, hashring.NodeWeight{NodeID: id, Weight: w})
	}

	// 集群首次建环: 直接作为 active 种子环, 无需迁移。
	if oldRing == nil {
		nr, err := hashring.New(0, members)
		if err != nil {
			return nil, errInvalidArg("build initial ring: %v", err)
		}
		v, err := s.st.SeedFirstRing(ctx, reasonOr(req.Reason, "initial ring"), weights, s.ringToVNodes(nr))
		if err != nil {
			return nil, errInternal("seed ring: %v", err)
		}
		if err := s.st.SetWeights(ctx, weights); err != nil {
			return nil, errInternal("set weights: %v", err)
		}
		s.invalidate(v)
		return &pb.MigrationInfo{
			MigrationId: "",
			ToRing:      uint64(v),
			State:       store.MigCommitted,
			Total:       0,
			CreatedAt:   time.Now().UTC().Format(timeLayout),
		}, nil
	}

	// 构建 staging 新环。
	newVersion := oldVer + 1
	nr, err := hashring.New(newVersion, members)
	if err != nil {
		return nil, errInvalidArg("build ring: %v", err)
	}
	if _, err := s.st.CreateRing(ctx, reasonOr(req.Reason, "rebalance"), weights, s.ringToVNodes(nr)); err != nil {
		return nil, errInternal("create ring: %v", err)
	}
	if err := s.st.SetWeights(ctx, weights); err != nil {
		return nil, errInternal("set weights: %v", err)
	}
	s.invalidate(newVersion)

	// 路由变化量: 只对"已登记存在的键"计算。环差异是潜在量, 这里才是实际要迁的量。
	keys, err := s.st.AllKeys(ctx)
	if err != nil {
		return nil, errInternal("load keys: %v", err)
	}
	keyStrs := make([]string, 0, len(keys))
	for _, k := range keys {
		keyStrs = append(keyStrs, k.Key)
	}
	moves := hashring.Diff(oldRing, nr, keyStrs)
	mid := fmtMigrationID(newVersion)
	m := &store.Migration{
		ID: mid, FromRing: oldVer, ToRing: newVersion, State: store.MigPlanned,
		Items: make([]store.MigrationItem, 0, len(moves)),
	}
	for _, mv := range moves {
		m.Items = append(m.Items, store.MigrationItem{
			Key: mv.Key, KeyHash: mv.KeyHash,
			FromNode: mv.FromNode, ToNode: mv.ToNode, State: store.ItemPending,
		})
	}
	if err := s.st.CreateMigration(ctx, m); err != nil {
		return nil, errInternal("create migration: %v", err)
	}
	info := s.migrationToProto(m)
	return info, nil
}

const timeLayout = "2006-01-02T15:04:05Z07:00"

func reasonOr(r, def string) string {
	if r == "" {
		return def
	}
	return r
}

func fmtMigrationID(newRing int64) string {
	return "m-" + itoa(newRing)
}

func itoa(v int64) string {
	return formatInt(v)
}
