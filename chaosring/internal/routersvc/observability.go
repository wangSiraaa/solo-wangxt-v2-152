package routersvc

import (
	"context"
	"sort"

	"chaosring/internal/hashring"
	"chaosring/internal/pb"
	"chaosring/internal/store"
)

// KeyDistribution 返回某环上各节点的虚拟节点数与"实际登记键"的归属分布。
//
// 两个口径并列展示, 便于教学区分:
//   - vnodes: 理论容量份额(权重决定);
//   - owned_keys: 给定样本键在该环上的真实落点, 路由变化量按这个口径与迁移量对照。
func (s *Service) KeyDistribution(ctx context.Context, req *pb.DistributionRequest) (*pb.DistributionResponse, error) {
	if err := s.checkAdmin(req.AdminToken); err != nil {
		return nil, err
	}
	version := int64(req.RingVersion)
	var r *hashring.Ring
	if version == 0 {
		var ver int64
		var err error
		r, ver, err = s.activeRing(ctx)
		if err != nil {
			return nil, err
		}
		version = ver
	} else {
		var err error
		r, err = s.getRing(ctx, version)
		if err != nil {
			return nil, err
		}
	}

	var keys []string
	if req.KeyPrefix != "" || req.SampleLimit != 0 {
		recs, kerr := s.st.KeysByPrefix(ctx, req.KeyPrefix, req.SampleLimit)
		if kerr != nil {
			return nil, errInternal("sample keys: %v", kerr)
		}
		for _, k := range recs {
			if !k.Deleted {
				keys = append(keys, k.Key)
			}
		}
	} else {
		recs, kerr := s.st.AllKeys(ctx)
		if kerr != nil {
			return nil, errInternal("load keys: %v", kerr)
		}
		for _, k := range recs {
			if !k.Deleted {
				keys = append(keys, k.Key)
			}
		}
	}

	owned := map[string]int{}
	for _, k := range keys {
		owned[r.OwnerOfKey(k)]++
	}

	members := r.Members()
	shares := make([]*pb.NodeShare, 0, len(members))
	for _, m := range members {
		n := len(keys)
		pct := 0.0
		if n > 0 {
			pct = float64(owned[m.NodeID]) / float64(n) * 100
		}
		shares = append(shares, &pb.NodeShare{
			NodeId:    m.NodeID,
			Vnodes:    int32(r.VNodeCountOf(m.NodeID)),
			OwnedKeys: int32(owned[m.NodeID]),
			SharePct:  round2(pct),
		})
	}
	sort.Slice(shares, func(i, j int) bool { return shares[i].NodeId < shares[j].NodeId })
	return &pb.DistributionResponse{
		RingVersion:         uint64(version),
		Shares:              shares,
		TotalRegisteredKeys: int32(len(keys)),
	}, nil
}

// HotStats 聚合所有节点的本地热点计数。节点不可用时跳过并继续,
// 让教学演示在单节点故障下仍能看到其余节点的统计。
func (s *Service) HotStats(ctx context.Context, req *pb.HotStatsRequest) (*pb.HotStatsResponse, error) {
	if err := s.checkAdmin(req.AdminToken); err != nil {
		return nil, err
	}
	nodes, err := s.st.ListNodes(ctx)
	if err != nil {
		return nil, errInternal("list nodes: %v", err)
	}
	global := map[string]*pb.KeyHot{}
	out := &pb.HotStatsResponse{}
	for _, n := range nodes {
		c, derr := s.nodeClientFor(ctx, n.ID)
		if derr != nil {
			continue
		}
		st, gerr := c.client.Stats(ctx, &pb.StatsRequest{NodeToken: c.token})
		if gerr != nil {
			continue
		}
		out.Nodes = append(out.Nodes, st)
		for _, kh := range st.TopKeys {
			g := global[kh.Key]
			if g == nil {
				g = &pb.KeyHot{Key: kh.Key}
				global[kh.Key] = g
			}
			g.Gets += kh.Gets
			g.Puts += kh.Puts
		}
	}
	for _, g := range global {
		out.GlobalTop = append(out.GlobalTop, g)
	}
	sort.Slice(out.GlobalTop, func(i, j int) bool {
		a, b := out.GlobalTop[i].Gets+out.GlobalTop[i].Puts,
			out.GlobalTop[j].Gets+out.GlobalTop[j].Puts
		if a != b {
			return a > b
		}
		return out.GlobalTop[i].Key < out.GlobalTop[j].Key
	})
	if len(out.GlobalTop) > 20 {
		out.GlobalTop = out.GlobalTop[:20]
	}
	return out, nil
}

func round2(v float64) float64 {
	return float64(int64(v*100+0.5)) / 100
}

// RingDiff 独立复算两个环之间对已登记键的路由变化量, 供与实际迁移量对账。
func (s *Service) RingDiff(ctx context.Context, req *pb.RingDiffRequest) (*pb.RingDiffResponse, error) {
	if err := s.checkAdmin(req.AdminToken); err != nil {
		return nil, err
	}
	from, err := s.getRing(ctx, int64(req.FromRing))
	if err != nil {
		return nil, errFailedPrecondition("from ring: %v", err)
	}
	to, err := s.getRing(ctx, int64(req.ToRing))
	if err != nil {
		return nil, errFailedPrecondition("to ring: %v", err)
	}
	var recs []store.KeyRec
	if req.KeyPrefix != "" {
		recs, err = s.st.KeysByPrefix(ctx, req.KeyPrefix, 0)
	} else {
		recs, err = s.st.AllKeys(ctx)
	}
	if err != nil {
		return nil, errInternal("load keys: %v", err)
	}
	out := &pb.RingDiffResponse{FromRing: req.FromRing, ToRing: req.ToRing}
	for _, k := range recs {
		f := from.Owner(k.Hash)
		t := to.Owner(k.Hash)
		if f == t {
			continue
		}
		out.Changed++
		out.Moves = append(out.Moves, &pb.MigrationItem{
			Key: k.Key, KeyHash: k.Hash, FromNode: f, ToNode: t,
		})
	}
	return out, nil
}
