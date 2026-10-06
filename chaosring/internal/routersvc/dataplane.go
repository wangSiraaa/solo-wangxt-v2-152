package routersvc

import (
	"context"
	"errors"

	"chaosring/internal/hashring"
	"chaosring/internal/pb"
	"chaosring/internal/store"
)

// pickRing 选择本次请求使用的环: ring_hint 优先(在途请求), 否则 active。
// 返回环与版本。
func (s *Service) pickRing(ctx context.Context, hint uint64) (*hashring.Ring, int64, error) {
	if hint != 0 {
		return s.ringForHint(ctx, hint)
	}
	return s.activeRing(ctx)
}

// inflightMove 返回当前在途迁移中某键的迁移条目(没有则 nil)。
// 仅当迁移处于 replicating/replicated 等"未切换"阶段时数据面需要双写。
func (s *Service) inflightMove(ctx context.Context, key string) (*store.Migration, *store.MigrationItem, error) {
	m, err := s.st.ActiveUncommittedMigration(ctx)
	if err != nil || m == nil {
		return nil, nil, err
	}
	if m.State == store.MigCommitted || m.State == store.MigAborted {
		return nil, nil, nil
	}
	for i := range m.Items {
		if m.Items[i].Key == key {
			return m, &m.Items[i], nil
		}
	}
	return m, nil, nil
}

// Get 按 active(或 hint 旧环)路由读取。
//
// 迁移期间的读取一致性:
//   - 默认走 active 旧环 -> 旧主, 永远读到已提交值;
//   - 客户端若持有旧环 hint, 同样解析到旧主;
//   - 新环 staging 不对外提供读, 避免读到"复制了但未切换"的半成品。
func (s *Service) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	r, ver, err := s.pickRing(ctx, req.RingHint)
	if err != nil {
		return nil, err
	}
	owner := r.OwnerOfKey(req.Key)
	c, err := s.nodeClientFor(ctx, owner)
	if err != nil {
		return nil, errUnavailable("dial owner %s: %v", owner, err)
	}
	gr, err := c.client.Get(ctx, &pb.NodeGetRequest{Key: req.Key, NodeToken: c.token})
	if err != nil {
		return nil, errUnavailable("get at %s: %v", owner, err)
	}
	if !gr.Found {
		return &pb.GetResponse{Found: false, NodeId: owner, RingVersion: uint64(ver)}, nil
	}
	return &pb.GetResponse{
		Found: true, Value: gr.Value, Deleted: gr.Deleted,
		NodeId: owner, RingVersion: uint64(ver),
	}, nil
}

// Put 按 active 环写旧主; 若该键在在途迁移中, 同时把新值写进新主暂存区,
// 保证"复制窗口内的新写"不会在切换后丢失(双写)。
//
// 版本号由注册表单调分配, 旧环在途写无法用旧版本覆盖新版本。
func (s *Service) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	r, ver, err := s.pickRing(ctx, req.RingHint)
	if err != nil {
		return nil, err
	}
	keyHash := hashring.HashString(req.Key)
	owner := r.Owner(keyHash)

	m, move, err := s.inflightMove(ctx, req.Key)
	if err != nil {
		return nil, errInternal("check inflight: %v", err)
	}
	// 双写目标: 仅当请求按"当前 active 旧环"路由, 且该键确实要迁走。
	dualTarget := ""
	if req.RingHint == 0 && move != nil {
		dualTarget = move.ToNode
	}

	version, err := s.st.RegisterWrite(ctx, req.Key, keyHash, owner, ver, false)
	if err != nil {
		return nil, errInternal("register write: %v", err)
	}

	c, err := s.nodeClientFor(ctx, owner)
	if err != nil {
		return nil, errUnavailable("dial owner %s: %v", owner, err)
	}
	if _, err := c.client.Put(ctx, &pb.NodePutRequest{
		Key: req.Key, Value: req.Value, Version: uint64(version), NodeToken: c.token,
	}); err != nil {
		return nil, errUnavailable("put at %s: %v", owner, err)
	}

	dual := false
	if dualTarget != "" && m != nil {
		if tc, derr := s.nodeClientFor(ctx, dualTarget); derr == nil {
			// 写进同一迁移的暂存区。失败时该条目在提交前会因版本落后而被识别,
			// 但为了教学明确性, 双写失败直接让本次 Put 失败, 要求重试。
			if _, serr := tc.client.StageCopy(ctx, &pb.StageCopyRequest{
				MigrationId: m.ID, Key: req.Key, Value: req.Value,
				Version: uint64(version), NodeToken: tc.token,
			}); serr != nil {
				return nil, errUnavailable("dual-write staging at %s failed: %v", dualTarget, serr)
			}
			dual = true
		} else {
			return nil, errUnavailable("dual-write: dial %s: %v", dualTarget, derr)
		}
	}

	return &pb.PutResponse{
		NodeId: owner, Version: uint64(version),
		RingVersion: uint64(ver), DualWritten: dual,
	}, nil
}

// Delete 与 Put 同构: 旧主写墓碑, 在途迁移时向新主暂存区复制删除。
func (s *Service) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	r, ver, err := s.pickRing(ctx, req.RingHint)
	if err != nil {
		return nil, err
	}
	keyHash := hashring.HashString(req.Key)
	owner := r.Owner(keyHash)

	m, move, err := s.inflightMove(ctx, req.Key)
	if err != nil {
		return nil, err
	}
	dualTarget := ""
	if req.RingHint == 0 && move != nil {
		dualTarget = move.ToNode
	}

	version, err := s.st.RegisterWrite(ctx, req.Key, keyHash, owner, ver, true)
	if err != nil {
		return nil, errInternal("register delete: %v", err)
	}
	c, err := s.nodeClientFor(ctx, owner)
	if err != nil {
		return nil, errUnavailable("dial owner %s: %v", owner, err)
	}
	if _, err := c.client.Delete(ctx, &pb.NodeDeleteRequest{
		Key: req.Key, Version: uint64(version), NodeToken: c.token,
	}); err != nil {
		return nil, errUnavailable("delete at %s: %v", owner, err)
	}
	dual := false
	if dualTarget != "" && m != nil {
		if tc, derr := s.nodeClientFor(ctx, dualTarget); derr == nil {
			if _, serr := tc.client.StageCopy(ctx, &pb.StageCopyRequest{
				MigrationId: m.ID, Key: req.Key, Deleted: true,
				Version: uint64(version), NodeToken: tc.token,
			}); serr != nil {
				return nil, errUnavailable("dual-write tombstone at %s failed: %v", dualTarget, serr)
			}
			dual = true
		} else {
			return nil, errUnavailable("dual-write: dial %s: %v", dualTarget, derr)
		}
	}
	return &pb.DeleteResponse{
		NodeId: owner, Version: uint64(version),
		RingVersion: uint64(ver), DualWritten: dual,
	}, nil
}

// Route 只解析属主不转发, 供教学/客户端 SDK 观察"键在各环版本上落在哪"。
// 旧环已退役会明确返回 retired=true 而不是悄悄改路由。
func (s *Service) Route(ctx context.Context, req *pb.RouteRequest) (*pb.RouteResponse, error) {
	if req.RingHint != 0 {
		rec, err := s.st.GetRing(ctx, int64(req.RingHint))
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				return nil, errNotFound("ring %d does not exist", req.RingHint)
			}
			return nil, err
		}
		if rec.State == store.RingRetired {
			return &pb.RouteResponse{Retired: true, RingVersion: req.RingHint}, nil
		}
		r, err := s.getRing(ctx, int64(req.RingHint))
		if err != nil {
			return nil, err
		}
		owner := r.OwnerOfKey(req.Key)
		addr := ""
		if n, err := s.st.GetNode(ctx, owner); err == nil {
			addr = n.Address
		}
		return &pb.RouteResponse{NodeId: owner, Address: addr, RingVersion: req.RingHint}, nil
	}
	r, ver, err := s.activeRing(ctx)
	if err != nil {
		return nil, err
	}
	owner := r.OwnerOfKey(req.Key)
	addr := ""
	if n, err := s.st.GetNode(ctx, owner); err == nil {
		addr = n.Address
	}
	return &pb.RouteResponse{NodeId: owner, Address: addr, RingVersion: uint64(ver)}, nil
}
