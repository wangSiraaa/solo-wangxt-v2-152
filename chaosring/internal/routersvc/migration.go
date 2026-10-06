package routersvc

import (
	"context"
	"errors"

	"chaosring/internal/pb"
	"chaosring/internal/store"
)

// StartMigration 开始复制阶段。逐条:
//  1. 从旧主读取键的当前值/版本(墓碑同样传播);
//  2. StageCopy 到新主的暂存区(不进新主正式 map);
//  3. 明细置 replicated; 任何一步失败置 failed, 迁移单置 failed。
//
// 复制全程不动 active 环, 路由结果不变。已经 replicated 的条目在重试时跳过,
// 因此节点中途失败恢复后是断点续传。
func (s *Service) StartMigration(ctx context.Context, req *pb.StartMigrationRequest) (*pb.MigrationInfo, error) {
	if err := s.checkAdmin(req.AdminToken); err != nil {
		return nil, err
	}
	m, err := s.loadMigration(ctx, req.MigrationId)
	if err != nil {
		return nil, err
	}
	switch m.State {
	case store.MigPlanned, store.MigReplicating, store.MigFailed:
	default:
		return nil, errFailedPrecondition("migration %s in state %s cannot be started", m.ID, m.State)
	}
	if err := s.st.SetMigrationState(ctx, m.ID, store.MigReplicating); err != nil {
		return nil, errInternal("state: %v", err)
	}
	m.State = store.MigReplicating

	if err := s.copyItems(ctx, m); err != nil {
		return nil, err
	}
	return s.MigrationProgress(ctx, &pb.MigrationProgressRequest{
		AdminToken: req.AdminToken, MigrationId: m.ID,
	})
}

// loadMigration 读取迁移单并检查存在性。
func (s *Service) loadMigration(ctx context.Context, id string) (*store.Migration, error) {
	if id == "" {
		return nil, errInvalidArg("migration_id required")
	}
	m, err := s.st.GetMigration(ctx, id)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, errNotFound("migration %q not found", id)
		}
		return nil, errInternal("load migration: %v", err)
	}
	return m, nil
}

// MigrationProgress 返回迁移进度(对账用)。
func (s *Service) MigrationProgress(ctx context.Context, req *pb.MigrationProgressRequest) (*pb.MigrationInfo, error) {
	if err := s.checkAdmin(req.AdminToken); err != nil {
		return nil, err
	}
	m, err := s.loadMigration(ctx, req.MigrationId)
	if err != nil {
		return nil, err
	}
	return s.migrationToProto(m), nil
}

// CommitSwitch 显式切换。这是"复制完成"之后的独立一步。
//
// 默认(force=false):
//  1. 要求全部条目 replicated;
//  2. 对每个目标节点发 Promote(暂存区提升为正式数据);
//  3. 提升 active 环(旧 active -> old);
//  4. 明细置 promoted, key_registry 属主改派到新环。
//
// force=true 仅用于教学: 允许在有 failed/pending 条目时切换, 以演示
// "把复制完成当作已安全切换"的反面——会立即产生数据缺口。
func (s *Service) CommitSwitch(ctx context.Context, req *pb.CommitSwitchRequest) (*pb.MigrationInfo, error) {
	if err := s.checkAdmin(req.AdminToken); err != nil {
		return nil, err
	}
	m, err := s.loadMigration(ctx, req.MigrationId)
	if err != nil {
		return nil, err
	}
	switch m.State {
	case store.MigReplicated, store.MigFailed:
		// replicated 正常提交; failed 仅允许 force 教学提交
	case store.MigCommitted:
		return s.migrationToProto(m), nil
	default:
		return nil, errFailedPrecondition("migration %s in state %s cannot be committed", m.ID, m.State)
	}

	cnt, err := s.st.MigrationCounters(ctx, m.ID)
	if err != nil {
		return nil, errInternal("counters: %v", err)
	}
	unfinished := cnt.Pending
	if unfinished > 0 && !req.Force {
		return nil, errFailedPrecondition(
			"refuse to switch: %d/%d items not replicated (replication complete != safe switch); "+
				"use RetryMigration or force=true to demo the unsafe path",
			unfinished, cnt.Total)
	}

	// 按目标节点分组, 每个节点一次 Promote。
	toKeys := map[string][]string{}
	for _, it := range m.Items {
		if it.State == store.ItemReplicated || it.State == store.ItemPromoted {
			toKeys[it.ToNode] = append(toKeys[it.ToNode], it.Key)
		}
	}
	for nodeID, keys := range toKeys {
		c, err := s.nodeClientFor(ctx, nodeID)
		if err != nil {
			return nil, errUnavailable("dial %s: %v", nodeID, err)
		}
		if _, err := c.client.Promote(ctx, &pb.PromoteRequest{
			MigrationId: m.ID,
			Keys:        keys,
			NodeToken:   c.token,
			TargetRing:  uint64(m.ToRing),
		}); err != nil {
			return nil, errUnavailable("promote at %s: %v", nodeID, err)
		}
	}

	// 提升 active 环: 旧环 -> old, 目标 staging -> active。
	if err := s.st.PromoteRing(ctx, m.ToRing); err != nil {
		return nil, errInternal("promote ring: %v", err)
	}

	// 迁移条目状态推进 + 注册表属主改派到新环。
	reassign := map[string]string{}
	for _, it := range m.Items {
		if it.State == store.ItemReplicated {
			reassign[it.Key] = it.ToNode
		}
	}
	if len(reassign) > 0 {
		if err := s.st.ReassignOwner(ctx, reassign, m.ToRing); err != nil {
			return nil, errInternal("reassign owners: %v", err)
		}
	}
	if _, err := s.st.MarkAllPromoted(ctx, m.ID); err != nil {
		return nil, errInternal("mark promoted: %v", err)
	}
	// force 路径下未复制条目的属主也指向新环, 让缺口可见(新主确实没有这些键)。
	// 这些条目保留 failed 终态: 迁移整体 committed, 但缺口永久记录在明细里。
	if req.Force {
		forceReassign := map[string]string{}
		for _, it := range m.Items {
			if it.State == store.ItemFailed || it.State == store.ItemPending ||
				it.State == store.ItemReplicating {
				forceReassign[it.Key] = it.ToNode
			}
		}
		if len(forceReassign) > 0 {
			_ = s.st.ReassignOwner(ctx, forceReassign, m.ToRing)
		}
	}
	if err := s.st.SetMigrationState(ctx, m.ID, store.MigCommitted); err != nil {
		return nil, err
	}
	s.invalidate(m.FromRing)
	s.invalidate(m.ToRing)

	return s.MigrationProgress(ctx, &pb.MigrationProgressRequest{
		AdminToken: req.AdminToken, MigrationId: m.ID,
	})
}

// AbortMigration 放弃迁移: 丢弃目标节点暂存数据, 目标环保持 staging(不再推进)。
// 不影响旧 active 环, 正式流量全程未受影响。
func (s *Service) AbortMigration(ctx context.Context, req *pb.AbortMigrationRequest) (*pb.MigrationInfo, error) {
	if err := s.checkAdmin(req.AdminToken); err != nil {
		return nil, err
	}
	m, err := s.loadMigration(ctx, req.MigrationId)
	if err != nil {
		return nil, err
	}
	switch m.State {
	case store.MigPlanned, store.MigReplicating, store.MigReplicated, store.MigFailed:
	default:
		return nil, errFailedPrecondition("migration %s in state %s cannot be aborted", m.ID, m.State)
	}
	seen := map[string]bool{}
	for _, it := range m.Items {
		if seen[it.ToNode] {
			continue
		}
		seen[it.ToNode] = true
		c, derr := s.nodeClientFor(ctx, it.ToNode)
		if derr != nil {
			// 目标节点连不上时仍可在编排层放弃; 节点恢复后其 staging 会自然成为孤儿,
			// 由 RetireOldRing/后续清理带走。此处记录但不阻断放弃。
			continue
		}
		_, _ = c.client.DropStaging(ctx, &pb.DropStagingRequest{
			MigrationId: m.ID, NodeToken: c.token,
		})
	}
	if err := s.st.SetMigrationState(ctx, m.ID, store.MigAborted); err != nil {
		return nil, errInternal("state: %v", err)
	}
	return s.MigrationProgress(ctx, &pb.MigrationProgressRequest{
		AdminToken: req.AdminToken, MigrationId: m.ID,
	})
}

// RetryMigration 断点续传: 只重试 pending/failed 条目, replicated 的不重复复制。
func (s *Service) RetryMigration(ctx context.Context, req *pb.RetryMigrationRequest) (*pb.MigrationInfo, error) {
	if err := s.checkAdmin(req.AdminToken); err != nil {
		return nil, err
	}
	m, err := s.loadMigration(ctx, req.MigrationId)
	if err != nil {
		return nil, err
	}
	switch m.State {
	case store.MigFailed, store.MigReplicating, store.MigPlanned, store.MigReplicated:
	default:
		return nil, errFailedPrecondition("migration %s in state %s cannot be retried", m.ID, m.State)
	}
	if m.State != store.MigReplicated {
		if err := s.st.SetMigrationState(ctx, m.ID, store.MigReplicating); err != nil {
			return nil, err
		}
	}
	m.State = store.MigReplicating
	// 重新加载, 让 copyItems 从数据库视角看待续传条目。
	if err := s.copyItems(ctx, m); err != nil {
		return nil, err
	}
	return s.MigrationProgress(ctx, &pb.MigrationProgressRequest{
		AdminToken: req.AdminToken, MigrationId: m.ID,
	})
}

// RetireOldRing 显式退役旧环。要求其目标迁移已 committed;
// 退役后旧环不再服务在途请求, 旧主键上的迁移副本被清理。
func (s *Service) RetireOldRing(ctx context.Context, req *pb.RetireRingRequest) (*pb.RetireRingResponse, error) {
	if err := s.checkAdmin(req.AdminToken); err != nil {
		return nil, err
	}
	rec, err := s.st.GetRing(ctx, int64(req.RingVersion))
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, errNotFound("ring %d not found", req.RingVersion)
		}
		return nil, err
	}
	if rec.State != store.RingOld {
		return nil, errFailedPrecondition("ring %d is %s, only 'old' rings can be retired", req.RingVersion, rec.State)
	}
	mig, err := s.st.MigrationByToRing(ctx, int64(req.RingVersion)+1)
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		return nil, errInternal("find migration: %v", err)
	}
	dropped := int32(0)
	if mig != nil {
		// 通知旧主清理"已经搬走"的键(教学清理; 用 DropStaging 不适用,
		// 这里删正式副本)。为了简洁, 通过迁移条目统计数量并逐个 Delete。
		byFrom := map[string][]string{}
		for _, it := range mig.Items {
			if it.State == store.ItemPromoted {
				byFrom[it.FromNode] = append(byFrom[it.FromNode], it.Key)
			}
		}
		for nodeID, keys := range byFrom {
			c, derr := s.nodeClientFor(ctx, nodeID)
			if derr != nil {
				continue
			}
			resp, derr := c.client.PurgeKeys(ctx, &pb.PurgeKeysRequest{
				Keys: keys, NodeToken: c.token,
			})
			if derr == nil {
				dropped += resp.Purged
			}
		}
	}
	if err := s.st.RetireRing(ctx, int64(req.RingVersion)); err != nil {
		return nil, errFailedPrecondition("%v", err)
	}
	s.invalidate(int64(req.RingVersion))
	return &pb.RetireRingResponse{RetiredRing: req.RingVersion, KeysDroppedAtOldOwners: dropped}, nil
}
