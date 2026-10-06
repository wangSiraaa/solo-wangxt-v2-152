package routersvc

import (
	"context"
	"strconv"

	"chaosring/internal/pb"
	"chaosring/internal/store"
)

// Resume 中断恢复(启动时可自动调用, 也暴露为管理 API)。
//
// 恢复原则:
//  1. 绝不自动切换。复制是复制, 切换是切换——恢复过程也不打破这条边界。
//  2. 对每个"未终结"迁移, 重新尝试复制未完成条目(幂等断点续传:
//     节点的 WAL 保留已复制字节, 已 replicated 的条目直接跳过)。
//  3. 若目标/源节点仍不可用, 迁移保持 failed, 列入 failed_migrations,
//     交给管理员 RetryMigration 或 AbortMigration。
//  4. 若上一次在"已切换、未记账完"窗口崩溃(环已 active 但迁移单未置 committed),
//     则补做幂等的提交记账(Promote/属主改派/置 committed)。
func (s *Service) Resume(ctx context.Context, req *pb.ResumeRequest) (*pb.ResumeResponse, error) {
	if err := s.checkAdmin(req.AdminToken); err != nil {
		return nil, err
	}
	resp := &pb.ResumeResponse{}

	activeVer, err := s.st.ActiveRingVersion(ctx)
	if err == nil {
		resp.ActiveRing = strconv.FormatInt(activeVer, 10)
	}

	rings, err := s.st.ListRings(ctx)
	if err != nil {
		return nil, errInternal("list rings: %v", err)
	}
	ringState := map[int64]store.RingState{}
	for _, r := range rings {
		ringState[r.Version] = r.State
	}

	migs, err := s.st.ListMigrations(ctx)
	if err != nil {
		return nil, errInternal("list migrations: %v", err)
	}
	for _, m := range migs {
		switch m.State {
		case store.MigCommitted, store.MigAborted:
			continue
		}

		// 崩溃窗口修复: 目标环已经是 active, 但迁移单没来得及置 committed。
		if ringState[m.ToRing] == store.RingActive {
			if err := s.finishCommitBookkeeping(ctx, &m); err != nil {
				resp.FailedMigrations = append(resp.FailedMigrations, m.ID)
			} else {
				resp.RecoveredMigrations = append(resp.RecoveredMigrations, m.ID)
			}
			continue
		}

		// planned/replicating/replicated/failed 且目标环仍为 staging:
		// 安全地续传复制, 但不提交。
		if ringState[m.ToRing] == store.RingStaging {
			if err := s.copyItems(ctx, &m); err != nil {
				resp.FailedMigrations = append(resp.FailedMigrations, m.ID)
				continue
			}
			fresh, gerr := s.st.GetMigration(ctx, m.ID)
			if gerr != nil {
				resp.FailedMigrations = append(resp.FailedMigrations, m.ID)
				continue
			}
			switch fresh.State {
			case store.MigReplicated:
				resp.RecoveredMigrations = append(resp.RecoveredMigrations, m.ID)
			case store.MigFailed:
				resp.FailedMigrations = append(resp.FailedMigrations, m.ID)
			default:
				resp.FailedMigrations = append(resp.FailedMigrations, m.ID)
			}
		}
	}
	return resp, nil
}

// finishCommitBookkeeping 补做"环已切换后"的幂等记账:
// 对目标节点重发 Promote(节点侧幂等)、改派属主、置 committed。
// 不会再次调用 PromoteRing(环已是 active)。
func (s *Service) finishCommitBookkeeping(ctx context.Context, m *store.Migration) error {
	fresh, err := s.st.GetMigration(ctx, m.ID)
	if err != nil {
		return err
	}
	toKeys := map[string][]string{}
	for _, it := range fresh.Items {
		if it.State == store.ItemReplicated || it.State == store.ItemPromoted {
			toKeys[it.ToNode] = append(toKeys[it.ToNode], it.Key)
		}
	}
	for nodeID, keys := range toKeys {
		c, derr := s.nodeClientFor(ctx, nodeID)
		if derr != nil {
			return derr
		}
		if _, err := c.client.Promote(ctx, &pb.PromoteRequest{
			MigrationId: m.ID, Keys: keys, NodeToken: c.token,
			TargetRing: uint64(m.ToRing),
		}); err != nil {
			return err
		}
	}
	reassign := map[string]string{}
	for _, it := range fresh.Items {
		if it.State == store.ItemReplicated {
			reassign[it.Key] = it.ToNode
		}
	}
	if len(reassign) > 0 {
		if err := s.st.ReassignOwner(ctx, reassign, m.ToRing); err != nil {
			return err
		}
	}
	if _, err := s.st.MarkAllPromoted(ctx, m.ID); err != nil {
		return err
	}
	return s.st.SetMigrationState(ctx, m.ID, store.MigCommitted)
}
