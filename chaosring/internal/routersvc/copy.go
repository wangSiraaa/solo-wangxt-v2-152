package routersvc

import (
	"context"

	"chaosring/internal/pb"
	"chaosring/internal/store"
)

// copyItems 执行(或续传)迁移复制。
//
//	states 过滤: 初次从 pending/failed 开始; 续传只跑未完成条目。
//	对每条: 旧主 Get -> 新主 StageCopy。全部在暂存区, 不改正式数据。
//
// 任一节点不可用 -> 该条置 failed, 迁移单置 failed, 已完成条目保持 replicated。
// 这正是"迁移途中失败"教学案例: 进度落库, 节点恢复后 RetryMigration 断点续传。
func (s *Service) copyItems(ctx context.Context, m *store.Migration) error {
	items, err := s.st.ItemsInState(ctx, m.ID,
		store.ItemPending, store.ItemReplicating, store.ItemFailed)
	if err != nil {
		return errInternal("load items: %v", err)
	}

	srcClients := map[string]*nodeClient{}
	dstClients := map[string]*nodeClient{}
	client := func(id string, cache map[string]*nodeClient) (*nodeClient, error) {
		if c, ok := cache[id]; ok {
			return c, nil
		}
		c, err := s.nodeClientFor(ctx, id)
		if err != nil {
			return nil, err
		}
		cache[id] = c
		return c, nil
	}

	anyFailed := false
	for _, it := range items {
		from, err := client(it.FromNode, srcClients)
		if err != nil {
			s.markItemFailure(ctx, m.ID, it.Key, "dial source: "+err.Error())
			anyFailed = true
			continue
		}
		to, err := client(it.ToNode, dstClients)
		if err != nil {
			s.markItemFailure(ctx, m.ID, it.Key, "dial destination: "+err.Error())
			anyFailed = true
			continue
		}

		// 1) 从旧主读取当前值(含墓碑与版本)。
		gr, err := from.client.Get(ctx, &pb.NodeGetRequest{Key: it.Key, NodeToken: from.token})
		if err != nil {
			s.markItemFailure(ctx, m.ID, it.Key, "source get: "+err.Error())
			anyFailed = true
			continue
		}
		if !gr.Found {
			// 源端没有正式值(可能已被退役清理), 无法复制: 标记失败而非静默丢键。
			s.markItemFailure(ctx, m.ID, it.Key, "source has no value for key")
			anyFailed = true
			continue
		}

		// 2) 复制到新主暂存区。
		if _, err := to.client.StageCopy(ctx, &pb.StageCopyRequest{
			MigrationId: m.ID,
			Key:         it.Key,
			Value:       gr.Value,
			Deleted:     gr.Deleted,
			Version:     gr.Version,
			NodeToken:   to.token,
		}); err != nil {
			s.markItemFailure(ctx, m.ID, it.Key, "stage copy: "+err.Error())
			anyFailed = true
			continue
		}
		if err := s.st.SetItemState(ctx, m.ID, it.Key, store.ItemReplicated, ""); err != nil {
			return errInternal("mark replicated: %v", err)
		}
	}

	if anyFailed {
		_ = s.st.SetMigrationState(ctx, m.ID, store.MigFailed)
		// 不返回 gRPC 错误: 迁移单本身已落库为 failed, 由调用方读进度。
		return nil
	}
	if err := s.st.SetMigrationState(ctx, m.ID, store.MigReplicated); err != nil {
		return errInternal("mark replicated: %v", err)
	}
	return nil
}

// markItemFailure 记录条目失败(失败原因保留在 last_error, 便于教学讲解)。
func (s *Service) markItemFailure(ctx context.Context, mid, key, reason string) {
	_ = s.st.SetItemState(ctx, mid, key, store.ItemFailed, reason)
}

// migrationToProto 转换迁移单及计数。
func (s *Service) migrationToProto(m *store.Migration) *pb.MigrationInfo {
	info := &pb.MigrationInfo{
		MigrationId: m.ID,
		FromRing:    uint64(m.FromRing),
		ToRing:      uint64(m.ToRing),
		State:       m.State,
		CreatedAt:   m.CreatedAt.UTC().Format(timeLayout),
		UpdatedAt:   m.UpdatedAt.UTC().Format(timeLayout),
	}
	for _, it := range m.Items {
		info.Items = append(info.Items, &pb.MigrationItem{
			Key: it.Key, KeyHash: it.KeyHash,
			FromNode: it.FromNode, ToNode: it.ToNode,
			State: it.State, LastError: it.LastError,
		})
		switch it.State {
		case store.ItemPending, store.ItemReplicating:
			info.Pending++
			info.Total++
		case store.ItemFailed:
			info.Failed++
			info.Pending++ // 未完成口径
			info.Total++
		case store.ItemReplicated:
			info.Replicated++
			info.Total++
		case store.ItemPromoted:
			info.Promoted++
			info.Replicated++ // promoted 必然先 replicated
			info.Total++
		}
	}
	return info
}
