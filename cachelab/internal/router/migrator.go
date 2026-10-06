package router

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	cachelabv1 "cachelab/gen/cachelab/v1"
	"cachelab/internal/ring"
	"cachelab/internal/store"
)

const importBatchSize = 256

// errInjectedFault 教学用故障注入:模拟迁移器在复制途中崩溃。
var errInjectedFault = errors.New("injected fault: migrator crashed mid-copy")

// StartMigration 启动异步复制。计划进入 COPYING;每个区间:
// 先在源节点计数(expected_keys,即"路由变化量"的键数口径),
// 再流式导出 → 批量导入目标节点 → 落库 COPIED。
// 进度全部落 PostgreSQL,进程崩溃后可用 ResumeMigration 续跑。
func (s *Server) StartMigration(ctx context.Context, req *cachelabv1.StartMigrationRequest) (*cachelabv1.StartMigrationResponse, error) {
	plan, err := s.st.GetPlan(ctx, req.PlanId)
	if err != nil {
		return nil, err
	}
	if plan.State != store.PlanPrepared {
		return nil, status.Errorf(codes.FailedPrecondition,
			"plan %d is %s, want PREPARED (use ResumeMigration to continue an interrupted plan)",
			plan.PlanID, plan.State)
	}
	if err := s.st.SetPlanState(ctx, plan.PlanID, store.PlanCopying); err != nil {
		return nil, err
	}
	if err := s.runCopy(plan.PlanID, int(req.FailAfterRanges)); err != nil {
		if errors.Is(err, errInjectedFault) {
			return nil, status.Errorf(codes.Aborted, "migration interrupted: %v (plan stays COPYING, call ResumeMigration)", err)
		}
		return nil, err
	}
	return &cachelabv1.StartMigrationResponse{State: store.PlanCopied}, nil
}

// ResumeMigration 从中断点续跑:已 COPIED 的区间跳过,其余重新复制(导入幂等)。
func (s *Server) ResumeMigration(ctx context.Context, req *cachelabv1.ResumeMigrationRequest) (*cachelabv1.ResumeMigrationResponse, error) {
	plan, err := s.st.GetPlan(ctx, req.PlanId)
	if err != nil {
		return nil, err
	}
	if plan.State != store.PlanCopying {
		return nil, status.Errorf(codes.FailedPrecondition,
			"plan %d is %s, want COPYING", plan.PlanID, plan.State)
	}
	if err := s.runCopy(plan.PlanID, -1); err != nil {
		return nil, err
	}
	return &cachelabv1.ResumeMigrationResponse{State: store.PlanCopied}, nil
}

// runCopy 执行复制循环。failAfter >= 0 时在复制完指定数量的区间后注入崩溃。
func (s *Server) runCopy(planID int64, failAfter int) error {
	s.migratingMu.Lock()
	if s.migrating {
		s.migratingMu.Unlock()
		return status.Error(codes.FailedPrecondition, "another migration is running")
	}
	s.migrating = true
	s.migratingMu.Unlock()
	defer func() {
		s.migratingMu.Lock()
		s.migrating = false
		s.migratingMu.Unlock()
	}()

	ctx := context.Background() // 复制不受单次 RPC 生命周期限制
	plan, err := s.st.GetPlan(ctx, planID)
	if err != nil {
		return err
	}

	// 首次启动:为所有区间记录 expected_keys(复制前源区间键数)。
	if plan.State == store.PlanCopying {
		for _, rg := range plan.Ranges {
			if rg.State == store.RangePending && rg.ExpectedKeys == 0 {
				n, err := s.countRange(ctx, rg.FromNode, rg.StartHex, rg.EndHex)
				if err != nil {
					return fmt.Errorf("count expected keys on %s: %w", rg.FromNode, err)
				}
				if err := s.st.SetRangeExpected(ctx, planID, rg.RangeID, n); err != nil {
					return err
				}
			}
		}
	}

	copied := 0
	for _, rg := range plan.Ranges {
		if rg.State == store.RangeCopied || rg.State == store.RangeCleaned {
			continue // 断点续跑:跳过已完成区间
		}
		if failAfter >= 0 && copied >= failAfter {
			return errInjectedFault
		}
		if err := s.st.SetRangeState(ctx, planID, rg.RangeID, store.RangeCopying, rg.CopiedKeys); err != nil {
			return err
		}
		n, err := s.copyRange(ctx, rg)
		if err != nil {
			return fmt.Errorf("copy range %d (%s -> %s): %w", rg.RangeID, rg.FromNode, rg.ToNode, err)
		}
		if err := s.st.SetRangeState(ctx, planID, rg.RangeID, store.RangeCopied, n); err != nil {
			return err
		}
		copied++
	}
	return s.st.SetPlanState(ctx, planID, store.PlanCopied)
}

// copyRange 流式复制单个区间:源导出 → 目标批量导入,返回复制键数。
func (s *Server) copyRange(ctx context.Context, rg store.Range) (int64, error) {
	src, err := s.nodeConn(ctx, rg.FromNode)
	if err != nil {
		return 0, err
	}
	dst, err := s.nodeConn(ctx, rg.ToNode)
	if err != nil {
		return 0, err
	}
	srcCtx, err := s.nodeCtx(ctx, rg.FromNode)
	if err != nil {
		return 0, err
	}
	dstCtx, err := s.nodeCtx(ctx, rg.ToNode)
	if err != nil {
		return 0, err
	}

	sctx, cancel := context.WithTimeout(srcCtx, 10*time.Minute)
	defer cancel()
	stream, err := src.ExportRange(sctx, &cachelabv1.ExportRangeRequest{
		StartHex: rg.StartHex, EndHex: rg.EndHex,
	})
	if err != nil {
		return 0, err
	}
	var total int64
	batch := make([]*cachelabv1.KeyValue, 0, importBatchSize)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		bctx, bcancel := context.WithTimeout(dstCtx, time.Minute)
		_, err := dst.ImportBatch(bctx, &cachelabv1.ImportBatchRequest{Items: batch})
		bcancel()
		if err != nil {
			return err
		}
		total += int64(len(batch))
		batch = batch[:0]
		return nil
	}
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return total, err
		}
		batch = append(batch, resp.Kv)
		if len(batch) >= importBatchSize {
			if err := flush(); err != nil {
				return total, err
			}
		}
	}
	if err := flush(); err != nil {
		return total, err
	}
	return total, nil
}

// countRange 统计节点上某区间的键数。
func (s *Server) countRange(ctx context.Context, nodeID, startHex, endHex string) (int64, error) {
	c, err := s.nodeConn(ctx, nodeID)
	if err != nil {
		return 0, err
	}
	nctx, err := s.nodeCtx(ctx, nodeID)
	if err != nil {
		return 0, err
	}
	sctx, cancel := context.WithTimeout(nctx, time.Minute)
	defer cancel()
	stream, err := c.ExportRange(sctx, &cachelabv1.ExportRangeRequest{
		StartHex: startHex, EndHex: endHex, CountOnly: true,
	})
	if err != nil {
		return 0, err
	}
	resp, err := stream.Recv()
	if err != nil {
		return 0, err
	}
	return resp.Count, nil
}

// MigrationStatus 报告计划进度:路由变化量(expected)与实际迁移量(copied)对照。
func (s *Server) MigrationStatus(ctx context.Context, req *cachelabv1.MigrationStatusRequest) (*cachelabv1.MigrationStatusResponse, error) {
	plan, err := s.st.GetPlan(ctx, req.PlanId)
	if err != nil {
		return nil, err
	}
	resp := &cachelabv1.MigrationStatusResponse{
		State: plan.State, FromVersion: plan.FromVersion, ToVersion: plan.ToVersion,
	}
	var ringRanges []ring.Range
	for _, rg := range plan.Ranges {
		resp.Ranges = append(resp.Ranges, &cachelabv1.RangeProgress{
			RangeId: int32(rg.RangeID), StartHex: rg.StartHex, EndHex: rg.EndHex,
			FromNode: rg.FromNode, ToNode: rg.ToNode, State: rg.State,
			ExpectedKeys: rg.ExpectedKeys, CopiedKeys: rg.CopiedKeys,
		})
		resp.TotalExpectedKeys += rg.ExpectedKeys
		resp.TotalCopiedKeys += rg.CopiedKeys
		start, _ := parseHexLoose(rg.StartHex)
		end, _ := parseHexLoose(rg.EndHex)
		ringRanges = append(ringRanges, ring.Range{Start: start, End: end})
	}
	resp.ChangedFraction = ring.ChangedFraction(ringRanges)
	return resp, nil
}

func parseHexLoose(s string) (uint64, error) {
	var v uint64
	_, err := fmt.Sscanf(s, "%016x", &v)
	return v, err
}

// SwitchRing 切换路由到新环。
// 关键教学点:复制完成 ≠ 可以安全切换。切换前逐区间重新核对
// 源节点与目标节点的键数;任何不一致都会拒绝切换并把该区间打回重拷。
func (s *Server) SwitchRing(ctx context.Context, req *cachelabv1.SwitchRingRequest) (*cachelabv1.SwitchRingResponse, error) {
	plan, err := s.st.GetPlan(ctx, req.PlanId)
	if err != nil {
		return nil, err
	}
	if plan.State != store.PlanCopied {
		return nil, status.Errorf(codes.FailedPrecondition,
			"plan %d is %s, want COPIED: copy completion is required before switch", plan.PlanID, plan.State)
	}
	// 切换前校验:每个区间 源键数 == 目标键数(静默期下必须相等)。
	var mismatched []string
	for _, rg := range plan.Ranges {
		srcN, err := s.countRange(ctx, rg.FromNode, rg.StartHex, rg.EndHex)
		if err != nil {
			return nil, fmt.Errorf("verify range %d source: %w", rg.RangeID, err)
		}
		dstN, err := s.countRange(ctx, rg.ToNode, rg.StartHex, rg.EndHex)
		if err != nil {
			return nil, fmt.Errorf("verify range %d dest: %w", rg.RangeID, err)
		}
		if srcN != dstN {
			mismatched = append(mismatched,
				fmt.Sprintf("range %d (%s->%s): src=%d dst=%d", rg.RangeID, rg.FromNode, rg.ToNode, srcN, dstN))
			if err := s.st.ResetRangeForRetry(ctx, plan.PlanID, rg.RangeID); err != nil {
				return nil, err
			}
		}
	}
	if len(mismatched) > 0 {
		if err := s.st.SetPlanState(ctx, plan.PlanID, store.PlanCopying); err != nil {
			return nil, err
		}
		return nil, status.Errorf(codes.FailedPrecondition,
			"verification failed, ranges reset for re-copy: %v", mismatched)
	}

	if err := s.st.SwitchActive(ctx, plan.FromVersion, plan.ToVersion); err != nil {
		return nil, err
	}
	if err := s.st.SetPlanState(ctx, plan.PlanID, store.PlanSwitched); err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.active = plan.ToVersion
	s.mu.Unlock()
	s.broadcastActiveVersion(plan.ToVersion)
	return &cachelabv1.SwitchRingResponse{ActiveRingVersion: plan.ToVersion}, nil
}

// broadcastActiveVersion 通知所有在线节点新的活跃版本(尽力而为;
// 节点加入时也会通过 JoinResponse 获得当前版本)。
func (s *Server) broadcastActiveVersion(version int64) {
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
		c, err := s.nodeConn(context.Background(), n.NodeID)
		if err != nil {
			continue
		}
		nctx, err := s.nodeCtx(context.Background(), n.NodeID)
		if err != nil {
			continue
		}
		sctx, cancel := context.WithTimeout(nctx, 5*time.Second)
		_, _ = c.SetActiveVersion(sctx, &cachelabv1.SetActiveVersionRequest{Version: version})
		cancel()
	}
}

// Cleanup 切换完成后删除源节点上已迁出的数据。
// 只有到达 SWITCHED 状态才允许清理 —— 源数据是切换前的最后备份。
func (s *Server) Cleanup(ctx context.Context, req *cachelabv1.CleanupRequest) (*cachelabv1.CleanupResponse, error) {
	plan, err := s.st.GetPlan(ctx, req.PlanId)
	if err != nil {
		return nil, err
	}
	if plan.State != store.PlanSwitched {
		return nil, status.Errorf(codes.FailedPrecondition,
			"plan %d is %s, want SWITCHED: source data must survive until switch completes",
			plan.PlanID, plan.State)
	}
	var deleted int64
	for _, rg := range plan.Ranges {
		c, err := s.nodeConn(ctx, rg.FromNode)
		if err != nil {
			return nil, err
		}
		nctx, err := s.nodeCtx(ctx, rg.FromNode)
		if err != nil {
			return nil, err
		}
		sctx, cancel := context.WithTimeout(nctx, time.Minute)
		resp, err := c.DeleteRange(sctx, &cachelabv1.DeleteRangeRequest{
			StartHex: rg.StartHex, EndHex: rg.EndHex,
		})
		cancel()
		if err != nil {
			return nil, fmt.Errorf("cleanup range %d on %s: %w", rg.RangeID, rg.FromNode, err)
		}
		deleted += resp.Deleted
		if err := s.st.SetRangeState(ctx, plan.PlanID, rg.RangeID, store.RangeCleaned, rg.CopiedKeys); err != nil {
			return nil, err
		}
	}
	if err := s.st.SetPlanState(ctx, plan.PlanID, store.PlanDone); err != nil {
		return nil, err
	}
	return &cachelabv1.CleanupResponse{DeletedKeys: deleted}, nil
}
