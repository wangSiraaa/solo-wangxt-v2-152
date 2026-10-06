// Package routersvc 实现路由器: 一致性哈希路由 + 迁移编排 + 教学观测。
//
// 关键不变量(对应教学检查点):
//  1. 哈希算法/字节序固定在 hashring 包, 路由结果有公开测试向量背书。
//  2. 权重变化只创建"staging 新环"和迁移计划, 不改变正式路由;
//     旧环仍为 active, 在途请求可按 ring_hint 继续解析旧环。
//  3. 复制(StageCopy 到目标暂存区)与切换(Promote+提升 active 环)是两个
//     显式阶段: state=replicated 绝不等于 committed。
//  4. 路由变化量(Diff 出的迁移条目数)与实际迁移量(逐条 state 推进)
//     全部持久化, 可以一一对账。
//  5. 未知节点无法加入: 注册需管理员令牌 + 预共享 enrollment_secret;
//     数据面对节点的所有调用都带 node_token。
package routersvc

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"chaosring/internal/hashring"
	"chaosring/internal/pb"
	"chaosring/internal/store"
)

// Config 路由器配置。
type Config struct {
	Store            *store.Store
	AdminToken       string // 所有管理面 API 令牌
	EnrollmentSecret string // 节点注册预共享秘密
}

// Service 路由器实现。
type Service struct {
	pb.UnimplementedRouterServer

	st               *store.Store
	adminToken       string
	enrollmentSecret string

	// 环缓存: version -> *hashring.Ring, 命中即可路由, 免查库。
	ringMu sync.RWMutex
	rings  map[int64]*hashring.Ring

	// 节点 gRPC 连接缓存: nodeID -> client。
	clientMu sync.Mutex
	clients  map[string]*nodeClient
}

type nodeClient struct {
	conn   *grpc.ClientConn
	client pb.KVNodeClient
	token  string
	addr   string
}

// New 创建路由器。
func New(cfg Config) *Service {
	return &Service{
		st:               cfg.Store,
		adminToken:       cfg.AdminToken,
		enrollmentSecret: cfg.EnrollmentSecret,
		rings:            map[int64]*hashring.Ring{},
		clients:          map[string]*nodeClient{},
	}
}

// Resume 是启动后与显式 API 共用的恢复入口(见 recovery.go)。
func (s *Service) checkAdmin(tok string) error {
	if subtle.ConstantTimeCompare([]byte(tok), []byte(s.adminToken)) != 1 {
		return errPermission("invalid admin token")
	}
	return nil
}

func (s *Service) checkEnrollment(secret string) error {
	if subtle.ConstantTimeCompare([]byte(secret), []byte(s.enrollmentSecret)) != 1 {
		return errPermission("invalid enrollment secret: unknown nodes may not join")
	}
	return nil
}

func newToken() (string, error) {
	b := make([]byte, 24)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

// ---- 环缓存 ----

// getRing 取出(必要时从 PG 重建)某版本环。重建只依赖固定标签哈希,
// 因此路由器重启后路由结果与重启前逐键一致。
func (s *Service) getRing(ctx context.Context, version int64) (*hashring.Ring, error) {
	s.ringMu.RLock()
	r, ok := s.rings[version]
	s.ringMu.RUnlock()
	if ok {
		return r, nil
	}
	rec, err := s.st.GetRing(ctx, version)
	if err != nil {
		return nil, err
	}
	members := make([]hashring.NodeWeight, 0, len(rec.Weights))
	for id, w := range rec.Weights {
		members = append(members, hashring.NodeWeight{NodeID: id, Weight: uint32(w)})
	}
	nr, err := hashring.New(version, members)
	if err != nil {
		return nil, fmt.Errorf("rebuild ring %d: %w", version, err)
	}
	s.ringMu.Lock()
	s.rings[version] = nr
	s.ringMu.Unlock()
	return nr, nil
}

func (s *Service) invalidate(version int64) {
	s.ringMu.Lock()
	delete(s.rings, version)
	s.ringMu.Unlock()
}

func (s *Service) activeRing(ctx context.Context) (*hashring.Ring, int64, error) {
	v, err := s.st.ActiveRingVersion(ctx)
	if err != nil {
		return nil, 0, err
	}
	r, err := s.getRing(ctx, v)
	return r, v, err
}

// ringForHint 解析在途请求指定的环版本。active 环总是可用。
func (s *Service) ringForHint(ctx context.Context, hint uint64) (*hashring.Ring, int64, error) {
	if hint == 0 {
		return s.activeRing(ctx)
	}
	v := int64(hint)
	rec, err := s.st.GetRing(ctx, v)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, v, errNotFound("ring %d does not exist", v)
		}
		return nil, v, err
	}
	switch rec.State {
	case store.RingRetired:
		return nil, v, errRetired("ring %d has been retired; use the active ring", v)
	case store.RingStaging:
		return nil, v, errFailedPrecondition("ring %d is staging (not active); in-flight requests must use an active/old ring", v)
	}
	r, err := s.getRing(ctx, v)
	return r, v, err
}

// ---- 节点客户端 ----

func (s *Service) nodeClientFor(ctx context.Context, nodeID string) (*nodeClient, error) {
	s.clientMu.Lock()
	defer s.clientMu.Unlock()
	if c, ok := s.clients[nodeID]; ok {
		return c, nil
	}
	n, err := s.st.GetNode(ctx, nodeID)
	if err != nil {
		return nil, fmt.Errorf("node %s: %w", nodeID, err)
	}
	conn, err := grpc.NewClient(n.Address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(), grpc.WithTimeout(3*time.Second))
	if err != nil {
		return nil, fmt.Errorf("dial node %s (%s): %w", nodeID, n.Address, err)
	}
	c := &nodeClient{
		conn:   conn,
		client: pb.NewKVNodeClient(conn),
		token:  n.Token,
		addr:   n.Address,
	}
	s.clients[nodeID] = c
	return c, nil
}

// Close 释放全部节点连接。
func (s *Service) Close() {
	s.clientMu.Lock()
	defer s.clientMu.Unlock()
	for _, c := range s.clients {
		_ = c.conn.Close()
	}
}

func (s *Service) ringToVNodes(r *hashring.Ring) []store.VNodeRow {
	vs := r.VNodes()
	out := make([]store.VNodeRow, 0, len(vs))
	for _, v := range vs {
		out = append(out, store.VNodeRow{Hash: v.Hash, NodeID: v.NodeID, Index: v.Index, Label: v.Label})
	}
	return out
}
