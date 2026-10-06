// Package nodesvc 实现本地键值节点的 gRPC 服务。
//
// 数据组织:
//
//	data:    正式 map[string]entry, 仅由 active 环上的客户端写/路由
//	staging: 按 migrationID 隔离的暂存区, 迁移复制只写这里
//
// 安全模型:
// 节点不自组网, 不接受服务发现式的自由加入。所有数据面调用必须携带
// 注册时颁发的 node_token; Fail 故障注入需要单独的管理员令牌。
// 令牌不匹配一律返回 PermissionDenied。
//
// 教学故障注入:
// Fail 后服务对所有 RPC 返回 Unavailable, 模拟宕机; Recover 恢复。
// WAL 保证重启后 data/staging 都还在, 迁移可以断点续传。
package nodesvc

import (
	"context"
	"sort"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"chaosring/internal/kvlog"
	"chaosring/internal/pb"
)

type entry struct {
	value   []byte
	version int64
	deleted bool
}

type keyHot struct {
	gets int64
	puts int64
}

// Server 是一个 KV 节点。
type Server struct {
	pb.UnimplementedKVNodeServer

	nodeID     string
	token      string
	adminToken string
	dataDir    string
	log        *kvlog.Logger

	mu       sync.RWMutex
	data     map[string]entry
	staging  map[string]map[string]entry // mid -> key -> entry
	hotMu    sync.Mutex
	hot      map[string]*keyHot
	failed   bool
	getTotal int64
	putTotal int64
	ringVer  uint64
}

// Config 构造参数。
type Config struct {
	NodeID     string
	Token      string
	AdminToken string
	DataDir    string
}

// New 构造节点服务器并重放 WAL(中断恢复)。
func New(cfg Config) (*Server, error) {
	lg, err := kvlog.Open(cfg.DataDir)
	if err != nil {
		return nil, err
	}
	s := &Server{
		nodeID:     cfg.NodeID,
		token:      cfg.Token,
		adminToken: cfg.AdminToken,
		dataDir:    cfg.DataDir,
		log:        lg,
		data:       map[string]entry{},
		staging:    map[string]map[string]entry{},
		hot:        map[string]*keyHot{},
	}
	if err := kvlog.Replay(cfg.DataDir, s.apply); err != nil {
		return nil, err
	}
	return s, nil
}

// apply 在重放期间应用一条 WAL 记录(不加锁: 此时尚未对外服务)。
func (s *Server) apply(r kvlog.Record) error {
	switch r.Op {
	case "put":
		if cur, ok := s.data[r.Key]; ok && r.Version < cur.version {
			return nil
		}
		s.data[r.Key] = entry{value: r.Value, version: r.Version, deleted: false}
	case "del":
		if cur, ok := s.data[r.Key]; ok && r.Version < cur.version {
			return nil
		}
		s.data[r.Key] = entry{version: r.Version, deleted: true}
	case "stage":
		m := s.staging[r.Mid]
		if m == nil {
			m = map[string]entry{}
			s.staging[r.Mid] = m
		}
		if cur, ok := m[r.Key]; ok && r.Version < cur.version {
			return nil
		}
		m[r.Key] = entry{value: r.Value, version: r.Version, deleted: r.Deleted}
	case "promote":
		s.promoteLocked(r.Mid, uint64(r.Version))
	case "drop":
		delete(s.staging, r.Mid)
	case "purge":
		delete(s.data, r.Key)
	}
	return nil
}

// promoteLocked 把某迁移暂存区提升进正式 map(调用方持写锁或处于重放期)。
// 返回提升条数; 墓碑键计入 tombstones。
func (s *Server) promoteLocked(mid string, targetRing uint64) (promoted, tombstones int) {
	m := s.staging[mid]
	if m != nil {
		for k, e := range m {
			if cur, ok := s.data[k]; ok && e.version < cur.version {
			} else {
				s.data[k] = e
				promoted++
				if e.deleted {
					tombstones++
				}
			}
		}
		delete(s.staging, mid)
	}
	if targetRing > 0 {
		s.ringVer = targetRing
	}
	return promoted, tombstones
}

func (s *Server) checkToken(tok string) error {
	if tok != s.token {
		return status.Error(codes.PermissionDenied,
			"invalid node token: unknown node is not allowed to drive this server")
	}
	return nil
}

func (s *Server) checkFailed() error {
	if s.failed {
		return status.Error(codes.Unavailable, "node is failed (injected)")
	}
	return nil
}

func (s *Server) bumpHot(k string, put bool) {
	h := s.hot[k]
	if h == nil {
		h = &keyHot{}
		s.hot[k] = h
	}
	if put {
		h.puts++
		s.putTotal++
	} else {
		h.gets++
		s.getTotal++
	}
}

// ---- 数据面 ----

func cloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	c := make([]byte, len(b))
	copy(c, b)
	return c
}

func (s *Server) Put(ctx context.Context, req *pb.NodePutRequest) (*pb.NodePutResponse, error) {
	if err := s.checkToken(req.NodeToken); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkFailed(); err != nil {
		return nil, err
	}
	if cur, ok := s.data[req.Key]; ok && int64(req.Version) < cur.version {
		// 旧写保护: 返回当前更高版本, 路由器据此感知在途旧写冲突。
		return &pb.NodePutResponse{Version: uint64(cur.version)}, nil
	}
	if err := s.log.Append(kvlog.Record{
		Op: "put", Key: req.Key, Version: int64(req.Version), Value: req.Value,
	}); err != nil {
		return nil, status.Errorf(codes.Internal, "wal: %v", err)
	}
	s.data[req.Key] = entry{value: cloneBytes(req.Value), version: int64(req.Version)}
	s.hotMu.Lock()
	s.bumpHot(req.Key, true)
	s.hotMu.Unlock()
	return &pb.NodePutResponse{Version: req.Version}, nil
}

func (s *Server) Get(ctx context.Context, req *pb.NodeGetRequest) (*pb.NodeGetResponse, error) {
	if err := s.checkToken(req.NodeToken); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := s.checkFailed(); err != nil {
		return nil, err
	}
	e, ok := s.data[req.Key]
	if !ok {
		return &pb.NodeGetResponse{Found: false}, nil
	}
	if e.deleted {
		return &pb.NodeGetResponse{Found: true, Deleted: true, Version: uint64(e.version)}, nil
	}
	s.hotMu.Lock()
	s.bumpHot(req.Key, false)
	s.hotMu.Unlock()
	return &pb.NodeGetResponse{Found: true, Value: cloneBytes(e.value), Version: uint64(e.version)}, nil
}

func (s *Server) Delete(ctx context.Context, req *pb.NodeDeleteRequest) (*pb.NodeDeleteResponse, error) {
	if err := s.checkToken(req.NodeToken); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkFailed(); err != nil {
		return nil, err
	}
	if err := s.log.Append(kvlog.Record{
		Op: "del", Key: req.Key, Version: int64(req.Version),
	}); err != nil {
		return nil, status.Errorf(codes.Internal, "wal: %v", err)
	}
	s.data[req.Key] = entry{version: int64(req.Version), deleted: true}
	return &pb.NodeDeleteResponse{}, nil
}

func (s *Server) ListKeys(ctx context.Context, req *pb.ListKeysRequest) (*pb.ListKeysResponse, error) {
	if err := s.checkToken(req.NodeToken); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := s.checkFailed(); err != nil {
		return nil, err
	}
	var keys []string
	if req.MigrationId == "" {
		for k, e := range s.data {
			if !e.deleted {
				keys = append(keys, k)
			}
		}
	} else {
		for k := range s.staging[req.MigrationId] {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	return &pb.ListKeysResponse{Keys: keys}, nil
}

// ---- 迁移复制 / 提升 / 丢弃 ----

func (s *Server) StageCopy(ctx context.Context, req *pb.StageCopyRequest) (*pb.StageCopyResponse, error) {
	if err := s.checkToken(req.NodeToken); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkFailed(); err != nil {
		return nil, err
	}
	m := s.staging[req.MigrationId]
	if m == nil {
		m = map[string]entry{}
		s.staging[req.MigrationId] = m
	}
	already := false
	if cur, ok := m[req.Key]; ok {
		already = true
		if int64(req.Version) < cur.version {
			// 幂等重发的旧版本: 视为已接受, 但不回退数据。
			return &pb.StageCopyResponse{Accepted: true, AlreadyPresent: true}, nil
		}
	}
	if err := s.log.Append(kvlog.Record{
		Op: "stage", Mid: req.MigrationId, Key: req.Key,
		Version: int64(req.Version), Value: req.Value, Deleted: req.Deleted,
	}); err != nil {
		return nil, status.Errorf(codes.Internal, "wal: %v", err)
	}
	m[req.Key] = entry{
		value: cloneBytes(req.Value), version: int64(req.Version), deleted: req.Deleted,
	}
	return &pb.StageCopyResponse{Accepted: true, AlreadyPresent: already}, nil
}

func (s *Server) Promote(ctx context.Context, req *pb.PromoteRequest) (*pb.PromoteResponse, error) {
	if err := s.checkToken(req.NodeToken); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkFailed(); err != nil {
		return nil, err
	}
	if _, ok := s.staging[req.MigrationId]; !ok {
		// 幂等: 可能之前已经提升(重试场景)。
		if req.TargetRing > 0 {
			s.ringVer = req.TargetRing
		}
		return &pb.PromoteResponse{}, nil
	}
	if err := s.log.Append(kvlog.Record{
		Op: "promote", Mid: req.MigrationId, Version: int64(req.TargetRing),
	}); err != nil {
		return nil, status.Errorf(codes.Internal, "wal: %v", err)
	}
	promoted, tombstones := s.promoteLocked(req.MigrationId, req.TargetRing)
	return &pb.PromoteResponse{Promoted: int32(promoted), DroppedTombstones: int32(tombstones)}, nil
}

func (s *Server) DropStaging(ctx context.Context, req *pb.DropStagingRequest) (*pb.DropStagingResponse, error) {
	if err := s.checkToken(req.NodeToken); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkFailed(); err != nil {
		return nil, err
	}
	n := len(s.staging[req.MigrationId])
	if err := s.log.Append(kvlog.Record{Op: "drop", Mid: req.MigrationId}); err != nil {
		return nil, status.Errorf(codes.Internal, "wal: %v", err)
	}
	delete(s.staging, req.MigrationId)
	return &pb.DropStagingResponse{Dropped: int32(n)}, nil
}

// PurgeKeys 在旧环退役后回收已迁走键的本地副本。不写墓碑:
// 该节点在 active 环上已不再是这些键的属主, 墓碑反而会污染未来写。
func (s *Server) PurgeKeys(ctx context.Context, req *pb.PurgeKeysRequest) (*pb.PurgeKeysResponse, error) {
	if err := s.checkToken(req.NodeToken); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkFailed(); err != nil {
		return nil, err
	}
	purged := 0
	for _, k := range req.Keys {
		if _, ok := s.data[k]; !ok {
			continue
		}
		if err := s.log.Append(kvlog.Record{Op: "purge", Key: k}); err != nil {
			return nil, status.Errorf(codes.Internal, "wal: %v", err)
		}
		delete(s.data, k)
		purged++
	}
	return &pb.PurgeKeysResponse{Purged: int32(purged)}, nil
}

// ---- 故障注入 / 统计 / 握手 ----

func (s *Server) Fail(ctx context.Context, req *pb.FailRequest) (*pb.FailResponse, error) {
	if req.AdminToken != s.adminToken {
		return nil, status.Error(codes.PermissionDenied, "admin token required for Fail")
	}
	s.mu.Lock()
	s.failed = true
	s.mu.Unlock()
	return &pb.FailResponse{}, nil
}

func (s *Server) Recover(ctx context.Context, req *pb.RecoverRequest) (*pb.RecoverResponse, error) {
	if err := s.checkToken(req.NodeToken); err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.failed = false
	s.mu.Unlock()
	return &pb.RecoverResponse{}, nil
}

func (s *Server) Stats(ctx context.Context, req *pb.StatsRequest) (*pb.NodeStats, error) {
	if err := s.checkToken(req.NodeToken); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	live := 0
	for _, e := range s.data {
		if !e.deleted {
			live++
		}
	}
	staging := 0
	for _, m := range s.staging {
		staging += len(m)
	}
	tops := make([]*pb.KeyHot, 0, len(s.hot))
	s.hotMu.Lock()
	for k, h := range s.hot {
		tops = append(tops, &pb.KeyHot{Key: k, Gets: uint64(h.gets), Puts: uint64(h.puts)})
	}
	getTotal, putTotal := s.getTotal, s.putTotal
	s.hotMu.Unlock()
	sort.Slice(tops, func(i, j int) bool {
		a, b := tops[i].Gets+tops[i].Puts, tops[j].Gets+tops[j].Puts
		if a != b {
			return a > b
		}
		return tops[i].Key < tops[j].Key
	})
	if len(tops) > 10 {
		tops = tops[:10]
	}
	return &pb.NodeStats{
		NodeId:       s.nodeID,
		KeyCount:     uint64(live),
		StagingCount: uint64(staging),
		GetCount:     uint64(getTotal),
		PutCount:     uint64(putTotal),
		TopKeys:      tops,
	}, nil
}

func (s *Server) Handshake(ctx context.Context, req *pb.HandshakeRequest) (*pb.HandshakeResponse, error) {
	if err := s.checkToken(req.NodeToken); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return &pb.HandshakeResponse{
		Ok:          req.NodeId == s.nodeID,
		NodeId:      s.nodeID,
		RingVersion: s.ringVer,
	}, nil
}

// NodeID 返回节点 ID(供 main 注册使用)。
func (s *Server) NodeID() string { return s.nodeID }
