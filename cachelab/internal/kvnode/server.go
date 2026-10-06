// Package kvnode 实现本地键值节点:内存存储 + 数据面读写 +
// 受令牌保护的迁移控制面(导出/导入/删除区间)。
package kvnode

import (
	"context"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	cachelabv1 "cachelab/gen/cachelab/v1"
	"cachelab/internal/hash"
)

// 控制面方法:仅 Router 可调用,需携带 x-node-token。
var controlMethods = map[string]bool{
	"/cachelab.v1.KVNode/ExportRange":      true,
	"/cachelab.v1.KVNode/ImportBatch":      true,
	"/cachelab.v1.KVNode/DeleteRange":      true,
	"/cachelab.v1.KVNode/Stats":            true,
	"/cachelab.v1.KVNode/SetActiveVersion": true,
}

type Server struct {
	cachelabv1.UnimplementedKVNodeServer

	nodeID string
	token  string

	mu   sync.RWMutex
	data map[string][]byte
	hits map[string]int64

	activeVersion atomic.Int64
}

func New(nodeID, token string) *Server {
	return &Server{nodeID: nodeID, token: token, data: map[string][]byte{}, hits: map[string]int64{}}
}

// SetActive 设置节点已知的活跃环版本(由 Router 在切换后广播)。
func (s *Server) SetActive(v int64) { s.activeVersion.Store(v) }

// KeyCount 返回当前键数(测试与观测用)。
func (s *Server) KeyCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data)
}

// ---- 拦截器:控制面方法校验令牌 ----

func (s *Server) checkToken(ctx context.Context) error {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return status.Error(codes.Unauthenticated, "missing metadata")
	}
	vals := md.Get("x-node-token")
	if len(vals) == 0 || vals[0] != s.token {
		return status.Error(codes.Unauthenticated, "invalid node token")
	}
	return nil
}

func (s *Server) UnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if controlMethods[info.FullMethod] {
			if err := s.checkToken(ctx); err != nil {
				return nil, err
			}
		}
		return handler(ctx, req)
	}
}

func (s *Server) StreamInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if controlMethods[info.FullMethod] {
			if err := s.checkToken(ss.Context()); err != nil {
				return err
			}
		}
		return handler(srv, ss)
	}
}

// ---- 数据面 ----

// checkWriteVersion 拒绝携带过旧环版本的写:
// 切换后仍按旧路由写来的请求必须重试新路由,否则数据会写到已迁出的节点上。
func (s *Server) checkWriteVersion(v int64) error {
	if cur := s.activeVersion.Load(); v < cur {
		return status.Errorf(codes.FailedPrecondition,
			"stale ring version %d, current %d: re-route and retry", v, cur)
	}
	return nil
}

func (s *Server) Get(_ context.Context, req *cachelabv1.GetRequest) (*cachelabv1.GetResponse, error) {
	s.mu.RLock()
	v, ok := s.data[req.Key]
	s.mu.RUnlock()
	s.mu.Lock()
	s.hits[req.Key]++
	s.mu.Unlock()
	if !ok {
		return &cachelabv1.GetResponse{Found: false}, nil
	}
	return &cachelabv1.GetResponse{Found: true, Value: v}, nil
}

func (s *Server) Put(_ context.Context, req *cachelabv1.PutRequest) (*cachelabv1.PutResponse, error) {
	if err := s.checkWriteVersion(req.RingVersion); err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.data[req.Key] = req.Value
	s.hits[req.Key]++
	s.mu.Unlock()
	return &cachelabv1.PutResponse{}, nil
}

func (s *Server) Delete(_ context.Context, req *cachelabv1.DeleteRequest) (*cachelabv1.DeleteResponse, error) {
	if err := s.checkWriteVersion(req.RingVersion); err != nil {
		return nil, err
	}
	s.mu.Lock()
	_, existed := s.data[req.Key]
	delete(s.data, req.Key)
	s.mu.Unlock()
	return &cachelabv1.DeleteResponse{Existed: existed}, nil
}

// ---- 控制面(迁移) ----

func parseRange(startHex, endHex string) (uint64, uint64, error) {
	start, err := hash.ParseHex(strings.TrimSpace(startHex))
	if err != nil {
		return 0, 0, err
	}
	end, err := hash.ParseHex(strings.TrimSpace(endHex))
	if err != nil {
		return 0, 0, err
	}
	return start, end, nil
}

// ExportRange 流式导出 (start, end] 内的键值;count_only 时只返回计数。
func (s *Server) ExportRange(req *cachelabv1.ExportRangeRequest, stream grpc.ServerStreamingServer[cachelabv1.ExportRangeResponse]) error {
	start, end, err := parseRange(req.StartHex, req.EndHex)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	// 先在锁内快照,再在锁外流式发送,避免长时间阻塞写。
	type kv struct {
		k string
		v []byte
	}
	var items []kv
	var count int64
	s.mu.RLock()
	for k, v := range s.data {
		if hash.InRange(hash.KeyPoint(k), start, end) {
			count++
			if !req.CountOnly {
				items = append(items, kv{k, v})
			}
		}
	}
	s.mu.RUnlock()

	if req.CountOnly {
		return stream.Send(&cachelabv1.ExportRangeResponse{Count: count})
	}
	for _, it := range items {
		if err := stream.Send(&cachelabv1.ExportRangeResponse{
			Kv: &cachelabv1.KeyValue{Key: it.k, Value: it.v},
		}); err != nil {
			return err
		}
	}
	return nil
}

// ImportBatch 幂等导入(upsert),重复执行安全 —— 这是断点续拷的前提。
func (s *Server) ImportBatch(_ context.Context, req *cachelabv1.ImportBatchRequest) (*cachelabv1.ImportBatchResponse, error) {
	s.mu.Lock()
	for _, item := range req.Items {
		s.data[item.Key] = item.Value
	}
	s.mu.Unlock()
	return &cachelabv1.ImportBatchResponse{Imported: int64(len(req.Items))}, nil
}

// DeleteRange 仅在切换完成后由 Cleanup 调用,删除已迁出的源数据。
func (s *Server) DeleteRange(_ context.Context, req *cachelabv1.DeleteRangeRequest) (*cachelabv1.DeleteRangeResponse, error) {
	start, end, err := parseRange(req.StartHex, req.EndHex)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	var deleted int64
	s.mu.Lock()
	for k := range s.data {
		if hash.InRange(hash.KeyPoint(k), start, end) {
			delete(s.data, k)
			deleted++
		}
	}
	s.mu.Unlock()
	return &cachelabv1.DeleteRangeResponse{Deleted: deleted}, nil
}

func (s *Server) Stats(_ context.Context, _ *cachelabv1.StatsRequest) (*cachelabv1.StatsResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	resp := &cachelabv1.StatsResponse{KeyCount: int64(len(s.data))}
	for k, n := range s.hits {
		resp.HotKeys = append(resp.HotKeys, &cachelabv1.HotKey{Key: k, Accesses: n})
	}
	sort.Slice(resp.HotKeys, func(i, j int) bool { return resp.HotKeys[i].Accesses > resp.HotKeys[j].Accesses })
	if len(resp.HotKeys) > 100 {
		resp.HotKeys = resp.HotKeys[:100]
	}
	return resp, nil
}

func (s *Server) SetActiveVersion(_ context.Context, req *cachelabv1.SetActiveVersionRequest) (*cachelabv1.SetActiveVersionResponse, error) {
	s.activeVersion.Store(req.Version)
	return &cachelabv1.SetActiveVersionResponse{}, nil
}
