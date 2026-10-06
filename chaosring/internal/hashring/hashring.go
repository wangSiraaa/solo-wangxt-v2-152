// Package hashring 实现"固定、可验证"的一致性哈希环。
//
// 设计约束(教学要求):
//   - 哈希算法固定为 FNV-1a 64-bit, 显式实现, 不使用 Go map 迭代序或
//     hash/fnv 以外的任何语言默认哈希(实际本包连 hash/fnv 都不依赖,
//     常量与乘加步骤全部写死, 便于逐行对照公开测试向量)。
//   - 字节序固定: 所有多字节整数一律大端(binary.BigEndian);
//     字符串一律按其 UTF-8 字节序列参与哈希。
//   - 虚拟节点标签格式固定: "vn1:" + nodeID + ":" + decimal(index)
//     index 取值 [0, weight*VNodesPerWeight)。权重上调时新增高位下标,
//     下调时删除高位下标, 保证"标签相同的虚拟节点位置不变",
//     使节点权重变化引起的键移动最小且可预测。
package hashring

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
)

// FNV-1a 64-bit 公开常量(http://www.isthe.com/chongo/tech/comp/fnv/)。
const (
	fnvOffset64 uint64 = 14695981039346656037 // 0xcbf29ce484222325
	fnvPrime64  uint64 = 1099511628211        // 0x100000001b3
)

// VNodesPerWeight 每单位权重对应的虚拟节点数。固定值, 不可按环境改动,
// 否则测试向量与历史环数据全部失效。
const VNodesPerWeight = 128

// FNV1a64 对输入字节计算 FNV-1a 64-bit。
//
// 步骤: hash = offset; 对每个字节: hash ^= b; hash *= prime。
// 输出本身是 uint64; 当需要把其它整数并入哈希时, 调用方必须使用
// Uint64Bytes / Uint32Bytes 取大端表示, 禁止使用本机内存表示。
func FNV1a64(data []byte) uint64 {
	h := fnvOffset64
	for _, b := range data {
		h ^= uint64(b)
		h *= fnvPrime64
	}
	return h
}

// HashString 是路由层对"键"取哈希的唯一入口: 对键的 UTF-8 字节做 FNV-1a。
func HashString(s string) uint64 { return FNV1a64([]byte(s)) }

// Uint64Bytes 返回固定大端字节序(教学中禁止依赖主机字节序)。
func Uint64Bytes(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

// Uint32Bytes 同上。
func Uint32Bytes(v uint32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, v)
	return b
}

// VNodeLabel 返回虚拟节点的固定规范标签。
func VNodeLabel(nodeID string, index uint32) string {
	return "vn1:" + nodeID + ":" + fmt.Sprintf("%d", index)
}

// VNodeHash 计算虚拟节点在环上的位置。哈希对象为标签的 UTF-8 字节。
func VNodeHash(nodeID string, index uint32) uint64 {
	return HashString(VNodeLabel(nodeID, index))
}

// NodeWeight 描述环成员。Weight=0 时该节点保留成员身份但不获得虚拟节点,
// 即不承载任何新键(用于"权重为零摘流"教学案例)。
type NodeWeight struct {
	NodeID string
	Weight uint32
}

// VNode 是环上的一个虚拟节点。
type VNode struct {
	Hash   uint64
	NodeID string
	Index  uint32
	Label  string
}

// Ring 是某一版本下不可变的环快照。
type Ring struct {
	Version int64
	// 权重快照, 仅 weight>0 的成员拥有虚拟节点。
	weights map[string]uint32
	vnodes  []VNode // 按 Hash 升序
}

var (
	ErrEmptyRing   = errors.New("hashring: ring has no weighted node")
	ErrNoSuchNode  = errors.New("hashring: node not present in ring")
	ErrDuplicateOK = errors.New("hashring: duplicate vnode hash (theoretical only)")
)

// New 根据成员权重构造环。weight=0 的成员被记录但不上环。
// 若任意 weight*VNodesPerWeight 超过 uint32 范围将报错。
func New(version int64, members []NodeWeight) (*Ring, error) {
	r := &Ring{Version: version, weights: make(map[string]uint32, len(members))}
	for _, m := range members {
		if _, exists := r.weights[m.NodeID]; exists {
			return nil, fmt.Errorf("hashring: duplicate node %q", m.NodeID)
		}
		r.weights[m.NodeID] = m.Weight
		if m.Weight == 0 {
			continue
		}
		if uint64(m.Weight)*VNodesPerWeight > uint64(^uint32(0)) {
			return nil, fmt.Errorf("hashring: node %q weight too large", m.NodeID)
		}
		count := m.Weight * VNodesPerWeight
		for i := uint32(0); i < count; i++ {
			label := VNodeLabel(m.NodeID, i)
			r.vnodes = append(r.vnodes, VNode{
				Hash:   HashString(label),
				NodeID: m.NodeID,
				Index:  i,
				Label:  label,
			})
		}
	}
	if len(r.vnodes) == 0 {
		return nil, ErrEmptyRing
	}
	sort.Slice(r.vnodes, func(i, j int) bool {
		if r.vnodes[i].Hash != r.vnodes[j].Hash {
			return r.vnodes[i].Hash < r.vnodes[j].Hash
		}
		// 哈希碰撞在 64 位空间理论存在; 用 (node,index) 做确定性次序,
		// 并让两节点仍然都是环成员(查找时只比较 Hash 即可, 不影响正确性)。
		if r.vnodes[i].NodeID != r.vnodes[j].NodeID {
			return r.vnodes[i].NodeID < r.vnodes[j].NodeID
		}
		return r.vnodes[i].Index < r.vnodes[j].Index
	})
	return r, nil
}

// Owner 返回哈希 h 顺时针方向遇到的第一个虚拟节点所属物理节点。
func (r *Ring) Owner(h uint64) string {
	i := sort.Search(len(r.vnodes), func(i int) bool { return r.vnodes[i].Hash >= h })
	if i == len(r.vnodes) {
		i = 0
	}
	return r.vnodes[i].NodeID
}

// OwnerOfKey 是路由便捷方法。
func (r *Ring) OwnerOfKey(key string) string { return r.Owner(HashString(key)) }

// VNodes 返回环虚拟节点的拷贝(供持久化), 顺序为环顺序。
func (r *Ring) VNodes() []VNode {
	out := make([]VNode, len(r.vnodes))
	copy(out, r.vnodes)
	return out
}

// Weight 返回成员权重(含 0)。
func (r *Ring) Weight(nodeID string) (uint32, bool) {
	w, ok := r.weights[nodeID]
	return w, ok
}

// Members 返回成员权重快照。
func (r *Ring) Members() []NodeWeight {
	out := make([]NodeWeight, 0, len(r.weights))
	for id, w := range r.weights {
		out = append(out, NodeWeight{NodeID: id, Weight: w})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].NodeID < out[j].NodeID })
	return out
}

// VNodeCountOf 返回节点拥有的虚拟节点数。
func (r *Ring) VNodeCountOf(nodeID string) int {
	w, ok := r.weights[nodeID]
	if !ok {
		return 0
	}
	return int(w) * VNodesPerWeight
}

// Move 表示一个键在两环之间的归属变化。
type Move struct {
	Key      string
	KeyHash  uint64
	FromNode string // 旧环节点; 新增场景为 "" 表示键从未被承载
	ToNode   string // 新环节点
}

// Diff 比较新旧两环, 对给定键集合返回归属变化(路由变化量)。
//
//	新增节点: From 为旧环 Owner, To 为新环 Owner(From==To 的键不动)。
//	权重归零: 旧环 Owner 是被摘流节点的键, To 必然变成其它节点。
//	传入空键集时返回空计划——环变化本身不产生迁移, 只有"存在的键"才迁。
func Diff(oldRing, newRing *Ring, keys []string) []Move {
	var moves []Move
	for _, k := range keys {
		h := HashString(k)
		from := oldRing.Owner(h)
		to := newRing.Owner(h)
		if from != to {
			moves = append(moves, Move{Key: k, KeyHash: h, FromNode: from, ToNode: to})
		}
	}
	sort.Slice(moves, func(i, j int) bool {
		if moves[i].KeyHash != moves[j].KeyHash {
			return moves[i].KeyHash < moves[j].KeyHash
		}
		return moves[i].Key < moves[j].Key
	})
	return moves
}
