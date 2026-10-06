// Package ring 实现一致性哈希环:构建、定位、版本间差异(迁移计划)。
//
// 环上位置为 uint64,区间约定为 (start, end](左开右闭),
// end < start 表示跨零回绕。键 k 的属主为环上第一个 point >= h(k) 的虚拟节点。
package ring

import (
	"sort"

	"cachelab/internal/hash"
)

// VNode 是环上一个虚拟节点。
type VNode struct {
	Point  uint64
	NodeID string
}

// Range 表示一段需要迁移的哈希空间 (Start, End]。
type Range struct {
	Start uint64 // 开区间端点
	End   uint64 // 闭区间端点,End < Start 表示跨零回绕
	From  string
	To    string
}

// Ring 是按点位升序排列的虚拟节点环。不可变,可安全并发读。
type Ring struct {
	vnodes []VNode
}

// VNodesPerWeight 是每单位权重对应的虚拟节点数(固定,影响环的可比性)。
const VNodesPerWeight = 128

// Build 由节点权重表构建新环。权重 <= 0 的节点不上环。
func Build(weights map[string]int) *Ring {
	var vn []VNode
	for nodeID, w := range weights {
		for i := 0; i < w*VNodesPerWeight; i++ {
			vn = append(vn, VNode{Point: hash.VnodePoint(nodeID, uint32(i)), NodeID: nodeID})
		}
	}
	sort.Slice(vn, func(i, j int) bool { return vn[i].Point < vn[j].Point })
	return &Ring{vnodes: vn}
}

// FromVNodes 由持久化数据还原环(用于重启恢复)。
func FromVNodes(vn []VNode) *Ring {
	sort.Slice(vn, func(i, j int) bool { return vn[i].Point < vn[j].Point })
	return &Ring{vnodes: vn}
}

// VNodes 导出全部虚拟节点(用于持久化)。
func (r *Ring) VNodes() []VNode {
	out := make([]VNode, len(r.vnodes))
	copy(out, r.vnodes)
	return out
}

// Empty 报告环是否为空。
func (r *Ring) Empty() bool { return len(r.vnodes) == 0 }

// Owner 返回环上位置的属主节点;空环返回空串。
func (r *Ring) Owner(point uint64) string {
	if len(r.vnodes) == 0 {
		return ""
	}
	i := sort.Search(len(r.vnodes), func(i int) bool { return r.vnodes[i].Point >= point })
	if i == len(r.vnodes) {
		i = 0
	}
	return r.vnodes[i].NodeID
}

// OwnerOfKey 返回键的属主节点。
func (r *Ring) OwnerOfKey(key string) string {
	return r.Owner(hash.KeyPoint(key))
}

// Fractions 返回各节点在环上占据的哈希空间比例(预测键分布)。
func (r *Ring) Fractions() map[string]float64 {
	out := map[string]float64{}
	n := len(r.vnodes)
	if n == 0 {
		return out
	}
	const full = 1 << 64
	for i, v := range r.vnodes {
		prev := r.vnodes[(i-1+n)%n].Point
		var size float64
		if v.Point > prev {
			size = float64(v.Point - prev)
		} else if v.Point < prev {
			size = float64(v.Point) + (full - float64(prev))
		} // 相等点位尺寸为 0
		out[v.NodeID] += size / full
	}
	return out
}

// Diff 计算从 old 环切换到 new 环时,哈希空间属主发生变化的区间集合。
// 返回的区间两两不相交,其并集恰好是"路由结果改变"的键空间——
// 这就是迁移计划要复制的全部数据范围。
func Diff(oldR, newR *Ring) []Range {
	cutSet := map[uint64]struct{}{}
	for _, v := range oldR.vnodes {
		cutSet[v.Point] = struct{}{}
	}
	for _, v := range newR.vnodes {
		cutSet[v.Point] = struct{}{}
	}
	if len(cutSet) == 0 {
		return nil
	}
	cuts := make([]uint64, 0, len(cutSet))
	for p := range cutSet {
		cuts = append(cuts, p)
	}
	sort.Slice(cuts, func(i, j int) bool { return cuts[i] < cuts[j] })

	n := len(cuts)
	type seg struct {
		start, end uint64
		from, to   string
	}
	// 基本区间 (cuts[i-1], cuts[i]],i 按环形顺序;区间内属主恒定。
	segs := make([]seg, 0, n)
	for i := 0; i < n; i++ {
		end := cuts[i]
		start := cuts[(i-1+n)%n]
		from, to := oldR.Owner(end), newR.Owner(end)
		if from == to {
			continue // 属主未变,无需迁移
		}
		segs = append(segs, seg{start: start, end: end, from: from, to: to})
	}
	if len(segs) == 0 {
		return nil
	}
	// 合并相邻且 (from,to) 相同的基本区间。
	merged := []seg{segs[0]}
	for _, s := range segs[1:] {
		last := &merged[len(merged)-1]
		if last.from == s.from && last.to == s.to && last.end == s.start {
			last.end = s.end
		} else {
			merged = append(merged, s)
		}
	}
	// 环形首尾可能还需合并一次(跨零区间)。
	if len(merged) > 1 {
		first, last := merged[0], merged[len(merged)-1]
		if first.from == last.from && first.to == last.to && last.end == first.start {
			merged[0] = seg{start: last.start, end: first.end, from: first.from, to: first.to}
			merged = merged[:len(merged)-1]
		}
	}
	out := make([]Range, len(merged))
	for i, s := range merged {
		out[i] = Range{Start: s.start, End: s.end, From: s.from, To: s.to}
	}
	return out
}

// ChangedFraction 返回迁移区间占整个哈希空间的比例(路由变化量)。
func ChangedFraction(ranges []Range) float64 {
	const full = 1 << 64
	var total float64
	for _, r := range ranges {
		if r.End > r.Start {
			total += float64(r.End - r.Start)
		} else if r.End < r.Start {
			total += float64(r.End) + (full - float64(r.Start))
		}
	}
	return total / full
}
