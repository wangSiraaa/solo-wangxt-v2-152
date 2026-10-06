// Package hash 定义集群唯一的键定位哈希:FNV-1a 64。
//
// 算法与字节序完全固定,不依赖任何语言默认哈希(如 Go 的 maphash /
// 运行时 map 哈希),保证跨进程、跨机器、跨重启结果一致。
// 正确性由公开测试向量锁定(见 fnv_test.go)。
package hash

import (
	"encoding/binary"
	"fmt"
)

// FNV-1a 64 位参数(公开常量,见 http://www.isthe.com/chongo/tech/comp/fnv/)。
const (
	offsetBasis64 uint64 = 14695981039346656037 // 0xcbf29ce484222325
	prime64       uint64 = 1099511628211        // 0x100000001b3
)

// Sum64 计算 FNV-1a 64 位哈希:逐字节异或后乘素数,溢出按 mod 2^64 回绕。
func Sum64(b []byte) uint64 {
	h := offsetBasis64
	for _, c := range b {
		h ^= uint64(c)
		h *= prime64
	}
	return h
}

// Mix64 是 splitmix64 终末混合器(公开算法,常数来自 Steele & Vigna)。
//
// FNV-1a 是逐字节线性结构:仅尾部字节不同的输入(如 "node#0"、"node#1"
// 或顺序键 "key-00001"/"key-00002")哈希值仅差素数的整数倍,会挤在环上
// 一段极小的弧内。终末混合提供雪崩效应,使环上点位均匀分布。
// 环上位置统一定义为 Mix64(Sum64(material)),算法与常数全部固定。
func Mix64(x uint64) uint64 {
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}

// KeyPoint 返回键在哈希环上的位置。
func KeyPoint(key string) uint64 {
	return Mix64(Sum64([]byte(key)))
}

// VnodePoint 返回虚拟节点在环上的位置。
// 输入编码固定为: nodeID || '#' || big-endian uint32(index)。
// 字节序显式指定为 BigEndian,与平台无关。
func VnodePoint(nodeID string, index uint32) uint64 {
	buf := make([]byte, 0, len(nodeID)+1+4)
	buf = append(buf, nodeID...)
	buf = append(buf, '#')
	var idx [4]byte
	binary.BigEndian.PutUint32(idx[:], index)
	buf = append(buf, idx[:]...)
	return Mix64(Sum64(buf))
}

// Hex 将环上位置编码为定宽 16 字符小写十六进制。
// 定宽编码的字典序与数值序一致,可直接用于 PostgreSQL 文本列排序与比较。
func Hex(p uint64) string {
	return fmt.Sprintf("%016x", p)
}

// ParseHex 是 Hex 的逆变换。
func ParseHex(s string) (uint64, error) {
	var v uint64
	_, err := fmt.Sscanf(s, "%016x", &v)
	if err != nil {
		return 0, fmt.Errorf("invalid ring point hex %q: %w", s, err)
	}
	return v, nil
}

// InRange 报告 p 是否落在区间 (start, end] 内。
// end < start 表示跨零回绕区间。
func InRange(p, start, end uint64) bool {
	if start < end {
		return p > start && p <= end
	}
	if start > end {
		return p > start || p <= end
	}
	return false // start == end:空区间
}
