package hashring

import (
	"encoding/binary"
	"testing"
)

// TestFNV1a64PublicVectors 使用 FNV 官方公开测试向量验证哈希实现。
// 向量来源: http://www.isthe.com/chongo/tech/comp/fnv/index.html#FNV-1a
// (Landon Curtis Noll 的 FNV 页面, 32/64 位 FNV-1 与 FNV-1a 空串及
// "a", "foobar" 向量为业界标准参考值)。
//
// 这一步直接保证"哈希算法固定、不依赖语言默认 hash"。
func TestFNV1a64PublicVectors(t *testing.T) {
	// 非 ASCII 单字节 0xff: h = (offset ^ 0xff) * prime (运行时模 2^64 乘法)
	off := fnvOffset64
	ffHash := (off ^ uint64(0xff)) * fnvPrime64
	cases := []struct {
		in   string
		want uint64
	}{
		{"", 0xcbf29ce484222325},
		{"a", 0xaf63dc4c8601ec8c},
		{"foobar", 0x85944171f73967e8},
		// 证明输入按原始字节而非某种字符串规范化处理。
		{"\xff", ffHash},
	}
	for _, c := range cases {
		got := FNV1a64([]byte(c.in))
		if got != c.want {
			t.Errorf("FNV1a64(%q) = %#016x, want %#016x", c.in, got, c.want)
		}
	}
}

// TestEndiannessFixed 锁定字节序: Uint64Bytes 必须是大端。
func TestEndiannessFixed(t *testing.T) {
	got := Uint64Bytes(0x0102030405060708)
	want := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("Uint64Bytes not big-endian: got % x", got)
		}
	}
	if binary.BigEndian.Uint64(got) != 0x0102030405060708 {
		t.Fatal("big-endian round trip failed")
	}
	b := Uint32Bytes(0x0a0b0c0d)
	if b[0] != 0x0a || b[3] != 0x0d {
		t.Fatalf("Uint32Bytes not big-endian: % x", b)
	}
}

// TestVNodeLabelStable 锁定虚拟节点标签格式与位置, 标签一旦变化,
// 所有历史环版本的路由都会漂移——这正是权重调整时必须保持的不变量。
func TestVNodeLabelStable(t *testing.T) {
	if got, want := VNodeLabel("nA", 7), "vn1:nA:7"; got != want {
		t.Fatalf("label = %q, want %q", got, want)
	}
	// FNV1a64("vn1:nA:0") 公开可复算
	want := FNV1a64([]byte("vn1:nA:0"))
	if VNodeHash("nA", 0) != want {
		t.Fatal("vnode hash must be FNV1a64 of fixed label")
	}
}

// TestRingGoldenVectors 固定三节点小环的路由金样例。
// 金样例由本文件 helper 首次计算并锁定; 哈希本身已由上面的公开向量
// 独立保证, 这里锁定的是"标签规则 + 环查找规则"这一层组合行为。
func TestRingGoldenVectors(t *testing.T) {
	r, err := New(1, []NodeWeight{
		{NodeID: "nA", Weight: 1},
		{NodeID: "nB", Weight: 1},
		{NodeID: "nC", Weight: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(r.VNodes()) != 3*VNodesPerWeight {
		t.Fatalf("vnodes = %d", len(r.VNodes()))
	}
	// 固定键 -> 固定哈希 -> 固定属主, 三者全部锁定。
	// 哈希列是 FNV1a64(键 UTF-8 字节), 属主列是该环下的顺时针查找结果。
	// 标签规则/字节序/查找规则任一被改动, 都会在这里暴露。
	cases := []struct {
		key  string
		hash uint64
		want string
	}{
		{"user:1", 0xf7fd9aaa75081ceb, "nC"},
		{"user:2", 0xf7fd9baa75081e9e, "nC"},
		{"user:3", 0xf7fd9caa75082051, "nC"},
		{"user:100", 0x5f2807232998b4e3, "nA"},
		{"order:42", 0x0cef508660eaa9b1, "nC"},
		{"hotkey:🔥", 0x41099e446530a03d, "nA"},
	}
	for _, c := range cases {
		if h := HashString(c.key); h != c.hash {
			t.Fatalf("HashString(%q) = %#016x, want %#016x", c.key, h, c.hash)
		}
		if got := r.OwnerOfKey(c.key); got != c.want {
			t.Fatalf("owner(%q) = %s, want %s", c.key, got, c.want)
		}
	}
	// 确定性: 重新构造同权重环, 结果必须逐键相同
	r2, _ := New(1, []NodeWeight{{NodeID: "nA", Weight: 1}, {NodeID: "nB", Weight: 1}, {NodeID: "nC", Weight: 1}})
	for _, c := range cases {
		if r2.OwnerOfKey(c.key) != c.want {
			t.Fatalf("nondeterministic owner for %q", c.key)
		}
	}
	// 环顺序必须严格升序, 供 sort.Search 正确工作
	vs := r.VNodes()
	for i := 1; i < len(vs); i++ {
		if vs[i-1].Hash > vs[i].Hash {
			t.Fatal("vnode order not sorted")
		}
	}
}

// TestWeightChangeMinimalMoves 验证权重上调只"新增"高位下标虚拟节点,
// 已有标签的位置完全不变。
func TestWeightChangeMinimalMoves(t *testing.T) {
	base := []NodeWeight{{NodeID: "nA", Weight: 1}, {NodeID: "nB", Weight: 1}}
	r1, _ := New(1, base)
	r2, _ := New(2, []NodeWeight{{NodeID: "nA", Weight: 2}, {NodeID: "nB", Weight: 1}})

	pos := map[string]uint64{}
	for _, v := range r1.VNodes() {
		pos[v.Label] = v.Hash
	}
	kept := 0
	for _, v := range r2.VNodes() {
		if h, ok := pos[v.Label]; ok {
			if h != v.Hash {
				t.Fatalf("existing vnode %q moved!", v.Label)
			}
			kept++
		}
	}
	if kept != len(pos) {
		t.Fatalf("lost %d existing vnodes after weight increase", len(pos)-kept)
	}
	if len(r2.VNodes()) != len(pos)+VNodesPerWeight {
		t.Fatalf("weight increase must add exactly %d vnodes", VNodesPerWeight)
	}
}

// TestZeroWeightNodeDrains 权重为 0 的节点保留成员身份但不获虚拟节点,
// 任何键都不应再路由到它。
func TestZeroWeightNodeDrains(t *testing.T) {
	r1, _ := New(1, []NodeWeight{{NodeID: "nA", Weight: 1}, {NodeID: "nB", Weight: 1}})
	r2, _ := New(2, []NodeWeight{{NodeID: "nA", Weight: 1}, {NodeID: "nB", Weight: 0}})

	if w, ok := r2.Weight("nB"); !ok || w != 0 {
		t.Fatal("zero-weight node must remain a recorded member")
	}
	if r2.VNodeCountOf("nB") != 0 {
		t.Fatal("zero-weight node must own no vnodes")
	}
	// 用一组固定键验证: 旧环上属于 nB 的键, 在新环全部改属 nA
	var movedToA int
	var drained []string
	keys := []string{"key:0", "key:1", "key:2", "key:3", "key:4", "key:5", "key:6", "key:7"}
	for _, k := range keys {
		if r1.OwnerOfKey(k) == "nB" {
			drained = append(drained, k)
			if r2.OwnerOfKey(k) != "nA" {
				t.Fatalf("key %q not drained to nA", k)
			}
			movedToA++
		}
	}
	if len(drained) == 0 {
		t.Fatal("test setup expected some keys owned by nB")
	}
	moves := Diff(r1, r2, keys)
	if len(moves) != movedToA {
		t.Fatalf("Diff moves=%d want %d", len(moves), movedToA)
	}
	for _, m := range moves {
		if m.FromNode != "nB" || m.ToNode != "nA" {
			t.Fatalf("unexpected move %+v", m)
		}
	}
}

// TestAddNodeDiff 新增节点: 只有一部分键改属新节点, 其余不动;
// 且路由变化量 == Diff 报告量, 供与实际迁移量逐条对照。
func TestAddNodeDiff(t *testing.T) {
	r1, _ := New(1, []NodeWeight{{NodeID: "nA", Weight: 1}, {NodeID: "nB", Weight: 1}})
	r2, _ := New(2, []NodeWeight{
		{NodeID: "nA", Weight: 1}, {NodeID: "nB", Weight: 1}, {NodeID: "nC", Weight: 1},
	})
	keys := []string{"alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta"}
	moves := Diff(r1, r2, keys)
	for _, m := range moves {
		if m.ToNode != "nC" {
			t.Fatalf("adding nC can only move keys onto nC, got %+v", m)
		}
		// 独立复算, 防止 Diff 依赖内部状态
		if r1.Owner(m.KeyHash) != m.FromNode || r2.Owner(m.KeyHash) != "nC" {
			t.Fatalf("move %+v inconsistent with rings", m)
		}
	}
	// 不变的键一定不出现在计划中
	changed := map[string]bool{}
	for _, m := range moves {
		changed[m.Key] = true
	}
	for _, k := range keys {
		same := r1.OwnerOfKey(k) == r2.OwnerOfKey(k)
		if same && changed[k] {
			t.Fatalf("key %q reported moved but owner unchanged", k)
		}
		if !same && !changed[k] {
			t.Fatalf("key %q owner changed but not in plan", k)
		}
	}
}

// TestEmptyRing 全零权重无法成环, 构造必须失败。
func TestEmptyRing(t *testing.T) {
	if _, err := New(1, []NodeWeight{{NodeID: "nA", Weight: 0}}); err != ErrEmptyRing {
		t.Fatalf("want ErrEmptyRing, got %v", err)
	}
}
