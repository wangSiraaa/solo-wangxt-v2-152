package hash

import (
	"hash/fnv"
	"testing"
)

// 公开测试向量,来源:FNV 官方参考页
// http://www.isthe.com/chongo/tech/comp/fnv/ (FNV-1a 64-bit)。
// 这些向量把哈希算法"钉死",任何实现偏差都会在这里暴露。
func TestSum64_PublicVectors(t *testing.T) {
	cases := []struct {
		in   string
		want uint64
	}{
		{"", 0xcbf29ce484222325},
		{"a", 0xaf63dc4c8601ec8c},
		{"foobar", 0x85944171f73967e8},
	}
	for _, c := range cases {
		if got := Sum64([]byte(c.in)); got != c.want {
			t.Errorf("Sum64(%q) = %#x, want %#x", c.in, got, c.want)
		}
	}
}

// 与 Go 标准库的同名固定算法实现交叉验证(非语言默认哈希)。
func TestSum64_CrossCheckStdlib(t *testing.T) {
	inputs := []string{"key-0001", "用户键", "\x00\xff binary", "cachelab"}
	for _, in := range inputs {
		h := fnv.New64a()
		h.Write([]byte(in))
		if got, want := Sum64([]byte(in)), h.Sum64(); got != want {
			t.Errorf("Sum64(%q) = %#x, stdlib = %#x", in, got, want)
		}
	}
}

// 虚拟节点编码的字节序固定:同一输入在任何平台上必须得到同一点位。
func TestVnodePoint_DeterministicEncoding(t *testing.T) {
	// "n1#" + big-endian(0) 的字节为 "n1#\x00\x00\x00\x00",
	// 点位 = Mix64(Sum64(该字节序列)),算法与字节序全部显式。
	want := Mix64(Sum64([]byte{'n', '1', '#', 0, 0, 0, 0}))
	if got := VnodePoint("n1", 0); got != want {
		t.Errorf("VnodePoint(n1,0) = %#x, want %#x", got, want)
	}
	// 索引 1 与索引 256 仅末两字节不同,结果必须不同(编码真的包含索引)。
	if VnodePoint("n1", 1) == VnodePoint("n1", 256) {
		t.Error("VnodePoint 未区分不同索引")
	}
}

// 雪崩性质:仅尾部不同的输入(顺序键)必须散开到环各处,
// 否则一致性哈希的虚拟节点会挤在一小段弧上。
func TestMix64_Avalanche(t *testing.T) {
	prev := KeyPoint("key-00000")
	spread := map[uint8]bool{}
	for i := 1; i < 100; i++ {
		p := KeyPoint("key-" + string(rune('0'+i/10)) + string(rune('0'+i%10)) + "00")
		spread[uint8(p>>61)] = true // 环均分 8 段,记录落在哪段
		_ = prev
		prev = p
	}
	if len(spread) < 4 {
		t.Errorf("顺序键的点位未散开,仅覆盖 %d/8 弧段", len(spread))
	}
}

func TestHexRoundTrip(t *testing.T) {
	for _, p := range []uint64{0, 1, 0xdeadbeef, 1<<63 + 7, ^uint64(0)} {
		s := Hex(p)
		if len(s) != 16 {
			t.Fatalf("Hex(%#x) 长度 = %d, 应为 16", p, len(s))
		}
		back, err := ParseHex(s)
		if err != nil || back != p {
			t.Errorf("ParseHex(Hex(%#x)) = %#x, %v", p, back, err)
		}
	}
}

func TestInRange(t *testing.T) {
	// 普通区间 (10, 20]
	if !InRange(15, 10, 20) || InRange(10, 10, 20) || !InRange(20, 10, 20) || InRange(21, 10, 20) {
		t.Error("普通区间边界错误")
	}
	// 回绕区间 (250, 5]:包含 251..255, 0..5
	if !InRange(255, 250, 5) || !InRange(0, 250, 5) || !InRange(5, 250, 5) || InRange(100, 250, 5) {
		t.Error("回绕区间边界错误")
	}
}
