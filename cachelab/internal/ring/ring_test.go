package ring

import (
	"fmt"
	"math/rand"
	"testing"

	"cachelab/internal/hash"
)

// 性质测试:对任意键,diff 区间覆盖 ⟺ 属主发生变化。
// 这是"路由变化量 = 实际迁移范围"的数学保证。
func TestDiff_CoversExactlyChangedKeys(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	for trial := 0; trial < 20; trial++ {
		wOld := map[string]int{"n1": 1, "n2": 1, "n3": 1}
		wNew := map[string]int{"n1": 1, "n2": 1, "n3": 1, "n4": 1}
		if trial%2 == 1 {
			wNew["n2"] = 3 // 权重大幅上调
		}
		oldR, newR := Build(wOld), Build(wNew)
		ranges := Diff(oldR, newR)

		for i := 0; i < 2000; i++ {
			key := fmt.Sprintf("key-%d-%d", trial, rng.Int63())
			p := hash.KeyPoint(key)
			changed := oldR.OwnerOfKey(key) != newR.OwnerOfKey(key)
			inAny := false
			for _, r := range ranges {
				if hash.InRange(p, r.Start, r.End) {
					inAny = true
					// 区间端点必须与实际属主一致
					if r.From != oldR.Owner(p) || r.To != newR.Owner(p) {
						t.Fatalf("区间端点属主不符: key=%s range=%+v", key, r)
					}
				}
			}
			if changed != inAny {
				t.Fatalf("key=%s changed=%v inAny=%v", key, changed, inAny)
			}
		}
	}
}

// 权重为零的节点必须完全退出环,其原区间全部迁出。
func TestDiff_WeightZeroEvictsNode(t *testing.T) {
	oldR := Build(map[string]int{"n1": 1, "n2": 1})
	newR := Build(map[string]int{"n1": 1, "n2": 0})
	ranges := Diff(oldR, newR)
	if len(ranges) == 0 {
		t.Fatal("权重归零应产生迁移区间")
	}
	for _, r := range ranges {
		if r.From != "n2" {
			t.Errorf("存在非 n2 迁出的区间: %+v", r)
		}
		if r.To != "n1" {
			t.Errorf("迁入方应为 n1: %+v", r)
		}
	}
	// n2 在新环上不再拥有任何键
	for i := 0; i < 5000; i++ {
		if newR.OwnerOfKey(fmt.Sprintf("k%d", i)) == "n2" {
			t.Fatal("权重为零的节点仍拥有键")
		}
	}
}

// 环构建是确定性的:相同权重表 → 相同环(教学可复现)。
func TestBuild_Deterministic(t *testing.T) {
	w := map[string]int{"a": 1, "b": 2, "c": 3}
	r1, r2 := Build(w), Build(w)
	v1, v2 := r1.VNodes(), r2.VNodes()
	if len(v1) != len(v2) || len(v1) != 6*VNodesPerWeight {
		t.Fatalf("vnode 数不符: %d vs %d", len(v1), len(v2))
	}
	for i := range v1 {
		if v1[i] != v2[i] {
			t.Fatalf("环构建不确定: 位置 %d 不同", i)
		}
	}
}

// 权重越大,占据的哈希空间比例越大(单调性,宽松界)。
func TestFractions_ProportionalToWeight(t *testing.T) {
	r := Build(map[string]int{"small": 1, "large": 4})
	f := r.Fractions()
	if f["large"] <= f["small"] {
		t.Errorf("权重 4 的占比(%.3f)应大于权重 1(%.3f)", f["large"], f["small"])
	}
	sum := 0.0
	for _, v := range f {
		sum += v
	}
	if sum < 0.999 || sum > 1.001 {
		t.Errorf("占比总和应为 1,实际 %f", sum)
	}
}

// 新增节点时,理论迁移比例应接近 1/N(一致性哈希的核心性质)。
func TestChangedFraction_AddNode(t *testing.T) {
	oldR := Build(map[string]int{"n1": 1, "n2": 1, "n3": 1})
	newR := Build(map[string]int{"n1": 1, "n2": 1, "n3": 1, "n4": 1})
	frac := ChangedFraction(Diff(oldR, newR))
	// 期望约 0.25,允许虚拟节点随机性带来的偏差
	if frac < 0.15 || frac > 0.35 {
		t.Errorf("新增第 4 节点的迁移比例 %.3f 偏离 0.25 过多", frac)
	}
}
