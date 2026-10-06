package store

import "time"

// Node 是集群成员登记信息。
type Node struct {
	ID             string
	Address        string
	Weight         int
	Enabled        bool
	Token          string
	RegisteredRing int64
	CreatedAt      time.Time
}

// RingState 环生命周期状态。
type RingState string

const (
	RingStaging RingState = "staging" // 已建环, 复制中, 不承载正式流量
	RingActive  RingState = "active"  // 当前正式流量
	RingOld     RingState = "old"     // 被替代, 宽限期内可服务在途请求
	RingRetired RingState = "retired" // 已退役, 拒绝解析
)

// RingRecord 环元数据 + 权重快照。
type RingRecord struct {
	Version     int64
	Reason      string
	State       RingState
	Weights     map[string]int
	CreatedAt   time.Time
	ActivatedAt *time.Time
}

// VNodeRow 虚拟节点持久化行。Hash 是 uint64 位模式。
type VNodeRow struct {
	Hash   uint64
	NodeID string
	Index  uint32
	Label  string
}

// KeyRec 键注册表行。
type KeyRec struct {
	Key          string
	Hash         uint64
	CurrentOwner string
	FirstSeen    int64
	Version      uint64
	Deleted      bool
	UpdatedAt    time.Time
}

// 迁移状态。
const (
	MigPlanned     = "planned"
	MigReplicating = "replicating"
	MigReplicated  = "replicated" // 全部条目复制完成, 但尚未切换
	MigCommitted   = "committed"  // 已显式提交, active 已切换
	MigAborted     = "aborted"
	MigFailed      = "failed" // 有条目失败且未恢复(中断恢复后的可见状态)
)

// 迁移条目状态。
const (
	ItemPending     = "pending"
	ItemReplicating = "replicating"
	ItemReplicated  = "replicated"
	ItemFailed      = "failed"
	ItemPromoted    = "promoted"
)

// Migration 迁移单及明细。
type Migration struct {
	ID        string
	FromRing  int64
	ToRing    int64
	State     string
	CreatedAt time.Time
	UpdatedAt time.Time
	Items     []MigrationItem
}

// MigrationItem 单键迁移进度。
type MigrationItem struct {
	Key       string
	KeyHash   uint64
	FromNode  string
	ToNode    string
	State     string
	LastError string
	UpdatedAt time.Time
}

// Counters 汇总计数, 对应 API 返回的 pending/replicated/...。
type Counters struct {
	Total, Pending, Replicated, Failed, Promoted int
}
