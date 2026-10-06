// Package kvlog 是本地 KV 节点的只追加 WAL(JSON lines)。
//
// 节点进程被杀死/重启后, 正式数据与各迁移的 staging 暂存区都从这里恢复,
// 因此"迁移途中失败"(节点宕机或路由器重启)不会丢失已复制字节,
// 路由器可以据此断点续传而不是从头复制。
//
// 记录格式固定为一行一个 JSON 对象, 字段名不允许改动(磁盘兼容):
//
//	{"op":"put","key":"...","version":3,"value_b64":"..."}
//	{"op":"del","key":"...","version":4}
//	{"op":"stage","mid":"m-2","key":"...","version":3,"value_b64":"...","deleted":false}
//	{"op":"promote","mid":"m-2"}
//	{"op":"drop","mid":"m-2"}
package kvlog

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// Record 是一条 WAL 记录。Value 为原始字节, 落盘时用标准 base64。
type Record struct {
	Op      string `json:"op"`
	Mid     string `json:"mid,omitempty"`
	Key     string `json:"key,omitempty"`
	Version int64  `json:"version,omitempty"`
	Value   []byte `json:"-"`
	Deleted bool   `json:"deleted,omitempty"`
}

// wireRecord 是磁盘 JSON 形态(value 走 base64)。
type wireRecord struct {
	Op       string `json:"op"`
	Mid      string `json:"mid,omitempty"`
	Key      string `json:"key,omitempty"`
	Version  int64  `json:"version,omitempty"`
	ValueB64 string `json:"value_b64,omitempty"`
	Deleted  bool   `json:"deleted,omitempty"`
}

// Logger 并发安全的只追加日志。
type Logger struct {
	mu sync.Mutex
	f  *os.File
	w  *bufio.Writer
}

// Open 打开(或创建) dataDir/wal.jsonl。
func Open(dataDir string) (*Logger, error) {
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(dataDir, "wal.jsonl"),
		os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &Logger{f: f, w: bufio.NewWriterSize(f, 4096)}, nil
}

// Append 追加一条记录并 fsync, 保证崩溃后可见。
func (l *Logger) Append(r Record) error {
	wr := wireRecord{Op: r.Op, Mid: r.Mid, Key: r.Key, Version: r.Version, Deleted: r.Deleted}
	if r.Value != nil {
		wr.ValueB64 = base64.StdEncoding.EncodeToString(r.Value)
	}
	line, err := json.Marshal(wr)
	if err != nil {
		return err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, err := l.w.Write(line); err != nil {
		return err
	}
	if err := l.w.WriteByte('\n'); err != nil {
		return err
	}
	if err := l.w.Flush(); err != nil {
		return err
	}
	return l.f.Sync()
}

// Close 关闭日志文件。
func (l *Logger) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.w.Flush(); err != nil {
		return err
	}
	return l.f.Close()
}

// Replay 从头到尾回放所有记录, 每条调用 cb。WAL 损坏(半行/坏 JSON)
// 在最后一行以外出现会报错; 末尾半行(崩溃写了一半)被截断忽略。
func Replay(dataDir string, cb func(Record) error) error {
	f, err := os.Open(filepath.Join(dataDir, "wal.jsonl"))
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()

	r := bufio.NewReader(f)
	for {
		line, readErr := r.ReadBytes('\n')
		if len(line) > 0 {
			// 末尾无换行但 JSON 完整, 同样接受
			var wr wireRecord
			if jerr := json.Unmarshal(line, &wr); jerr != nil {
				if readErr == io.EOF {
					break // 末尾半行: 崩溃残留, 忽略
				}
				if readErr != nil {
					return readErr
				}
				return jerr
			}
			rec := Record{Op: wr.Op, Mid: wr.Mid, Key: wr.Key, Version: wr.Version, Deleted: wr.Deleted}
			if wr.ValueB64 != "" {
				v, derr := base64.StdEncoding.DecodeString(wr.ValueB64)
				if derr != nil {
					return derr
				}
				rec.Value = v
			}
			if err := cb(rec); err != nil {
				return err
			}
		}
		if readErr != nil {
			if readErr == io.EOF {
				break
			}
			return readErr
		}
	}
	return nil
}
