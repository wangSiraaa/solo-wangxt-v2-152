package kvlog

import (
	"os"
	"path/filepath"
	"testing"
)

func TestAppendReplayRoundTrip(t *testing.T) {
	dir := t.TempDir()
	lg, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	recs := []Record{
		{Op: "put", Key: "a", Version: 1, Value: []byte("hello")},
		{Op: "put", Key: "b", Version: 2, Value: []byte{0, 1, 2, 255}},
		{Op: "stage", Mid: "m-2", Key: "a", Version: 1, Value: []byte("hello")},
		{Op: "del", Key: "a", Version: 3},
	}
	for _, r := range recs {
		if err := lg.Append(r); err != nil {
			t.Fatal(err)
		}
	}
	if err := lg.Close(); err != nil {
		t.Fatal(err)
	}

	var got []Record
	if err := Replay(dir, func(r Record) error { got = append(got, r); return nil }); err != nil {
		t.Fatal(err)
	}
	if len(got) != len(recs) {
		t.Fatalf("replayed %d, want %d", len(got), len(recs))
	}
	if string(got[0].Value) != "hello" || got[0].Version != 1 {
		t.Fatalf("rec0 mismatch: %+v", got[0])
	}
	if len(got[1].Value) != 4 || got[1].Value[3] != 255 {
		t.Fatalf("binary value not preserved: %v", got[1].Value)
	}
	if got[2].Mid != "m-2" {
		t.Fatalf("mid not preserved: %+v", got[2])
	}
	if got[3].Op != "del" || got[3].Version != 3 {
		t.Fatalf("del mismatch: %+v", got[3])
	}
}

func TestTornTailIgnored(t *testing.T) {
	dir := t.TempDir()
	lg, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := lg.Append(Record{Op: "put", Key: "ok", Version: 1, Value: []byte("v")}); err != nil {
		t.Fatal(err)
	}
	if err := lg.Close(); err != nil {
		t.Fatal(err)
	}
	// 模拟崩溃时写了一半的尾行(不是合法 JSON)
	f, err := os.OpenFile(filepath.Join(dir, "wal.jsonl"), os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(`{"op":"put","key":"hal`); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	n := 0
	if err := Replay(dir, func(r Record) error { n++; return nil }); err != nil {
		t.Fatalf("torn tail should be ignored, got error: %v", err)
	}
	if n != 1 {
		t.Fatalf("replayed %d records after torn tail, want 1", n)
	}
}

func TestReplayMissingFile(t *testing.T) {
	if err := Replay(t.TempDir(), func(Record) error { return nil }); err != nil {
		t.Fatalf("missing WAL must replay as empty, got %v", err)
	}
}
