package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// openIndex mirrors the file handling in
// NewIndex without the feed.
func openIndex(t *testing.T) *Index {
	t.Helper()
	i := &Index{Mods: make(map[string]Entry)}
	f, err := os.OpenFile(logFile, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if err := i.load(f); err != nil {
		t.Fatal(err)
	}
	i.logF = f
	return i
}

func TestIndexLogRoundTrip(t *testing.T) {
	dir := t.TempDir()
	indexFile = filepath.Join(dir, "index.json")
	logFile = filepath.Join(dir, "fetch.jsonl")
	dst = filepath.Join(dir, "modules")
	tmp = filepath.Join(dir, "tmp")
	if err := os.MkdirAll(tmp, 0o755); err != nil {
		t.Fatal(err)
	}

	// Run 1: entries arrive from the feed,
	// then two are fetched, and one of those
	// is later superseded.
	i := openIndex(t)
	i.UpdatedAt = time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)
	i.Mods["example.com/a"] = Entry{Release: "v1.2.0", Prerelease: "v1.3.0-rc1"}
	i.Mods["example.com/b"] = Entry{Pseudo: "v0.0.0-20250101000000-abcdefabcdef"}
	i.Mods["example.com/c"] = Entry{}
	if err := i.checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := i.Record("example.com/a", "v1.2.0", 4096); err != nil {
		t.Fatal(err)
	}
	i.Reject("example.com/b", "v0.0.0-20250101000000-abcdefabcdef")
	if err := i.Record("example.com/a", "v1.2.1", 5000); err != nil {
		t.Fatal(err)
	}

	// The log now has the compacted entries
	// plus three appends, and a crash here
	// must lose none of them.
	raw, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSuffix(string(raw), "\n"), "\n")
	if len(lines) != 3+3 {
		t.Fatalf("got %d log lines, want 6:\n%s", len(lines), raw)
	}
	for _, l := range lines {
		if !strings.HasPrefix(l, `{"Path":"example.com/`) || strings.Contains(l, `""`) || strings.Contains(l, "UpdatedAt") {
			t.Errorf("bad log line: %s", l)
		}
	}
	i.logF.Close()

	// Run 2: replay without a clean Close.
	j := openIndex(t)
	want := map[string]Entry{
		"example.com/a": {Release: "v1.2.0", Prerelease: "v1.3.0-rc1", OnDisk: "v1.2.1", Bytes: 5000},
		"example.com/b": {Pseudo: "v0.0.0-20250101000000-abcdefabcdef", Rejected: "v0.0.0-20250101000000-abcdefabcdef"},
		"example.com/c": {},
	}
	if len(j.Mods) != len(want) {
		t.Fatalf("got %d entries, want %d", len(j.Mods), len(want))
	}
	for path, e := range want {
		if j.Mods[path] != e {
			t.Errorf("%s: got %+v, want %+v", path, j.Mods[path], e)
		}
	}

	// Cursor was written by checkpoint and is a single object.
	raw, err = os.ReadFile(indexFile)
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != `{"UpdatedAt":"2026-09-03T12:00:00Z"}`+"\n" {
		t.Errorf("bad index.json: %s", raw)
	}

	// Close compacts: one line per path, same contents.
	j.UpdatedAt = i.UpdatedAt
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	raw, err = os.ReadFile(logFile)
	if err != nil {
		t.Fatal(err)
	}
	if n := strings.Count(string(raw), "\n"); n != len(want) {
		t.Errorf("after compact got %d lines, want %d:\n%s", n, len(want), raw)
	}
	k := openIndex(t)
	defer k.logF.Close()
	for path, e := range want {
		if k.Mods[path] != e {
			t.Errorf("after compact %s: got %+v, want %+v", path, k.Mods[path], e)
		}
	}
}

func TestIndexLoadRejectsUnknownFields(t *testing.T) {
	i := &Index{Mods: make(map[string]Entry)}
	err := i.load(strings.NewReader(`{"Path":"example.com/a","Latest":"v1.0.0"}` + "\n"))
	if err == nil {
		t.Fatal("expected an error for an unknown field")
	}
}

func TestEntryLatest(t *testing.T) {
	tests := []struct {
		name string
		feed []string
		want string
	}{
		{"release beats prerelease", []string{"v1.0.0", "v1.1.0-rc1"}, "v1.0.0"},
		{"release beats newer pseudo", []string{"v0.9.0", "v1.0.0-0.20250101000000-abcdefabcdef"}, "v0.9.0"},
		{"prerelease beats pseudo", []string{"v0.0.0-20250101000000-abcdefabcdef", "v0.1.0-beta"}, "v0.1.0-beta"},
		{"pseudo only", []string{"v0.0.0-20240101000000-abcdefabcdef", "v0.0.0-20250101000000-abcdefabcdef"}, "v0.0.0-20250101000000-abcdefabcdef"},
		{"incompatible ignored", []string{"v1.2.0", "v2.0.0+incompatible"}, "v1.2.0"},
		{"incompatible only", []string{"v2.0.0+incompatible"}, ""},
		{"order independent", []string{"v1.1.0-rc1", "v1.0.0"}, "v1.0.0"},
		{"backport after newer", []string{"v1.10.0", "v1.9.1"}, "v1.10.0"},
	}
	for _, tt := range tests {
		var e Entry
		for _, v := range tt.feed {
			e.observe(v)
		}
		if got := e.Latest(); got != tt.want {
			t.Errorf("%s: got %q, want %q", tt.name, got, tt.want)
		}
	}
}

func TestEntryObserveReportsChange(t *testing.T) {
	var e Entry
	if !e.observe("v0.0.0-20250101000000-abcdefabcdef") {
		t.Error("first pseudo-version should change Latest")
	}
	if !e.observe("v0.1.0") {
		t.Error("release should displace pseudo-version")
	}
	if e.observe("v0.2.0-rc1") {
		t.Error("prerelease below a release should not change Latest")
	}
	if e.observe("v0.0.9") {
		t.Error("older release should not change Latest")
	}
	if e.observe("v3.0.0+incompatible") {
		t.Error("+incompatible should never change Latest")
	}
}
