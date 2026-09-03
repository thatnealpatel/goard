package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestIndexSaveLoad(t *testing.T) {
	indexFile = filepath.Join(t.TempDir(), "@INDEX")

	want := &Index{
		Mods: map[string]Entry{
			"example.com/a": {Release: "v1.2.0", Prerelease: "v1.3.0-rc1", OnDisk: "v1.2.0", Bytes: 4096},
			"example.com/b": {Pseudo: "v0.0.0-20250101000000-abcdefabcdef", Rejected: "v0.0.0-20250101000000-abcdefabcdef"},
			"example.com/c": {},
		},
		UpdatedAt: time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC),
	}
	if err := want.Save(); err != nil {
		t.Fatal(err)
	}

	raw, err := os.ReadFile(indexFile)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSuffix(string(raw), "\n"), "\n")
	if len(lines) != 1+len(want.Mods) {
		t.Fatalf("got %d lines, want %d:\n%s", len(lines), 1+len(want.Mods), raw)
	}
	if !strings.HasPrefix(lines[0], `{"UpdatedAt":`) || strings.Contains(lines[0], "Path") {
		t.Errorf("bad header line: %s", lines[0])
	}
	for _, l := range lines[1:] {
		if strings.Contains(l, "UpdatedAt") || strings.Contains(l, `""`) {
			t.Errorf("entry line has header or empty fields: %s", l)
		}
	}

	f, err := os.Open(indexFile)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	got := &Index{Mods: make(map[string]Entry)}
	if err := got.load(f); err != nil {
		t.Fatal(err)
	}
	if !got.UpdatedAt.Equal(want.UpdatedAt) {
		t.Errorf("UpdatedAt: got %v, want %v", got.UpdatedAt, want.UpdatedAt)
	}
	if len(got.Mods) != len(want.Mods) {
		t.Fatalf("got %d entries, want %d", len(got.Mods), len(want.Mods))
	}
	for path, e := range want.Mods {
		if got.Mods[path] != e {
			t.Errorf("%s: got %+v, want %+v", path, got.Mods[path], e)
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
