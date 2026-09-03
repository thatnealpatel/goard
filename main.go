// Copyright 2025 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package main implements a program that pulls
// an opinionated subset of modules from the Go
// Module Mirror.
package main

import (
	"archive/zip"
	"bufio"
	"bytes"
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/mod/modfile"
	"golang.org/x/mod/module"
	"golang.org/x/mod/semver"
)

var (
	httpClient = &http.Client{
		Timeout: 60 * time.Minute,
		Transport: &http.Transport{
			MaxConnsPerHost:     2048,
			MaxIdleConns:        2048,
			MaxIdleConnsPerHost: 2048,
			ForceAttemptHTTP2:   true,
		},
	}

	dst       string
	tmp       string
	indexFile string   // feed cursor, a small JSON object
	logFile   string   // module entries, append-only JSON lines
	largeLog  *os.File // large.jsonl: kept modules over largeThreshold
)

// largeThreshold is the uncompressed
// size of a module's zip above which it
// is noted in large.jsonl for review.
const largeThreshold = 128 << 20

// inMemoryZipMax is the largest zip
// buffered in memory rather than streamed
// to a temp file. With 256 workers it
// bounds download buffers at 4 GiB.
const inMemoryZipMax = 16 << 20

type largeRecord struct {
	Path, Version string
	Size          uint64 // uncompressed bytes of every file in the zip
	Written       int64  // bytes extracted
}

func main() {
	var (
		profile   = flag.Bool("pprof", false, "enable profiling endpoint.")
		indexOnly = flag.Bool("index", false, "update index only")
	)
	flag.Parse()

	log.SetFlags(log.Lshortfile | log.Flags())

	home, err := os.UserHomeDir()
	if err != nil {
		log.Fatal(err)
	}
	root := filepath.Join(home, ".goard")
	dst = filepath.Join(root, "modules")
	tmp = filepath.Join(root, "tmp")
	indexFile = filepath.Join(root, "index.json")
	logFile = filepath.Join(root, "fetch.jsonl")

	// Anything left in tmp is from an interrupted run and was never recorded in
	// the index.
	if err := errors.Join(os.MkdirAll(dst, 0o755), os.RemoveAll(tmp), os.MkdirAll(tmp, 0o755)); err != nil {
		log.Fatal(err)
	}
	largeLog, err = os.OpenFile(filepath.Join(root, "large.jsonl"), os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o644)
	if err != nil {
		log.Fatal(err)
	}
	defer largeLog.Close()

	if *profile {
		go func() {
			runtime.SetBlockProfileRate(1) // aggresive
			http.ListenAndServe(":6060", nil)
		}()
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	log.Println("using:", root)

	// Fetch the index from disk, or create
	// a new index from scratch.
	index := NewIndex(ctx)
	defer func() {
		if err := index.Close(); err != nil {
			log.Println("failed to save index:", err)
		}
	}()

	// Incrementally update the index in-place.
	if err = index.Update(ctx); err != nil {
		log.Panicf("failed to update index: %v", err)
	}

	if *indexOnly {
		return
	}

	var (
		bar                = newProgress("Fetching modules...", int64(len(index.Mods)))
		alreadyExists      int64
		alreadyRejected    int64
		incrementalUpdates = make(map[string]string)
	)
	for path, e := range index.Mods {
		version := e.Latest()
		switch version {
		case e.OnDisk:
			alreadyExists++
		case e.Rejected:
			alreadyRejected++
		default:
			incrementalUpdates[path] = version
			continue
		}
		bar.Increment()
	}

	var (
		wg  sync.WaitGroup
		sem = make(chan struct{}, 256)
	)
	for path, version := range incrementalUpdates {
		if err := ctx.Err(); err != nil { // slower than select+case, but ok
			bar.Finish()
			log.Println(path, version, err)
			break
		}

		sem <- struct{}{}
		wg.Go(func() {
			defer func() { <-sem }()
			defer bar.Increment()

			if strings.Contains(path, "/vendor/") ||
				strings.Contains(path, "/kubernetes/staging/") {
				vendor.Add(1)
				return
			}

			// TODO(nealpatel): Investigate what makes these spam.
			if strings.HasPrefix(path, "github.com/bbiswy/") ||
				strings.HasPrefix(path, "github.com/wMc27rFqQaH7tQxv3/") {
				spam.Add(1)
				return
			}

			escapedPath, err := module.EscapePath(path)
			if err != nil {
				invalidName.Add(1)
				index.Reject(path, version)
				return
			}
			modDir := escapedPath + "@" + version

			modBytes, err := fetchMod(ctx, path, version)
			switch err {
			case nil:
			case errorInvalidName:
				invalidName.Add(1)
				index.Reject(path, version)
				return
			case errorGone:
				gone.Add(1)
				return
			default:
				unknown.Add(1)
				return
			}

			mod, err := modfile.ParseLax(path+"@"+version, modBytes, nil)
			switch {
			case err != nil:
				invalidGoMod.Add(1)
				index.Reject(path, version)
				return
			case mod == nil || mod.Module == nil:
				// Without this invariant, panic
				// at runtime happens: e.g.
				// github.com/Maka8ka/Faygo/client@v0.0.0-20220420085059-439b6b39f779
				// e.g.
				// github.com/maka8ka/faygo/client@v0.0.0-20220420085059-439b6b39f779
				nilModOrModule.Add(1)
				index.Reject(path, version)
				return
			case mod.Module.Mod.Path != path:
				mismatchedGoMod.Add(1)
				index.Reject(path, version)
				return
			default:
			}

			tail, err := fetchZipTail(ctx, path, version)
			switch err {
			case nil:
			case errorGone:
				gone.Add(1)
				return
			default:
				unknown.Add(1)
				return
			}
			size := tail.size
			tailBytes.Add(int64(len(tail.data)))

			// Prefilter on the central directory:
			// skip the full download unless the
			// zip contains a go.mod and at least
			// one .go file that would be kept.
			// Zips whose central directory does
			// not fit in the tail are downloaded
			// unconditionally and filtered below.
			if !tail.whole {
				switch hasGoMod, hasGoSrc, ok := scanZipTail(tail); {
				case !ok:
					prefilterFallback.Add(1)
				case !hasGoMod, !hasGoSrc:
					prefilterSkipped.Add(1)
					prefilterSaved.Add(size - int64(len(tail.data)))
					if !hasGoMod {
						noGoMod.Add(1)
					} else {
						noGoCode.Add(1)
					}
					index.Reject(path, version)
					return
				}
			}

			if size > 0 && strings.HasPrefix(tail.url, "https://storage.googleapis.com/") {
				gcsBytes.Add(size)
			}

			// Prevent an unlucky cluster of large
			// modules from potentially causing
			// an OOM while not slowing down the
			// common path.
			var (
				z          *zip.Reader
				zipCleanup = func() {}
			)
			defer func() { zipCleanup() }()
			switch {
			case tail.whole:
				prefilterWhole.Add(1)
				z, err = zip.NewReader(bytes.NewReader(tail.data), size)
				if err != nil {
					bar.Error(path, " ", version, " ", err)
					return
				}
			case size > inMemoryZipMax || size < 0:
				f, err := fetchZipToFile(ctx, path, version)
				switch err {
				case nil:
				case errorGone:
					gone.Add(1)
					return
				default:
					unknown.Add(1)
					return
				}
				zipCleanup = func() { f.Close(); os.Remove(f.Name()) }
				fi, err := f.Stat()
				if err != nil {
					bar.Error(path, " ", version, " ", err)
					return
				}
				z, err = zip.NewReader(f, fi.Size())
				if err != nil {
					bar.Error(path, " ", version, " ", err)
					return
				}
			default:
				zipBytes, err := fetchZip(ctx, path, version)
				switch err {
				case nil:
				case errorGone:
					gone.Add(1)
					return
				default:
					unknown.Add(1)
					return
				}
				z, err = zip.NewReader(bytes.NewReader(zipBytes), int64(len(zipBytes)))
				if err != nil {
					bar.Error(path, " ", version, " ", err)
					return
				}
			}

			if size > 0 {
				allBytes.Add(size)
			}

			var hasGoMod, hasGoSrc bool
			var modSize uint64

			for _, f := range z.File {
				modSize += f.UncompressedSize64
				if isGoSrc(f.Name) {
					hasGoSrc = true
				}
				if strings.HasSuffix(f.Name, "/go.mod") {
					hasGoMod = true
				}
			}

			switch {
			case !hasGoMod:
				noGoMod.Add(1)
				index.Reject(path, version)
				return
			case !hasGoSrc:
				noGoCode.Add(1)
				nonGoSize.Add(modSize)
				index.Reject(path, version)
				return
			}

			good.Add(1)

			var (
				extracted bool
				written   int64
				zipPrefix = path + "@" + version + "/"
				buf       = make([]byte, 32*1024)
			)
			for _, f := range z.File {
				if ignoreFile(f.Name) {
					continue
				}

				rel := strings.TrimPrefix(f.Name, zipPrefix)
				outPath := filepath.Join(tmp, modDir, rel)
				if err := os.MkdirAll(filepath.Dir(outPath), 0o755); err != nil {
					bar.Error(path, " ", version, " ", err)
					return
				}

				src, err := z.Open(f.Name)
				if err != nil {
					bar.Error(path, " ", version, " ", err)
					return
				}

				out, err := os.Create(outPath)
				if err != nil {
					src.Close()
					bar.Error(path, " ", version, " ", err)
					return
				}

				n, err := io.CopyBuffer(struct{ io.Writer }{out}, src, buf)
				src.Close()
				out.Close()
				if err != nil {
					bar.Error(path, " ", version, " ", err)
					return
				}

				if strings.HasSuffix(f.Name, ".go") {
					onlyGoSrcFiles.Add(1)
					onlyGoSrcFilesSize.Add(f.UncompressedSize64)
				}
				extracted = true
				written += n
				goFiles.Add(1)
				goBytes.Add(n)
			}

			if !extracted {
				return
			}

			if err := os.MkdirAll(filepath.Join(dst, filepath.Dir(modDir)), 0o755); err != nil {
				bar.Error(path, " ", version, " ", err)
				return
			}
			// The index is canonical: a directory it does not know about is
			// a leftover from an interrupted run, not a cached module.
			if err := os.RemoveAll(filepath.Join(dst, modDir)); err != nil {
				bar.Error(path, " ", version, " ", err)
				return
			}
			if err := os.Rename(filepath.Join(tmp, modDir), filepath.Join(dst, modDir)); err != nil {
				bar.Error(path, " ", version, " ", err)
				return
			}
			if err := index.Record(path, version, written); err != nil {
				bar.Error(path, " ", version, " ", err)
				return
			}
			if modSize > largeThreshold {
				large.Add(1)
				b, _ := json.Marshal(largeRecord{Path: path, Version: version, Size: modSize, Written: written})
				if _, err := largeLog.Write(append(b, '\n')); err != nil {
					bar.Error(path, " ", version, " ", err)
				}
			}
		})
	}

	wg.Wait()
	bar.Finish()

	fmt.Fprintf(os.Stderr, "\n")
	fmt.Fprintf(os.Stderr, "Unique modules:       % 9d\n", len(index.Mods))
	fmt.Fprintf(os.Stderr, "Already cached:       % 9d\n", alreadyExists)
	fmt.Fprintf(os.Stderr, "Already rejected:     % 9d\n", alreadyRejected)
	fmt.Fprintf(os.Stderr, "Vendor paths:         % 9d\n", vendor.Load())
	fmt.Fprintf(os.Stderr, "Spam:                 % 9d\n", spam.Load())
	fmt.Fprintf(os.Stderr, "Invalid names:        % 9d\n", invalidName.Load())
	fmt.Fprintf(os.Stderr, "Gone:                 % 9d\n", gone.Load())
	fmt.Fprintf(os.Stderr, "Nil:                  % 9d\n", nilModOrModule.Load())
	fmt.Fprintf(os.Stderr, "Unknown:              % 9d\n", unknown.Load())
	fmt.Fprintf(os.Stderr, "Invalid go.mod:       % 9d\n", invalidGoMod.Load())
	fmt.Fprintf(os.Stderr, "Mismatching go.mod:   % 9d\n", mismatchedGoMod.Load())
	fmt.Fprintf(os.Stderr, "No go.mod file:       % 9d\n", noGoMod.Load())
	nonGoGiB := float64(nonGoSize.Load()) / float64(1<<30)
	fmt.Fprintf(os.Stderr, "No .go files:         % 9d (%.1f GiB downloaded)\n", noGoCode.Load(), nonGoGiB)
	fmt.Fprintf(os.Stderr, "Large (>%dM):         % 9d\n", largeThreshold>>20, large.Load())
	savedGiB := float64(prefilterSaved.Load()) / float64(1<<30)
	fmt.Fprintf(os.Stderr, "Prefiltered:          % 9d (%.1f GiB not downloaded)\n", prefilterSkipped.Load(), savedGiB)
	fmt.Fprintf(os.Stderr, "                      ---------\n")
	fmt.Fprintf(os.Stderr, "Valid:                % 9d\n", good.Load())
	fmt.Fprintf(os.Stderr, "Pruned:               % 9d\n", index.pruned.Load())
	fmt.Fprintf(os.Stderr, "\n")
	fmt.Fprintf(os.Stderr, "Used index that was updated at %s.\n", index.UpdatedAt.Format("2006-01-02 15:04:05 MST"))
	fmt.Fprintf(os.Stderr, "Downloaded %d bytes (%d from GCS).\n", allBytes.Load(), gcsBytes.Load())
	fmt.Fprintf(os.Stderr, "Tail probes fetched %d bytes (%d whole zips, %d directory fallbacks).\n", tailBytes.Load(), prefilterWhole.Load(), prefilterFallback.Load())
	fmt.Fprintf(os.Stderr, "Wrote %d Go-related files (%d bytes).\n", goFiles.Load(), goBytes.Load())
	fmt.Fprintf(os.Stderr, "Wrote %d .go source files (%d bytes).\n", onlyGoSrcFiles.Load(), onlyGoSrcFilesSize.Load())
	fmt.Fprintf(os.Stderr, "Corpus on disk: %d bytes.\n", index.Total())
}

var (
	gone            atomic.Int64
	unknown         atomic.Int64
	nilModOrModule  atomic.Int64
	invalidName     atomic.Int64
	vendor          atomic.Int64
	spam            atomic.Int64
	mismatchedGoMod atomic.Int64
	invalidGoMod    atomic.Int64
	noGoCode        atomic.Int64
	noGoMod         atomic.Int64
	large           atomic.Int64
	gcsBytes        atomic.Int64
	good            atomic.Int64
	goBytes         atomic.Int64
	allBytes        atomic.Int64
	goFiles         atomic.Int64
	nonGoSize       atomic.Uint64

	// Since goBytes and goFiles count all
	// the extensions in ignoreFile it's not
	// a true representation.
	onlyGoSrcFiles     atomic.Int64
	onlyGoSrcFilesSize atomic.Uint64

	// Central-directory prefilter instrumentation.
	prefilterSkipped  atomic.Int64 // zips not downloaded thanks to the rule
	prefilterSaved    atomic.Int64 // compressed bytes not downloaded
	prefilterWhole    atomic.Int64 // zips served entirely by the tail fetch
	prefilterFallback atomic.Int64 // central directory exceeded the tail
	tailBytes         atomic.Int64 // bytes fetched by tail probes
)

type progress struct {
	prefix string
	total  int64
	done   atomic.Int64
	errs   atomic.Int64
	start  time.Time

	mu      sync.Mutex
	lastErr string
}

func newProgress(prefix string, total int64) *progress {
	return &progress{prefix: prefix, total: total, start: time.Now()}
}

func (p *progress) Increment() { p.render(p.done.Add(1)) }

func (p *progress) Error(args ...any) {
	msg := fmt.Sprint(args...)
	if i := strings.LastIndexByte(msg, ':'); i != -1 {
		msg = strings.TrimSpace(msg[i+1:])
	}
	p.mu.Lock()
	p.lastErr = msg
	p.mu.Unlock()
	p.errs.Add(1)
}

func (p *progress) render(done int64) {
	elapsed := time.Since(p.start).Truncate(time.Second)
	p.mu.Lock()
	lastErr := p.lastErr
	p.mu.Unlock()
	var counter string
	if p.total > 0 {
		counter = fmt.Sprintf("%d/%d", done, p.total)
	} else {
		counter = strconv.FormatInt(done, 10)
	}
	if errs := p.errs.Load(); errs > 0 {
		fmt.Fprintf(os.Stderr, "\r\x1b[2K%s %s %s [%d errors] %s", p.prefix, counter, elapsed, errs, lastErr)
	} else {
		fmt.Fprintf(os.Stderr, "\r\x1b[2K%s %s %s", p.prefix, counter, elapsed)
	}
}

func (p *progress) Finish() {
	p.render(p.done.Load())
	fmt.Fprint(os.Stderr, "\n")
}

func NewIndex(ctx context.Context) *Index {
	var i Index

	i.Mods = make(map[string]Entry)

	// The cursor is only advanced after the
	// log holds everything the feed reported
	// up to it, so a missing or stale cursor
	// at worst replays feed entries, which
	// is idempotent.
	var m meta
	switch b, err := os.ReadFile(indexFile); {
	case errors.Is(err, os.ErrNotExist):
		log.Println("building new INDEX (this may take a while)")
	case err != nil:
		log.Fatalf("cannot read %q: %v", indexFile, err)
	default:
		if err := json.Unmarshal(b, &m, json.RejectUnknownMembers(true)); err != nil {
			log.Fatalf("cannot decode %q: %v", indexFile, err)
		}
	}

	f, err := os.OpenFile(logFile, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0o644)
	if err != nil {
		log.Fatalf("cannot open %q: %v", logFile, err)
	}
	if err := i.load(f); err != nil {
		log.Fatalf("cannot replay %q: %v", logFile, err)
	}
	i.logF = f

	// Set last such that duplicates since
	// last pull may occur; however, we will
	// not miss any new entries.
	i.last = m.UpdatedAt
	i.UpdatedAt = time.Now()

	// Pre-seed [Index] with the next page to
	// incrementally update the cache.
	if err := i.nextPage(ctx); err != nil {
		log.Fatalf("cannot get first page: %v", err)
	}

	return &i
}

// Entry is everything the index knows
// about one module path.
//
// Release, Prerelease, and Pseudo hold
// the highest version seen in each class.
// Latest picks among them the way the go
// command resolves @latest. +incompatible
// versions are never recorded: they have
// no go.mod by definition and would
// always be rejected.
type Entry struct {
	Release    string `json:",omitzero"`
	Prerelease string `json:",omitzero"`
	Pseudo     string `json:",omitzero"`

	OnDisk   string `json:",omitzero"` // version extracted under dst, if any
	Rejected string `json:",omitzero"` // version evaluated and skipped; retried only if Latest moves
	Bytes    int64  `json:",omitzero"` // bytes written for OnDisk
}

// record is one line of the log: an Entry
// keyed by Path. A path may appear many
// times; the last line wins on replay.
type record struct {
	Path string
	Entry
}

// meta is the contents of indexFile: how
// far into the feed the log is known to
// be complete.
type meta struct {
	UpdatedAt time.Time
}

// Latest returns the version @latest
// resolves to, or "" if the module has
// no eligible version.
func (e Entry) Latest() string {
	switch {
	case e.Release != "":
		return e.Release
	case e.Prerelease != "":
		return e.Prerelease
	default:
		return e.Pseudo
	}
}

// observe folds a version from the index
// feed into the entry and reports whether
// Latest changed.
func (e *Entry) observe(v string) bool {
	if semver.Build(v) == "+incompatible" {
		return false
	}
	slot := &e.Release
	switch {
	case module.IsPseudoVersion(v):
		slot = &e.Pseudo
	case semver.Prerelease(v) != "":
		slot = &e.Prerelease
	}
	if semver.Compare(v, *slot) <= 0 {
		return false
	}
	before := e.Latest()
	*slot = v
	return e.Latest() != before
}

type Index struct {
	mu   sync.Mutex
	Mods map[string]Entry

	UpdatedAt time.Time

	pruned atomic.Int64
	last   time.Time
	d      *jsontext.Decoder
	body   io.ReadCloser

	logF *os.File // fetch.jsonl, opened O_APPEND
	err  error    // first append failure, reported by Close
}

// load replays a log into the map, last
// line per path winning.
func (i *Index) load(r io.Reader) error {
	dec := jsontext.NewDecoder(bufio.NewReaderSize(r, 1<<20))
	for {
		var rec record
		err := json.UnmarshalDecode(dec, &rec, json.RejectUnknownMembers(true))
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		i.Mods[rec.Path] = rec.Entry
	}
}

// append writes the current entry for
// path to the log in one write call.
// Callers hold mu.
func (i *Index) append(path string) {
	if i.err != nil {
		return
	}
	b, err := json.Marshal(record{Path: path, Entry: i.Mods[path]})
	if err == nil {
		_, err = i.logF.Write(append(b, '\n'))
	}
	i.err = err
}

// Record marks version as extracted under dst with n bytes
// written, pruning any previously extracted version of the
// module.
func (i *Index) Record(path, version string, n int64) error {
	i.mu.Lock()
	defer i.mu.Unlock()
	e := i.Mods[path]
	if e.OnDisk != "" && e.OnDisk != version {
		ep, err := module.EscapePath(path)
		if err != nil {
			return err
		}
		if err := os.RemoveAll(filepath.Join(dst, ep+"@"+e.OnDisk)); err != nil {
			return err
		}
		i.pruned.Add(1)
	}
	e.OnDisk, e.Bytes = version, n
	i.Mods[path] = e
	i.append(path)
	return i.err
}

// Reject marks version as evaluated and not
// worth extracting. It is not retried until
// the module's Latest moves past it.
func (i *Index) Reject(path, version string) {
	i.mu.Lock()
	defer i.mu.Unlock()
	e := i.Mods[path]
	e.Rejected = version
	i.Mods[path] = e
	i.append(path)
}

// Total returns the bytes written for every module on disk.
func (i *Index) Total() (n int64) {
	i.mu.Lock()
	defer i.mu.Unlock()
	for _, e := range i.Mods {
		n += e.Bytes
	}
	return n
}

func (i *Index) Update(ctx context.Context) error {
	bar := newProgress("Updating index...", 0)

	var newMod, changedMod int
	for {
		v, err := i.next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		bar.Increment()

		e, ok := i.Mods[v.Path]
		if !e.observe(v.Version) {
			continue
		}
		i.Mods[v.Path] = e
		if ok {
			changedMod++
		} else {
			newMod++
		}
	}

	bar.Finish()
	log.Printf("index: %d new modules, %d new versions", newMod, changedMod)

	// Feed results are not logged as they
	// arrive, so make the log complete
	// before advancing the cursor past them.
	return i.checkpoint()
}

// checkpoint compacts the log to the
// current map, then records the cursor.
// In that order: a crash between the
// two replays feed entries, which is
// harmless, whereas the reverse would
// skip them.
func (i *Index) checkpoint() error {
	if err := i.compact(); err != nil {
		return err
	}
	b, err := json.Marshal(meta{UpdatedAt: i.UpdatedAt})
	if err != nil {
		return err
	}
	p := filepath.Join(tmp, filepath.Base(indexFile))
	if err := os.WriteFile(p, append(b, '\n'), 0o644); err != nil {
		return err
	}
	return os.Rename(p, indexFile)
}

// compact rewrites the log as one line
// per path and reopens it for appending.
// Only called when no workers are
// running.
func (i *Index) compact() error {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.err != nil {
		return i.err
	}
	p := filepath.Join(tmp, filepath.Base(logFile))
	f, err := os.Create(p)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(f)
	enc := jsontext.NewEncoder(w)
	for path, e := range i.Mods {
		if err := json.MarshalEncode(enc, record{Path: path, Entry: e}); err != nil {
			f.Close()
			return err
		}
	}
	if err := w.Flush(); err != nil {
		f.Close()
		return err
	}
	if err := os.Rename(p, logFile); err != nil {
		f.Close()
		return err
	}
	// f is now logFile and positioned at its
	// end, so it simply becomes the handle
	// further appends go through.
	i.logF.Close()
	i.logF = f
	return nil
}

func (i *Index) Close() error {
	if i.body != nil {
		i.body.Close()
		i.body = nil
	}
	err := i.checkpoint()
	i.logF.Close()
	return err
}

func (i *Index) next(ctx context.Context) (*Version, error) {
	v := &Version{}
	err := json.UnmarshalDecode(i.d, v)
	if err == io.EOF {
		if err := i.nextPage(ctx); err != nil {
			return nil, err
		}
		err = json.UnmarshalDecode(i.d, v)
	}
	if err != nil {
		return nil, err
	}
	i.last = v.Timestamp
	return v, nil
}

func (i *Index) nextPage(ctx context.Context) error {
	if i.body != nil {
		i.body.Close()
		i.body = nil
	}
	url := "https://index.golang.org/index?since=" + i.last.Add(1).Format(time.RFC3339Nano)
	res, err := httpClient.Do(newRequestWithContext(ctx, "GET", url))
	if err != nil {
		return err
	}
	if res.StatusCode != http.StatusOK {
		res.Body.Close()
		return fmt.Errorf("GET %q: %v", url, res.Status)
	}
	i.body = res.Body
	i.d = jsontext.NewDecoder(res.Body)
	return nil
}

func newRequestWithContext(ctx context.Context, method, url string) *http.Request {
	req, err := http.NewRequestWithContext(ctx, method, url, nil)
	if err != nil {
		panic(err)
	}
	return req
}

type Version struct {
	Path, Version string
	Timestamp     time.Time
}

var errorInvalidName = errors.New("invalid name")

func proxyURL(path, version, suffix string) (string, error) {
	p, err := module.EscapePath(path)
	if err != nil {
		return "", errorInvalidName
	}
	v, err := module.EscapeVersion(version)
	if err != nil {
		return "", errorInvalidName
	}
	return "https://proxy.golang.org/cached-only/" + p + "/@v/" + v + suffix, nil
}

var errorGone = errors.New("410 Gone")

func fetchMod(ctx context.Context, path, version string) ([]byte, error) {
	url, err := proxyURL(path, version, ".mod")
	if err != nil {
		return nil, err
	}
	res, err := httpClient.Do(newRequestWithContext(ctx, "GET", url))
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode == http.StatusGone || res.StatusCode == http.StatusNotFound {
		return nil, errorGone
	}
	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET %q: %v", url, res.Status)
	}
	return io.ReadAll(res.Body)
}

// tailFetchSize is how much of the end of
// a zip is fetched with a Range request.
// It must comfortably exceed the 64 KiB
// maximum EOCD comment so archive/zip can
// always locate the directory, and covers
// the full central directory for all but
// many-thousand-file modules.
const tailFetchSize = 256 << 10

type zipTail struct {
	data  []byte // last len(data) bytes of the zip, or the whole zip
	size  int64  // total zip size (-1 if unknown)
	url   string // final URL after redirects
	whole bool   // data is the complete zip
}

// fetchZipTail requests the last tailFetchSize bytes of the zip. For zips no
// larger than that the response is the entire zip; otherwise the tail contains
// the central directory for prefiltering.
func fetchZipTail(ctx context.Context, path, version string) (*zipTail, error) {
	url, err := proxyURL(path, version, ".zip")
	if err != nil {
		return nil, err
	}
	req := newRequestWithContext(ctx, "GET", url)
	req.Header.Set("Range", fmt.Sprintf("bytes=-%d", tailFetchSize))
	res, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	switch res.StatusCode {
	case http.StatusGone, http.StatusNotFound:
		return nil, errorGone

	case http.StatusPartialContent:
		cr := res.Header.Get("Content-Range") // "bytes <start>-<end>/<total>"
		slash := strings.LastIndexByte(cr, '/')
		if slash == -1 {
			return nil, fmt.Errorf("GET %q: unparseable Content-Range %q", url, cr)
		}
		total, err := strconv.ParseInt(cr[slash+1:], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("GET %q: unparseable Content-Range %q", url, cr)
		}
		data, err := io.ReadAll(res.Body)
		if err != nil {
			return nil, err
		}
		return &zipTail{data: data, size: total, url: res.Request.URL.String(), whole: total <= int64(len(data))}, nil

	case http.StatusOK:
		// The server ignored the Range; the body is the whole zip.
		if res.ContentLength >= 0 && res.ContentLength <= inMemoryZipMax {
			data, err := io.ReadAll(res.Body)
			if err != nil {
				return nil, err
			}
			return &zipTail{data: data, size: int64(len(data)), url: res.Request.URL.String(), whole: true}, nil
		}
		// Too large (or unknown size) to buffer: hand back only the metadata and let
		// the caller refetch to a file.
		return &zipTail{size: res.ContentLength, url: res.Request.URL.String()}, nil

	default:
		return nil, fmt.Errorf("GET %q: %v", url, res.Status)
	}
}

// tailReaderAt exposes the tail of a
// zip as a ReaderAt over the full file,
// erroring on reads before the tail
// so archive/zip fails cleanly when
// the central directory is not fully
// contained in it.
type tailReaderAt struct {
	data []byte
	off  int64 // offset of data[0] within the full file
}

func (r *tailReaderAt) ReadAt(p []byte, off int64) (int, error) {
	pos := off - r.off
	if pos < 0 {
		return 0, errors.New("read before tail")
	}
	if pos >= int64(len(r.data)) {
		return 0, io.EOF
	}
	n := copy(p, r.data[pos:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

// scanZipTail reads the central directory out of a partial
// zip. ok is false when the directory is not fully contained
// in the tail (or the tail is absent), in which case the zip
// must be downloaded to decide.
func scanZipTail(t *zipTail) (hasGoMod, hasGoSrc, ok bool) {
	if t.data == nil {
		return false, false, false
	}
	z, err := zip.NewReader(&tailReaderAt{data: t.data, off: t.size - int64(len(t.data))}, t.size)
	if err != nil {
		return false, false, false
	}
	for _, f := range z.File {
		if strings.HasSuffix(f.Name, "/go.mod") {
			hasGoMod = true
		}
		if isGoSrc(f.Name) {
			hasGoSrc = true
		}
	}
	return hasGoMod, hasGoSrc, true
}

// isGoSrc reports whether name is a .go
// file that extraction would keep.
func isGoSrc(name string) bool {
	return strings.HasSuffix(name, ".go") && !ignoreFile(name)
}

func fetchZipToFile(ctx context.Context, path, version string) (*os.File, error) {
	url, err := proxyURL(path, version, ".zip")
	if err != nil {
		return nil, err
	}
	res, err := httpClient.Do(newRequestWithContext(ctx, "GET", url))
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode == http.StatusGone || res.StatusCode == http.StatusNotFound {
		return nil, errorGone
	}
	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET %q: %v", url, res.Status)
	}
	f, err := os.CreateTemp(tmp, "*.zip")
	if err != nil {
		return nil, err
	}
	if _, err := io.Copy(f, res.Body); err != nil {
		f.Close()
		os.Remove(f.Name())
		return nil, err
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		f.Close()
		os.Remove(f.Name())
		return nil, err
	}
	return f, nil
}

func fetchZip(ctx context.Context, path, version string) ([]byte, error) {
	url, err := proxyURL(path, version, ".zip")
	if err != nil {
		return nil, err
	}
	res, err := httpClient.Do(newRequestWithContext(ctx, "GET", url))
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode == http.StatusGone || res.StatusCode == http.StatusNotFound {
		return nil, errorGone
	}
	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET %q: %v", url, res.Status)
	}
	// I experimented with using Range
	// requests to back the zip ReaderAt,
	// but it was extremely slow.
	return io.ReadAll(res.Body)
}

func ignoreFile(name string) bool {
	name = strings.ToLower(name)
	if strings.Contains(name, "/.") {
		return true
	}
	if strings.Contains(name, "/_") {
		return true
	}
	if strings.Contains(name, "/testdata/") {
		return true
	}
	for _, ext := range []string{
		".go", ".s", ".syso",
		".c", ".cc", ".cpp", ".cxx",
		".h", ".hh", ".hpp", ".hxx",
		".f", ".for", ".f90", ".m",
		".swig", ".swigcxx",
	} {
		if strings.HasSuffix(name, ext) {
			return false
		}
	}
	if strings.HasSuffix(name, "/go.mod") {
		return false
	}
	if strings.HasSuffix(name, "/go.sum") {
		return false
	}
	return true
}
