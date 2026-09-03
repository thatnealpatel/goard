// Copyright 2025 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package main implements a program that pulls
// an opinionated subset of modules from the Go
// Module Mirror.
package main

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
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
	indexFile string
)

func main() {
	var (
		profile   = flag.Bool("pprof", false, "enable profiling endpoint.")
		verify    = flag.Bool("verify", false, "recover index from disk before updating")
		indexOnly = flag.Bool("index", false, "update index only")
	)
	flag.Parse()

	log.SetFlags(log.Lshortfile | log.Flags())

	gob.Register(map[string]string{})
	gob.Register(time.Time{})
	gob.Register(Index{})

	home, err := os.UserHomeDir()
	if err != nil {
		log.Fatal(err)
	}
	dst = filepath.Join(home, ".gomodproxy")
	tmp = filepath.Join(dst, "@TEMP")
	indexFile = filepath.Join(dst, "@INDEX")
	if err := errors.Join(os.MkdirAll(dst, 0o755), os.MkdirAll(tmp, 0o755)); err != nil {
		log.Fatal(err)
	}

	if *profile {
		go func() {
			runtime.SetBlockProfileRate(1) // aggresive
			http.ListenAndServe(":6060", nil)
		}()
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	log.Println("using:", dst)

	// Fetch the index from disk, or create
	// a new index from scratch.
	index := NewIndex(ctx)
	defer func() {
		if err := index.Close(); err != nil {
			log.Println("failed to save index:", err)
		}
	}()

	if *verify {
		if err := index.Recover(); err != nil {
			log.Panicf("failed to recover index: %v", err)
		}
	}

	// Incrementally update the index in-place.
	if err = index.Update(ctx); err != nil {
		log.Panicf("failed to update index: %v", err)
	}

	if *indexOnly {
		return
	}

	var (
		bar                = newProgress("Fetching modules...", int64(len(index.Latest)))
		alreadyExists      int64
		incrementalUpdates = make(map[string]string)
	)
	for path, version := range index.Latest {
		if index.OnDisk[path] == version {
			alreadyExists++
			bar.Increment()
			continue
		}
		incrementalUpdates[path] = version
	}

	var (
		wg       sync.WaitGroup
		sem      = make(chan struct{}, 256)
		lastSave atomic.Int64
	)
	lastSave.Store(time.Now().UnixMilli())
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

			// A non-recoverable signal could cause an
			// inconsistent state to occur in which a
			// latest module on disk does not need to
			// be fetched but does need to be updated
			// in the i.OnDisk.
			escapedPath, err := module.EscapePath(path)
			if err != nil {
				bar.Error(path, " ", version, " ", err)
				return
			}
			modDir := escapedPath + "@" + version
			if _, err := os.Stat(filepath.Join(dst, modDir)); err == nil {
				if err := index.Record(path, version); err != nil {
					bar.Error(path, " ", version, " ", err)
				}
				return
			}

			modBytes, err := fetchMod(ctx, path, version)
			switch err {
			case nil:
			case errorInvalidName:
				invalidName.Add(1)
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
				return
			case mod == nil || mod.Module == nil:
				// Without this invariant, panic
				// at runtime happens: e.g.
				// github.com/Maka8ka/Faygo/client@v0.0.0-20220420085059-439b6b39f779
				// e.g.
				// github.com/maka8ka/faygo/client@v0.0.0-20220420085059-439b6b39f779
				nilModOrModule.Add(1)
				return
			case mod.Module.Mod.Path != path:
				mismatchedGoMod.Add(1)
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
			// skip the full download unless
			// the zip contains a go.mod. Zips
			// whose central directory does not
			// fit in the tail are downloaded
			// unconditionally and filtered below.
			if !tail.whole {
				switch hasGoMod, ok := scanZipTail(tail); {
				case !ok:
					prefilterFallback.Add(1)
				case !hasGoMod:
					prefilterSkipped.Add(1)
					prefilterSaved.Add(size - int64(len(tail.data)))
					noGoMod.Add(1)
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
			case size > 100<<20 || size < 0:
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

			var hasGoMod, hasGoFiles bool
			var modSize uint64

			for _, f := range z.File {
				modSize += f.UncompressedSize64
				if strings.HasSuffix(f.Name, ".go") {
					hasGoFiles = true
				}
				if strings.HasSuffix(f.Name, "/go.mod") {
					hasGoMod = true
				}
			}

			if !hasGoFiles {
				noGoCode.Add(1)
				nonGoSize.Add(modSize)
			}

			if !hasGoMod {
				noGoMod.Add(1)
				return
			}

			good.Add(1)

			var (
				extracted bool
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
			if err := os.Rename(filepath.Join(tmp, modDir), filepath.Join(dst, modDir)); err != nil {
				bar.Error(path, " ", version, " ", err)
				return
			}
			if err := index.Record(path, version); err != nil {
				bar.Error(path, " ", version, " ", err)
				return
			}

			if ts := lastSave.Load(); time.Since(time.UnixMilli(ts)) > 30*time.Second {
				if lastSave.CompareAndSwap(ts, time.Now().UnixMilli()) {
					if err := index.Save(); err != nil {
						bar.Error("checkpoint: ", err)
					}
				}
			}
		})
	}

	wg.Wait()
	bar.Finish()

	fmt.Fprintf(os.Stderr, "\n")
	fmt.Fprintf(os.Stderr, "Unique modules:       % 9d\n", len(index.Latest))
	fmt.Fprintf(os.Stderr, "Already cached:       % 9d\n", alreadyExists)
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
	fmt.Fprintf(os.Stderr, "No .go files:         % 9d (%.1f GiB)\n", noGoCode.Load(), nonGoGiB)
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

	_, err := os.Stat(indexFile)
	switch {
	case errors.Is(err, os.ErrNotExist):
		log.Println("building new INDEX (this may take a while)")
		i = Index{
			Latest: make(map[string]string),
			OnDisk: make(map[string]string),
		}

	case err != nil:
		log.Fatalf("cannot stat %q: %v", indexFile, err)

	default:
		file, err := os.ReadFile(indexFile)
		if err != nil {
			log.Fatalf("cannot read %q: %v", indexFile, err)
		}
		if err := gob.NewDecoder(bytes.NewBuffer(file)).Decode(&i); err != nil {
			log.Fatalf("cannot decode %q: %v", indexFile, err)
		}

		// Backup the previous INDEX.
		i.backupFile = indexFile + "." + strconv.Itoa(int(time.Now().UnixMilli()))
		f, err := os.Create(i.backupFile)
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()
		if err = gob.NewEncoder(f).Encode(&i); err != nil {
			log.Fatalf("could not backup %s: %v", indexFile, err)
		}
	}

	// Set last such that duplicates since
	// last pull may occur; however, we will
	// not miss any new entries.
	i.last = i.UpdatedAt
	i.UpdatedAt = time.Now()

	// Pre-seed [Index] with the next page to
	// incrementally update the cache.
	if err := i.nextPage(ctx); err != nil {
		log.Fatalf("cannot get first page: %v", err)
	}

	return &i
}

type Index struct {
	mu     sync.Mutex
	Latest map[string]string
	OnDisk map[string]string

	UpdatedAt time.Time

	pruned     atomic.Int64
	backupFile string
	last       time.Time
	d          *json.Decoder
	body       io.ReadCloser
}

func (i *Index) Record(path, version string) error {
	i.mu.Lock()
	defer i.mu.Unlock()
	if old, ok := i.OnDisk[path]; ok && old != version {
		ep, err := module.EscapePath(path)
		if err != nil {
			return err
		}
		if err := os.RemoveAll(filepath.Join(dst, ep+"@"+old)); err != nil {
			return err
		}
		i.pruned.Add(1)
	}
	i.OnDisk[path] = version
	return nil
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

		vers, ok := i.Latest[v.Path]
		if !ok {
			newMod++
		}
		if c := semver.Compare(v.Version, vers); c >= 0 {
			i.Latest[v.Path] = v.Version
			if ok && c > 0 {
				changedMod++
			}
		}
	}

	bar.Finish()
	log.Printf("index: %d new modules, %d new versions", newMod, changedMod)
	return nil
}

func (i *Index) Save() error {
	i.mu.Lock()
	defer i.mu.Unlock()
	f, err := os.CreateTemp(filepath.Dir(indexFile), "@INDEX.tmp.*")
	if err != nil {
		return err
	}
	if err := gob.NewEncoder(f).Encode(i); err != nil {
		f.Close()
		os.Remove(f.Name())
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(f.Name())
		return err
	}
	return os.Rename(f.Name(), indexFile)
}

func (i *Index) Recover() error {
	fmt.Fprint(os.Stderr, "Verifying index... (may take a while)")

	var recovered int
	var bar *progress
	err := filepath.WalkDir(dst, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if !d.IsDir() {
			return nil
		}
		if d.Name()[0] == '@' {
			return filepath.SkipDir
		}
		if !strings.ContainsRune(d.Name(), '@') {
			return nil
		}

		rel, err := filepath.Rel(dst, path)
		if err != nil {
			return nil
		}
		lastAt := strings.LastIndexByte(rel, '@')
		escapedPath := rel[:lastAt]
		version := rel[lastAt+1:]

		modPath, err := module.UnescapePath(escapedPath)
		if err != nil {
			return filepath.SkipDir
		}

		if _, ok := i.OnDisk[modPath]; !ok {
			i.OnDisk[modPath] = version
			recovered++
			if bar == nil {
				fmt.Fprint(os.Stderr, "\n")
				bar = newProgress("Recovering index...", 0)
			}
			bar.Increment()
		}
		return filepath.SkipDir
	})
	if err != nil {
		return err
	}
	if recovered == 0 {
		fmt.Fprint(os.Stderr, " ok\n")
	} else {
		bar.Finish()
		log.Printf("recovered %d modules from disk", recovered)
	}
	return nil
}

func (i *Index) Close() error {
	if i.body != nil {
		i.body.Close()
		i.body = nil
	}
	if err := i.Save(); err != nil {
		return err
	}
	if i.backupFile != "" {
		os.Remove(i.backupFile)
	}
	return nil
}

func (i *Index) next(ctx context.Context) (*Version, error) {
	v := &Version{}
	err := i.d.Decode(v)
	if err == io.EOF {
		if err := i.nextPage(ctx); err != nil {
			return nil, err
		}
		err = i.d.Decode(v)
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
	i.d = json.NewDecoder(res.Body)
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
		if res.ContentLength >= 0 && res.ContentLength <= 100<<20 {
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

// scanZipTail reads the central directory out of
// a partial zip. ok is false when the directory
// is not fully contained in the tail (or the
// tail is absent), in which case the zip must be
// downloaded to decide.
func scanZipTail(t *zipTail) (hasGoMod, ok bool) {
	if t.data == nil {
		return false, false
	}
	z, err := zip.NewReader(&tailReaderAt{data: t.data, off: t.size - int64(len(t.data))}, t.size)
	if err != nil {
		return false, false
	}
	for _, f := range z.File {
		if strings.HasSuffix(f.Name, "/go.mod") {
			return true, true
		}
	}
	return false, true
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
