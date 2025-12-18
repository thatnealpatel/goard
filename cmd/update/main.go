// Copyright 2025 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cheggaaa/pb/v3"
	gzip "github.com/klauspost/pgzip"
	"golang.org/x/mod/modfile"
	"golang.org/x/mod/module"
	"golang.org/x/mod/semver"
	"golang.org/x/mod/sumdb/tlog"
)

var httpClient = &http.Client{
	Timeout: 60 * time.Minute,
	Transport: &http.Transport{
		MaxIdleConnsPerHost: 1024,
	},
}

var pbTemplate pb.ProgressBarTemplate = `{{string . "prefix"}} {{counters . }} {{bar . }} {{percent . }} {{etime . }}`

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

	// Since goBytes and goFiles count
	// all the extensions in ignoreFile
	// it's not a true representation.
	onlyGoSrcFiles     atomic.Int64
	onlyGoSrcFilesSize atomic.Uint64

	// Metadata for the Go modules
	// containing no .go files.
	sizePerExt sync.Map
)

// localGoModCache is the top-level
// directory where all the downloaded
// source code lives.
var localGoModCache = filepath.Join(os.Getenv("HOME"), "go-ecosystem", "snapshots", "HEAD")

// set approxOnDiskModules to approximate
// number of modules on disk for nicer tui.
const approxOnDiskModules = 1_870_670

// TODO(nealpatel): Refactor this to be
// much easier to read.
func main() {
	gob.Register(map[string]string{})
	gob.Register(time.Time{})
	gob.Register(Index{})

	profile := flag.Bool("pprof", false, "enable profiling endpoint.")
	compress := flag.Bool("z", false, "compress the output tar archive with gzip")
	all := flag.Bool("all", false, "include potential forks (mismatching and missing go.mod)")
	flag.Parse()

	if *profile {
		go func() {
			runtime.SetBlockProfileRate(1) // production grade = 10000
			http.ListenAndServe("localhost:6060", nil)
		}()
	}

	log.SetFlags(log.Lshortfile | log.Flags())
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	log.Println("using: ", localGoModCache)

	// If data directory does not exist,
	// create it for caching.
	_, err := os.Stat("data")
	switch err {
	case nil:
	case os.ErrNotExist:
		if err := os.Mkdir("data", 0755); err != nil {
			log.Fatalf("cannot mkdir: %v", err)
		}
	default:
		log.Fatal(err)
	}

	var bar *pb.ProgressBar

	// Fetch the index from disk, or
	// create a new index from scratch.
	index := NewIndex(ctx)

	// Incrementally update the index in-place.
	if err = index.Update(ctx, bar); err != nil {
		log.Fatalf("failed to update index: %v", err)
	}

	// Save the updated index for re-use.
	if err = index.Save(); err != nil {
		log.Fatalf("failed to save index: %v", err)
	}

	// Instead of naively downloading all
	// the code again, walk the local cache
	// for all the (mod, vers) tuples.
	modVersCache, err := alreadyDownloaded(ctx, bar, localGoModCache)
	if err != nil {
		log.Fatalf("failed to walk local disk for already downloaded modules: %v", err)
	}

	// TODO(nealpatel): Consider how an
	// auxillary tool can be used to
	// prune non-latest versions from a
	// cache that gets incrementally
	// updated.
	_ = 0

	// TODO(nealpatel) Remove outMu since the
	// filesystem will synchronize for us.
	outMu := &sync.Mutex{}

	// TODO(nealpatel): Need to incrementally
	// store these new versions at the HEAD
	// of the module cache instead of tar.gz.
	var out io.WriteCloser = os.Stdout
	if *compress {
		out = gzip.NewWriter(out)
	}
	tw := tar.NewWriter(out)

	bar = pbTemplate.Start(len(index.Latest)).Set("prefix", "Fetching modules...")

	var (
		alreadyExists      int64
		incrementalUpdates = make(map[string]string)
	)
	for path, version := range index.Latest {
		// TODO: Use the official semvar
		// equals function?
		if modVersCache[path] == version {
			alreadyExists++
			bar.Increment()
			continue
		}
		incrementalUpdates[path] = version
	}

	var wg sync.WaitGroup
	upstreamSem := make(chan struct{}, 250)
	gcpSem := make(chan struct{}, 1500)

	for path, version := range incrementalUpdates {
		if err := ctx.Err(); err != nil { // slower than select+case, but ok
			bar.Finish()
			log.Println(path, version, err)
			break
		}

		upstreamSem <- struct{}{}
		wg.Go(func() {
			releaseOnce := &sync.Once{}
			defer releaseOnce.Do(func() { <-upstreamSem })
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

			modBytes, err := fetchMod(ctx, path, version)
			switch err {
			case errorInvalidName:
				invalidName.Add(1)
				return
			case errorGone:
				gone.Add(1)
				return
			default:
				unknown.Add(1)
				return
			case nil:
			}

			mod, err := modfile.ParseLax(path+"@"+version, modBytes, nil)
			switch {
			case err != nil:
				invalidGoMod.Add(1)
				return
			case mod == nil || mod.Module == nil:
				// Without this invariant, panic at runtime happens:
				// e.g. github.com/Maka8ka/Faygo/client@v0.0.0-20220420085059-439b6b39f779
				// e.g. github.com/maka8ka/faygo/client@v0.0.0-20220420085059-439b6b39f779
				nilModOrModule.Add(1)
				return
			case mod.Module.Mod.Path != path && !*all:
				mismatchedGoMod.Add(1)
				return
			default:
			}

			url, size, err := fetchZipHead(ctx, path, version)
			switch err {
			case nil:
			case errorGone:
				gone.Add(1)
				return
			default:
				unknown.Add(1)
				return
			}

			// Swap the sem based on the data source.
			if strings.HasPrefix(url, "https://storage.googleapis.com/") {
				gcsBytes.Add(size)
				releaseOnce.Do(func() { <-upstreamSem })
				gcpSem <- struct{}{}
				defer func() { <-gcpSem }()
			}

			// Download the source zip for path@version.
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

			allBytes.Add(size)

			zipBytesReader := bytes.NewReader(zipBytes)
			z, err := zip.NewReader(zipBytesReader, size)
			if err != nil {
				log.Println(path, version, err)
				return
			}

			var hasGoMod, hasGoFiles bool
			var extractedSize uint64
			var modSize uint64

			// Walk through the files in the zip.
			for _, f := range z.File {
				modSize += f.UncompressedSize64
				if strings.HasSuffix(f.Name, ".go") {
					onlyGoSrcFiles.Add(1)
					onlyGoSrcFilesSize.Add(f.UncompressedSize64)
					hasGoFiles = true
				}
				if strings.HasSuffix(f.Name, "/go.mod") {
					hasGoMod = true
				}
				if !ignoreFile(f.Name) {
					extractedSize += f.UncompressedSize64
				}
			}

			// Go modules containing 0% Go content
			// are very intersting.
			if !hasGoFiles {
				noGoCode.Add(1)
				nonGoSize.Add(modSize)
			}

			if !hasGoMod && !*all {
				noGoMod.Add(1)
				return
			}

			good.Add(1)

			// TODO(nealpatel) At this point, we know
			// that we do not have this particular
			// mod@vers saved. Instead of writing into
			// an archive, update the localGoModCache
			// in-place with this new version.

			// TODO(nealpatel) Remove outMu since the
			// filesystem will synchronize for us.
			outMu.Lock()
			defer outMu.Unlock()
			for _, f := range z.File {
				if ignoreFile(f.Name) {
					continue
				}

				src, err := z.Open(f.Name)
				if err != nil {
					log.Println(path, version, err)
					return
				}

				hdr := &tar.Header{
					Name: f.Name,
					Mode: 0664,
					Size: int64(f.UncompressedSize64),
				}
				if err := tw.WriteHeader(hdr); err != nil {
					log.Println(path, version, err)
					return
				}

				n, err := io.Copy(tw, src)
				if err != nil {
					log.Println(path, version, err)
					return
				}

				goFiles.Add(1)
				goBytes.Add(n)
			}
		})
	}

	wg.Wait()
	bar.Finish()

	if err := tw.Close(); err != nil {
		log.Println(err)
	}
	if err := out.Close(); err != nil {
		log.Println(err)
	}

	bar.Finish()

	// ==================================================================== //
	// *****                     Dump statistics.                     ***** //
	// ==================================================================== //
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
	if !*all {
		fmt.Fprintf(os.Stderr, "Mismatching go.mod:   % 9d\n", mismatchedGoMod.Load())
		fmt.Fprintf(os.Stderr, "No go.mod file:       % 9d\n", noGoMod.Load())
	}
	nonGoGiB := float64(nonGoSize.Load()) / float64(1<<30)
	fmt.Fprintf(os.Stderr, "No .go files:         % 9d (%.1f GiB)\n", noGoCode.Load(), nonGoGiB)
	fmt.Fprintf(os.Stderr, "                      ---------\n")
	fmt.Fprintf(os.Stderr, "Valid:                % 9d\n", good.Load())
	fmt.Fprintf(os.Stderr, "\n")
	fmt.Fprintf(os.Stderr, "Used index that as updated at %s.\n", index.UpdatedAt.Format("2006-01-02 15:04:05 MST"))
	fmt.Fprintf(os.Stderr, "Downloaded %d bytes (%d from GCS).\n", allBytes.Load(), gcsBytes.Load())
	fmt.Fprintf(os.Stderr, "Wrote %d Go-related files (%d bytes).\n", goFiles.Load(), goBytes.Load())
	fmt.Fprintf(os.Stderr, "Wrote %d .go source files (%d bytes).\n", onlyGoSrcFiles.Load(), onlyGoSrcFilesSize.Load())
}

// alreadyDownloaded provides a
// mapping of mod -> vers in order to
// perform incremental updates to locally
// cached source code.
func alreadyDownloaded(ctx context.Context, bar *pb.ProgressBar, root string) (map[string]string, error) {
	bar = pbTemplate.Start(approxOnDiskModules).Set("prefix", "Building cache...")
	defer bar.Finish()

	var (
		roots sync.Map

		mu    sync.Mutex
		cache = make(map[string]string, approxOnDiskModules)
	)

	// Agnostic to whether or not a go.mod exists.
	modVersResolver := func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if _, found := roots.Load(path); found {
			return nil
		}

		_, modVers, found := strings.Cut(path, localGoModCache)
		if !found {
			log.Printf("unexpected: local cache prefix was not found?")
			return nil
		}
		mod, vers, found := strings.Cut(modVers, "@")
		if !found {
			return nil
		}

		mu.Lock()
		cache[mod[1:]] = vers // mod contains a leading slash
		mu.Unlock()
		bar.Increment()

		// since d is a directory,
		// this will skip looking
		// into the mod@vers dir.
		return filepath.SkipDir
	}

	var wg sync.WaitGroup
	wsem := make(chan struct{}, 1<<11)

	dirs, err := os.ReadDir(root)
	if err != nil {
		return nil, err
	}
	log.Printf("walking %d top-level directories ...", len(dirs))

	/*
		% la HEAD | awk '{print $5 " " $9}' | sort -h | tail -10
		86016 gopkg.in
		135168 gitee.com
		163840 gitlab.com
		192512 .
		16674816 github.com
	*/
	for _, dir := range dirs {
		path := filepath.Join(root, dir.Name())
		roots.Store(path, struct{}{})

		// Spawn a goroutine for each top-level
		// directory to shard the walking.
		err = filepath.WalkDir(path, func(path string, d fs.DirEntry, werr error) error {
			if werr != nil {
				return werr
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			if _, found := roots.Load(path); found {
				return nil
			}

			roots.Store(path, struct{}{})
			wsem <- struct{}{}
			wg.Go(func() {
				defer func() { <-wsem }()
				_ = filepath.WalkDir(path, modVersResolver)
			})
			return filepath.SkipDir
		})
		if err != nil {
			return nil, err
		}
	}
	wg.Wait()
	return cache, nil
}

func NewIndex(ctx context.Context) *Index {
	var i Index
	i.outFile = "data/HEAD.index"

	// Backup the current HEAD if it already exists.
	_, err := os.Stat(i.outFile)
	switch err {
	case nil:
		// Read the current HEAD.
		file, err := os.ReadFile(i.outFile)
		if err != nil {
			log.Fatalf("cannot read %q: %v", i.outFile, err)
		}
		gob.NewDecoder(bytes.NewBuffer(file)).Decode(&i)

		// Backup the previous HEAD.
		f, err := os.Create(fmt.Sprintf("data/HEAD.%s.index", i.UpdatedAt.Format("20060102_150405")))
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()
		if err = gob.NewEncoder(f).Encode(i); err != nil {
			log.Panicf("could backup %s: %v", i.outFile, err)
		}

	case os.ErrNotExist:
		log.Printf("no %q found; building new HEAD from scratch (this may take a while)", i.outFile)
		i = Index{Latest: make(map[string]string)}

	default:
		log.Fatalf("cannot stat %q: %v", i.outFile, err)
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
	Latest        map[string]string
	UpdatedAt     time.Time
	TotalVersions uint64

	outFile string

	last time.Time
	d    *json.Decoder
}

func (i *Index) Update(ctx context.Context, bar *pb.ProgressBar) error {
	latest, err := fetchLatest(ctx)
	if err != nil {
		return err
	}
	tree, err := tlog.ParseTree(latest)
	if err != nil {
		return err
	}
	N := tree.N - int64(i.TotalVersions)

	if N <= 0 {
		const cacheFmt = "\tLatest: %d\n\tTotal Versions: %d\n\tUpdatedAt: %s"
		log.Printf("no updates for index\n{\n%s\n}",
			fmt.Sprintf(cacheFmt,
				len(i.Latest),
				i.TotalVersions,
				i.UpdatedAt.Format("2006-01-02 15:04:05 UTC")),
		)
		return nil
	}

	bar = pbTemplate.Start64(N).Set("prefix", "Updating index...")
	defer bar.Finish()

	i.TotalVersions = uint64(tree.N)

	var linesSeen uint64
	for {
		v, err := i.next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		linesSeen++
		bar.Increment()

		if semver.Compare(v.Version, i.Latest[v.Path]) >= 0 {
			i.Latest[v.Path] = v.Version
		}
	}

	return nil
}

func (i *Index) Save() error {
	if err := os.Remove(i.outFile); err != nil {
		log.Printf("warn: cannot remove %q: %v", i.outFile, err)
	}
	f, err := os.Create(i.outFile)
	if err != nil {
		return err
	}
	defer f.Close()
	if err = gob.NewEncoder(f).Encode(*i); err != nil {
		return err
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
	url := "https://index.golang.org/index?since=" + i.last.Add(1).Format(time.RFC3339Nano)
	req, err := httpClient.Do(newRequestWithContext(ctx, "GET", url))
	if err != nil {
		return err
	}
	i.d = json.NewDecoder(req.Body)
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

func fetchLatest(ctx context.Context) ([]byte, error) {
	url := "https://sum.golang.org/latest"
	res, err := httpClient.Do(newRequestWithContext(ctx, "GET", url))
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET %q: %v", url, res.Status)
	}
	return io.ReadAll(res.Body)
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

func fetchZipHead(ctx context.Context, path, version string) (string, int64, error) {
	url, err := proxyURL(path, version, ".zip")
	if err != nil {
		return "", 0, err
	}
	res, err := httpClient.Do(newRequestWithContext(ctx, "HEAD", url))
	if err != nil {
		return "", 0, err
	}
	defer res.Body.Close()
	if res.StatusCode == http.StatusGone || res.StatusCode == http.StatusNotFound {
		return "", 0, errorGone
	}
	if res.StatusCode != http.StatusOK {
		return "", 0, fmt.Errorf("HEAD %q: %v", url, res.Status)
	}
	return res.Request.URL.String(), res.ContentLength, nil
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
	// I experimented with using Range requests to back the zip
	// ReaderAt, but it was extremely slow.
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
