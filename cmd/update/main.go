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
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"sort"
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
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

var httpClient = &http.Client{
	Timeout: 60 * time.Minute,
	Transport: &http.Transport{
		MaxIdleConnsPerHost: 1024,
	},
}

var pbTemplate pb.ProgressBarTemplate = `{{string . "prefix"}} {{counters . }} {{bar . }} {{percent . }} {{etime . }}`

var (
	// TODO: Use atomics instead?
	gone            int64
	unknown         int64
	nilModOrModule  int64
	invalidName     int64
	vendor          int64
	spam            int64
	mismatchedGoMod int64
	invalidGoMod    int64
	noGoCode        int64
	noGoMod         int64
	gcsBytes        int64
	good            int64
	goBytes         int64
	allBytes        int64
	goFiles         int64
	alreadyExists   int64

	nonGoSize uint64

	// Since goBytes and goFiles count
	// all the extensions in ignoreFile
	// it's not a true representation.
	onlyGoSrcFiles     uint64
	onlyGoSrcFilesSize uint64
)

// localGoModCache is the top-level
// directory where all the downloaded
// source code lives.
var localGoModCache = filepath.Join(os.Getenv("HOME"), "go-ecosystem", "snapshots", "HEAD")

func main() {
	gob.Register(map[string]string{})
	gob.Register(time.Time{})
	gob.Register(IndexCache{})

	cpuprofile := flag.String("cpuprofile", "", "write cpu profile to `FILE`")
	memprofile := flag.String("memprofile", "", "write memory profile to `FILE`")
	compress := flag.Bool("z", false, "compress the output tar archive with gzip")
	all := flag.Bool("all", false, "include potential forks (mismatching and missing go.mod)")
	flag.Parse()

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal("could not create CPU profile: ", err)
		}
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal("could not start CPU profile: ", err)
		}
		defer pprof.StopCPUProfile()
	}

	log.SetFlags(log.Lshortfile | log.Flags())
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	var bar *pb.ProgressBar

	log.Println("local gomodcache: ", localGoModCache)

	// If data/HEAD.index exists, it
	// will be read, backed up, updated,
	// and re-cached as HEAD.index.
	indexCache := buildIndex(ctx, bar)

	// Instead of downloading all modules,
	// recursively walk the existing on-
	// disk modules and build a mod->vers
	// cache of what's present already.
	modVersCache := alreadyDownloaded(ctx, bar, localGoModCache)

	// TODO(nealpatel): Consider how an
	// auxillary tool can be used to
	// prune non-latest versions from a
	// cache that gets incrementally
	// updated.
	_ = 0

	// TODO(nealpatel): Need to incrementally
	// store these new versions at the HEAD
	// of the module cache instead of tar.gz.
	outMu := &sync.Mutex{}
	var out io.WriteCloser = os.Stdout
	if *compress {
		out = gzip.NewWriter(out)
	}
	tw := tar.NewWriter(out)

	// logs.txt has a loose structure
	// that documents large modules
	// that contain no .go files.
	lf, err := os.Create("data/mois.txt") // 'modules of interest'
	if err != nil {
		log.Fatal(err)
	}
	defer lf.Close()
	dlog := log.New(lf, "", os.O_RDWR)
	dlog.SetFlags(0)

	// TODO(nealpatel): Change to use
	// channels since weights are not
	// used; also use a wg instead of
	// the errgroup.
	sem := semaphore.NewWeighted(250)  // originally 200
	gcp := semaphore.NewWeighted(1500) // originally 500 (GCS is slow and can take the QPS)
	g, ctx := errgroup.WithContext(ctx)

	// Metadata for the files.
	var sizePerExt sync.Map

	bar = pbTemplate.Start(len(indexCache.Latest)).Set("prefix", "Fetching modules...")
	for path, version := range indexCache.Latest {
		if err := ctx.Err(); err != nil {
			bar.Finish()
			log.Println(err)
			break
		}

		// TODO: Use the official semvar
		// equals function?
		if modVersCache[path] == version {
			alreadyExists++
			bar.Increment()
			continue
		}

		if err := sem.Acquire(ctx, 1); err != nil {
			bar.Finish()
			log.Println(err)
			break
		}

		// TODO(nealpatel): Can remove this?
		path, version := path, version
		g.Go(func() error {
			releaseOnce := &sync.Once{}
			defer releaseOnce.Do(func() { sem.Release(1) })
			defer bar.Increment()

			if strings.Contains(path, "/vendor/") ||
				strings.Contains(path, "/kubernetes/staging/") {
				atomic.AddInt64(&vendor, 1)
				return nil
			}

			// TODO(nealpatel): Investigate what makes these spam.
			if strings.HasPrefix(path, "github.com/bbiswy/") ||
				strings.HasPrefix(path, "github.com/wMc27rFqQaH7tQxv3/") {
				atomic.AddInt64(&spam, 1)
				return nil
			}

			modBytes, err := fetchMod(ctx, path, version)
			switch err {
			case errorInvalidName:
				atomic.AddInt64(&invalidName, 1)
				return nil
			case errorGone:
				atomic.AddInt64(&gone, 1)
				return nil
			default:
				atomic.AddInt64(&unknown, 1)
				return nil
			case nil:
			}

			mod, err := modfile.ParseLax(path+"@"+version, modBytes, nil)
			switch {
			case err != nil:
				atomic.AddInt64(&invalidGoMod, 1)
				return nil
			case mod == nil || mod.Module == nil:
				// Without this invariant, panic at runtime happens:
				// e.g. github.com/Maka8ka/Faygo/client@v0.0.0-20220420085059-439b6b39f779
				// e.g. github.com/maka8ka/faygo/client@v0.0.0-20220420085059-439b6b39f779
				atomic.AddInt64(&nilModOrModule, 1)
				return nil
			case mod.Module.Mod.Path != path && !*all:
				atomic.AddInt64(&mismatchedGoMod, 1)
				return nil
			default:
			}

			url, size, err := fetchZipHead(ctx, path, version)
			switch err {
			case nil:
			case errorGone:
				atomic.AddInt64(&gone, 1)
				return nil
			default:
				atomic.AddInt64(&unknown, 1)
				return nil
			}

			// Swap the sem based on the data source.
			if strings.HasPrefix(url, "https://storage.googleapis.com/") {
				atomic.AddInt64(&gcsBytes, size)
				releaseOnce.Do(func() { sem.Release(1) })
				if err := gcp.Acquire(ctx, 1); err != nil {
					return err
				}
				defer gcp.Release(1)
			}

			// Download the source zip for path@version.
			zipBytes, err := fetchZip(ctx, path, version)
			switch err {
			case nil:
			case errorGone:
				atomic.AddInt64(&gone, 1)
				return nil
			default:
				atomic.AddInt64(&unknown, 1)
				return nil
			}

			atomic.AddInt64(&allBytes, size)

			zipBytesReader := bytes.NewReader(zipBytes)
			z, err := zip.NewReader(zipBytesReader, size)
			if err != nil {
				return err
			}

			var hasGoMod, hasGoFiles bool
			var extractedSize uint64
			var totalSize uint64
			ls := make([][2]any, 0, len(z.File))
			exts := make(map[string]uint64)
			modVers := path + "@" + version

			// Walk through the files in the zip.
			for _, f := range z.File {
				_, localName, _ := strings.Cut(f.Name, modVers)

				// sort by size later
				ls = append(ls, [2]any{"." + localName, f.UncompressedSize64})

				// since we only consider this size
				// when hasGoFiles is false, it is
				// equivalent to conditionally adding
				// up the sizes of non-Go files.
				totalSize += f.UncompressedSize64

				idx := strings.LastIndex(localName, ".")
				if idx >= 0 && idx+1 < len(localName) {
					exts[localName[idx+1:]] += f.UncompressedSize64
				}

				if strings.HasSuffix(f.Name, ".go") {
					atomic.AddUint64(&onlyGoSrcFiles, 1)
					atomic.AddUint64(&onlyGoSrcFilesSize, f.UncompressedSize64)
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
				atomic.AddInt64(&noGoCode, 1)
				atomic.AddUint64(&nonGoSize, totalSize)
				for ext, size := range exts {
					v, _ := sizePerExt.LoadOrStore(ext, (&atomic.Uint64{}))
					v.(*atomic.Uint64).Add(size)
				}

				// If the module itself isn't at
				// least 537MiB, don't log more
				// details about it.
				if totalSize>>29 < 1 {
					return nil
				}

				// Descending sort the files by size.
				sort.Slice(ls, func(i, j int) bool {
					return ls[i][1].(uint64) > ls[j][1].(uint64)
				})

				files := make([]string, len(ls))
				for i, any2 := range ls {
					mib := float64(any2[1].(uint64)) / float64(1<<20)
					files[i] = fmt.Sprintf("[%4.0f MiB] %s", mib, any2[0].(string))
				}

				outMu.Lock()
				defer outMu.Unlock()

				dlog.Printf("module: %q", modVers)
				dlog.Printf("num_files: %d", len(ls))
				dlog.Printf("mod_size: %d B", size)
				list := make([]string, 0, len(exts))
				for ext := range exts {
					list = append(list, fmt.Sprintf("%q", ext))
				}
				dlog.Printf("num_extensions: %d", len(list))
				// This format allows for easy folding in vim.
				dlog.Printf("extensions {\n\t%s\n}", strings.Join(list, "\n\t"))
				dlog.Printf("files {\n\t%s\n}", strings.Join(files, "\n\t"))
				dlog.Printf("\n\n")
				return nil
			}

			if !hasGoMod && !*all {
				atomic.AddInt64(&noGoMod, 1)
				return nil
			}

			atomic.AddInt64(&good, 1)

			// TODO(nealpatel) perform incremental
			// updates over the local cache.
			for _, f := range z.File {
				_ = f
			}

			outMu.Lock()
			defer outMu.Unlock()
			for _, f := range z.File {
				if ignoreFile(f.Name) {
					continue
				}

				src, err := z.Open(f.Name)
				if err != nil {
					return err
				}

				hdr := &tar.Header{
					Name: f.Name,
					Mode: 0664,
					Size: int64(f.UncompressedSize64),
				}
				if err := tw.WriteHeader(hdr); err != nil {
					return err
				}

				n, err := io.Copy(tw, src)
				if err != nil {
					return err
				}

				atomic.AddInt64(&goFiles, 1)
				atomic.AddInt64(&goBytes, n)
			}

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		bar.Finish()
		log.Println(err)
	}
	if err := tw.Close(); err != nil {
		log.Println(err)
	}
	if err := out.Close(); err != nil {
		log.Println(err)
	}

	bar.Finish()

	fmt.Fprintf(os.Stderr, "\n")
	fmt.Fprintf(os.Stderr, "Unique modules:       % 9d\n", len(indexCache.Latest))
	fmt.Fprintf(os.Stderr, "Already cached:       % 9d\n", alreadyExists)
	fmt.Fprintf(os.Stderr, "Vendor paths:         % 9d\n", vendor)
	fmt.Fprintf(os.Stderr, "Spam:                 % 9d\n", spam)
	fmt.Fprintf(os.Stderr, "Invalid names:        % 9d\n", invalidName)
	fmt.Fprintf(os.Stderr, "Gone:                 % 9d\n", gone)
	fmt.Fprintf(os.Stderr, "Nil:                  % 9d\n", nilModOrModule)
	fmt.Fprintf(os.Stderr, "Unknown:              % 9d\n", unknown)
	fmt.Fprintf(os.Stderr, "Invalid go.mod:       % 9d\n", invalidGoMod)
	if !*all {
		fmt.Fprintf(os.Stderr, "Mismatching go.mod:   % 9d\n", mismatchedGoMod)
		fmt.Fprintf(os.Stderr, "No go.mod file:       % 9d\n", noGoMod)
	}
	nonGoGiB := float64(nonGoSize) / float64(1<<30)
	fmt.Fprintf(os.Stderr, "No .go files:         % 9d (%.1f GiB)\n", noGoCode, nonGoGiB)
	fmt.Fprintf(os.Stderr, "                      -------\n")
	fmt.Fprintf(os.Stderr, "Valid:                % 9d\n", good)

	// Extract and sort the per-extension size.
	var extSize [][2]any
	sizePerExt.Range(func(key, value any) bool {
		ext, _ := key.(string)
		c, _ := value.(*atomic.Uint64)
		extSize = append(extSize, [2]any{ext, c.Load()})
		return true
	})
	sort.Slice(extSize, func(i, j int) bool {
		return extSize[i][1].(uint64) > extSize[j][1].(uint64)
	})

	// Preview to stderr.
	const N = 20
	fmt.Fprintf(os.Stderr, "Top-%d Size by Extension (across all Go-less modules):\n", N)
	for _, any2 := range extSize[:N] {
		fmt.Fprintf(os.Stderr, "    %-30s %5.1f GiB\n",
			any2[0].(string), float64(any2[1].(uint64))/float64(1<<30))
	}

	// Dump the full statistics to logfile.
	metadata := make([]string, len(extSize))
	for i, any2 := range extSize {
		metadata[i] = fmt.Sprintf("%-30s    %.3f GiB",
			any2[0].(string), float64(any2[1].(uint64))/float64(1<<30))
	}
	dlog.Printf("File Ext by Size (n=%d):\n\t%s\n", len(metadata), strings.Join(metadata, "\n\t"))

	fmt.Fprintf(os.Stderr, "\n")
	fmt.Fprintf(os.Stderr, "Used index that as updated at %s.\n", indexCache.UpdatedAt.Format("2006-01-02 15:04:05 MST"))
	fmt.Fprintf(os.Stderr, "Downloaded %d bytes (%d from GCS).\n", allBytes, gcsBytes)
	fmt.Fprintf(os.Stderr, "Wrote %d Go-related files (%d bytes).\n", goFiles, goBytes)
	fmt.Fprintf(os.Stderr, "Wrote %d .go source files (%d bytes).\n", onlyGoSrcFiles, onlyGoSrcFilesSize)
	totalGiB := (float64(allBytes) / float64(1<<30))
	fmt.Fprintf(os.Stderr, "Module Proxy is saturated with %.2f%% Go-less modules.\n",
		(nonGoGiB/totalGiB)*100)

	if *memprofile != "" {
		f, err := os.Create(*memprofile)
		if err != nil {
			log.Fatal("could not create memory profile: ", err)
		}
		defer f.Close()
		runtime.GC() // get up-to-date statistics
		if err := pprof.WriteHeapProfile(f); err != nil {
			log.Fatal("could not write memory profile: ", err)
		}
	}
}

// buildIndex will backup and update
// an existing index or create a new
// index from scratch.
func buildIndex(ctx context.Context, bar *pb.ProgressBar) (indexCache IndexCache) {
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
		log.Fatalf("cannot stat: %v", err)
	}

	// Backup the current HEAD if it
	// already exists.
	const head = "data/HEAD.index"
	_, err = os.Stat(head)
	switch err {
	case nil:
		// Read the current HEAD.
		file, err := os.ReadFile(head)
		if err != nil {
			log.Fatalf("cannot read %q: %v", head, err)
		}
		buf := bytes.NewBuffer(file)
		gob.NewDecoder(buf).Decode(&indexCache)

		// Backup the previous HEAD.
		f, err := os.Create(fmt.Sprintf("data/HEAD.%s.index", indexCache.UpdatedAt.Format("20060102_150405")))
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()
		if err = gob.NewEncoder(f).Encode(indexCache); err != nil {
			log.Panicf("could backup %s: %v", head, err)
		}
		if err = os.Remove(head); err != nil {
			log.Fatalf("cannot remove %s: %v", head, err)
		}

	case os.ErrNotExist:
		log.Printf("no ./data/HEAD.index found; building new HEAD from scratch (this may take a while)")
		indexCache = IndexCache{Latest: make(map[string]string)}

	default:
		log.Fatalf("cannot stat data/HEAD.index: %v", err)
	}

	updateIndex(ctx, bar, &indexCache)

	// Cache the updated index as HEAD.index
	f, err := os.Create(head)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	if err = gob.NewEncoder(f).Encode(indexCache); err != nil {
		log.Panicf("could not encode index: %v", err)
	}

	return
}

func updateIndex(ctx context.Context, bar *pb.ProgressBar, cache *IndexCache) {
	latest, err := fetchLatest(ctx)
	if err != nil {
		log.Fatal(err)
	}
	tree, err := tlog.ParseTree(latest)
	if err != nil {
		log.Fatal(err)
	}
	N := tree.N - int64(cache.TotalVersions)

	if N <= 0 {
		const cacheFmt = "\tLatest: %d\n\tTotal Versions: %d\n\tUpdatedAt: %s"
		log.Printf("no updates for index\n{\n%s\n}",
			fmt.Sprintf(cacheFmt,
				len(cache.Latest),
				cache.TotalVersions,
				cache.UpdatedAt.Format("2006-01-02 15:04:05 UTC")),
		)
		return
	}
	bar = pbTemplate.Start64(N).Set("prefix", "Updating index...")

	// Create a new Index walker using
	// the previously cached timestamp.
	start := time.Now()
	i := NewIndex(ctx, cache.UpdatedAt)
	cache.UpdatedAt = start
	cache.TotalVersions = uint64(tree.N)

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

		if semver.Compare(v.Version, cache.Latest[v.Path]) >= 0 {
			cache.Latest[v.Path] = v.Version
		}
	}

	bar.Finish()
}

// alreadyDownloaded provides a
// mapping of mod -> vers in order to
// perform incremental updates to locally
// cached source code.
func alreadyDownloaded(ctx context.Context, bar *pb.ProgressBar, root string) (cache map[string]string) {
	var mu sync.Mutex
	cache = make(map[string]string, 1<<21) // nearly 2M entries cached
	roots := make(map[string]bool, 1<<12)  // ~4000 top-level roots (e.g. github.com)

	cacheLatest := func(root string) error {
		modBytes, err := os.ReadFile(filepath.Join(root, "go.mod"))
		if err != nil {
			return err
		}
		if _, err = modfile.ParseLax(root, modBytes, nil); err != nil {
			return err
		}
		const prefix = "/usr/local/google/home/nealpatel/go-ecosystem/snapshots/20250807/"
		_, modVers, _ := strings.Cut(root, prefix)
		mod, vers, _ := strings.Cut(modVers, "@")
		mu.Lock()
		cache[mod] = vers
		mu.Unlock()
		return nil
	}

	modVersResolver := func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if strings.Contains(d.Name(), "@") {
			err := cacheLatest(path)
			if err == nil {
				bar.Increment()
				return filepath.SkipDir
			}
			return nil // try again deeper
		}
		return nil
	}

	// an estimate just for nice tui.
	bar = pbTemplate.Start(1_870_670).Set("prefix", "Building cache...")

	var wg sync.WaitGroup
	wsem := make(chan struct{}, 512)

	shardedDirWalker := func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		mu.Lock()
		pass := roots[path]
		mu.Unlock()
		if pass {
			return nil
		}

		wsem <- struct{}{}
		wg.Go(func() {
			defer func() { <-wsem }()
			filepath.WalkDir(path, modVersResolver) // ignores error
		})

		// for example, inside of github.com,
		// once spawning for a given username,
		// skip the rest of its contents.
		return filepath.SkipDir // this might have small edge cases
	}

	dirs, err := os.ReadDir(root)
	if err != nil {
		log.Fatalf("cannot read top level dirs: %v", err)
	}

	for _, dir := range dirs {
		path := filepath.Join(root, dir.Name())
		mu.Lock()
		roots[path] = true
		mu.Unlock()

		wsem <- struct{}{}
		wg.Go(func() {
			defer func() { <-wsem }()
			_ = filepath.WalkDir(path, shardedDirWalker)
		})
	}

	wg.Wait()
	bar.Finish()

	if err != nil {
		log.Fatalf("couldn't build cache of latest: %v", err)
	}
	log.Println("cached %d latest versions")
	return
}

// TODO(nealpatel): Roll functionality into
// this instead of free functions?
type IndexCache struct {
	Latest        map[string]string
	UpdatedAt     time.Time
	TotalVersions uint64
}

func NewIndex(ctx context.Context, lastUpdate time.Time) *Index {
	i := &Index{last: lastUpdate}
	if err := i.nextPage(ctx); err != nil {
		panic(err)
	}
	return i
}

type Index struct {
	last time.Time
	d    *json.Decoder
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
