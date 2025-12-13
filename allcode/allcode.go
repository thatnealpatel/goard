// Copyright 2021 The Go Authors. All rights reserved.
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
	"log"
	"net/http"
	"os"
	"os/signal"
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

	nonGoSize uint64
)

func main() {
	gob.Register(map[string]string{})
	cpuprofile := flag.String("cpuprofile", "", "write cpu profile to `FILE`")
	memprofile := flag.String("memprofile", "", "write memory profile to `FILE`")
	compress := flag.Bool("z", false, "compress the output tar archive with gzip")
	index := flag.String("file", "", "read module index from `FILE`")
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

	var latestVersions map[string]string
	var bar *pb.ProgressBar

	// Either fetch and cache a fresh index,
	// or use a cached version of a previous
	// index.
	if *index == "" {
		latestVersions = fetchIndex(ctx, bar)
		// cache it after fetching
		*index = fmt.Sprintf("data/%s_index.gob", time.Now().Format("2006-01-02"))
		f, err := os.Create(*index)
		if err != nil {
			log.Fatal(err)
		}
		if err = gob.NewEncoder(f).Encode(latestVersions); err != nil {
			log.Printf("could not encode index: %v", err)
		}
	} else {
		file, err := os.ReadFile(*index)
		if err != nil {
			log.Fatalf("cannot read %q: %v", *index, err)
		}
		buf := bytes.NewBuffer(file)
		gob.NewDecoder(buf).Decode(&latestVersions)
	}

	outMu := &sync.Mutex{}
	var out io.WriteCloser = os.Stdout
	if *compress {
		out = gzip.NewWriter(out)
	}
	tw := tar.NewWriter(out)

	// logs.txt has a loose structure
	// that documents large modules
	// that contain no .go files.
	fd, err := os.Create("logs.txt")
	if err != nil {
		log.Fatal(err)
	}
	defer fd.Close()
	dlog := log.New(fd, "", os.O_RDWR)
	dlog.SetFlags(0)

	bar = pbTemplate.Start(len(latestVersions)).Set("prefix", "Fetching modules...")

	// TODO: Change to use a channel, the
	// weights are never used. Also, change
	// errgroup to sync.WaitGroup.
	sem := semaphore.NewWeighted(250)  // +50
	gcp := semaphore.NewWeighted(1000) // +500 (GCS is slow and can take the QPS)
	g, ctx := errgroup.WithContext(ctx)

	// Metadata for the files.
	var sizePerExt sync.Map

	for path, version := range latestVersions {
		if err := ctx.Err(); err != nil {
			bar.Finish()
			log.Println(err)
			break
		}

		if err := sem.Acquire(ctx, 1); err != nil {
			bar.Finish()
			log.Println(err)
			break
		}

		// In general, fetching the index is far more
		// expensive than it used to be; instead of
		// fail-fast, document all errors and continue
		// greedily building the local module cache..
		path, version := path, version
		g.Go(func() error {
			releaseOnce := &sync.Once{}
			defer releaseOnce.Do(func() { sem.Release(1) })
			defer bar.Increment()

			if strings.Contains(path, "/vendor/") || strings.Contains(path, "/kubernetes/staging/") {
				atomic.AddInt64(&vendor, 1)
				return nil
			}
			if strings.HasPrefix(path, "github.com/bbiswy/") ||
				strings.HasPrefix(path, "github.com/wMc27rFqQaH7tQxv3/") {
				atomic.AddInt64(&spam, 1)
				return nil
			}
			modBytes, err := fetchMod(ctx, path, version)
			if err == errorInvalidName {
				atomic.AddInt64(&invalidName, 1)
				return nil
			}
			if err == errorGone {
				atomic.AddInt64(&gone, 1)
				return nil
			}
			if err != nil {
				atomic.AddInt64(&unknown, 1)
				return nil
			}
			mod, err := modfile.ParseLax(path+"@"+version, modBytes, nil)
			if err != nil {
				atomic.AddInt64(&invalidGoMod, 1)
				return nil
			}
			if mod == nil || mod.Module == nil {
				// Interestingly, it appears the following
				// two modules cause a panic due to a nil
				// pointer; however, nothing interesting
				// about them other than the fact they don't
				// compile at this version and are now
				// archived in GitHub:
				// - github.com/Maka8ka/Faygo/client@v0.0.0-20220420085059-439b6b39f779
				// - github.com/maka8ka/faygo/client@v0.0.0-20220420085059-439b6b39f779
				atomic.AddInt64(&nilModOrModule, 1)
				return nil
			}
			if mod.Module.Mod.Path != path && !*all {
				atomic.AddInt64(&mismatchedGoMod, 1)
				return nil
			}

			url, size, err := fetchZipHead(ctx, path, version)
			if err == errorGone {
				atomic.AddInt64(&gone, 1)
				return nil
			}
			if err != nil {
				atomic.AddInt64(&unknown, 1)
				return nil
			}
			if strings.HasPrefix(url, "https://storage.googleapis.com/") {
				atomic.AddInt64(&gcsBytes, size)
				releaseOnce.Do(func() { sem.Release(1) })
				gcp.Acquire(ctx, 1)
				defer gcp.Release(1)
			}

			zipBytes, err := fetchZip(ctx, path, version)
			if err == errorGone {
				atomic.AddInt64(&gone, 1)
				return nil
			}
			if err != nil {
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
					hasGoFiles = true
				}
				if strings.HasSuffix(f.Name, "/go.mod") {
					hasGoMod = true
				}
				if !ignoreFile(f.Name) {
					extractedSize += f.UncompressedSize64
				}
			}

			// Go modules containing 0% Go content.
			if !hasGoFiles {
				atomic.AddInt64(&noGoCode, 1)           // module count of no .go files
				atomic.AddUint64(&nonGoSize, totalSize) // no .go files counted
				for ext, size := range exts {
					v, found := sizePerExt.Load(ext)
					if !found {
						c := &atomic.Uint64{}
						c.Add(size)
						sizePerExt.Store(ext, c)
						v, _ = sizePerExt.Load(ext)
					}
					v.(*atomic.Uint64).Add(size)
				}

				// If the module itself isn't at
				// least 537MiB, don't log more
				// details about it.
				if totalSize>>29 < 1 {
					return nil
				}

				sort.Slice(ls, func(i, j int) bool {
					return ls[i][1].(uint64) > ls[j][1].(uint64)
				})
				files := make([]string, len(ls))
				for i, vals2 := range ls {
					mib := float64(vals2[1].(uint64)) / float64(1<<20)
					files[i] = fmt.Sprintf("[%4.0f MiB] %s", mib, vals2[0].(string))
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

			// Potentially old Go modules.
			if !hasGoMod && !*all {
				atomic.AddInt64(&noGoMod, 1)
				return nil
			}

			atomic.AddInt64(&good, 1)
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
	fmt.Fprintf(os.Stderr, "Unique modules:       % 7d\n", len(latestVersions))
	fmt.Fprintf(os.Stderr, "Vendor paths:         % 7d\n", vendor)
	fmt.Fprintf(os.Stderr, "Spam:                 % 7d\n", spam)
	fmt.Fprintf(os.Stderr, "Invalid names:        % 7d\n", invalidName)
	fmt.Fprintf(os.Stderr, "Gone:                 % 7d\n", gone)
	fmt.Fprintf(os.Stderr, "Nil:                  % 7d\n", nilModOrModule)
	fmt.Fprintf(os.Stderr, "Unknown:              % 7d\n", unknown)
	fmt.Fprintf(os.Stderr, "Invalid go.mod:       % 7d\n", invalidGoMod)
	if !*all {
		fmt.Fprintf(os.Stderr, "Mismatching go.mod:   % 7d -\n", mismatchedGoMod)
		fmt.Fprintf(os.Stderr, "No go.mod file:       % 7d -\n", noGoMod)
	}
	nonGoGiB := float64(nonGoSize) / float64(1<<30)
	fmt.Fprintf(os.Stderr, "No .go files:         % 7d (%.1f GiB)\n", noGoCode, nonGoGiB)
	fmt.Fprintf(os.Stderr, "                      -------\n")
	fmt.Fprintf(os.Stderr, "Valid:                % 7d\n", good)

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
	for _, vals2 := range extSize[:N] {
		fmt.Fprintf(os.Stderr, "    %-30s %5.1f GiB\n",
			vals2[0].(string), float64(vals2[1].(uint64))/float64(1<<30))
	}

	// Dump the full statistics to logfile.
	metadata := make([]string, len(extSize))
	for i, vals2 := range extSize {
		metadata[i] = fmt.Sprintf("%-30s    %.3f GiB",
			vals2[0].(string), float64(vals2[1].(uint64))/float64(1<<30))
	}
	dlog.Printf("File Ext by Size (n=%d):\n\t%s\n", len(metadata), strings.Join(metadata, "\n\t"))

	fmt.Fprintf(os.Stderr, "\n")
	fmt.Fprintf(os.Stderr, "Used index version %q.\n", *index)
	fmt.Fprintf(os.Stderr, "Downloaded %d bytes (%d from GCS).\n", allBytes, gcsBytes)
	fmt.Fprintf(os.Stderr, "Wrote %d Go files (%d bytes).\n", goFiles, goBytes)
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

func newRequestWithContext(ctx context.Context, method, url string) *http.Request {
	req, err := http.NewRequestWithContext(ctx, method, url, nil)
	if err != nil {
		panic(err)
	}
	return req
}

// fetchIndex gets the current module index
// from sum.golang.org that is required to
// walk and download Go modules.
func fetchIndex(ctx context.Context, bar *pb.ProgressBar) map[string]string {
	latest, err := fetchLatest(ctx)
	if err != nil {
		log.Fatal(err)
	}
	tree, err := tlog.ParseTree(latest)
	if err != nil {
		log.Fatal(err)
	}
	bar = pbTemplate.Start64(tree.N).Set("prefix", "Fetching index...")
	latestVersions := make(map[string]string)
	i, err := NewIndex(ctx)
	if err != nil {
		log.Fatal(err)
	}

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

		if semver.Compare(v.Version, latestVersions[v.Path]) >= 0 {
			latestVersions[v.Path] = v.Version
		}
	}
	bar.Finish()
	return latestVersions
}

type Index struct {
	last time.Time
	d    *json.Decoder
}

func NewIndex(ctx context.Context) (*Index, error) {
	i := &Index{}
	if err := i.nextPage(ctx); err != nil {
		return nil, err
	}
	return i, nil
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

type Version struct {
	Path, Version string
	Timestamp     time.Time
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
