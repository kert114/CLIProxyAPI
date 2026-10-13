package githubactivity

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSnapshotCacheReusesSavedSnapshotAcrossClients(t *testing.T) {
	directory := t.TempDir()
	now := time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)
	scope := SnapshotScope{Viewer: "person-id", Account: "kert114", Organization: "60xAI", Since: now.Add(-24 * time.Hour), Until: now}
	cache := NewSnapshotCache(directory)
	cache.now = func() time.Time { return now }
	fetched := 0
	load := func(context.Context) (Snapshot, error) {
		fetched++
		return Snapshot{Account: "kert114", Organization: "60xAI", CheckedAt: now.Format(time.RFC3339), OpenCount: 7}, nil
	}
	first, err := cache.Fetch(context.Background(), scope, false, load)
	if err != nil || first.OpenCount != 7 || first.CacheStatus != "refreshed" {
		t.Fatalf("first snapshot=%+v err=%v", first, err)
	}
	restarted := NewSnapshotCache(directory)
	restarted.now = func() time.Time { return now.Add(30 * time.Second) }
	second, err := restarted.Fetch(context.Background(), scope, false, load)
	if err != nil || second.OpenCount != 7 || second.CacheStatus != "fresh" || fetched != 1 {
		t.Fatalf("saved snapshot=%+v fetches=%d err=%v", second, fetched, err)
	}
}

func TestSnapshotCacheRetainsLastSuccessWhenRefreshFails(t *testing.T) {
	now := time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)
	cache := NewSnapshotCache(t.TempDir())
	cache.now = func() time.Time { return now }
	scope := SnapshotScope{Viewer: "person-id", Repository: "owner/repo"}
	checkedAt := now.Format(time.RFC3339)
	_, err := cache.Fetch(context.Background(), scope, false, func(context.Context) (Snapshot, error) {
		return Snapshot{Repository: "owner/repo", OpenCount: 3, CheckedAt: checkedAt}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Minute)
	got, err := cache.Fetch(context.Background(), scope, false, func(context.Context) (Snapshot, error) {
		return Snapshot{}, errors.New("private upstream error")
	})
	if err != nil || got.OpenCount != 3 || got.CheckedAt != checkedAt || got.CacheStatus != "stale" || !got.RefreshError {
		t.Fatalf("last successful snapshot=%+v err=%v", got, err)
	}
}

func TestSnapshotCacheCanceledCallerDoesNotStartFetch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	called := false
	cache := NewSnapshotCache(t.TempDir())
	_, err := cache.Fetch(ctx, SnapshotScope{Viewer: "person-id", Repository: "owner/repo"}, false, func(context.Context) (Snapshot, error) {
		called = true
		return Snapshot{}, nil
	})
	if !errors.Is(err, context.Canceled) || called {
		t.Fatalf("canceled caller err=%v upstream called=%v", err, called)
	}
}

func TestSnapshotCacheSharesFetchAfterFirstCallerCancels(t *testing.T) {
	cache := NewSnapshotCache(t.TempDir())
	scope := SnapshotScope{Viewer: "person-id", Repository: "owner/repo"}
	started := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseFetch := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseFetch()
	var calls atomic.Int32
	load := func(ctx context.Context) (Snapshot, error) {
		if calls.Add(1) == 1 {
			close(started)
		}
		select {
		case <-release:
			return Snapshot{OpenCount: 9}, nil
		case <-ctx.Done():
			return Snapshot{}, ctx.Err()
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	firstDone := make(chan error, 1)
	go func() {
		_, err := cache.Fetch(ctx, scope, false, load)
		firstDone <- err
	}()
	<-started
	cancel()
	if err := <-firstDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled caller err=%v", err)
	}
	secondDone := make(chan error, 1)
	go func() {
		got, err := cache.Fetch(context.Background(), scope, false, load)
		if err == nil && got.OpenCount != 9 {
			err = errors.New("missing shared snapshot")
		}
		secondDone <- err
	}()
	releaseFetch()
	if err := <-secondDone; err != nil || calls.Load() != 1 {
		t.Fatalf("shared upstream calls=%d, want 1; err=%v", calls.Load(), err)
	}
}

func TestSnapshotCacheBacksOffFailedRefreshUntilExplicitRetry(t *testing.T) {
	now := time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)
	cache := NewSnapshotCache(t.TempDir())
	cache.now = func() time.Time { return now }
	scope := SnapshotScope{Viewer: "person-id", Repository: "owner/repo"}
	_, err := cache.Fetch(context.Background(), scope, false, func(context.Context) (Snapshot, error) {
		return Snapshot{OpenCount: 2}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Minute)
	calls := 0
	failure := func(context.Context) (Snapshot, error) { calls++; return Snapshot{}, ErrUnavailable }
	for range 2 {
		got, err := cache.Fetch(context.Background(), scope, false, failure)
		if err != nil || !got.RefreshError || got.CacheStatus != "stale" {
			t.Fatalf("failed refresh snapshot=%+v err=%v", got, err)
		}
	}
	if calls != 1 {
		t.Fatalf("automatic retry repeated upstream failure %d times", calls)
	}
	_, _ = cache.Fetch(context.Background(), scope, true, failure)
	if calls != 2 {
		t.Fatalf("explicit retry did not fetch: calls=%d", calls)
	}
}

func TestClientCachedSnapshotVerifiesViewerBeforeReuse(t *testing.T) {
	viewer := "kert114"
	queries := 0
	client := &Client{Cache: NewSnapshotCache(t.TempDir()), Run: func(_ context.Context, input []byte) ([]byte, error) {
		if strings.Contains(string(input), "SnapshotViewer") {
			return []byte(`{"data":{"viewer":{"id":"id-` + viewer + `","login":"` + viewer + `"}}}`), nil
		}
		queries++
		return []byte(`{"data":{"repository":{"nameWithOwner":"owner/repo","url":"https://github.com/owner/repo","pullRequests":{"totalCount":4,"nodes":[]},"defaultBranchRef":null},"created":{"nodes":[]},"merged":{}}}`), nil
	}}
	scope := SnapshotScope{Repository: "owner/repo", Since: time.Date(2026, 10, 12, 0, 0, 0, 0, time.UTC), Until: time.Date(2026, 10, 13, 0, 0, 0, 0, time.UTC)}
	for range 2 {
		got, err := client.FetchCached(context.Background(), scope, false)
		if err != nil || got.OpenCount != 4 {
			t.Fatalf("snapshot=%+v err=%v", got, err)
		}
	}
	if queries != 1 {
		t.Fatalf("repeated repository queries=%d", queries)
	}
	viewer = "other-person"
	_, err := client.FetchCached(context.Background(), scope, false)
	if err != nil || queries != 2 {
		t.Fatalf("viewer switch reused private snapshot: queries=%d err=%v", queries, err)
	}
}

func TestSnapshotCacheBoundsStoredWindows(t *testing.T) {
	directory := t.TempDir()
	cache := NewSnapshotCache(directory)
	now := time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)
	cache.now = func() time.Time { return now }
	for index := range 70 {
		now = now.Add(time.Second)
		_, err := cache.Fetch(context.Background(), SnapshotScope{Viewer: "person-id", Repository: fmt.Sprintf("owner/repo-%d", index)}, false, func(context.Context) (Snapshot, error) { return Snapshot{OpenCount: 1}, nil })
		if err != nil {
			t.Fatal(err)
		}
	}
	files, err := os.ReadDir(directory)
	if err != nil || len(files) > 64 || len(cache.entries) > 64 {
		t.Fatalf("cache grew without bound: files=%d entries=%d err=%v", len(files), len(cache.entries), err)
	}
}

func TestSnapshotCacheForceExpiryCorruptionAndPrivateFiles(t *testing.T) {
	now := time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)
	directory := filepath.Join(t.TempDir(), "snapshots")
	cache := NewSnapshotCache(directory)
	cache.now = func() time.Time { return now }
	scope := SnapshotScope{Viewer: "person-id", Repository: "owner/repo"}
	calls := 0
	load := func(context.Context) (Snapshot, error) {
		calls++
		return Snapshot{Repository: "owner/repo", OpenCount: calls}, nil
	}
	_, _ = cache.Fetch(context.Background(), scope, false, load)
	got, err := cache.Fetch(context.Background(), scope, true, load)
	if err != nil || got.OpenCount != 2 || got.CacheStatus != "refreshed" {
		t.Fatalf("explicit refresh=%+v err=%v", got, err)
	}
	files, err := os.ReadDir(directory)
	if err != nil || len(files) != 1 {
		t.Fatalf("saved files=%d err=%v", len(files), err)
	}
	file := filepath.Join(directory, files[0].Name())
	if runtime.GOOS != "windows" {
		for _, path := range []string{directory, file} {
			info, errStat := os.Stat(path)
			if errStat != nil || info.Mode().Perm()&0077 != 0 {
				t.Fatalf("snapshot is not private: path=%s err=%v", path, errStat)
			}
		}
	}
	if errWrite := os.WriteFile(file, []byte(`not JSON`), 0600); errWrite != nil {
		t.Fatal(errWrite)
	}
	restarted := NewSnapshotCache(directory)
	restarted.now = cache.now
	got, err = restarted.Fetch(context.Background(), scope, false, load)
	if err != nil || got.OpenCount != 3 {
		t.Fatalf("corrupt saved snapshot was not replaced: %+v err=%v", got, err)
	}
	now = now.Add(25 * time.Hour)
	_, err = restarted.Fetch(context.Background(), scope, false, func(context.Context) (Snapshot, error) { return Snapshot{}, errors.New("private failure") })
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("expired snapshot must not be served: err=%v", err)
	}
}

func TestSnapshotCachePrunesExpiredFilesOnSave(t *testing.T) {
	now := time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)
	directory := t.TempDir()
	cache := NewSnapshotCache(directory)
	cache.now = func() time.Time { return now }
	load := func(context.Context) (Snapshot, error) { return Snapshot{OpenCount: 1}, nil }
	_, _ = cache.Fetch(context.Background(), SnapshotScope{Viewer: "person-id", Repository: "owner/old"}, false, load)
	files, _ := os.ReadDir(directory)
	for _, file := range files {
		if err := os.Chtimes(filepath.Join(directory, file.Name()), now, now); err != nil {
			t.Fatal(err)
		}
	}
	now = now.Add(25 * time.Hour)
	_, err := cache.Fetch(context.Background(), SnapshotScope{Viewer: "person-id", Repository: "owner/new"}, false, load)
	files, _ = os.ReadDir(directory)
	if err != nil || len(files) != 1 || len(cache.entries) != 1 {
		t.Fatalf("expired storage remained: files=%d memory=%d err=%v", len(files), len(cache.entries), err)
	}
}
