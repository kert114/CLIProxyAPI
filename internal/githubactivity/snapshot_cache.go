package githubactivity

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

const snapshotFreshness = time.Minute
const snapshotRetention = 24 * time.Hour
const maxSnapshotBytes = 8 << 20
const maxSnapshotEntries = 64
const maxSnapshotCacheBytes = 64 << 20

type SnapshotScope struct {
	Viewer       string
	Repository   string
	Account      string
	Organization string
	Since        time.Time
	Until        time.Time
}

type savedSnapshot struct {
	Version  int       `json:"version"`
	Key      string    `json:"key"`
	SavedAt  time.Time `json:"saved_at"`
	Snapshot Snapshot  `json:"snapshot"`
}

type SnapshotCache struct {
	directory string
	now       func() time.Time
	mu        sync.Mutex
	entries   map[string]savedSnapshot
	flights   map[string]*snapshotFlight
	failedAt  map[string]time.Time
	diskMu    sync.Mutex
}

type snapshotFlight struct {
	done     chan struct{}
	snapshot Snapshot
	err      error
}

func NewSnapshotCache(directory string) *SnapshotCache {
	return &SnapshotCache{directory: directory, now: time.Now, entries: make(map[string]savedSnapshot), flights: make(map[string]*snapshotFlight), failedAt: make(map[string]time.Time)}
}

func (c *SnapshotCache) Fetch(ctx context.Context, scope SnapshotScope, force bool, load func(context.Context) (Snapshot, error)) (Snapshot, error) {
	if errContext := ctx.Err(); errContext != nil {
		return Snapshot{}, errContext
	}
	encoded, errEncode := json.Marshal(scope)
	if errEncode != nil {
		return Snapshot{}, ErrUnavailable
	}
	digest := sha256.Sum256(encoded)
	key := hex.EncodeToString(digest[:])
	c.mu.Lock()
	saved, exists := c.entries[key]
	if !exists {
		saved, exists = c.read(key)
	}
	if exists && !force && !c.failedAt[key].IsZero() && c.now().Sub(c.failedAt[key]) < snapshotFreshness && c.now().Sub(saved.SavedAt) < snapshotRetention {
		c.mu.Unlock()
		snapshot := saved.Snapshot
		snapshot.CacheStatus = "stale"
		snapshot.RefreshError = true
		return snapshot, nil
	}
	if exists && !force && c.now().Sub(saved.SavedAt) >= 0 && c.now().Sub(saved.SavedAt) < snapshotFreshness {
		c.entries[key] = saved
		c.pruneMemory()
		c.mu.Unlock()
		snapshot := saved.Snapshot
		snapshot.CacheStatus = "fresh"
		return snapshot, nil
	}
	flight, running := c.flights[key]
	if !running {
		flight = &snapshotFlight{done: make(chan struct{})}
		c.flights[key] = flight
		go c.refresh(context.WithoutCancel(ctx), key, saved, exists, flight, load)
	}
	c.mu.Unlock()
	select {
	case <-ctx.Done():
		return Snapshot{}, ctx.Err()
	case <-flight.done:
		return flight.snapshot, flight.err
	}
}

func (c *SnapshotCache) refresh(ctx context.Context, key string, saved savedSnapshot, exists bool, flight *snapshotFlight, load func(context.Context) (Snapshot, error)) {
	snapshot, errFetch := load(ctx)
	c.mu.Lock()
	if errFetch != nil {
		age := c.now().Sub(saved.SavedAt)
		if exists && age >= 0 && age < snapshotRetention {
			c.entries[key] = saved
			c.failedAt[key] = c.now()
			flight.snapshot = saved.Snapshot
			flight.snapshot.CacheStatus = "stale"
			flight.snapshot.RefreshError = true
		} else {
			flight.err = ErrUnavailable
		}
	} else {
		delete(c.failedAt, key)
		snapshot.CacheStatus = ""
		snapshot.RefreshError = false
		saved = savedSnapshot{Version: 1, Key: key, SavedAt: c.now(), Snapshot: snapshot}
		c.entries[key] = saved
		flight.snapshot = snapshot
		flight.snapshot.CacheStatus = "refreshed"
	}
	c.pruneMemory()
	c.mu.Unlock()
	if errFetch == nil {
		c.diskMu.Lock()
		c.write(saved)
		c.pruneDisk()
		c.diskMu.Unlock()
	}
	c.mu.Lock()
	delete(c.flights, key)
	close(flight.done)
	c.mu.Unlock()
}

func (c *SnapshotCache) pruneMemory() {
	for key, saved := range c.entries {
		if c.now().Sub(saved.SavedAt) >= snapshotRetention {
			delete(c.entries, key)
			delete(c.failedAt, key)
		}
	}
	for len(c.entries) > maxSnapshotEntries {
		oldest := ""
		for key, saved := range c.entries {
			if oldest == "" || saved.SavedAt.Before(c.entries[oldest].SavedAt) {
				oldest = key
			}
		}
		delete(c.entries, oldest)
		delete(c.failedAt, oldest)
	}
}

func (c *SnapshotCache) pruneDisk() {
	if c.directory == "" {
		return
	}
	entries, errRead := os.ReadDir(c.directory)
	if errRead != nil {
		return
	}
	files := make([]os.FileInfo, 0, len(entries))
	var bytes int64
	for _, entry := range entries {
		key := strings.TrimSuffix(entry.Name(), ".json")
		digest, errKey := hex.DecodeString(key)
		if errKey != nil || len(digest) != sha256.Size || entry.Name() != key+".json" || !entry.Type().IsRegular() {
			continue
		}
		info, errInfo := entry.Info()
		if errInfo != nil {
			continue
		}
		if c.now().Sub(info.ModTime()) >= snapshotRetention {
			c.remove(info.Name())
			continue
		}
		files = append(files, info)
		bytes += info.Size()
	}
	sort.Slice(files, func(i, j int) bool { return files[i].ModTime().Before(files[j].ModTime()) })
	for len(files) > maxSnapshotEntries || bytes > maxSnapshotCacheBytes {
		c.remove(files[0].Name())
		bytes -= files[0].Size()
		files = files[1:]
	}
}

func (c *SnapshotCache) remove(name string) {
	if errRemove := os.Remove(filepath.Join(c.directory, name)); errRemove != nil && !os.IsNotExist(errRemove) {
		log.Warn("GitHub activity snapshot could not be removed")
	}
}

func (c *SnapshotCache) read(key string) (savedSnapshot, bool) {
	if c.directory == "" {
		return savedSnapshot{}, false
	}
	file, errOpen := os.Open(filepath.Join(c.directory, key+".json"))
	if errOpen != nil {
		return savedSnapshot{}, false
	}
	defer func() {
		if errClose := file.Close(); errClose != nil {
			log.Warn("GitHub activity snapshot could not be closed")
		}
	}()
	raw, errRead := io.ReadAll(io.LimitReader(file, maxSnapshotBytes+1))
	var saved savedSnapshot
	if errRead != nil || len(raw) > maxSnapshotBytes || json.Unmarshal(raw, &saved) != nil || saved.Version != 1 || saved.Key != key {
		return savedSnapshot{}, false
	}
	age := c.now().Sub(saved.SavedAt)
	return saved, age >= 0 && age < snapshotRetention
}

func (c *SnapshotCache) write(saved savedSnapshot) {
	if c.directory == "" {
		return
	}
	raw, errEncode := json.Marshal(saved)
	if errEncode != nil || len(raw) > maxSnapshotBytes || os.MkdirAll(c.directory, 0700) != nil {
		log.Warn("GitHub activity snapshot could not be saved")
		return
	}
	file, errCreate := os.CreateTemp(c.directory, ".snapshot-*")
	if errCreate != nil {
		log.Warn("GitHub activity snapshot could not be saved")
		return
	}
	name := file.Name()
	defer func() {
		if errRemove := os.Remove(name); errRemove != nil && !os.IsNotExist(errRemove) {
			log.Warn("GitHub activity temporary snapshot could not be removed")
		}
	}()
	_, errWrite := file.Write(raw)
	errClose := file.Close()
	if errWrite == nil && errClose == nil {
		if errRename := os.Rename(name, filepath.Join(c.directory, saved.Key+".json")); errRename != nil {
			log.Warn("GitHub activity snapshot could not be saved")
		}
	} else {
		log.Warn("GitHub activity snapshot could not be saved")
	}
}
