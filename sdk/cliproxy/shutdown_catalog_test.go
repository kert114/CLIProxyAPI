package cliproxy

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/config"
)

type blockedCatalogPolicyRead struct {
	context.Context
	armed   chan struct{}
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (ctx *blockedCatalogPolicyRead) Err() error {
	select {
	case <-ctx.armed:
		ctx.once.Do(func() {
			close(ctx.entered)
			<-ctx.release
		})
	default:
	}
	return ctx.Context.Err()
}

func TestShutdownFencesPendingCatalogPolicyUpdate(t *testing.T) {
	runCtx, cancelRun := context.WithCancel(t.Context())
	defer cancelRun()
	policyCtx := &blockedCatalogPolicyRead{
		Context: runCtx,
		armed:   make(chan struct{}),
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	release := sync.OnceFunc(func() { close(policyCtx.release) })
	defer release()

	registry.SetLocalModelCatalogs(true)
	initial := config.ModelCatalogs{}
	service := &Service{
		cfg:             &config.Config{Models: initial, ShutdownTimeoutSeconds: 2},
		admissionClosed: make(chan struct{}),
		runCancel:       cancelRun,
	}
	registry.StartModelCatalogUpdaters(policyCtx, initial, false)
	t.Cleanup(func() { registry.UpdateModelCatalogSources(initial, false) })
	current := service.currentConfig().CloneForRuntime()
	catalog, err := filepath.Abs("../../internal/registry/models/models.json")
	if err != nil {
		t.Fatal(err)
	}
	current.Models.Catalog = catalog
	if commit := service.commitConfigUpdate(current); commit.cfg == nil {
		t.Fatal("ordinary catalog reload was rejected")
	}
	refreshed, err := registry.RefreshModelCatalogs(t.Context())
	if err != nil || refreshed.Sources.Catalog != catalog {
		t.Fatalf("ordinary reload's catalog = %q, err = %v, want %q", refreshed.Sources.Catalog, err, catalog)
	}

	// This policy lookup holds the same registry lock a watcher reload needs.
	close(policyCtx.armed)
	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		_, _ = registry.RefreshModelCatalogsWithPolicy(runCtx, initial, false)
	}()
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	select {
	case <-policyCtx.entered:
	case <-deadline.C:
		t.Fatal("catalog policy lookup did not acquire the lock")
	}

	next := current.CloneForRuntime()
	next.Models.Catalog = filepath.Join(t.TempDir(), "late-catalog.json")
	data, err := os.ReadFile(catalog)
	if err != nil {
		t.Fatal(err)
	}
	if errWrite := os.WriteFile(next.Models.Catalog, data, 0o600); errWrite != nil {
		t.Fatal(errWrite)
	}
	reloadDone := make(chan struct{})
	go func() {
		defer close(reloadDone)
		service.commitConfigUpdate(next)
	}()
	stack := make([]byte, 1<<20)
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for !bytes.Contains(stack[:runtime.Stack(stack, true)], []byte("registry.UpdateModelCatalogSources")) {
		select {
		case <-ticker.C:
		case <-deadline.C:
			t.Fatal("reload did not reach the held catalog policy lock")
		}
	}

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if err := service.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown with the catalog mutex held: %v", err)
	}
	release()
	select {
	case <-readDone:
	case <-time.After(3 * time.Second):
		t.Fatal("policy lookup did not finish")
	}
	select {
	case <-reloadDone:
	case <-time.After(3 * time.Second):
		t.Fatal("pending catalog reload did not finish")
	}
	refreshed, err = registry.RefreshModelCatalogs(t.Context())
	if err != nil || refreshed.Sources.Catalog != catalog {
		t.Fatalf("catalog after abandoned reload = %q, err = %v, want %q", refreshed.Sources.Catalog, err, catalog)
	}
}
