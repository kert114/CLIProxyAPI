package cliproxy

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
)

func TestServiceRun_CancelsInFlightWatcherReload(t *testing.T) {
	cfg := &config.Config{Host: "127.0.0.1", AuthDir: t.TempDir()}
	cfg.KVStore.Disabled = true
	service, err := NewBuilder().WithConfig(cfg).
		WithConfigPath(filepath.Join(cfg.AuthDir, "fixture.yaml")).
		WithWatcherFactory(func(_ string, _ string, reload func(*config.Config)) (*WatcherWrapper, error) {
			return &WatcherWrapper{start: func(context.Context) error {
				go reload(cfg.CloneForRuntime())
				return nil
			}}, nil
		}).Build()
	if err != nil {
		t.Fatal(err)
	}
	entered, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	service.updateServerClientsContextFn = func(ctx context.Context, _ *config.Config) bool {
		close(entered)
		select {
		case <-ctx.Done():
			close(canceled)
		case <-release:
		}
		return false
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { defer close(done); _ = service.Run(ctx) }()
	defer func() {
		cancel()
		close(release)
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("fixture service did not exit")
		}
	}()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("watcher did not begin reloading")
	}
	cancel()
	select {
	case <-canceled:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("watcher reload did not receive service cancellation")
	}
}
