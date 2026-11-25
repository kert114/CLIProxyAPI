package cliproxy

import (
	"context"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

type shutdownProbeExecutor struct {
	coreauth.ProviderExecutor
	started   chan context.Context
	sawCancel chan struct{}
	release   chan struct{}
}

func (*shutdownProbeExecutor) Identifier() string { return "shutdown-probe" }

func (x *shutdownProbeExecutor) Refresh(ctx context.Context, auth *coreauth.Auth) (*coreauth.Auth, error) {
	x.started <- ctx
	<-ctx.Done()
	close(x.sawCancel)
	<-x.release
	return auth, ctx.Err()
}

func TestShutdownRegressionRefreshDeadline(t *testing.T) {
	t.Run("zero", func(t *testing.T) { runShutdownRefreshProbe(t, t.TempDir(), 0) })
	t.Run("reload_to_zero", func(t *testing.T) { runShutdownRefreshProbe(t, t.TempDir(), 5) })
}

func runShutdownRefreshProbe(t *testing.T, directory string, timeout int) {
	address, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := address.Addr().(*net.TCPAddr).Port
	_ = address.Close()
	raw := fmt.Sprintf("server:\n  host: 127.0.0.1\n  port: %d\n  shutdown-timeout: %d\noauth:\n  auth-dir: %s\n", port, timeout, directory)
	cfg, err := config.ParseConfigBytes([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	cfg.KVStore.Disabled = true
	manager := coreauth.NewManager(nil, nil, nil)
	fake := &shutdownProbeExecutor{
		started:   make(chan context.Context, 1),
		sawCancel: make(chan struct{}),
		release:   make(chan struct{}),
	}
	defer close(fake.release)
	manager.RegisterExecutor(fake)
	if _, err := manager.Register(t.Context(), &coreauth.Auth{
		ID: "shutdown-probe", Provider: fake.Identifier(), Status: coreauth.StatusActive,
		Metadata: map[string]any{"refresh_interval_seconds": 1},
	}); err != nil {
		t.Fatal(err)
	}
	hookEntered := make(chan struct{})
	releaseHook := make(chan struct{})
	unblockHook := sync.OnceFunc(func() { close(releaseHook) })
	defer unblockHook()
	waitForCancellation := func() {
		close(hookEntered)
		<-releaseHook
	}
	hooks := Hooks{}
	if timeout == 0 {
		hooks.OnAfterStart = func(*Service) { waitForCancellation() }
	}
	service, err := NewBuilder().WithConfig(cfg).WithConfigPath(filepath.Join(directory, "fixture.yaml")).
		WithCoreAuthManager(manager).
		WithHooks(hooks).
		WithWatcherFactory(func(_, _ string, reload func(*config.Config)) (*WatcherWrapper, error) {
			return &WatcherWrapper{start: func(context.Context) error {
				if timeout > 0 {
					next := cfg.CloneForRuntime()
					next.ShutdownTimeoutSeconds = 0
					reload(next)
					waitForCancellation()
				}
				return nil
			}}, nil
		}).Build()
	if err != nil {
		t.Fatal(err)
	}
	runCtx, cancel := context.WithTimeout(t.Context(), 900*time.Millisecond)
	defer cancel()
	shutdown := make(chan error, 1)
	go func() { shutdown <- service.Run(runCtx) }()
	waitProbe(t, hookEntered, "startup hook", 5*time.Second)
	var refreshCtx context.Context
	select {
	case refreshCtx = <-fake.started:
	case <-time.After(7 * time.Second):
		t.Fatal("real auto-refresh loop did not start the fixture refresh")
	}
	_, hasDeadline := refreshCtx.Deadline()
	select {
	case <-fake.sawCancel:
		if !hasDeadline || refreshCtx.Err() != context.DeadlineExceeded || context.Cause(refreshCtx) != context.DeadlineExceeded {
			t.Errorf("worker deadline visible=%v; ctx.Err()=%v; context.Cause(ctx)=%v", hasDeadline, refreshCtx.Err(), context.Cause(refreshCtx))
		}
	case <-time.After(2 * time.Second):
		t.Error("timeout=0 did not propagate Run cancellation to the refresh while startup hook was in flight")
	}
	unblockHook()
	if err := <-shutdown; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	waitProbe(t, fake.sawCancel, "refresh cancellation after teardown", time.Second)
}

func waitProbe(t *testing.T, ch <-chan struct{}, what string, timeout time.Duration) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(timeout):
		t.Fatalf("timed out waiting for %s", what)
	}
}
