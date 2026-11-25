package cliproxy

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

type shutdownRefreshExecutor struct {
	coreauth.ProviderExecutor
	started  chan struct{}
	canceled chan struct{}
}

func (*shutdownRefreshExecutor) Identifier() string { return "shutdown-refresh-test" }

func (e *shutdownRefreshExecutor) Refresh(ctx context.Context, auth *coreauth.Auth) (*coreauth.Auth, error) {
	close(e.started)
	<-ctx.Done()
	close(e.canceled)
	return auth, ctx.Err()
}

func TestServiceRun_ZeroShutdownTimeoutCancelsRefreshDuringStartup(t *testing.T) {
	for _, tc := range []struct {
		name           string
		initialTimeout int
	}{
		{name: "configured_zero"},
		{name: "reloaded_to_zero", initialTimeout: 5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &config.Config{
				Host:                   "127.0.0.1",
				AuthDir:                t.TempDir(),
				ShutdownTimeoutSeconds: tc.initialTimeout,
			}
			cfg.KVStore.Disabled = true
			manager := coreauth.NewManager(nil, nil, nil)
			refresh := &shutdownRefreshExecutor{started: make(chan struct{}), canceled: make(chan struct{})}
			manager.RegisterExecutor(refresh)
			if _, errRegister := manager.Register(t.Context(), &coreauth.Auth{
				ID:       refresh.Identifier(),
				Provider: refresh.Identifier(),
				Status:   coreauth.StatusActive,
				Metadata: map[string]any{"refresh_interval_seconds": 1},
			}); errRegister != nil {
				t.Fatal(errRegister)
			}

			startupBlocked := make(chan struct{})
			releaseStartup := make(chan struct{})
			blockStartup := func() {
				close(startupBlocked)
				<-releaseStartup
			}
			hooks := Hooks{}
			if tc.initialTimeout == 0 {
				hooks.OnAfterStart = func(*Service) { blockStartup() }
			}
			service, errBuild := NewBuilder().WithConfig(cfg).
				WithConfigPath(filepath.Join(cfg.AuthDir, "config.yaml")).
				WithCoreAuthManager(manager).
				WithHooks(hooks).
				WithWatcherFactory(func(_, _ string, reload func(*config.Config)) (*WatcherWrapper, error) {
					return &WatcherWrapper{start: func(context.Context) error {
						if tc.initialTimeout > 0 {
							next := cfg.CloneForRuntime()
							next.ShutdownTimeoutSeconds = 0
							reload(next)
							blockStartup()
						}
						return nil
					}}, nil
				}).
				Build()
			if errBuild != nil {
				t.Fatal(errBuild)
			}

			runCtx, cancel := context.WithCancel(t.Context())
			runDone := make(chan error, 1)
			go func() { runDone <- service.Run(runCtx) }()
			t.Cleanup(func() {
				cancel()
				close(releaseStartup)
				select {
				case errRun := <-runDone:
					if !errors.Is(errRun, context.Canceled) {
						t.Errorf("Run returned %v, want context.Canceled", errRun)
					}
				case <-time.After(5 * time.Second):
					t.Error("Run did not return after releasing startup")
				}
			})
			select {
			case <-startupBlocked:
			case <-time.After(5 * time.Second):
				t.Fatal("startup did not reach the blocker")
			}
			if got := service.server.ShutdownTimeoutSeconds(); got != 0 {
				t.Fatalf("effective shutdown timeout = %d, want zero", got)
			}
			select {
			case <-refresh.started:
			case <-time.After(5 * time.Second):
				t.Fatal("auth refresh did not start")
			}

			cancel()
			select {
			case <-refresh.canceled:
			case <-time.After(time.Second):
				t.Fatal("zero shutdown timeout did not cancel the active auth refresh while startup was blocked")
			}
		})
	}
}
