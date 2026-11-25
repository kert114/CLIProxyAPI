package cliproxy

import (
	"context"
	"errors"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/pluginhost"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"
)

type blockingShutdownThinkingProvider struct {
	entered chan struct{}
	release chan struct{}
}

func (p blockingShutdownThinkingProvider) Identifier() string {
	close(p.entered)
	<-p.release
	return "shutdown-test"
}

func (blockingShutdownThinkingProvider) ApplyThinking(context.Context, pluginapi.ThinkingApplyRequest) (pluginapi.PayloadResponse, error) {
	return pluginapi.PayloadResponse{}, nil
}

func TestShutdownJoinsPluginTeardownPastDrainDeadline(t *testing.T) {
	for _, tc := range []struct {
		name string
		hold time.Duration
	}{
		{name: "initiating caller waits", hold: 3 * time.Second},
		{name: "later caller waits after timeout", hold: 6 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				host := pluginhost.New()
				provider := blockingShutdownThinkingProvider{entered: make(chan struct{}), release: make(chan struct{})}
				release := sync.OnceFunc(func() { close(provider.release) })
				defer release()
				host.RegisterPluginForTest("remaining", pluginapi.Plugin{
					Capabilities: pluginapi.Capabilities{ThinkingApplier: provider},
				})
				host.RegisterPluginForTest("unloading", pluginapi.Plugin{})
				// Unloading one plugin refreshes the other's thinking registration while
				// holding the host's apply lock.
				unloaded := make(chan bool, 1)
				go func() { unloaded <- host.UnloadPluginContext(t.Context(), "unloading") }()
				<-provider.entered

				service := &Service{
					cfg:             &config.Config{ShutdownTimeoutSeconds: 2},
					admissionClosed: make(chan struct{}),
					pluginHost:      host,
				}
				ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
				defer cancel()
				first := make(chan error, 1)
				go func() { first <- service.Shutdown(ctx) }()
				<-service.admissionClosed
				<-time.After(tc.hold)
				synctest.Wait()

				expired := ctx.Err() != nil
				if !expired {
					select {
					case err := <-first:
						t.Fatalf("Shutdown returned %v while the plugin apply lock was held", err)
					default:
					}
				} else {
					if err := <-first; !errors.Is(err, context.DeadlineExceeded) {
						t.Fatalf("Shutdown with the apply lock held returned %v, want deadline exceeded", err)
					}
					retryCtx, cancelRetry := context.WithTimeout(t.Context(), time.Second)
					defer cancelRetry()
					if err := service.Shutdown(retryCtx); !errors.Is(err, context.DeadlineExceeded) {
						t.Fatalf("fresh Shutdown with the apply lock held returned %v, want deadline exceeded", err)
					}
				}
				release()
				if !<-unloaded {
					t.Fatal("fixture plugin was not unloaded")
				}
				joinCtx, cancelJoin := context.WithTimeout(t.Context(), time.Second)
				defer cancelJoin()
				if err := service.Shutdown(joinCtx); err != nil {
					t.Fatalf("fresh Shutdown: %v", err)
				}
				if !expired {
					if err := <-first; err != nil {
						t.Fatalf("initiating Shutdown: %v", err)
					}
				}
				if plugins := host.RegisteredPlugins(); len(plugins) != 0 {
					t.Fatalf("Shutdown reported success with %d plugins still registered", len(plugins))
				}
			})
		})
	}
}
