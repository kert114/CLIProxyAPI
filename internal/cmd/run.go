// Package cmd provides command-line interface functionality for the CLI Proxy API server.
// It includes authentication flows for various AI service providers, service startup,
// and other command-line operations.
package cmd

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/api"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/pluginhost"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy"
	log "github.com/sirupsen/logrus"
)

// StartService builds and runs the proxy service using the exported SDK.
// It creates a new proxy service instance, sets up signal handling for graceful shutdown,
// and starts the service with the provided configuration.
//
// Parameters:
//   - cfg: The application configuration
//   - configPath: The path to the configuration file
//   - localPassword: Optional password accepted for local management requests
func StartService(cfg *config.Config, configPath string, localPassword string) {
	StartServiceWithPluginHost(cfg, configPath, localPassword, nil)
}

// StartServiceWithPluginHost builds and runs the proxy service with a shared plugin host.
func StartServiceWithPluginHost(cfg *config.Config, configPath string, localPassword string, host *pluginhost.Host, serverOptions ...api.ServerOption) {
	builder := cliproxy.NewBuilder().
		WithConfig(cfg).
		WithConfigPath(configPath).
		WithLocalManagementPassword(localPassword)
	if host != nil {
		builder = builder.WithPluginHost(host)
	}
	if len(serverOptions) > 0 {
		builder = builder.WithServerOptions(serverOptions...)
	}

	var initialBudget time.Duration
	if cfg != nil {
		initialBudget = config.ShutdownDuration(cfg.ShutdownTimeoutSeconds)
	}
	ctxSignal, publishService, finish := standaloneSignalContext(initialBudget)
	defer finish()

	runCtx := ctxSignal
	if localPassword != "" {
		var keepAliveCancel context.CancelFunc
		runCtx, keepAliveCancel = context.WithCancel(ctxSignal)
		builder = builder.WithServerOptions(api.WithKeepAliveEndpoint(10*time.Second, func() {
			log.Warn("keep-alive endpoint idle for 10s, shutting down")
			keepAliveCancel()
		}))
	}

	service, err := builder.Build()
	if err != nil {
		log.Errorf("failed to build proxy service: %v", err)
		return
	}

	publishService(service)
	err = service.Run(runCtx)
	if err != nil && !errors.Is(err, context.Canceled) {
		log.Errorf("proxy service exited with error: %v", err)
	}
}

const standaloneExitGrace = 2 * time.Second

// standaloneSignalContext handles both legacy cancellation and the positive
// shutdown budget. Keeping the Run context alive through the drain keeps its
// workers available to serve admitted requests.
func standaloneSignalContext(initialBudget time.Duration) (context.Context, func(*cliproxy.Service), func()) {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	ctx, cancel := context.WithCancelCause(context.Background())
	var service atomic.Pointer[cliproxy.Service]
	ready := make(chan struct{})
	exiting := make(chan struct{})
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		select {
		case <-ctx.Done():
			return
		case sig := <-signals:
			cause := fmt.Errorf("%s signal received", sig)
			current := service.Load()
			budget := initialBudget
			if current != nil {
				budget = current.ShutdownTimeout()
			}
			if budget <= 0 {
				cancel(cause)
				return
			}

			// Arm the watchdog before any service work. A blocked log sink
			// must never hold up the process exit.
			watchdog := time.AfterFunc(min(budget, math.MaxInt64-standaloneExitGrace)+standaloneExitGrace, func() {
				go fmt.Fprintln(os.Stderr, "proxy shutdown exceeded its deadline; forcing exit")
				os.Exit(1)
			})
			defer watchdog.Stop()
			if current == nil {
				select {
				case <-ready:
					current = service.Load()
				case <-exiting:
					return
				}
			}
			_ = current.ShutdownWithBudget(context.Background(), budget)
			cancel(cause)
			<-exiting
		}
	}()

	publish := func(s *cliproxy.Service) {
		service.Store(s)
		close(ready)
	}
	finish := func() {
		cancel(nil)
		signal.Stop(signals)
		close(exiting)
		<-finished
	}
	return ctx, publish, finish
}

// StartServiceBackground starts the proxy service in a background goroutine
// and returns a cancel function for shutdown and a done channel.
func StartServiceBackground(cfg *config.Config, configPath string, localPassword string) (cancel func(), done <-chan struct{}) {
	return StartServiceBackgroundWithPluginHost(cfg, configPath, localPassword, nil)
}

// StartServiceBackgroundWithPluginHost starts the proxy service with a shared plugin host.
func StartServiceBackgroundWithPluginHost(cfg *config.Config, configPath string, localPassword string, host *pluginhost.Host, serverOptions ...api.ServerOption) (cancel func(), done <-chan struct{}) {
	builder := cliproxy.NewBuilder().
		WithConfig(cfg).
		WithConfigPath(configPath).
		WithLocalManagementPassword(localPassword)
	if host != nil {
		builder = builder.WithPluginHost(host)
	}
	if len(serverOptions) > 0 {
		builder = builder.WithServerOptions(serverOptions...)
	}

	ctx, cancelFn := context.WithCancel(context.Background())
	doneCh := make(chan struct{})

	service, err := builder.Build()
	if err != nil {
		log.Errorf("failed to build proxy service: %v", err)
		close(doneCh)
		return cancelFn, doneCh
	}

	go func() {
		defer close(doneCh)
		if err := service.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Errorf("proxy service exited with error: %v", err)
		}
	}()

	return cancelFn, doneCh
}

// WaitForCloudDeploy waits indefinitely for shutdown signals in cloud deploy mode
// when no configuration file is available.
func WaitForCloudDeploy() {
	// Clarify that we are intentionally idle for configuration and not running the API server.
	log.Info("Cloud deploy mode: No config found; standing by for configuration. API server is not started. Press Ctrl+C to exit.")

	ctxSignal, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// Block until shutdown signal is received
	<-ctxSignal.Done()
	log.Info("Cloud deploy mode: Shutdown signal received; exiting")
}
