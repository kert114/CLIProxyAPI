package cliproxy

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/api"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/home"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/redisqueue"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/runtime/executor"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/runtime/executor/helps"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/shutdown"
	sdkaccess "github.com/router-for-me/CLIProxyAPI/v8/sdk/access"
	sdkAuth "github.com/router-for-me/CLIProxyAPI/v8/sdk/auth"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/usage"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/config"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v8/sdk/translator"
	log "github.com/sirupsen/logrus"
)

const serviceShutdownTimeout = 30 * time.Second

// Run starts the service and blocks until the context is cancelled or the server stops.
// It initializes all components including authentication, file watching, HTTP server,
// and starts processing requests. The method blocks until the context is cancelled.
//
// Parameters:
//   - ctx: The context for controlling the service lifecycle
//
// Returns:
//   - error: An error if the service fails to start or run
func (s *Service) Run(ctx context.Context) error {
	if s == nil {
		return fmt.Errorf("cliproxy: service is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, runCancel := context.WithCancel(ctx)
	// Use the same live timeout for parent cancellation and HTTP teardown.
	drainTimeout := sync.OnceValue(s.shutdownTimeoutSeconds)
	runtimeCtx, cancelRuntime := context.WithCancelCause(context.WithoutCancel(ctx))
	runtimeCancel := func() { cancelRuntime(context.Cause(ctx)) }
	stopRuntimeCancel := context.AfterFunc(ctx, func() {
		if drainTimeout() <= 0 {
			runtimeCancel()
		}
	})
	defer stopRuntimeCancel()
	s.cfgMu.Lock()
	s.antigravityContext = runtimeCtx
	s.cfgMu.Unlock()
	s.homeMu.Lock()
	s.runCancel = runCancel
	s.runtimeCancel = runtimeCancel
	s.runDrainTimeout = drainTimeout
	s.homeMu.Unlock()
	defer func() {
		runCancel()
		runtimeCancel()
		s.homeMu.Lock()
		s.runCancel = nil
		s.runtimeCancel = nil
		s.runDrainTimeout = nil
		s.homeMu.Unlock()
	}()

	s.startModelCatalogUpdaters(runtimeCtx)

	usage.StartDefault(runtimeCtx)
	cfg := s.currentConfig()
	homeEnabled := cfg != nil && cfg.Home.Enabled
	if homeEnabled {
		s.cfgMu.Lock()
		forceHomeRuntimeConfig(s.cfg)
		cfg = s.cfg
		s.cfgMu.Unlock()
		s.applyKVStoreConfig(cfg)
		redisqueue.SetUsageStatisticsEnabled(true)
	}

	defer func() {
		if errShutdown := s.Shutdown(context.Background()); errShutdown != nil {
			log.Errorf("service shutdown returned error: %v", errShutdown)
		}
	}()

	if !homeEnabled {
		if errEnsureAuthDir := s.ensureAuthDir(); errEnsureAuthDir != nil {
			return errEnsureAuthDir
		}
		s.applyKVStoreConfig(cfg)
	}

	s.applyRetryConfig(cfg)
	s.configureCooldownStateStore(cfg)

	s.registerPluginAuthParser()
	if s.coreManager != nil && !homeEnabled {
		if errLoad := s.coreManager.Load(ctx); errLoad != nil {
			log.Warnf("failed to load auth store: %v", errLoad)
		}
		s.registerConfigAPIKeyAuths(coreauth.WithSkipPersist(ctx), cfg)
		if cfg != nil && cfg.SaveCooldownStatus {
			if errRestoreCooldown := s.coreManager.RestoreCooldownStates(ctx); errRestoreCooldown != nil {
				log.Warnf("failed to restore cooldown state: %v", errRestoreCooldown)
			}
		}
		s.registerAvailableExecutors(ctx, executorRegistrationOptions{
			includeBaseline: true,
			auths:           s.coreManager.List(),
		})
		interval := 15 * time.Minute
		s.coreManager.StartAutoRefresh(runtimeCtx, interval)
		log.Infof("core auth auto-refresh started (interval=%s)", interval)
	}

	if !homeEnabled {
		tokenResult, err := s.tokenProvider.Load(ctx, cfg)
		if err != nil && !errors.Is(err, context.Canceled) {
			return err
		}
		if tokenResult == nil {
			tokenResult = &TokenClientResult{}
		}

		apiKeyResult, err := s.apiKeyProvider.Load(ctx, cfg)
		if err != nil && !errors.Is(err, context.Canceled) {
			return err
		}
		if apiKeyResult == nil {
			apiKeyResult = &APIKeyClientResult{}
		}
	}

	// legacy clients removed; no caches to refresh

	s.ensureWebsocketGateway()
	if homeEnabled {
		s.registerAvailableExecutors(ctx, executorRegistrationOptions{
			includeBaseline: true,
		})
		// Home mode does not expose in-process Redis RESP usage output; usage is forwarded to home instead.
		redisqueue.SetEnabled(true)
	}

	// handlers no longer depend on legacy clients; pass nil slice initially
	server := api.NewServer(cfg, s.coreManager, s.accessManager, s.configPath, s.serverOptions...)
	s.cfgMu.Lock()
	s.server = server
	s.cfgMu.Unlock()
	s.syncPluginRuntimeConfig(ctx)
	if homeEnabled {
		s.configRuntimeMu.Lock()
		s.syncPluginModelRuntime(ctx)
		s.configRuntimeMu.Unlock()
	}

	if s.authManager == nil {
		s.authManager = newDefaultAuthManager()
	}

	if homeEnabled {
		s.startHomeSubscriber(runtimeCtx)
	}

	if s.server != nil && s.wsGateway != nil {
		s.server.AttachWebsocketRoute(s.wsGateway.Path(), s.wsGateway.Handler())
		s.server.SetWebsocketAuthChangeHandler(func(oldEnabled, newEnabled bool) {
			if oldEnabled == newEnabled {
				return
			}
			if !oldEnabled && newEnabled {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if errStop := s.wsGateway.Stop(ctx); errStop != nil {
					log.Warnf("failed to reset websocket connections after ws-auth change %t -> %t: %v", oldEnabled, newEnabled, errStop)
					return
				}
				log.Debugf("ws-auth enabled; existing websocket sessions terminated to enforce authentication")
				return
			}
			log.Debugf("ws-auth disabled; existing websocket sessions remain connected")
		})
	}

	if s.hooks.OnBeforeStart != nil {
		s.hooks.OnBeforeStart(s.currentConfig())
	}

	s.serverErr = make(chan error, 1)
	go func() {
		if errStart := s.server.Start(); errStart != nil {
			s.serverErr <- errStart
		} else {
			s.serverErr <- nil
		}
	}()

	time.Sleep(100 * time.Millisecond)
	if currentCfg := s.currentConfig(); currentCfg != nil {
		fmt.Printf("API server started successfully on: %s:%d\n", currentCfg.Host, currentCfg.Port)
	}

	s.configRuntimeMu.Lock()
	currentCfg := s.currentConfig()
	s.applyPprofConfig(currentCfg)
	s.applyDiscoveryConfig(currentCfg)
	s.configRuntimeMu.Unlock()

	if s.hooks.OnAfterStart != nil {
		s.hooks.OnAfterStart(s)
	}

	if !homeEnabled {
		var watcherWrapper *WatcherWrapper
		watcherCtx, watcherCancel := context.WithCancel(runtimeCtx)
		reloadCallback := func(newCfg *config.Config) { s.applyWatcherConfigUpdate(watcherCtx, newCfg) }

		authDir := ""
		if currentCfg := s.currentConfig(); currentCfg != nil {
			authDir = currentCfg.AuthDir
		}
		watcherWrapper, errCreate := s.watcherFactory(s.configPath, authDir, reloadCallback)
		if errCreate != nil {
			watcherCancel()
			return fmt.Errorf("cliproxy: failed to create watcher: %w", errCreate)
		}
		s.setWatcher(watcherWrapper)
		s.ensureAuthUpdateQueue(runtimeCtx)
		if s.authUpdates != nil {
			watcherWrapper.SetAuthUpdateQueue(s.authUpdates)
		}
		watcherWrapper.SetConfig(s.currentConfig())
		s.registerPluginAuthParser()

		s.cfgMu.Lock()
		s.watcherCancel = watcherCancel
		s.cfgMu.Unlock()
		if errStart := watcherWrapper.Start(watcherCtx); errStart != nil {
			return fmt.Errorf("cliproxy: failed to start watcher: %w", errStart)
		}
		log.Info("file watcher started for config and auth directory changes")
		s.configRuntimeMu.Lock()
		s.syncPluginModelRuntime(ctx)
		s.configRuntimeMu.Unlock()
	}

	s.registerModelRefreshCallback()
	if !homeEnabled {
		go s.runAntigravityModelRefresh(runtimeCtx)
	}

	select {
	case <-ctx.Done():
		log.Debug("service context cancelled, shutting down...")
		return ctx.Err()
	case errServer := <-s.serverErr:
		return errServer
	}
}

func (s *Service) shutdownTimeoutSeconds() int {
	s.cfgMu.RLock()
	server := s.server
	if server == nil && s.cfg != nil {
		timeout := s.cfg.ShutdownTimeoutSeconds
		s.cfgMu.RUnlock()
		return timeout
	}
	s.cfgMu.RUnlock()
	return server.ShutdownTimeoutSeconds()
}

// Shutdown stops background workers and shuts down the HTTP server.
// It ensures all resources are properly cleaned up and connections are closed.
// The shutdown is idempotent and can be called multiple times safely.
//
// Parameters:
//   - ctx: The context for controlling the shutdown timeout
//
// Returns:
//   - error: An error if shutdown fails
func (s *Service) Shutdown(ctx context.Context) error {
	if s == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, serviceShutdownTimeout)
	defer cancel()
	first := false
	s.shutdownOnce.Do(func() {
		first = true
		s.shutdownDone = make(chan struct{})
	})
	if !first {
		return shutdown.Wait(ctx, s.shutdownDone)
	}
	defer close(s.shutdownDone)

	s.homeMu.Lock()
	runCancel := s.runCancel
	runtimeCancel := s.runtimeCancel
	runDrainTimeout := s.runDrainTimeout
	s.homeMu.Unlock()
	if runDrainTimeout == nil {
		runDrainTimeout = s.shutdownTimeoutSeconds
	}
	drainTimeout := runDrainTimeout()
	s.cfgMu.RLock()
	server := s.server
	watcherCancel := s.watcherCancel
	s.cfgMu.RUnlock()

	var shutdownErr error
	recordError := func(component string, err error) {
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil && errors.Is(err, ctxErr) {
				log.WithError(err).Debugf("stopped waiting for %s", component)
			} else {
				log.WithError(err).Errorf("failed to stop %s", component)
			}
			if shutdownErr == nil {
				shutdownErr = err
			}
		}
	}
	if server != nil && drainTimeout > 0 {
		// Keep the request dependencies alive while the HTTP listener drains.
		recordError("API server", server.StopWithDrainTimeout(ctx, drainTimeout))
	}
	if runCancel != nil {
		runCancel()
	}
	if runtimeCancel != nil {
		runtimeCancel()
	}
	if watcherCancel != nil {
		watcherCancel()
	}
	if server != nil && drainTimeout <= 0 {
		recordError("API server", server.StopWithDrainTimeout(ctx, drainTimeout))
	}

	// Native transports must be closed even if the HTTP drain used the entire
	// deadline. The legacy component APIs below continue their cleanup if the
	// caller runs out of time to wait for a callback, filesystem, or library.
	if s.homeLifecycleMu.TryLock() {
		s.shutdownHome(ctx)
		s.homeLifecycleMu.Unlock()
	} else {
		recordError("Home workers", shutdown.Run(ctx, func() error {
			s.homeLifecycleMu.Lock()
			defer s.homeLifecycleMu.Unlock()
			s.shutdownHome(ctx)
			return nil
		}))
	}
	if s.wsGateway != nil {
		recordError("websocket gateway", s.wsGateway.Stop(ctx))
	}
	recordError("pprof server", s.shutdownPprof(ctx))

	if s.coreManager != nil {
		recordError("auth refresh and selector", shutdown.Run(ctx, func() error {
			s.coreManager.StopAutoRefresh()
			return nil
		}))
	}
	if watcher := s.currentWatcher(); watcher != nil {
		recordError("file watcher", shutdown.Run(ctx, watcher.Stop))
	}
	if s.authQueueStop != nil {
		s.authQueueStop()
		s.authQueueStop = nil
	}
	recordError("discovery advertiser", s.shutdownDiscovery(ctx))

	if s.pluginHost != nil {
		recordError("plugins", shutdown.Run(ctx, func() error {
			s.shutdownPlugins(ctx)
			return nil
		}))
	}
	if s.kvStoreManaged.Swap(false) {
		recordError("local kv store", s.shutdownKVStore(ctx))
	}
	helps.RetireProxyPoolPublisher(s.proxyPoolPublisher)
	usage.StopDefault()
	return shutdownErr
}

func (s *Service) shutdownHome(ctx context.Context) {
	homeSupervisorPending := false
	if supervisor := s.homeSupervisor; supervisor != nil {
		supervisor.cancel()
		select {
		case <-supervisor.done:
		case <-ctx.Done():
			if supervisor.cancelDrain != nil {
				supervisor.cancelDrain()
			}
			homeSupervisorPending = true
		}
	}
	s.homeMu.Lock()
	homeCancel := s.homeCancel
	homeClient := s.homeClient
	homeRegistry := s.homeRegistry
	homeDispatchBundle := s.homeDispatchBundle
	homeForwarder := s.homeLogForwarder
	homeForwarderClient := s.homeLogForwarderClient
	s.homeGeneration++
	s.homeCancel = nil
	s.homeClient = nil
	s.homeRegistry = nil
	s.homeDispatchBundle = nil
	s.homeDrainBound = 0
	s.homeLogForwarder = nil
	s.homeLogForwarderClient = nil
	s.homeMu.Unlock()
	if s.coreManager != nil {
		s.coreManager.ClearHomeDispatchBundle(homeDispatchBundle)
	}
	home.ClearCurrentIf(homeClient)
	if homeCancel != nil {
		homeCancel()
	}
	// A supervisor still exiting owns the registry drain and client close.
	if homeClient != nil {
		if homeSupervisorPending {
			homeClient.FenceDispatch()
		} else {
			_ = shutdown.Run(ctx, func() error {
				homeClient.Close()
				return nil
			})
		}
	}
	if homeForwarder != nil {
		if homeForwarderClient == homeClient {
			homeForwarder.Deactivate(homeClient)
		}
		homeForwarder.StopContext(ctx)
	}
	if homeRegistry != nil && !homeSupervisorPending {
		if errClose := shutdown.Run(ctx, homeRegistry.Close); errClose != nil {
			log.WithError(errClose).Warn("failed to close Home execution registry during shutdown")
		}
	}
}

func (s *Service) shutdownPlugins(ctx context.Context) {
	sdktranslator.SetPluginHooks(nil)
	sdkAuth.RegisterPluginAuthParser(nil)
	if watcher := s.currentWatcher(); watcher != nil {
		watcher.SetPluginAuthParser(nil)
	}
	s.pluginHost.ApplyConfig(ctx, &config.Config{})
	s.pluginHost.RegisterModels(ctx, registry.GetGlobalRegistry())
	s.registerAvailableExecutors(ctx, executorRegistrationOptions{
		includePlugins: true,
	})
	s.pluginHost.RegisterFrontendAuthProviders()
	s.pluginHost.ShutdownAllContext(ctx)
	if s.accessManager != nil {
		s.accessManager.SetProviders(sdkaccess.RegisteredProviders())
	}
}

func (s *Service) ensureAuthDir() error {
	cfg := s.currentConfig()
	authDir := ""
	if cfg != nil {
		authDir = cfg.AuthDir
	}
	info, err := os.Stat(authDir)
	if err != nil {
		if os.IsNotExist(err) {
			if mkErr := os.MkdirAll(authDir, 0o755); mkErr != nil {
				return fmt.Errorf("cliproxy: failed to create auth directory %s: %w", authDir, mkErr)
			}
			log.Infof("created missing auth directory: %s", authDir)
			return nil
		}
		return fmt.Errorf("cliproxy: error checking auth directory %s: %w", authDir, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("cliproxy: auth path exists but is not a directory: %s", authDir)
	}
	return nil
}

// startModelCatalogUpdaters applies the same catalog policy for SDK and CLI users.
func (s *Service) startModelCatalogUpdaters(ctx context.Context) {
	s.cfgMu.RLock()
	cfg := s.cfg
	s.cfgMu.RUnlock()
	proxyURL := ""
	if cfg != nil {
		registry.StartModelCatalogUpdaters(ctx, cfg.Models, cfg.Home.Enabled)
		proxyURL = cfg.ProxyURL
	}
	executor.StartXAIVersionUpdater(ctx, proxyURL)
}
