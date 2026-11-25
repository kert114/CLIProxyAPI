package cliproxy

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/api"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/home"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/kvstore"
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
	startupCtx, cancelStartup := context.WithCancel(ctx)
	defer cancelStartup()
	defer runCancel()

	// Freeze the live shutdown setting when stopping so worker cancellation and
	// teardown agree, including when a reload changed the mode during Run.
	var shutdownModeFrozen atomic.Bool
	drainTimeout := sync.OnceValue(func() int {
		timeout := s.shutdownTimeoutSeconds()
		shutdownModeFrozen.Store(true)
		return timeout
	})
	workerTimeout := func() int {
		if shutdownModeFrozen.Load() {
			return drainTimeout()
		}
		return s.shutdownTimeoutSeconds()
	}
	detached, cancelRuntime := context.WithCancelCause(context.WithoutCancel(ctx))
	workers := serviceWorkerContext{
		Context: detached,
		parent:  ctx,
		timeout: workerTimeout,
		done:    make(chan struct{}),
	}
	runtimeCancel := sync.OnceFunc(func() {
		cancelRuntime(context.Cause(ctx))
		close(workers.done)
	})
	stopRuntimeCancel := context.AfterFunc(ctx, func() {
		if drainTimeout() <= 0 {
			runtimeCancel()
		}
	})
	defer stopRuntimeCancel()
	defer runtimeCancel()

	s.cfgMu.Lock()
	if s.stopping {
		s.cfgMu.Unlock()
		return context.Canceled
	}
	s.antigravityContext = workers
	s.cancelStartup = cancelStartup
	s.homeMu.Lock()
	s.runCancel = runCancel
	s.runtimeCancel = runtimeCancel
	s.runDrainTimeout = drainTimeout
	s.homeMu.Unlock()
	s.cfgMu.Unlock()

	result := make(chan error, 1)
	go func() {
		var err error
		defer func() { result <- err }()
		err = s.run(startupCtx, workers)
	}()
	var errRun error
	select {
	case errRun = <-result:
	case <-ctx.Done():
		if drainTimeout() <= 0 {
			runtimeCancel()
			errRun = <-result
		} else {
			errRun = ctx.Err()
		}
	}
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), serviceShutdownTimeout)
	defer cancelShutdown()
	if errShutdown := s.Shutdown(shutdownCtx); errShutdown != nil {
		if drainTimeout() > 0 {
			go log.Errorf("service shutdown returned error: %v", errShutdown)
		} else {
			log.Errorf("service shutdown returned error: %v", errShutdown)
		}
	}
	return errRun
}

// serviceWorkerContext defers cancellation while draining. In zero mode its
// children observe the deadline and cancellation error of the original context.
type serviceWorkerContext struct {
	context.Context
	parent  context.Context
	timeout func() int
	done    chan struct{}
}

// Done deliberately differs from the embedded context's channel so derived
// contexts use this wrapper's Err when propagating cancellation.
func (c serviceWorkerContext) Done() <-chan struct{} { return c.done }

func (c serviceWorkerContext) Deadline() (time.Time, bool) {
	if c.timeout() <= 0 {
		return c.parent.Deadline()
	}
	return time.Time{}, false
}

func (c serviceWorkerContext) Err() error {
	err := c.Context.Err()
	if err != nil && c.timeout() <= 0 && c.parent.Err() != nil {
		return c.parent.Err()
	}
	return err
}

func (s *Service) run(ctx, runtimeCtx context.Context) error {

	s.startModelCatalogUpdaters(runtimeCtx)

	usage.StartDefault(runtimeCtx)
	cfg := s.currentConfig()
	homeEnabled := cfg != nil && cfg.Home.Enabled
	if homeEnabled {
		s.cfgMu.Lock()
		forceHomeRuntimeConfig(s.cfg)
		cfg = s.cfg
		s.cfgMu.Unlock()
		s.applyKVStoreConfigContext(ctx, cfg)
		redisqueue.SetUsageStatisticsEnabled(true)
	}

	if !homeEnabled {
		if errEnsureAuthDir := s.ensureAuthDir(); errEnsureAuthDir != nil {
			return errEnsureAuthDir
		}
		s.applyKVStoreConfigContext(ctx, cfg)
	}

	if s.startupStopped() {
		return context.Canceled
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
		if s.startupStopped() {
			return context.Canceled
		}
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

	if s.startupStopped() {
		return context.Canceled
	}
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
	if s.stopping {
		s.cfgMu.Unlock()
		return context.Canceled
	}
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
		s.startHomeSubscriberWithStartup(runtimeCtx, ctx)
	}

	if s.server != nil && s.wsGateway != nil {
		s.server.AttachWebsocketRoute(s.wsGateway.Path(), s.wsGateway.Handler())
		s.server.SetWebsocketAuthChangeHandler(func(oldEnabled, newEnabled bool) {
			if oldEnabled == newEnabled {
				return
			}
			if !oldEnabled && newEnabled {
				if s.shutdownTimeoutSeconds() <= 0 {
					_ = s.wsGateway.StopLegacy()
					return
				}
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

	if s.startupStopped() {
		return context.Canceled
	}
	serverErr := make(chan error, 1)
	go func() { serverErr <- server.Start() }()

	time.Sleep(100 * time.Millisecond)
	if currentCfg := s.currentConfig(); currentCfg != nil {
		fmt.Printf("API server started successfully on: %s:%d\n", currentCfg.Host, currentCfg.Port)
	}

	if s.startupStopped() {
		return context.Canceled
	}
	s.configRuntimeMu.Lock()
	if !s.startupStopped() {
		currentCfg := s.currentConfig()
		if s.shutdownTimeoutSeconds() > 0 {
			s.applyPprofConfigContext(ctx, currentCfg)
			s.applyDiscoveryConfigContext(ctx, currentCfg)
		} else {
			s.applyPprofConfig(currentCfg)
			s.applyDiscoveryConfig(currentCfg)
		}
	}
	s.configRuntimeMu.Unlock()
	if s.startupStopped() {
		return context.Canceled
	}

	if s.hooks.OnAfterStart != nil {
		s.hooks.OnAfterStart(s)
	}

	if s.startupStopped() {
		return context.Canceled
	}
	if !homeEnabled {
		var watcherWrapper *WatcherWrapper
		watcherCtx, watcherCancel := context.WithCancel(context.Background())
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
		s.cfgMu.Lock()
		if s.stopping {
			s.cfgMu.Unlock()
			watcherCancel()
			return context.Canceled
		}
		s.watcher = watcherWrapper
		s.watcherCancel = watcherCancel
		s.cfgMu.Unlock()
		s.ensureAuthUpdateQueue(runtimeCtx)
		watcherWrapper.SetAuthUpdateQueue(s.authUpdates)
		watcherWrapper.SetConfig(s.currentConfig())
		s.registerPluginAuthParser()

		if errStart := watcherWrapper.Start(watcherCtx); errStart != nil {
			return fmt.Errorf("cliproxy: failed to start watcher: %w", errStart)
		}
		log.Info("file watcher started for config and auth directory changes")
		s.configRuntimeMu.Lock()
		s.syncPluginModelRuntime(ctx)
		s.configRuntimeMu.Unlock()
	}

	if s.startupStopped() {
		return context.Canceled
	}
	s.registerModelRefreshCallback()
	if !homeEnabled {
		go s.runAntigravityModelRefresh(runtimeCtx)
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case errServer := <-serverErr:
		return errServer
	}
}

func (s *Service) startupStopped() bool {
	s.cfgMu.RLock()
	defer s.cfgMu.RUnlock()
	return s.stopping
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
	s.homeMu.Lock()
	drainTimeoutForRun := s.runDrainTimeout
	s.homeMu.Unlock()
	drainTimeout := s.shutdownTimeoutSeconds()
	if drainTimeoutForRun != nil {
		drainTimeout = drainTimeoutForRun()
	}
	if drainTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, serviceShutdownTimeout)
		defer cancel()
	}
	first := false
	s.shutdownOnce.Do(func() {
		first = true
		s.shutdownDone = make(chan struct{})
	})
	if !first {
		if drainTimeout <= 0 {
			<-s.shutdownDone
			return nil
		}
		return shutdown.Wait(ctx, s.shutdownDone)
	}
	defer close(s.shutdownDone)
	if drainTimeout <= 0 {
		return s.shutdownLegacy(ctx)
	}

	s.cfgMu.Lock()
	s.stopping = true
	server := s.server
	watcherCancel := s.watcherCancel
	cancelStartup := s.cancelStartup
	gateway := s.wsGateway
	authQueueStop := s.authQueueStop
	s.cfgMu.Unlock()
	if cancelStartup != nil {
		cancelStartup()
	}

	s.homeMu.Lock()
	runCancel := s.runCancel
	runtimeCancel := s.runtimeCancel
	s.homeMu.Unlock()

	var shutdownErr error
	recordError := func(component string, err error) {
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil && errors.Is(err, ctxErr) {
				go log.WithError(err).Debugf("stopped waiting for %s", component)
			} else {
				go log.WithError(err).Errorf("failed to stop %s", component)
			}
			if shutdownErr == nil {
				shutdownErr = err
			}
		}
	}
	if server != nil {
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
	if gateway != nil {
		recordError("websocket gateway", gateway.Shutdown(ctx))
	}
	recordError("pprof server", s.shutdownPprof(ctx))

	recordError("Home workers", shutdown.Run(ctx, func() error {
		s.homeLifecycleMu.Lock()
		defer s.homeLifecycleMu.Unlock()
		s.shutdownHome(ctx)
		return nil
	}))

	if s.coreManager != nil {
		recordError("auth refresh and selector", shutdown.Run(ctx, func() error {
			s.coreManager.StopAutoRefresh()
			return nil
		}))
	}
	if watcher := s.currentWatcher(); watcher != nil {
		recordError("file watcher", shutdown.Run(ctx, watcher.Stop))
	}
	if authQueueStop != nil {
		authQueueStop()
	}
	recordError("discovery advertiser", s.shutdownDiscovery(ctx))

	if s.pluginHost != nil {
		s.pluginHost.RetireSharedRuntime()
		recordError("plugins", shutdown.Run(ctx, func() error {
			s.shutdownPlugins(ctx)
			return nil
		}))
	}
	if s.kvStoreManaged.Swap(false) {
		recordError("local kv store", s.shutdownKVStore(ctx))
	}
	recordError("proxy pool", helps.RetireProxyPoolPublisherContext(ctx, s.proxyPoolPublisher))
	usage.StopDefault()
	return shutdownErr
}

func (s *Service) shutdownLegacy(ctx context.Context) error {
	var shutdownErr error
	s.homeMu.Lock()
	runCancel := s.runCancel
	runtimeCancel := s.runtimeCancel
	s.homeMu.Unlock()
	if runCancel != nil {
		runCancel()
	}
	if runtimeCancel != nil {
		runtimeCancel()
	}

	s.homeLifecycleMu.Lock()
	if supervisor := s.homeSupervisor; supervisor != nil {
		s.homeConfigCommitMu.Lock()
		supervisor.cancel()
		s.homeConfigCommitMu.Unlock()
		<-supervisor.done
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
	if homeRegistry != nil {
		if errClose := homeRegistry.Close(); errClose != nil {
			log.WithError(errClose).Warn("failed to close Home execution registry during shutdown")
		}
	}
	if homeClient != nil {
		homeClient.Close()
	}
	if homeForwarder != nil {
		if homeForwarderClient == homeClient {
			homeForwarder.Deactivate(homeClient)
		}
		homeForwarder.StopContext(context.Background())
	}
	s.homeLifecycleMu.Unlock()

	// legacy refresh loop removed; only stopping core auth manager below

	s.cfgMu.RLock()
	watcherCancel := s.watcherCancel
	s.cfgMu.RUnlock()
	if watcherCancel != nil {
		watcherCancel()
	}
	if s.coreManager != nil {
		s.coreManager.StopAutoRefresh()
	}
	if watcher := s.currentWatcher(); watcher != nil {
		if err := watcher.Stop(); err != nil {
			log.Errorf("failed to stop file watcher: %v", err)
			shutdownErr = err
		}
	}
	if s.wsGateway != nil {
		if err := s.wsGateway.StopLegacy(); err != nil {
			log.Errorf("failed to stop websocket gateway: %v", err)
			if shutdownErr == nil {
				shutdownErr = err
			}
		}
	}
	if s.authQueueStop != nil {
		s.authQueueStop()
		s.authQueueStop = nil
	}

	if errShutdownPprof := s.shutdownPprofLegacy(ctx); errShutdownPprof != nil {
		log.Errorf("failed to stop pprof server: %v", errShutdownPprof)
		if shutdownErr == nil {
			shutdownErr = errShutdownPprof
		}
	}

	if errShutdownDiscovery := s.shutdownDiscovery(context.Background()); errShutdownDiscovery != nil {
		log.Errorf("failed to stop discovery advertiser: %v", errShutdownDiscovery)
		if shutdownErr == nil {
			shutdownErr = errShutdownDiscovery
		}
	}

	// no legacy clients to persist

	if s.server != nil {
		if errStop := s.server.StopWithDrainTimeout(ctx, 0); errStop != nil {
			log.Errorf("error stopping API server: %v", errStop)
			if shutdownErr == nil {
				shutdownErr = errStop
			}
		}
	}

	if s.pluginHost != nil {
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

	s.kvStoreManaged.Store(false)
	if errShutdownKV := kvstore.ShutdownLocal(); errShutdownKV != nil {
		log.Errorf("failed to stop local kv store: %v", errShutdownKV)
		if shutdownErr == nil {
			shutdownErr = errShutdownKV
		}
	}

	helps.RetireProxyPoolPublisherLegacy(s.proxyPoolPublisher)
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
			go log.WithError(errClose).Warn("failed to close Home execution registry during shutdown")
		}
	}
}

func (s *Service) shutdownPlugins(ctx context.Context) {
	sdktranslator.ClearPluginHooks(s.pluginHost)
	sdkAuth.ClearPluginAuthParser(s.pluginHost)
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
