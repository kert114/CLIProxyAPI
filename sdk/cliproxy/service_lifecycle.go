package cliproxy

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/api"
	internalconfig "github.com/router-for-me/CLIProxyAPI/v8/internal/config"
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
	s.cfgMu.Lock()
	s.antigravityContext = ctx
	s.cfgMu.Unlock()
	s.homeMu.Lock()
	s.runCancel = runCancel
	s.homeMu.Unlock()
	defer func() {
		runCancel()
		s.homeMu.Lock()
		if s.runCancel != nil {
			s.runCancel = nil
		}
		s.homeMu.Unlock()
	}()

	s.startModelCatalogUpdaters(ctx)

	usage.StartDefault(ctx)
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
		if errShutdown := s.shutdownWithBudget(context.Background(), s.ShutdownTimeout(), true); errShutdown != nil {
			if s.shutdownState.Load().budget > 0 {
				shutdown.RunDetached(func() { log.Errorf("service shutdown returned error: %v", errShutdown) })
			} else {
				log.Errorf("service shutdown returned error: %v", errShutdown)
			}
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
		s.coreManager.StartAutoRefresh(ctx, interval)
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
		if s.admissionStopped() {
			return context.Canceled
		}

		apiKeyResult, err := s.apiKeyProvider.Load(ctx, cfg)
		if err != nil && !errors.Is(err, context.Canceled) {
			return err
		}
		if apiKeyResult == nil {
			apiKeyResult = &APIKeyClientResult{}
		}
	}
	if s.admissionStopped() {
		return context.Canceled
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
	s.admissionMu.Lock()
	select {
	case <-s.admissionClosed:
		s.admissionMu.Unlock()
		return context.Canceled
	default:
		s.server = server
		s.admissionMu.Unlock()
	}
	s.syncPluginRuntimeConfig(ctx)
	if s.admissionStopped() {
		return context.Canceled
	}
	if homeEnabled {
		s.configRuntimeMu.Lock()
		s.syncPluginModelRuntime(ctx)
		s.configRuntimeMu.Unlock()
	}

	if s.authManager == nil {
		s.authManager = newDefaultAuthManager()
	}

	if homeEnabled {
		s.startHomeSubscriber(ctx)
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
	if s.admissionStopped() {
		return context.Canceled
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
	if s.admissionStopped() {
		s.configRuntimeMu.Unlock()
		return context.Canceled
	}
	currentCfg := s.currentConfig()
	s.applyPprofConfig(currentCfg)
	s.applyDiscoveryConfig(currentCfg)
	s.configRuntimeMu.Unlock()

	if s.hooks.OnAfterStart != nil {
		s.hooks.OnAfterStart(s)
	}
	if s.admissionStopped() {
		return context.Canceled
	}

	if !homeEnabled {
		reloadCallback := func(newCfg *config.Config) {
			if s.admissionStopped() {
				return
			}
			if newCfg != nil && newCfg.ShutdownTimeoutSeconds > 0 {
				if ctx.Err() == nil {
					s.applyConfigUpdateWithAuthSynthesis(ctx, newCfg, false)
				}
			} else {
				s.applyWatcherConfigUpdate(newCfg)
			}
		}

		authDir := ""
		if currentCfg := s.currentConfig(); currentCfg != nil {
			authDir = currentCfg.AuthDir
		}
		var watcherWrapper *WatcherWrapper
		if s.ShutdownTimeout() > 0 {
			var errCreate error
			watcherWrapper, errCreate = s.createDrainableWatcher(ctx, authDir, reloadCallback)
			if errCreate != nil {
				return errCreate
			}
		} else {
			var errCreate error
			watcherWrapper, errCreate = s.watcherFactory(s.configPath, authDir, reloadCallback)
			if errCreate != nil {
				return fmt.Errorf("cliproxy: failed to create watcher: %w", errCreate)
			}
			if !s.setWatcher(watcherWrapper) {
				shutdown.RunDetached(func() { _ = watcherWrapper.Stop() })
				return context.Canceled
			}
		}
		s.ensureAuthUpdateQueue(ctx)
		s.cfgMu.RLock()
		authUpdates := s.authUpdates
		s.cfgMu.RUnlock()
		if s.admissionStopped() {
			return context.Canceled
		}
		if authUpdates != nil {
			watcherWrapper.SetAuthUpdateQueue(authUpdates)
		}
		watcherWrapper.SetConfig(s.currentConfig())
		s.registerPluginAuthParser()

		watcherCtx, watcherCancel := context.WithCancel(context.Background())
		s.cfgMu.Lock()
		if s.admissionStopped() {
			s.cfgMu.Unlock()
			watcherCancel()
			return context.Canceled
		}
		s.watcherCancel = watcherCancel
		s.cfgMu.Unlock()
		if errStart := watcherWrapper.Start(watcherCtx); errStart != nil {
			return fmt.Errorf("cliproxy: failed to start watcher: %w", errStart)
		}
		if s.admissionStopped() {
			return context.Canceled
		}
		log.Info("file watcher started for config and auth directory changes")
		s.configRuntimeMu.Lock()
		if !s.admissionStopped() {
			s.syncPluginModelRuntime(ctx)
		}
		s.configRuntimeMu.Unlock()
	}

	if s.admissionStopped() {
		return context.Canceled
	}
	s.registerModelRefreshCallback()
	if !homeEnabled {
		go s.runAntigravityModelRefresh(ctx)
	}

	select {
	case <-ctx.Done():
		if s.ShutdownTimeout() <= 0 {
			log.Debug("service context cancelled, shutting down...")
		}
		return ctx.Err()
	case errServer := <-s.serverErr:
		return errServer
	}
}

// createDrainableWatcher lets positive-mode Run abandon a blocked factory.
// The unbuffered result transfers ownership only if startup is still waiting.
func (s *Service) createDrainableWatcher(ctx context.Context, authDir string, reload func(*config.Config)) (*WatcherWrapper, error) {
	type result struct {
		watcher    *WatcherWrapper
		err        error
		panicValue any
	}
	created := make(chan result)
	accepted, abandoned := make(chan struct{}), make(chan struct{})
	keepWatcher := false
	defer func() {
		if keepWatcher {
			close(accepted)
		} else {
			close(abandoned)
		}
	}()
	go func() {
		defer func() {
			if value := recover(); value != nil {
				select {
				case created <- result{panicValue: value}:
				case <-abandoned:
				}
			}
		}()
		watcher, err := s.watcherFactory(s.configPath, authDir, func(cfg *config.Config) {
			select {
			case <-abandoned:
				return
			default:
				reload(cfg)
			}
		})
		select {
		case created <- result{watcher: watcher, err: err}:
			select {
			case <-accepted:
				return
			case <-abandoned:
			}
		case <-abandoned:
		}
		_ = watcher.Stop()
	}()

	select {
	case <-s.admissionClosed:
		return nil, context.Canceled
	case <-ctx.Done():
		return nil, ctx.Err()
	case result := <-created:
		if result.panicValue != nil {
			panic(result.panicValue)
		}
		if result.err != nil {
			return nil, fmt.Errorf("cliproxy: failed to create watcher: %w", result.err)
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if !s.setWatcher(result.watcher) {
			return nil, context.Canceled
		}
		keepWatcher = true
		return result.watcher, nil
	}
}

func (s *Service) admissionStopped() bool {
	select {
	case <-s.admissionClosed:
		return true
	default:
		return false
	}
}

type shutdownResult struct {
	err        error
	panicValue any
}

func (r *shutdownResult) recoverPanic() {
	if value := recover(); value != nil {
		r.panicValue = value
		r.err = errors.New("cliproxy: shutdown callback panicked")
	}
}

// ShutdownTimeout returns the current drain budget without waiting for a reload.
func (s *Service) ShutdownTimeout() time.Duration {
	if s == nil {
		return 0
	}
	cfg := s.shutdownConfig.Load()
	if cfg == nil {
		cfg = s.currentConfig()
	}
	if cfg == nil {
		return 0
	}
	return internalconfig.ShutdownDuration(cfg.ShutdownTimeoutSeconds)
}

// Shutdown stops the service. With a positive drain budget the API stops
// accepting immediately, and ctx bounds how long the caller waits. Legacy
// resource teardown may continue in the background after ctx expires.
func (s *Service) Shutdown(ctx context.Context) error {
	return s.ShutdownWithBudget(ctx, s.ShutdownTimeout())
}

// ShutdownWithBudget uses a snapshot of ShutdownTimeout. The standalone
// command uses the same snapshot for its process exit timer and HTTP drain.
func (s *Service) ShutdownWithBudget(ctx context.Context, budget time.Duration) error {
	return s.shutdownWithBudget(ctx, budget, false)
}

func (s *Service) shutdownWithBudget(ctx context.Context, budget time.Duration, fromRun bool) error {
	if s == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	state := s.shutdownState.Load()
	initiator := false
	if state == nil {
		if fromRun && budget <= 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, 30*time.Second)
			defer cancel()
		}
		state = &serviceShutdown{budget: budget, ctx: ctx}
		initiator = s.shutdownState.CompareAndSwap(nil, state)
		if !initiator {
			state = s.shutdownState.Load()
		}
	}
	if state.budget <= 0 {
		var err error
		state.once.Do(func() { err = s.shutdownResources(state.ctx, true) })
		return err
	}

	state.once.Do(func() {
		drainCtx, cancel := context.WithTimeout(state.ctx, state.budget)
		state.drainCtx = drainCtx

		s.admissionMu.Lock()
		server := s.server
		if server != nil {
			server.StopAccepting()
		}
		if s.admissionClosed != nil {
			close(s.admissionClosed)
		}
		s.admissionMu.Unlock()
		s.closePprofListeners()
		if s.discoveryManager != nil {
			s.discoveryManager.stopAccepting()
		}

		state.done = make(chan struct{})
		go func() {
			defer cancel()
			defer close(state.done)
			defer state.result.recoverPanic()
			profileDone := make(chan shutdownResult, 1)
			go func() {
				var result shutdownResult
				defer func() { profileDone <- result }()
				defer result.recoverPanic()
				if s.pprofServer != nil {
					result.err = s.pprofServer.shutdown(drainCtx, false)
				}
			}()
			var errDrain error
			if server != nil {
				errDrain = server.Drain(drainCtx)
			}
			// Leave the relay and request dependencies alive until HTTP has drained.
			profile := <-profileDone
			state.result.err = errors.Join(errDrain, profile.err, s.shutdownResources(drainCtx, false))
			if profile.panicValue != nil {
				panic(profile.panicValue)
			}
		}()
	})

	if fromRun {
		// Closing the listener wakes Run. Join the already-started drain
		// before its deferred runCancel stops workers serving that drain.
		ctx = state.drainCtx
	}
	result := func() error {
		if initiator && state.result.panicValue != nil && (fromRun || ctx.Err() == nil) {
			panic(state.result.panicValue)
		}
		return state.result.err
	}
	select {
	case <-state.done:
		return result()
	default:
	}
	select {
	case <-state.done:
		return result()
	case <-ctx.Done():
		if fromRun {
			select {
			case <-state.done:
				return result()
			default:
			}
		}
		return ctx.Err()
	}
}

func (s *Service) shutdownResources(ctx context.Context, stopAPI bool) error {
	var shutdownErr error

	s.homeMu.Lock()
	runCancel := s.runCancel
	s.homeMu.Unlock()
	if runCancel != nil {
		runCancel()
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
		homeForwarder.Stop()
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
		if err := s.wsGateway.Stop(ctx); err != nil {
			log.Errorf("failed to stop websocket gateway: %v", err)
			if shutdownErr == nil {
				shutdownErr = err
			}
		}
	}
	s.cfgMu.Lock()
	stopQueue := s.authQueueStop
	s.authQueueStop = nil
	s.cfgMu.Unlock()
	if stopQueue != nil {
		stopQueue()
	}

	if stopAPI {
		if errShutdownPprof := s.shutdownPprof(ctx); errShutdownPprof != nil {
			log.Errorf("failed to stop pprof server: %v", errShutdownPprof)
			if shutdownErr == nil {
				shutdownErr = errShutdownPprof
			}
		}
	}

	if errShutdownDiscovery := s.shutdownDiscovery(); errShutdownDiscovery != nil {
		log.Errorf("failed to stop discovery advertiser: %v", errShutdownDiscovery)
		if shutdownErr == nil {
			shutdownErr = errShutdownDiscovery
		}
	}

	// no legacy clients to persist

	if stopAPI && s.server != nil {
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

	if errShutdownKV := s.shutdownKVStore(); errShutdownKV != nil {
		log.Errorf("failed to stop local kv store: %v", errShutdownKV)
		if shutdownErr == nil {
			shutdownErr = errShutdownKV
		}
	}

	helps.RetireProxyPoolPublisher(s.proxyPoolPublisher)
	usage.StopDefault()
	return shutdownErr
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
