package cliproxy

import (
	"context"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/home"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/kvstore"
	log "github.com/sirupsen/logrus"
)

func (s *Service) applyKVStoreConfig(cfg *config.Config) {
	s.applyKVStoreConfigContext(context.Background(), cfg)
}

func (s *Service) applyKVStoreConfigContext(ctx context.Context, cfg *config.Config) {
	if s == nil {
		if errShutdown := kvstore.ShutdownLocal(); errShutdown != nil {
			log.Errorf("failed to shutdown local kv store: %v", errShutdown)
		}
		return
	}
	s.cfgMu.RLock()
	if s.stopping {
		s.cfgMu.RUnlock()
		return
	}
	s.kvStoreManaged.Store(true)
	s.cfgMu.RUnlock()
	enabled := cfg != nil && !cfg.Home.Enabled && home.Current() == nil && !cfg.KVStore.Disabled
	var path string
	if enabled {
		path = cfg.EffectiveKVStorePath()
	}
	_ = kvstore.ConfigureLocalContext(ctx, s.proxyPoolPublisher, enabled, path)
}

func (s *Service) shutdownKVStore(ctx context.Context) error {
	if s == nil {
		return nil
	}
	return kvstore.ShutdownLocalOwnedContext(ctx, s.proxyPoolPublisher)
}
