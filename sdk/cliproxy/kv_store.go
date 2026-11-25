package cliproxy

import (
	"context"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/home"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/kvstore"
	log "github.com/sirupsen/logrus"
)

func (s *Service) applyKVStoreConfig(cfg *config.Config) {
	if s == nil {
		if errShutdown := kvstore.ShutdownLocal(); errShutdown != nil {
			log.Errorf("failed to shutdown local kv store: %v", errShutdown)
		}
		return
	}
	s.kvStoreManaged.Store(true)
	if cfg == nil || cfg.Home.Enabled || home.Current() != nil || cfg.KVStore.Disabled {
		if errShutdown := kvstore.ShutdownLocal(); errShutdown != nil {
			log.Errorf("failed to shutdown local kv store: %v", errShutdown)
		}
		return
	}
	_ = kvstore.ConfigureLocal(true, cfg.EffectiveKVStorePath())
}

func (s *Service) shutdownKVStore(ctx context.Context) error {
	if s == nil {
		return nil
	}
	return kvstore.ShutdownLocalContext(ctx)
}
