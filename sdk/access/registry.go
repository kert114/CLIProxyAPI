package access

import (
	"context"
	"net/http"
	"strings"
	"sync"
)

// Provider validates credentials for incoming requests.
type Provider interface {
	Identifier() string
	Authenticate(ctx context.Context, r *http.Request) (*Result, *AuthError)
}

// Result conveys authentication outcome.
type Result struct {
	Provider  string
	Principal string
	Metadata  map[string]string
}

var (
	registryMu        sync.RWMutex
	registry          = make(map[string]Provider)
	order             []string
	exclusiveProvider string
	exclusiveOwner    any
	providerOwners    = make(map[string]any)
)

// RegisterProvider registers a pre-built provider instance for a given type identifier.
func RegisterProvider(typ string, provider Provider) {
	normalizedType := strings.TrimSpace(typ)
	if normalizedType == "" || provider == nil {
		return
	}

	registryMu.Lock()
	if _, exists := registry[normalizedType]; !exists {
		order = append(order, normalizedType)
	}
	registry[normalizedType] = provider
	delete(providerOwners, normalizedType)
	registryMu.Unlock()
}

// UnregisterProvider removes a provider by type identifier.
func UnregisterProvider(typ string) {
	normalizedType := strings.TrimSpace(typ)
	if normalizedType == "" {
		return
	}
	registryMu.Lock()
	removeProviderLocked(normalizedType)
	registryMu.Unlock()
}

func removeProviderLocked(normalizedType string) {
	delete(providerOwners, normalizedType)
	delete(registry, normalizedType)
	for index := range order {
		if order[index] != normalizedType {
			continue
		}
		order = append(order[:index], order[index+1:]...)
		break
	}
}

// SetExclusiveProvider restricts RegisteredProviders to a single provider key when present.
func SetExclusiveProvider(typ string) {
	normalizedType := strings.TrimSpace(typ)
	registryMu.Lock()
	exclusiveProvider = normalizedType
	exclusiveOwner = nil
	registryMu.Unlock()
}

// ClearExclusiveProvider removes any active provider restriction.
func ClearExclusiveProvider() {
	registryMu.Lock()
	exclusiveProvider = ""
	exclusiveOwner = nil
	registryMu.Unlock()
}

// OwnedProvider identifies one registration installed by a plugin host.
type OwnedProvider struct {
	Key      string
	Provider Provider
}

// ReplaceOwnedProviders publishes a host's providers and its exclusivity in one
// registry operation. Older hosts may only remove their own registrations.
func ReplaceOwnedProviders(owner any, providers []OwnedProvider, exclusive string) {
	registryMu.Lock()
	defer registryMu.Unlock()
	next := make(map[string]struct{}, len(providers))
	for _, registration := range providers {
		key := registration.Key
		if _, exists := registry[key]; !exists {
			order = append(order, key)
		}
		registry[key] = registration.Provider
		providerOwners[key] = owner
		next[key] = struct{}{}
	}
	for key, current := range providerOwners {
		if _, keep := next[key]; current == owner && !keep {
			removeProviderLocked(key)
		}
	}
	if exclusive != "" {
		exclusiveProvider, exclusiveOwner = exclusive, owner
	} else if exclusiveOwner == owner {
		exclusiveProvider, exclusiveOwner = "", nil
	}
}

// RegisteredProviders returns the global provider instances in registration order.
func RegisteredProviders() []Provider {
	registryMu.RLock()
	if len(order) == 0 {
		registryMu.RUnlock()
		return nil
	}
	if exclusiveProvider != "" {
		if provider, exists := registry[exclusiveProvider]; exists && provider != nil {
			registryMu.RUnlock()
			return []Provider{provider}
		}
	}
	providers := make([]Provider, 0, len(order))
	for _, providerType := range order {
		provider, exists := registry[providerType]
		if !exists || provider == nil {
			continue
		}
		providers = append(providers, provider)
	}
	registryMu.RUnlock()
	return providers
}
