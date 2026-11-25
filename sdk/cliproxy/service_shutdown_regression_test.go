package cliproxy

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/pluginhost"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/thinking"
	sdkaccess "github.com/router-for-me/CLIProxyAPI/v8/sdk/access"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

func shutdownRegressionAddress(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

func TestShutdownRegressionStartupShutdownListener(t *testing.T) {
	dir := t.TempDir()
	apiAddress, profileAddress := shutdownRegressionAddress(t), shutdownRegressionAddress(t)
	_, port, err := net.SplitHostPort(apiAddress)
	if err != nil {
		t.Fatal(err)
	}
	raw := fmt.Sprintf("server:\n  host: 127.0.0.1\n  port: %s\n  shutdown-timeout: 45\noauth:\n  auth-dir: %s\nobservability:\n  pprof:\n    enable: true\n    addr: %q\n", port, dir, profileAddress)
	cfg, err := config.ParseConfigBytes([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	cfg.KVStore.Disabled = true
	entered, release := make(chan struct{}), make(chan struct{})
	service, err := NewBuilder().WithConfig(cfg).WithConfigPath(filepath.Join(dir, "test.yaml")).
		WithWatcherFactory(func(string, string, func(*config.Config)) (*WatcherWrapper, error) {
			return &WatcherWrapper{start: func(context.Context) error { return nil }}, nil
		}).
		WithHooks(Hooks{OnBeforeStart: func(*config.Config) { close(entered); <-release }}).Build()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- service.Run(ctx) }()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		close(release)
		t.Fatal("startup hook was not entered")
	}
	if err := service.Shutdown(ctx); err != nil {
		t.Errorf("first Shutdown = %v", err)
	}
	cancel()
	close(release)
	select {
	case err := <-done:
		t.Logf("Run returned %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not exit")
	}
	defer func() { _ = service.shutdownPprof(context.Background()) }()
	client := &http.Client{Timeout: 100 * time.Millisecond}
	limit := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(limit) {
		response, err := client.Get("http://" + profileAddress + "/debug/pprof/")
		if err == nil {
			response.Body.Close()
			t.Fatalf("pprof responded %s after Shutdown and Run both returned", response.Status)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestShutdownRegressionRelayAuth(t *testing.T) {
	manager := coreauth.NewManager(nil, nil, nil)
	service := &Service{cfg: &config.Config{ShutdownTimeoutSeconds: 1}, coreManager: manager}
	service.ensureWebsocketGateway()
	relay := httptest.NewServer(service.wsGateway.Handler())
	defer relay.Close()
	socket, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(relay.URL, "http")+"/v1/ws", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer socket.Close()
	limit := time.Now().Add(time.Second)
	var id string
	for time.Now().Before(limit) {
		if auths := manager.List(); len(auths) == 1 {
			id = auths[0].ID
			break
		}
		time.Sleep(time.Millisecond)
	}
	if id == "" {
		t.Fatal("relay auth was not published")
	}
	ctx, cancel := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
	defer cancel()
	t.Logf("Shutdown returned %v", service.Shutdown(ctx))
	_ = socket.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	if _, _, err := socket.ReadMessage(); err == nil {
		t.Fatal("relay socket remained open")
	}
	if auth, ok := manager.GetByID(id); ok {
		t.Fatalf("stopped relay credential still registered with status=%v, disabled=%v", auth.Status, auth.Disabled)
	}
}

func TestShutdownRegressionResponsesWebsocket(t *testing.T) {
	dir := t.TempDir()
	address := shutdownRegressionAddress(t)
	_, port, _ := net.SplitHostPort(address)
	raw := fmt.Sprintf("server:\n  host: 127.0.0.1\n  port: %s\n  shutdown-timeout: 1\naccess:\n  api-keys: [throwaway-local-fixture]\noauth:\n  auth-dir: %s\n", port, dir)
	cfg, err := config.ParseConfigBytes([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	cfg.KVStore.Disabled = true
	path := filepath.Join(dir, "test.yaml")
	if err := os.WriteFile(path, []byte(raw), 0600); err != nil {
		t.Fatal(err)
	}
	ready := make(chan struct{})
	service, err := NewBuilder().WithConfig(cfg).WithConfigPath(path).
		WithWatcherFactory(func(string, string, func(*config.Config)) (*WatcherWrapper, error) {
			return &WatcherWrapper{start: func(context.Context) error { close(ready); return nil }}, nil
		}).Build()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- service.Run(ctx) }()
	select {
	case <-ready:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not start")
	}
	headers := make(http.Header)
	headers.Set("Authorization", "Bearer throwaway-local-fixture")
	conn, _, err := websocket.DefaultDialer.Dial("ws://"+address+"/v1/responses", headers)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	cancel()
	select {
	case err := <-done:
		t.Logf("Run returned %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not exit")
	}
	_ = conn.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	_, _, err = conn.ReadMessage()
	if e, ok := err.(net.Error); ok && e.Timeout() {
		t.Fatal("idle /v1/responses socket remained open after Run returned")
	}
	if err == nil {
		t.Fatal("expected socket close")
	}
}

type shutdownRegressionAccessProvider string

func (p shutdownRegressionAccessProvider) Identifier() string { return string(p) }
func (shutdownRegressionAccessProvider) Authenticate(context.Context, *http.Request) (*sdkaccess.Result, *sdkaccess.AuthError) {
	return nil, nil
}

type shutdownRegressionThinkingApplier struct{}

func (shutdownRegressionThinkingApplier) Apply(body []byte, _ thinking.ThinkingConfig, _ *registry.ModelInfo) ([]byte, error) {
	return body, nil
}

func TestShutdownRegressionPluginOwnership(t *testing.T) {
	service := &Service{cfg: &config.Config{ShutdownTimeoutSeconds: 1}, coreManager: coreauth.NewManager(nil, nil, nil), pluginHost: pluginhost.New()}
	service.executorRegistrationMu.Lock()
	unlock := sync.OnceFunc(service.executorRegistrationMu.Unlock)
	defer unlock()
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	stopped := make(chan error, 1)
	go func() { stopped <- service.Shutdown(ctx) }()
	select {
	case err := <-stopped:
		t.Logf("Shutdown returned %v while executor registration was held", err)
	case <-time.After(200 * time.Millisecond):
		unlock()
		t.Logf("Shutdown required the registration lock before returning: %v", <-stopped)
	}
	sdkaccess.RegisterProvider("review-exclusive", shutdownRegressionAccessProvider("review-exclusive"))
	sdkaccess.RegisterProvider("review-fallback", shutdownRegressionAccessProvider("review-fallback"))
	sdkaccess.SetExclusiveProvider("review-exclusive")
	defer sdkaccess.ClearExclusiveProvider()
	defer sdkaccess.UnregisterProvider("review-exclusive")
	defer sdkaccess.UnregisterProvider("review-fallback")
	if providers := sdkaccess.RegisteredProviders(); len(providers) != 1 || providers[0].Identifier() != "review-exclusive" {
		t.Fatal("replacement did not establish exclusivity")
	}
	if !thinking.RegisterPluginProvider("shutdown-replacement", "shutdown-replacement", 0, shutdownRegressionThinkingApplier{}) {
		t.Fatal("replacement could not register its thinking provider")
	}
	defer thinking.UnregisterPluginProviders("shutdown-replacement")
	unlock()
	limit := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(limit) {
		if thinking.GetProviderApplier("shutdown-replacement") == nil {
			t.Error("departed service cleared replacement's thinking provider")
			break
		}
		time.Sleep(time.Millisecond)
	}
	if providers := sdkaccess.RegisteredProviders(); len(providers) != 1 || providers[0].Identifier() != "review-exclusive" {
		t.Error("departed service cleared replacement's exclusive auth policy")
	}
}
