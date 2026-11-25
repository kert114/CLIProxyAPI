package cliproxy

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/pluginhost"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/wsrelay"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executionregistry"
)

type blockedShutdownSelector struct {
	coreauth.RoundRobinSelector
	stop func()
}

func (s *blockedShutdownSelector) Stop() { s.stop() }

type blockedShutdownAdvertiser struct {
	fakeAdvertiser
	stop func()
}

func (a *blockedShutdownAdvertiser) Stop() error {
	a.stop()
	return nil
}

func TestServiceShutdown_DeadlineBoundsComponentStops(t *testing.T) {
	for _, tc := range []struct {
		name    string
		install func(*Service, func())
	}{
		{name: "watcher", install: func(service *Service, block func()) {
			service.setWatcher(&WatcherWrapper{stop: func() error { block(); return nil }})
		}},
		{name: "selector", install: func(service *Service, block func()) {
			service.coreManager = coreauth.NewManager(nil, &blockedShutdownSelector{stop: block}, nil)
		}},
		{name: "discovery", install: func(service *Service, block func()) {
			service.discoveryManager = &discoveryAdvertiserManager{advertiser: &blockedShutdownAdvertiser{stop: block}}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			service := &Service{}
			entered, release, exited := make(chan struct{}), make(chan struct{}), make(chan struct{})
			tc.install(service, func() {
				close(entered)
				<-release
				close(exited)
			})
			defer func() {
				close(release)
				select {
				case <-exited:
				case <-time.After(time.Second):
					t.Error("component did not finish cleanup")
				}
			}()

			ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
			defer cancel()
			first := make(chan struct{})
			go func() { defer close(first); _ = service.Shutdown(ctx) }()
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("component shutdown was skipped")
			}
			select {
			case <-first:
			case <-time.After(500 * time.Millisecond):
				t.Fatal("component held shutdown beyond its deadline")
			}
		})
	}
}

func TestServiceShutdown_DeadlineBoundsConcurrentCaller(t *testing.T) {
	service := &Service{}
	entered, release, first := make(chan struct{}), make(chan struct{}), make(chan struct{})
	service.setWatcher(&WatcherWrapper{stop: func() error {
		close(entered)
		<-release
		return nil
	}})
	go func() { defer close(first); _ = service.Shutdown(t.Context()) }()
	defer func() {
		close(release)
		<-first
	}()
	<-entered

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	second := make(chan struct{})
	go func() { defer close(second); _ = service.Shutdown(ctx) }()
	select {
	case <-second:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("concurrent Shutdown ignored its own deadline")
	}
}

func TestServiceShutdown_DeadlineBoundsLifecycleLocks(t *testing.T) {
	for _, tc := range []struct {
		name string
		hold func(*Service) func()
	}{
		{name: "home_replacement", hold: func(s *Service) func() {
			s.homeLifecycleMu.Lock()
			return s.homeLifecycleMu.Unlock
		}},
		{name: "plugin_executor_reload", hold: func(s *Service) func() {
			s.coreManager = coreauth.NewManager(nil, nil, nil)
			s.pluginHost = pluginhost.New()
			s.executorRegistrationMu.Lock()
			return s.executorRegistrationMu.Unlock
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			service := &Service{}
			unlock := tc.hold(service)
			done := make(chan struct{})
			defer func() { unlock(); <-done }()
			ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
			defer cancel()
			go func() { defer close(done); _ = service.Shutdown(ctx) }()
			select {
			case <-done:
			case <-time.After(500 * time.Millisecond):
				t.Fatal("shutdown waited for a lifecycle lock past its deadline")
			}
		})
	}
}

func TestServiceShutdown_DetachesPprofConnectionAtDeadline(t *testing.T) {
	listener, errListen := net.Listen("tcp", "127.0.0.1:0")
	if errListen != nil {
		t.Fatal(errListen)
	}
	release := make(chan struct{})
	defer close(release)
	pprof := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "started\n")
		w.(http.Flusher).Flush()
		select {
		case <-release:
		case <-r.Context().Done():
		}
	})}
	defer func() { _ = pprof.Close() }()
	go func() { _ = pprof.Serve(listener) }()
	service := &Service{pprofServer: &pprofServer{server: pprof}}
	client := &http.Client{Timeout: 2 * time.Second}
	resp, errGet := client.Get("http://" + listener.Addr().String())
	if errGet != nil {
		t.Fatal(errGet)
	}
	defer func() { _ = resp.Body.Close() }()

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	_ = service.Shutdown(ctx)
	if _, errRead := io.ReadAll(resp.Body); !errors.Is(errRead, io.ErrUnexpectedEOF) {
		t.Fatalf("held pprof connection did not close: %v", errRead)
	}
}

func TestServiceShutdown_ClosesRelayWithBlockedDisconnect(t *testing.T) {
	connected, entered := make(chan struct{}), make(chan struct{})
	release, callbackDone := make(chan struct{}), make(chan struct{})
	relay := wsrelay.NewManager(wsrelay.Options{
		OnConnected: func(string) { close(connected) },
		OnDisconnected: func(context.Context, string, error) {
			close(entered)
			<-release
			close(callbackDone)
		},
	})
	server := httptest.NewServer(relay.Handler())
	defer server.Close()
	dialer := websocket.Dialer{HandshakeTimeout: time.Second}
	conn, _, errDial := dialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+relay.Path(), nil)
	if errDial != nil {
		t.Fatal(errDial)
	}
	defer func() { _ = conn.Close() }()
	defer func() {
		close(release)
		select {
		case <-callbackDone:
		case <-time.After(time.Second):
			t.Error("relay disconnect callback did not finish")
		}
	}()
	select {
	case <-connected:
	case <-time.After(time.Second):
		t.Fatal("relay session was not registered")
	}
	service := &Service{wsGateway: relay}
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); _ = service.Shutdown(ctx) }()

	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("relay disconnect did not begin")
	}
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	_, _, errRead := conn.ReadMessage()
	if errRead == nil || errors.Is(errRead, context.DeadlineExceeded) {
		t.Fatalf("held relay connection did not close: %v", errRead)
	}
	var netErr net.Error
	if errors.As(errRead, &netErr) && netErr.Timeout() {
		t.Fatalf("held relay connection did not close: %v", errRead)
	}
	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("relay's disconnect callback blocked shutdown past its deadline")
	}
}

func TestServiceShutdown_DeadlineBoundsDetachedRegistryClose(t *testing.T) {
	registry := executionregistry.New()
	dispatch, err := registry.BeginDispatch()
	if err != nil {
		t.Fatal(err)
	}
	scope, err := registry.Install(dispatch, executionregistry.ScopeSpec{RequestID: "fixture"})
	if err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	if err := scope.Bind(func() error {
		close(entered)
		<-release
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	defer func() { close(release); scope.End("fixture complete") }()
	service := &Service{homeRegistry: registry}
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	closed := make(chan struct{})
	go func() { defer close(closed); _ = service.Shutdown(ctx) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("scope close was not started")
	}
	select {
	case <-closed:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("scope close blocked shutdown past its deadline")
	}
}
