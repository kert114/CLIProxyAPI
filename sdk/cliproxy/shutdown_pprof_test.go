package cliproxy

import (
	"context"
	"errors"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
)

func TestShutdownPprofIgnoresAlreadyClosedListener(t *testing.T) {
	for _, tc := range []struct {
		name    string
		expired bool
	}{
		{name: "live caller"},
		{name: "expired caller", expired: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
			if err != nil {
				t.Fatal(err)
			}
			defer raw.Close()
			tracked, release, shutdownStarted := make(chan struct{}), make(chan struct{}), make(chan struct{})
			unblock := sync.OnceFunc(func() { close(release) })
			defer unblock()
			server := &http.Server{
				BaseContext: func(net.Listener) context.Context {
					// Serve tracks its listener before calling BaseContext.
					close(tracked)
					<-release
					return context.Background()
				},
			}
			server.RegisterOnShutdown(func() { close(shutdownStarted) })
			service := &Service{
				cfg: &config.Config{ShutdownTimeoutSeconds: 2},
				pprofServer: &pprofServer{
					server:    server,
					addr:      raw.Addr().String(),
					listeners: map[*http.Server]*net.TCPListener{server: raw},
				},
			}
			serving := make(chan error, 1)
			go func() { serving <- server.Serve(raw) }()

			wait := func(ch <-chan struct{}, action string) {
				t.Helper()
				select {
				case <-ch:
				case <-time.After(3 * time.Second):
					t.Fatalf("%s did not start", action)
				}
			}
			wait(tracked, "Serve")
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			rebind := func() {
				t.Helper()
				rebound, err := net.Listen("tcp4", raw.Addr().String())
				if err != nil {
					t.Fatalf("pprof address was still bound: %v", err)
				}
				defer rebound.Close()
			}
			done := make(chan error, 1)
			if tc.expired {
				expired, cancelExpired := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
				defer cancelExpired()
				if err := service.Shutdown(expired); !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("expired Shutdown = %v, want deadline exceeded", err)
				}
				rebind()
			}
			go func() { done <- service.Shutdown(ctx) }()
			wait(shutdownStarted, "net/http shutdown")
			if !tc.expired {
				rebind()
			}
			unblock()
			select {
			case err := <-done:
				if err != nil {
					t.Fatalf("Shutdown returned an error for the already-closed pprof listener: %v", err)
				}
			case <-ctx.Done():
				t.Fatal("Shutdown did not finish after Serve unregistered the listener")
			}
			select {
			case <-serving:
			case <-ctx.Done():
				t.Fatal("Serve did not exit")
			}
		})
	}
}
