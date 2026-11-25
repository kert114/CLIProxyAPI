package cliproxy

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/home"
)

func TestShutdownRegressionDuringHomeStartup(t *testing.T) {
	control, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer control.Close()
	stopControl := make(chan struct{})
	defer close(stopControl)
	defer home.ClearCurrent()
	address := shutdownRegressionAddress(t)
	_, port, _ := net.SplitHostPort(address)
	directory := t.TempDir()
	raw := fmt.Sprintf("server:\n  host: 127.0.0.1\n  port: %s\n  shutdown-timeout: 45\noauth:\n  auth-dir: %s\n", port, directory)
	cfg, err := config.ParseConfigBytes([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	cfg.Home.Enabled, cfg.Home.DisableClusterDiscovery = true, true
	cfg.Home.Host = "127.0.0.1"
	cfg.Home.Port = control.Addr().(*net.TCPAddr).Port

	go func() {
		for {
			conn, err := control.Accept()
			if err != nil {
				return
			}
			go serveShutdownProbeHomeWithHeldWrites(conn, raw, stopControl, nil, nil)
		}
	}()

	entered, release := make(chan struct{}), make(chan struct{})
	defer close(release)
	service, err := NewBuilder().WithConfig(cfg).WithConfigPath(filepath.Join(directory, "test.yaml")).
		WithHooks(Hooks{OnBeforeStart: func(*config.Config) {
			select {
			case <-entered:
			case <-time.After(3 * time.Second):
			}
		}}).Build()
	if err != nil {
		t.Fatal(err)
	}
	service.updateServerClientsContextFn = func(ctx context.Context, cfg *config.Config) bool {
		close(entered)
		select {
		case <-ctx.Done():
			return false
		case <-release:
			return false
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- service.Run(ctx) }()
	limit := time.Now().Add(4 * time.Second)
	for {
		conn, err := net.DialTimeout("tcp", address, 50*time.Millisecond)
		if err == nil {
			conn.Close()
			break
		}
		if time.Now().After(limit) {
			t.Fatal("HTTP listener didn't bind")
		}
		time.Sleep(10 * time.Millisecond)
	}
	select {
	case <-entered:
	default:
		t.Fatal("Home config worker didn't enter before HTTP listener bound")
	}

	started := time.Now()
	cancel()
	select {
	case err := <-done:
		t.Logf("Run returned %v at %s", err, time.Since(started))
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run = %v, want context cancellation", err)
		}
		if conn, errDial := net.DialTimeout("tcp", address, 100*time.Millisecond); errDial == nil {
			_ = conn.Close()
			t.Fatal("API listener remained open after Run returned")
		}
	case <-time.After(1200 * time.Millisecond):
		conn, err := net.DialTimeout("tcp", address, 100*time.Millisecond)
		if err == nil {
			conn.Close()
		}
		t.Fatalf("Run still blocked at %s; HTTP listener accepted = %v", time.Since(started), err == nil)
	}
}

func serveShutdownProbeHomeWithHeldWrites(conn net.Conn, raw string, stop <-chan struct{}, appLogStarted, releaseStarted chan<- struct{}) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	for {
		args, err := readRegistryTestRedisCommand(reader)
		if err != nil {
			return
		}
		switch {
		case len(args) > 0 && strings.EqualFold(args[0], "HELLO"):
			_, err = io.WriteString(conn, "%6\r\n$6\r\nserver\r\n$5\r\nredis\r\n$5\r\nproto\r\n:3\r\n$2\r\nid\r\n:1\r\n$4\r\nmode\r\n$10\r\nstandalone\r\n$4\r\nrole\r\n$6\r\nmaster\r\n$7\r\nmodules\r\n*0\r\n")
		case len(args) >= 2 && strings.EqualFold(args[0], "GET") && args[1] == "config":
			_, err = fmt.Fprintf(conn, "$%d\r\n%s\r\n", len(raw), raw)
		case len(args) >= 2 && strings.EqualFold(args[0], "GET") && args[1] == "plugin-tasks":
			_, err = io.WriteString(conn, "$2\r\n[]\r\n")
		case len(args) >= 3 && strings.EqualFold(args[0], "RPUSH") && args[1] == "app-log":
			if strings.Contains(args[2], "review-held-app-log") {
				appLogStarted <- struct{}{}
				<-stop
				return
			}
			_, err = io.WriteString(conn, ":1\r\n")
		case len(args) >= 2 && strings.EqualFold(args[0], "LPUSH") && args[1] == "concurrency-release":
			releaseStarted <- struct{}{}
			<-stop
			return
		case len(args) >= 2 && strings.EqualFold(args[0], "SUBSCRIBE") && args[1] == "config":
			_, _ = io.WriteString(conn, "*3\r\n$9\r\nsubscribe\r\n$6\r\nconfig\r\n:1\r\n")
			heartbeat := time.NewTicker(250 * time.Millisecond)
			defer heartbeat.Stop()
			for {
				select {
				case <-stop:
					return
				case <-heartbeat.C:
					if _, err := io.WriteString(conn, "*2\r\n$4\r\npong\r\n$0\r\n\r\n"); err != nil {
						return
					}
				}
			}
		default:
			_, err = io.WriteString(conn, "+OK\r\n")
		}
		if err != nil {
			return
		}
	}
}
