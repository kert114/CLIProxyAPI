package cliproxy

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/discovery"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
)

func shutdownFixture(t *testing.T, profile bool) (*config.Config, string, string, string) {
	t.Helper()
	registry.SetLocalModelCatalogs(true)
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	apiAddr := listener.Addr().String()
	var profileYAML, profileAddr string
	if profile {
		p, err := net.Listen("tcp4", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer p.Close()
		profileAddr = p.Addr().String()
		profileYAML = fmt.Sprintf("observability:\n  pprof:\n    enable: true\n    addr: %s\n", profileAddr)
	}
	dir := t.TempDir()
	cfg, err := config.ParseConfigBytes([]byte(fmt.Sprintf("server:\n  host: 127.0.0.1\n  port: %d\n  shutdown-timeout: 2\noauth:\n  auth-dir: %s\n%s", listener.Addr().(*net.TCPAddr).Port, dir, profileYAML)))
	if err != nil {
		t.Fatal(err)
	}
	cfg.KVStore.Disabled = true
	path := filepath.Join(dir, "fixture.yaml")
	if err := os.WriteFile(path, []byte("{}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	return cfg, path, apiAddr, profileAddr
}

func TestShutdownWatcherFactoryPanicReachesRun(t *testing.T) {
	const sentinel = "watcher fixture panic"
	if os.Getenv("CPA_SHUTDOWN_WATCHER_PANIC_CHILD") == "1" {
		cfg, path, _, _ := shutdownFixture(t, false)
		service, err := NewBuilder().WithConfig(cfg).WithConfigPath(path).
			WithWatcherFactory(func(string, string, func(*config.Config)) (*WatcherWrapper, error) {
				panic(sentinel)
			}).Build()
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if got := recover(); got != sentinel {
				t.Fatalf("Run caller recovered %v, want %s", got, sentinel)
			}
			t.Log("Run caller recovered the watcher factory panic")
		}()
		_ = service.Run(context.Background())
		t.Fatal("Run returned without the watcher factory panic")
		return
	}
	child := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestShutdownWatcherFactoryPanicReachesRun$", "-test.v", "-test.timeout=8s")
	child.Env = append(os.Environ(), "CPA_SHUTDOWN_WATCHER_PANIC_CHILD=1")
	output, err := child.CombinedOutput()
	t.Logf("child status=%v\n%s", err, output)
	if err != nil || !strings.Contains(string(output), "Run caller recovered the watcher factory panic") {
		t.Fatal("watcher factory panic did not reach the Run caller")
	}
}

func TestShutdownExpiredDeadlineClosesBothListeners(t *testing.T) {
	prev := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(prev)
	cfg, path, apiAddr, profileAddr := shutdownFixture(t, true)
	watcherStarted := make(chan struct{})
	service, err := NewBuilder().WithConfig(cfg).WithConfigPath(path).
		WithWatcherFactory(func(string, string, func(*config.Config)) (*WatcherWrapper, error) {
			return &WatcherWrapper{start: func(context.Context) error { close(watcherStarted); return nil }}, nil
		}).Build()
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	runCtx, stop := context.WithCancel(t.Context())
	defer stop()
	go func() { done <- service.Run(runCtx) }()
	defer func() {
		stop()
		select {
		case <-done:
		case <-time.After(4 * time.Second):
			t.Error("Run did not unwind")
		}
	}()
	select {
	case <-watcherStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("watcher not started")
	}
	for _, addr := range []string{apiAddr, profileAddr} {
		until := time.Now().Add(time.Second)
		for {
			c, err := net.DialTimeout("tcp4", addr, 50*time.Millisecond)
			if err == nil {
				c.Close()
				break
			}
			if time.Now().After(until) {
				t.Fatalf("listener at %s did not start: %v", addr, err)
			}
			time.Sleep(time.Millisecond)
		}
	}
	expired, cancel := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
	defer cancel()
	start := time.Now()
	err = service.Shutdown(expired)
	elapsed := time.Since(start)
	for _, endpoint := range []struct{ name, addr string }{{"API", apiAddr}, {"pprof", profileAddr}} {
		addr, resolveErr := net.ResolveTCPAddr("tcp4", endpoint.addr)
		if resolveErr != nil {
			t.Fatal(resolveErr)
		}
		l, bindErr := net.ListenTCP("tcp4", addr)
		if l != nil {
			l.Close()
		}
		t.Logf("immediate %s bind=%v after Shutdown returned %v in %s", endpoint.name, bindErr, err, elapsed)
		if bindErr != nil {
			t.Errorf("%s listener remained open after Shutdown returned", endpoint.name)
		}
	}
	if err != nil && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
}
func TestShutdownAbandonedWatcherCannotPublishConfig(t *testing.T) {
	cfg, path, _, _ := shutdownFixture(t, false)
	next, err := config.ParseConfigBytes([]byte("debug: true\n"))
	if err != nil {
		t.Fatal(err)
	}
	next.AuthDir, next.KVStore.Disabled = cfg.AuthDir, true
	entered, release, applied := make(chan struct{}), make(chan struct{}), make(chan struct{})
	unhold := sync.OnceFunc(func() { close(release) })
	defer unhold()
	service, err := NewBuilder().WithConfig(cfg).WithConfigPath(path).
		WithWatcherFactory(func(_, _ string, reload func(*config.Config)) (*WatcherWrapper, error) {
			close(entered)
			<-release
			reload(next)
			close(applied)
			return &WatcherWrapper{}, nil
		}).Build()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	run := make(chan error, 1)
	go func() { run <- service.Run(ctx) }()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("factory did not start")
	}
	cancel()
	select {
	case err := <-run:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return while the factory was held")
	}
	if err := service.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	unhold()
	select {
	case <-applied:
	case <-time.After(3 * time.Second):
		t.Fatal("factory did not invoke callback")
	}
	if service.currentConfig().Debug {
		t.Fatal("abandoned factory committed a configuration after shutdown")
	}
}

func TestShutdownWatcherStopPanicReachesCaller(t *testing.T) {
	const sentinel = "watcher Stop fixture panic"
	if os.Getenv("CPA_SHUTDOWN_WATCHER_STOP_PANIC_CHILD") == "1" {
		cfg, path, _, _ := shutdownFixture(t, false)
		started := make(chan struct{})
		service, err := NewBuilder().WithConfig(cfg).WithConfigPath(path).
			WithWatcherFactory(func(string, string, func(*config.Config)) (*WatcherWrapper, error) {
				return &WatcherWrapper{start: func(context.Context) error { close(started); return nil },
					stop: func() error { panic(sentinel) }}, nil
			}).Build()
		if err != nil {
			t.Fatal(err)
		}
		go func() { _ = service.Run(context.Background()) }()
		select {
		case <-started:
		case <-time.After(3 * time.Second):
			t.Fatal("watcher did not start")
		}
		defer func() {
			if got := recover(); got != sentinel {
				t.Fatalf("Shutdown caller recovered %v, want %s", got, sentinel)
			}
			t.Log("Shutdown caller recovered the watcher Stop panic")
		}()
		_ = service.Shutdown(context.Background())
		t.Fatal("Shutdown returned without the watcher Stop panic")
		return
	}
	child := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestShutdownWatcherStopPanicReachesCaller$", "-test.v", "-test.timeout=8s")
	child.Env = append(os.Environ(), "CPA_SHUTDOWN_WATCHER_STOP_PANIC_CHILD=1")
	output, err := child.CombinedOutput()
	t.Logf("child status=%v\n%s", err, output)
	if err != nil || !strings.Contains(string(output), "Shutdown caller recovered the watcher Stop panic") {
		t.Fatal("watcher Stop panic did not reach the Shutdown caller")
	}
}

func TestShutdownClosesMDNSListenerBeforeBlockedTeardown(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("the multicast bind assertion uses Linux UDP socket semantics")
	}
	ifaces, err := discovery.FilterInterfaces(nil, nil)
	if err != nil || len(ifaces) == 0 {
		t.Skipf("no multicast interface available: %v", err)
	}
	cfg, path, _, _ := shutdownFixture(t, false)
	cfg.Discovery.Enabled = true
	started, release := make(chan struct{}), make(chan struct{})
	releaseStop := sync.OnceFunc(func() { close(release) })
	service, err := NewBuilder().WithConfig(cfg).WithConfigPath(path).
		WithWatcherFactory(func(string, string, func(*config.Config)) (*WatcherWrapper, error) {
			return &WatcherWrapper{
				start: func(context.Context) error { close(started); return nil },
				stop:  func() error { <-release; return nil },
			}, nil
		}).Build()
	if err != nil {
		t.Fatal(err)
	}
	service.discoveryManager.refreshInterval = -1
	service.discoveryManager.buildSpec = func(*config.Config, int, bool) (discovery.ServiceSpec, error) {
		return discovery.ServiceSpec{
			InstanceName: "CPA-Drain-Fixture",
			ServiceType:  "_test-ai._tcp",
			Domain:       "local.",
			Port:         cfg.Port,
			Interfaces:   ifaces,
		}, nil
	}
	ctx, stop := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- service.Run(ctx) }()
	defer func() {
		releaseStop()
		stop()
		<-done
		_ = service.Shutdown(context.Background())
	}()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("service did not start")
	}
	service.discoveryManager.mu.Lock()
	advertising := service.discoveryManager.advertiser != nil
	service.discoveryManager.mu.Unlock()
	if !advertising {
		t.Skip("mDNS unavailable in this network")
	}
	if !hasMDNSSocket(t) {
		t.Fatal("advertiser published without a multicast socket")
	}
	expired, cancel := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
	defer cancel()
	if err := service.Shutdown(expired); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Shutdown returned %v, want deadline exceeded", err)
	}
	if hasMDNSSocket(t) {
		t.Fatal("mDNS listener remained open after Shutdown returned")
	}
}

// Inspect only sockets of this test process. Other packages may run mDNS
// fixtures concurrently in the same network namespace.
func hasMDNSSocket(t *testing.T) bool {
	t.Helper()
	sockets := make(map[string]bool)
	for _, table := range []string{"/proc/self/net/udp", "/proc/self/net/udp6"} {
		data, err := os.ReadFile(table)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		for _, line := range strings.Split(string(data), "\n") {
			fields := strings.Fields(line)
			if len(fields) > 9 && strings.HasSuffix(fields[1], ":14E9") {
				sockets["socket:["+fields[9]+"]"] = true
			}
		}
	}
	fds, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	for _, fd := range fds {
		target, err := os.Readlink(filepath.Join("/proc/self/fd", fd.Name()))
		if err == nil && sockets[target] {
			return true
		}
	}
	return false
}

func TestShutdownContainsPanicsAfterCallerReturns(t *testing.T) {
	const childEnv = "CPA_SHUTDOWN_LATE_PANIC"
	if os.Getenv(childEnv) == "" {
		for _, stage := range []string{"factory", "abandoned watcher Stop", "shutdown Stop"} {
			t.Run(stage, func(t *testing.T) {
				child := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestShutdownContainsPanicsAfterCallerReturns$", "-test.v", "-test.timeout=8s")
				child.Env = append(os.Environ(), childEnv+"="+stage)
				output, err := child.CombinedOutput()
				if err != nil || !strings.Contains(string(output), "late callback ran") {
					t.Fatalf("child returned %v\n%s", err, output)
				}
			})
		}
		return
	}
	stage := os.Getenv(childEnv)
	entered, started, release, panicking := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	unhold := sync.OnceFunc(func() { close(release) })
	defer unhold()
	cfg, path, _, _ := shutdownFixture(t, false)
	service, err := NewBuilder().WithConfig(cfg).WithConfigPath(path).
		WithWatcherFactory(func(string, string, func(*config.Config)) (*WatcherWrapper, error) {
			close(entered)
			if stage != "shutdown Stop" {
				<-release
			}
			if stage == "factory" {
				defer close(panicking)
				panic("late factory panic")
			}
			return &WatcherWrapper{
				start: func(context.Context) error { close(started); return nil },
				stop: func() error {
					<-release
					defer close(panicking)
					panic("late watcher Stop panic")
				},
			}, nil
		}).Build()
	if err != nil {
		t.Fatal(err)
	}
	ctx, stop := context.WithCancel(t.Context())
	defer stop()
	run := make(chan error, 1)
	go func() { run <- service.Run(ctx) }()
	<-entered
	if stage == "shutdown Stop" {
		<-started
		expired, cancel := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
		defer cancel()
		if err := service.Shutdown(expired); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("expired Shutdown returned %v", err)
		}
	} else {
		stop()
		<-run
		if err := service.Shutdown(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	unhold()
	<-panicking
	if stage == "shutdown Stop" {
		if err := service.Shutdown(context.Background()); err == nil {
			t.Fatal("later Shutdown did not report the panic")
		}
		<-run
	}
	t.Log("late callback ran")
}

func TestShutdownHeldProfileUsesDrainDeadline(t *testing.T) {
	for _, scenario := range []struct {
		name          string
		waitForCaller time.Duration
		hold          time.Duration
	}{
		{name: "caller deadline bounds wait", waitForCaller: 100 * time.Millisecond},
		{name: "configured budget exceeds legacy pprof cap", hold: 5250 * time.Millisecond},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			service := &Service{cfg: &config.Config{ShutdownTimeoutSeconds: 8}}
			entered, release := make(chan struct{}), make(chan struct{})
			profile := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				close(entered)
				<-release
				w.WriteHeader(http.StatusOK)
			}))
			defer profile.Close()
			unhold := sync.OnceFunc(func() { close(release) })
			defer unhold()
			profileClosing := make(chan struct{})
			profile.Config.RegisterOnShutdown(func() { close(profileClosing) })
			service.pprofServer = &pprofServer{server: profile.Config, addr: profile.Listener.Addr().String(), enabled: true}

			go func() {
				if response, err := profile.Client().Get(profile.URL); err == nil {
					_ = response.Body.Close()
				}
			}()
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("profile request did not start")
			}
			if scenario.waitForCaller > 0 {
				ctx, cancel := context.WithTimeout(t.Context(), scenario.waitForCaller)
				defer cancel()
				returned := make(chan error, 1)
				go func() { returned <- service.Shutdown(ctx) }()
				select {
				case err := <-returned:
					if !errors.Is(err, context.DeadlineExceeded) {
						t.Errorf("Shutdown returned %v, want deadline exceeded", err)
					}
					unhold()
					_ = service.Shutdown(context.Background())
				case <-time.After(time.Second):
					t.Error("held profile extended the caller's wait")
				}
				return
			}
			returned := make(chan error, 1)
			go func() { returned <- service.Shutdown(t.Context()) }()
			select {
			case <-profileClosing:
			case <-time.After(time.Second):
				t.Fatal("profile shutdown did not start")
			}
			select {
			case err := <-returned:
				t.Fatalf("Shutdown returned %v before the configured budget", err)
			case <-time.After(scenario.hold):
			}
			unhold()
			if err := <-returned; err != nil {
				t.Error(err)
			}
		})
	}
}

func TestShutdownAbandonsFactoryThatEnablesDrainBeforeReturning(t *testing.T) {
	cfg, path, _, _ := shutdownFixture(t, false)
	cfg.ShutdownTimeoutSeconds = 0
	enabled := cfg.CloneForRuntime()
	enabled.ShutdownTimeoutSeconds = 2
	late := cfg.CloneForRuntime()
	late.Debug = true
	entered, release, stopped := make(chan struct{}), make(chan struct{}), make(chan struct{})
	unhold := sync.OnceFunc(func() { close(release) })
	defer unhold()
	service, err := NewBuilder().WithConfig(cfg).WithConfigPath(path).
		WithWatcherFactory(func(_, _ string, reload func(*config.Config)) (*WatcherWrapper, error) {
			reload(enabled)
			close(entered)
			<-release
			reload(late)
			return &WatcherWrapper{stop: func() error { close(stopped); return nil }}, nil
		}).Build()
	if err != nil {
		t.Fatal(err)
	}
	run := make(chan error, 1)
	go func() { run <- service.Run(t.Context()) }()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("factory did not enable draining")
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if err := service.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	unhold()
	select {
	case <-run:
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return after the factory was released")
	}
	if service.currentConfig().Debug || service.currentWatcher() != nil {
		t.Error("factory published config or a watcher after shutdown")
	}
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Error("abandoned watcher was not stopped")
	}
}
