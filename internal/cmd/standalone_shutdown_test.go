//go:build unix

package cmd

import (
	"bytes"
	"errors"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	log "github.com/sirupsen/logrus"
)

func TestStandaloneShutdownWatchdog(t *testing.T) {
	if dir := os.Getenv("PROXY_SHUTDOWN_TEST_DIR"); dir != "" {
		runHeldStandaloneProxy(t, dir)
		return
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	port := listener.Addr().(*net.TCPAddr).Port
	_ = listener.Close()

	dir := t.TempDir()
	var output bytes.Buffer
	child := exec.Command(os.Args[0], "-test.run=^TestStandaloneShutdownWatchdog$")
	child.Env = append(os.Environ(), "PROXY_SHUTDOWN_TEST_DIR="+dir, "PROXY_SHUTDOWN_TEST_PORT="+strconv.Itoa(port))
	child.Stdout, child.Stderr = &output, &output
	if errStart := child.Start(); errStart != nil {
		t.Fatal(errStart)
	}
	done := make(chan error, 1)
	go func() { done <- child.Wait() }()
	defer func() {
		_ = child.Process.Kill()
		// Only read the buffer after Wait has joined the command's I/O copy.
		if done != nil {
			<-done
		}
		if t.Failed() {
			t.Log(output.String())
		}
	}()

	until := time.Now().Add(5 * time.Second)
	for {
		if _, errStat := os.Stat(filepath.Join(dir, "held")); errStat == nil {
			if conn, errDial := net.DialTimeout("tcp", address, 50*time.Millisecond); errDial == nil {
				_ = conn.Close()
				break
			}
		}
		if time.Now().After(until) {
			t.Fatal("child did not reach the held startup operation while accepting TCP")
		}
		time.Sleep(10 * time.Millisecond)
	}

	started := time.Now()
	if errSignal := child.Process.Signal(syscall.SIGTERM); errSignal != nil {
		t.Fatal(errSignal)
	}
	for {
		conn, errDial := net.DialTimeout("tcp", address, 25*time.Millisecond)
		if errors.Is(errDial, syscall.ECONNREFUSED) {
			t.Logf("new TCP refused after %s", time.Since(started))
			break
		}
		if conn != nil {
			_ = conn.Close()
		}
		if time.Since(started) > 200*time.Millisecond {
			t.Errorf("API still accepting TCP 200 ms after SIGTERM: %v", errDial)
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	select {
	case errExit := <-done:
		done = nil
		var exit *exec.ExitError
		if !errors.As(errExit, &exit) || exit.ExitCode() != 1 {
			t.Fatalf("stuck proxy exited after %s with %v, want watchdog exit status 1", time.Since(started), errExit)
		}
		t.Logf("watchdog exited with status 1 after %s", time.Since(started))
	case <-time.After(3300 * time.Millisecond):
		t.Fatal("stuck proxy exceeded the shutdown budget plus grace")
	}
}

func runHeldStandaloneProxy(t *testing.T, dir string) {
	port, err := strconv.Atoi(os.Getenv("PROXY_SHUTDOWN_TEST_PORT"))
	if err != nil {
		t.Fatal(err)
	}
	cfg := &config.Config{Host: "127.0.0.1", Port: port, AuthDir: dir, ShutdownTimeoutSeconds: 1}
	cfg.KVStore.Disabled = true
	path := filepath.Join(dir, "fixture.yaml")
	if errWrite := os.WriteFile(path, []byte("{}\n"), 0600); errWrite != nil {
		t.Fatal(errWrite)
	}

	registry.SetLocalModelCatalogs(true)
	log.AddHook(heldStartupLog{dir: dir})
	StartService(cfg, path, "")
}

type heldStartupLog struct{ dir string }

func (h heldStartupLog) Levels() []log.Level { return []log.Level{log.InfoLevel} }

func (h heldStartupLog) Fire(entry *log.Entry) error {
	if !strings.HasPrefix(entry.Message, "file watcher started") {
		return nil
	}
	if err := os.WriteFile(filepath.Join(h.dir, "held"), nil, 0600); err != nil {
		return err
	}
	select {}
}
