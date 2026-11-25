//go:build unix

package cmd

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/api"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy"
)

type drainWorkerProbe struct{ ctx context.Context }

func (p *drainWorkerProbe) Load(ctx context.Context, _ *config.Config) (*cliproxy.TokenClientResult, error) {
	p.ctx = ctx
	return &cliproxy.TokenClientResult{}, nil
}

func TestStandalonePreservesWorkerContextForEntireDrain(t *testing.T) {
	if rawPort := os.Getenv("CPA_LONG_DRAIN_PORT"); rawPort != "" {
		port, err := strconv.Atoi(rawPort)
		if err != nil {
			t.Fatal(err)
		}
		dir := os.Getenv("CPA_LONG_DRAIN_DIR")
		registry.SetLocalModelCatalogs(true)
		cfg := &config.Config{Host: "127.0.0.1", Port: port, AuthDir: dir, ShutdownTimeoutSeconds: 35}
		cfg.KVStore.Disabled = true
		worker := &drainWorkerProbe{}
		ready := make(chan struct{})
		ctx, publish, finish := standaloneSignalContext(35 * time.Second)
		defer finish()
		service, err := cliproxy.NewBuilder().WithConfig(cfg).WithConfigPath(filepath.Join(dir, "fixture.yaml")).
			WithHooks(cliproxy.Hooks{OnAfterStart: func(*cliproxy.Service) { close(ready) }}).
			WithTokenClientProvider(worker).WithServerOptions(api.WithEngineConfigurator(func(engine *gin.Engine) {
			engine.GET("/held-stream", func(c *gin.Context) {
				<-ready
				_, _ = io.WriteString(c.Writer, "first\n")
				c.Writer.Flush()
				<-c.Request.Context().Done()
			})
			engine.GET("/worker-stream", func(c *gin.Context) {
				_, _ = io.WriteString(c.Writer, "first\n")
				c.Writer.Flush()
				timer := time.NewTimer(31 * time.Second)
				defer timer.Stop()
				select {
				case <-timer.C:
					_, _ = io.WriteString(c.Writer, "final\n")
				case <-worker.ctx.Done():
					_, _ = fmt.Fprintf(c.Writer, "worker canceled: %s\n", worker.ctx.Err())
				}
			})
		})).Build()
		if err != nil {
			t.Fatal(err)
		}
		publish(service)
		if err := service.Run(ctx); err != nil && err != context.Canceled {
			t.Fatal(err)
		}
		return
	}
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	port := listener.Addr().(*net.TCPAddr).Port
	listener.Close()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "fixture.yaml"), []byte("{}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	child := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestStandalonePreservesWorkerContextForEntireDrain$", "-test.timeout=45s")
	child.Env = append(os.Environ(), "CPA_LONG_DRAIN_PORT="+strconv.Itoa(port), "CPA_LONG_DRAIN_DIR="+dir)
	child.Stdout, child.Stderr = os.Stdout, os.Stderr
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	reaped := false
	defer func() {
		if !reaped {
			child.Process.Kill()
			_ = child.Wait()
		}
	}()
	client := &http.Client{Timeout: 40 * time.Second}
	deadline := time.Now().Add(3 * time.Second)
	for {
		resp, err := client.Get("http://" + addr + "/healthz")
		if err == nil {
			resp.Body.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatal(err)
		}
		time.Sleep(5 * time.Millisecond)
	}
	openStream := func(path string) *http.Response {
		resp, err := client.Get("http://" + addr + path)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = resp.Body.Close() })
		first := make([]byte, len("first\n"))
		if _, err := io.ReadFull(resp.Body, first); err != nil || string(first) != "first\n" {
			t.Fatalf("%s first frame=%q, err=%v", path, first, err)
		}
		return resp
	}
	held := openStream("/held-stream")
	resp := openStream("/worker-stream")
	start := time.Now()
	if err := child.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	rest, err := io.ReadAll(resp.Body)
	elapsed := time.Since(start)
	t.Logf("remaining HTTP body after %s of a 35s budget: %q, err=%v", elapsed, rest, err)
	if err != nil || strings.TrimSpace(string(rest)) != "final" {
		t.Error("worker-backed HTTP stream lost its last frame before the configured drain deadline")
	}
	heldRest, errHeld := io.ReadAll(held.Body)
	if len(heldRest) != 0 || errHeld != io.ErrUnexpectedEOF {
		t.Errorf("held stream returned %q and %v, want forced connection closure", heldRest, errHeld)
	}
	if elapsed := time.Since(start); elapsed < 34*time.Second || elapsed > 37500*time.Millisecond {
		t.Errorf("held stream disconnected after %s, want the 35s shutdown deadline", elapsed)
	}
	exit := child.Wait()
	reaped = true
	if exit != nil {
		t.Fatal(exit)
	}
	t.Logf("process exited after %s of a 35s budget", time.Since(start))
	if elapsed := time.Since(start); elapsed > 37500*time.Millisecond {
		t.Errorf("process exit took %s, exceeding the shutdown deadline and grace", elapsed)
	}
}
