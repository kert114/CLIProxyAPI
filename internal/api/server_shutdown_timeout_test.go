package api

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
)

const (
	shutdownStreamFirst = "data: first\n\n"
	shutdownStreamLast  = "data: [DONE]\n\n"
)

func TestServerStop_ShutdownTimeout(t *testing.T) {
	for _, tc := range []struct {
		name         string
		drainSeconds int
		callerBudget time.Duration
		minDuration  time.Duration
		maxDuration  time.Duration
		completes    bool
	}{
		{name: "drains an active stream", drainSeconds: 5, callerBudget: 7 * time.Second, maxDuration: 5 * time.Second, completes: true},
		{name: "zero closes immediately", callerBudget: 7 * time.Second, maxDuration: 2 * time.Second},
		{name: "closes at the configured timeout", drainSeconds: 1, callerBudget: 7 * time.Second, minDuration: time.Second, maxDuration: 3 * time.Second},
		{name: "honors an earlier caller deadline", drainSeconds: 5, callerBudget: time.Second, minDuration: time.Second, maxDuration: 3 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			addr, release, stop := startShutdownStreamServer(t, tc.drainSeconds)
			client := &http.Client{Timeout: 8 * time.Second}
			resp, errGet := client.Get("http://" + addr + "/shutdown-test-stream")
			if errGet != nil {
				t.Fatal(errGet)
			}
			t.Cleanup(func() {
				if errClose := resp.Body.Close(); errClose != nil {
					t.Error(errClose)
				}
				client.CloseIdleConnections()
			})

			body := bufio.NewReader(resp.Body)
			first := make([]byte, len(shutdownStreamFirst))
			if _, errRead := io.ReadFull(body, first); resp.StatusCode != http.StatusOK || errRead != nil || string(first) != shutdownStreamFirst {
				t.Fatalf("stream start: status=%d chunk=%q err=%v", resp.StatusCode, first, errRead)
			}

			stopStarted := time.Now()
			stopCtx, cancel := context.WithTimeout(context.Background(), tc.callerBudget)
			defer cancel()
			stopOverdue := time.NewTimer(tc.maxDuration)
			defer stopOverdue.Stop()
			stopped := make(chan error, 1)
			go func() { stopped <- stop(stopCtx) }()
			waitForListenerToRefuseConnections(t, addr)

			if tc.completes {
				close(release)
				rest, errRead := io.ReadAll(body)
				if errRead != nil || string(rest) != shutdownStreamLast {
					t.Fatalf("stream was cut during drain: remaining=%q err=%v", rest, errRead)
				}
			}

			select {
			case errStop := <-stopped:
				if errStop != nil {
					t.Fatal(errStop)
				}
				if duration := time.Since(stopStarted); duration < tc.minDuration {
					t.Fatalf("shutdown returned in %v, before the %v drain ended", duration, tc.minDuration)
				}
			case <-stopOverdue.C:
				t.Fatalf("shutdown did not return within %v", tc.maxDuration)
			}

			if !tc.completes {
				if rest, errRead := io.ReadAll(body); len(rest) != 0 || !errors.Is(errRead, io.ErrUnexpectedEOF) {
					t.Fatalf("expected an interrupted stream: remaining=%q err=%v", rest, errRead)
				}
			}
		})
	}
}

func startShutdownStreamServer(t *testing.T, drainSeconds int) (string, chan struct{}, func(context.Context) error) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	cfg, errParse := config.ParseConfigBytes([]byte(fmt.Sprintf("server:\n  host: 127.0.0.1\n  shutdown-timeout: %d\n", drainSeconds)))
	if errParse != nil {
		t.Fatal(errParse)
	}
	cfg.Port = 0
	server := NewServer(cfg, nil, nil, "")
	release := make(chan struct{})
	server.engine.GET("/shutdown-test-stream", func(c *gin.Context) {
		c.Header("Content-Type", "text/event-stream")
		if _, errWrite := io.WriteString(c.Writer, shutdownStreamFirst); errWrite != nil {
			return
		}
		c.Writer.Flush()
		select {
		case <-release:
			if _, errWrite := io.WriteString(c.Writer, shutdownStreamLast); errWrite != nil {
				return
			}
			c.Writer.Flush()
		case <-c.Request.Context().Done():
		}
	})

	started := make(chan error, 1)
	go func() { started <- server.Start() }()
	t.Cleanup(func() {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if errStop := server.Stop(ctx); errStop != nil {
			t.Error(errStop)
		}
	})

	// The real server binds port zero, so no other process can race to claim it.
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	for {
		server.listenerMu.Lock()
		listener := server.muxBaseListener
		server.listenerMu.Unlock()
		if listener != nil {
			return listener.Addr().String(), release, server.Stop
		}
		select {
		case errStart := <-started:
			t.Fatalf("server exited before binding a listener: %v", errStart)
		case <-deadline.C:
			t.Fatal("server did not bind a listener")
		case <-ticker.C:
		}
	}
}

func waitForListenerToRefuseConnections(t *testing.T, addr string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()

	dialer := net.Dialer{}
	for {
		conn, errDial := dialer.DialContext(ctx, "tcp", addr)
		if errDial != nil && ctx.Err() == nil {
			return
		}
		if conn != nil {
			if errClose := conn.Close(); errClose != nil {
				t.Fatal(errClose)
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("new TCP connections were not refused: %v", errDial)
		case <-ticker.C:
		}
	}
}
