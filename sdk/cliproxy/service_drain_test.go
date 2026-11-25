package cliproxy

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/api"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/home"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/wsrelay"
)

func TestServiceShutdown_BoundsHomeDrainAndSupervisorWait(t *testing.T) {
	listener, errListen := net.Listen("tcp", "127.0.0.1:0")
	if errListen != nil {
		t.Fatal(errListen)
	}
	stop := make(chan struct{})
	t.Cleanup(func() {
		close(stop)
		_ = listener.Close()
		home.ClearCurrent()
	})
	go func() {
		for {
			conn, errAccept := listener.Accept()
			if errAccept != nil {
				return
			}
			go serveHomeShutdownConn(conn, stop)
		}
	}()

	service := newRegistryTestService(t, listener)
	service.startHomeSubscriber(t.Context())
	supervisor := service.homeSupervisor
	t.Cleanup(func() {
		supervisor.cancel()
		select {
		case <-supervisor.done:
		case <-time.After(5 * time.Second):
			t.Error("Home supervisor did not exit during cleanup")
		}
	})
	registry := waitForServiceRegistry(t, service, 5*time.Second)
	pending, errBegin := registry.BeginDispatch()
	if errBegin != nil {
		t.Fatal(errBegin)
	}
	defer pending.End()

	// Model HTTP having already used all but this much of the service deadline.
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	shutdownDone := make(chan struct{})
	go func() {
		defer close(shutdownDone)
		_ = service.Shutdown(ctx)
	}()
	bound := time.After(time.Second)
	select {
	case <-shutdownDone:
	case <-bound:
		t.Fatal("Shutdown waited past its deadline for a pending Home dispatch")
	}
	select {
	case <-supervisor.done:
	case <-bound:
		t.Fatal("Home supervisor kept draining after the service deadline")
	}
}

func serveHomeShutdownConn(conn net.Conn, stop <-chan struct{}) {
	defer func() { _ = conn.Close() }()
	reader := bufio.NewReader(conn)
	for {
		args, errRead := readRegistryTestRedisCommand(reader)
		if errRead != nil {
			return
		}
		var reply string
		switch {
		case len(args) > 0 && strings.EqualFold(args[0], "HELLO"):
			reply = "%6\r\n$6\r\nserver\r\n$5\r\nredis\r\n$5\r\nproto\r\n:3\r\n$2\r\nid\r\n:1\r\n$4\r\nmode\r\n$10\r\nstandalone\r\n$4\r\nrole\r\n$6\r\nmaster\r\n$7\r\nmodules\r\n*0\r\n"
		case len(args) >= 2 && strings.EqualFold(args[0], "GET") && args[1] == "config":
			writeRegistryTestConfig(conn, "credential-concurrency:\n  lifecycle-config-revision: 1\n  cpa-cancel-bound: 3s\n")
			continue
		case len(args) >= 2 && strings.EqualFold(args[0], "GET") && args[1] == "plugin-tasks":
			reply = "$2\r\n[]\r\n"
		case len(args) >= 2 && strings.EqualFold(args[0], "SUBSCRIBE") && args[1] == "config":
			_, _ = io.WriteString(conn, "*3\r\n$9\r\nsubscribe\r\n$6\r\nconfig\r\n:1\r\n")
			<-stop
			return
		default:
			reply = "+OK\r\n"
		}
		if _, errWrite := io.WriteString(conn, reply); errWrite != nil {
			return
		}
	}
}

func TestServiceShutdown_DrainsRelayBackedStream(t *testing.T) {
	relay := wsrelay.NewManager(wsrelay.Options{
		ProviderFactory: func(*http.Request) (string, error) { return "fixture", nil },
	})
	service, addr, serving := startDrainTestService(t, api.WithEngineConfigurator(func(engine *gin.Engine) {
		engine.GET(relay.Path(), gin.WrapH(relay.Handler()))
		engine.GET("/relay-stream", func(c *gin.Context) {
			events, errStream := relay.Stream(c.Request.Context(), "fixture", &wsrelay.HTTPRequest{
				Method: http.MethodGet,
				URL:    "http://fixture.test/stream",
			})
			if errStream != nil {
				c.String(http.StatusBadGateway, "%v", errStream)
				return
			}
			for event := range events {
				if event.Err != nil {
					_, _ = io.WriteString(c.Writer, event.Err.Error())
					return
				}
				if event.Type == wsrelay.MessageTypeStreamChunk {
					_, _ = c.Writer.Write(event.Payload)
					c.Writer.Flush()
				}
			}
		})
	}))
	service.wsGateway = relay
	dialer := websocket.Dialer{HandshakeTimeout: 3 * time.Second}
	peer, _, errDial := dialer.Dial("ws://"+addr+relay.Path(), nil)
	if errDial != nil {
		t.Fatal(errDial)
	}
	defer func() { _ = peer.Close() }()
	_ = peer.SetReadDeadline(time.Now().Add(10 * time.Second))

	client := &http.Client{Timeout: 10 * time.Second}
	type response struct {
		resp *http.Response
		err  error
	}
	responses := make(chan response, 1)
	go func() {
		resp, errGet := client.Get("http://" + addr + "/relay-stream")
		responses <- response{resp, errGet}
	}()

	var request wsrelay.Message
	if errRead := peer.ReadJSON(&request); errRead != nil {
		t.Fatal(errRead)
	}
	send := func(kind string, payload map[string]any) error {
		return peer.WriteJSON(wsrelay.Message{ID: request.ID, Type: kind, Payload: payload})
	}
	if errSend := send(wsrelay.MessageTypeStreamChunk, map[string]any{"data": "first\n"}); errSend != nil {
		t.Fatal(errSend)
	}
	result := <-responses
	if result.err != nil {
		t.Fatal(result.err)
	}
	defer func() { _ = result.resp.Body.Close() }()
	body := bufio.NewReader(result.resp.Body)
	if first, errRead := body.ReadString('\n'); first != "first\n" || errRead != nil {
		t.Fatalf("first relay chunk = %q, err = %v", first, errRead)
	}

	shutdown := make(chan error, 1)
	go func() { shutdown <- service.Shutdown(context.Background()) }()
	select {
	case errServe := <-serving:
		if errServe != nil {
			t.Fatal(errServe)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("proxy listener did not close")
	}

	errChunk := send(wsrelay.MessageTypeStreamChunk, map[string]any{"data": "final\n"})
	errEnd := send(wsrelay.MessageTypeStreamEnd, nil)
	remaining, errRead := io.ReadAll(body)
	if string(remaining) != "final\n" || errRead != nil || errChunk != nil || errEnd != nil {
		t.Fatalf("stream after listener closed = %q, read error = %v, relay write errors = %v / %v", remaining, errRead, errChunk, errEnd)
	}
	if errStop := <-shutdown; errStop != nil {
		t.Fatal(errStop)
	}
}

func startDrainTestService(t *testing.T, options ...api.ServerOption) (*Service, string, <-chan error) {
	t.Helper()
	listener, errListen := net.Listen("tcp", "127.0.0.1:0")
	if errListen != nil {
		t.Fatal(errListen)
	}
	addr := listener.Addr().String()
	cfg := &config.Config{
		Host:                   "127.0.0.1",
		Port:                   listener.Addr().(*net.TCPAddr).Port,
		AuthDir:                t.TempDir(),
		ShutdownTimeoutSeconds: 5,
	}
	if errClose := listener.Close(); errClose != nil {
		t.Fatal(errClose)
	}
	server := api.NewServer(cfg, nil, nil, filepath.Join(cfg.AuthDir, "fixture.yaml"), options...)
	service := &Service{cfg: cfg, server: server}
	t.Cleanup(func() {
		if errStop := service.Shutdown(context.Background()); errStop != nil {
			t.Error(errStop)
		}
	})
	serving := make(chan error, 1)
	go func() { serving <- server.Start() }()

	dialer := net.Dialer{}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		conn, errDial := dialer.DialContext(ctx, "tcp", addr)
		if errDial == nil {
			_ = conn.Close()
			return service, addr, serving
		}
		select {
		case errStart := <-serving:
			t.Fatalf("proxy stopped before listening: %v", errStart)
		case <-ctx.Done():
			t.Fatalf("proxy did not start listening: %v", errDial)
		case <-ticker.C:
		}
	}
}

func TestServiceShutdown_ClosesProxyListenerBeforeWaitingForProfile(t *testing.T) {
	service, addr, _ := startDrainTestService(t)
	profileActive := make(chan struct{})
	releaseProfile := make(chan struct{})
	release := sync.OnceFunc(func() { close(releaseProfile) })
	profile := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(profileActive)
		<-releaseProfile
		w.WriteHeader(http.StatusOK)
	}))
	defer profile.Close()
	defer release()
	profileStopping := make(chan struct{})
	profile.Config.RegisterOnShutdown(func() { close(profileStopping) })
	service.pprofServer = &pprofServer{server: profile.Config, addr: profile.Listener.Addr().String(), enabled: true}

	go func() {
		resp, errGet := profile.Client().Get(profile.URL + "/debug/pprof/goroutine")
		if errGet == nil {
			_ = resp.Body.Close()
		}
	}()
	select {
	case <-profileActive:
	case <-time.After(3 * time.Second):
		t.Fatal("profiling request did not start")
	}
	shutdown := make(chan error, 1)
	go func() { shutdown <- service.Shutdown(context.Background()) }()
	select {
	case <-profileStopping:
	case <-time.After(3 * time.Second):
		t.Fatal("profiling listener did not start shutdown")
	}

	conn, errDial := net.DialTimeout("tcp", addr, time.Second)
	if conn != nil {
		_ = conn.Close()
	}
	if !errors.Is(errDial, syscall.ECONNREFUSED) {
		t.Fatalf("TCP dial while profiling request is active = %v, want connection refused", errDial)
	}
	select {
	case errStop := <-shutdown:
		t.Fatalf("shutdown returned before profiling request completed: %v", errStop)
	default:
	}
	release()
	if errStop := <-shutdown; errStop != nil {
		t.Fatal(errStop)
	}
}
