package api

import (
	"context"
	"crypto/tls"
	"fmt"
	log "github.com/sirupsen/logrus"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func TestServerStop_TLSCloseDeadline(t *testing.T) {
	certSource := httptest.NewTLSServer(http.NotFoundHandler())
	certificate := certSource.TLS.Certificates[0]
	certSource.Close()

	// A buffered or backpressured TLS transport cannot necessarily write a close alert.
	rawServer, rawClient := net.Pipe()
	defer rawServer.Close()
	defer rawClient.Close()
	listener := newMuxListener(rawServer.LocalAddr(), 1)
	entered, release := make(chan struct{}), make(chan struct{})
	defer close(release)
	server := &Server{
		muxHTTPListener: listener,
		server: &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			close(entered)
			<-release
		})},
	}
	go server.server.Serve(listener)
	tracked, err := server.connections.Track(rawServer)
	if err != nil {
		t.Fatal(err)
	}
	upstream := tls.Server(tracked, &tls.Config{Certificates: []tls.Certificate{certificate}})
	client := tls.Client(rawClient, &tls.Config{InsecureSkipVerify: true})
	if err := listener.Put(upstream); err != nil {
		t.Fatal(err)
	}
	if err := rawClient.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := fmt.Fprint(client, "GET / HTTP/1.1\r\nHost: fixture\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("TLS handler did not accept request")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	started := time.Now()
	err = server.StopWithDrainTimeout(ctx, 1)
	elapsed := time.Since(started)
	t.Logf("StopWithDrainTimeout returned %v after %s with a 100ms caller deadline", err, elapsed)
	if elapsed > 500*time.Millisecond {
		t.Fatalf("deadline overrun: %s", elapsed)
	}
}

type holdDeadlineLog struct {
	once    sync.Once
	entered chan struct{}
	release chan struct{}
}

func (*holdDeadlineLog) Levels() []log.Level { return []log.Level{log.WarnLevel} }
func (h *holdDeadlineLog) Fire(entry *log.Entry) error {
	if entry.Message == "HTTP drain ended before all requests finished, closing remaining connections" {
		h.once.Do(func() {
			close(h.entered)
			<-h.release
		})
	}
	return nil
}

func TestServerStop_DeadlineLogging(t *testing.T) {
	h := &holdDeadlineLog{entered: make(chan struct{}), release: make(chan struct{})}
	logger := log.StandardLogger()
	hooks := logger.ReplaceHooks(make(log.LevelHooks))
	defer logger.ReplaceHooks(hooks)
	logger.AddHook(h)
	releaseLog := sync.OnceFunc(func() { close(h.release) })
	defer releaseLog()
	entered, disconnected := make(chan struct{}), make(chan struct{})
	loopback, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer loopback.Close()
	base := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprintln(w, "started")
		w.(http.Flusher).Flush()
		close(entered)
		<-r.Context().Done()
		close(disconnected)
	})}
	server := &Server{server: base}
	go base.Serve(loopback)
	response, err := (&http.Client{Timeout: time.Second}).Get("http://" + loopback.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	<-entered
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	returned := make(chan struct{})
	go func() { defer close(returned); _ = server.StopWithDrainTimeout(ctx, 1) }()
	select {
	case <-h.entered:
	case <-time.After(time.Second):
		t.Fatal("deadline warning did not reach sink")
	}
	select {
	case <-returned:
	case <-time.After(200 * time.Millisecond):
		t.Error("deadline warning blocked Stop")
	}
	select {
	case <-disconnected:
	case <-time.After(200 * time.Millisecond):
		t.Error("deadline warning blocked transport closure")
	}
}

type holdAPIStartupLog struct {
	addr    string
	entered chan struct{}
	release chan struct{}
}

func (*holdAPIStartupLog) Levels() []log.Level { return []log.Level{log.DebugLevel} }

func (h *holdAPIStartupLog) Fire(entry *log.Entry) error {
	if entry.Message == "Starting API server on "+h.addr {
		close(h.entered)
		<-h.release
	}
	return nil
}

func TestServerStop_PositiveBudgetClosesListenerBeforeZeroModeStartupPublishesMux(t *testing.T) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	_ = listener.Close()

	hook := &holdAPIStartupLog{addr: addr, entered: make(chan struct{}), release: make(chan struct{})}
	logger := log.StandardLogger()
	hooks := logger.ReplaceHooks(make(log.LevelHooks))
	defer logger.ReplaceHooks(hooks)
	level := logger.GetLevel()
	logger.SetLevel(log.DebugLevel)
	defer logger.SetLevel(level)
	logger.AddHook(hook)

	server := &Server{server: &http.Server{Addr: addr}}
	done := make(chan error, 1)
	go func() { done <- server.Start() }()
	defer func() {
		close(hook.release)
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(time.Second):
			t.Error("API startup did not unwind")
		}
	}()
	select {
	case <-hook.entered:
	case <-time.After(time.Second):
		t.Fatal("API startup did not reach the hook after binding the socket")
	}
	expired, cancel := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
	defer cancel()
	if err := server.StopWithDrainTimeout(expired, 1); err != nil {
		t.Fatal(err)
	}
	rebound, err := net.Listen("tcp4", addr)
	if err != nil {
		t.Fatalf("API socket remained bound after shutdown: %v", err)
	}
	_ = rebound.Close()
}
