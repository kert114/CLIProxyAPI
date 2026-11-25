package wsrelay

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

type singleConnListener struct {
	conn     net.Conn
	accepted sync.Once
	done     chan struct{}
	close    sync.Once
}

func (l *singleConnListener) Accept() (net.Conn, error) {
	var conn net.Conn
	l.accepted.Do(func() { conn = l.conn })
	if conn == nil {
		<-l.done
		return nil, net.ErrClosed
	}
	return conn, nil
}

func (l *singleConnListener) Addr() net.Addr { return l.conn.LocalAddr() }
func (l *singleConnListener) Close() error {
	l.close.Do(func() { close(l.done) })
	return nil
}

func TestRelayStopInterruptsTLSClose(t *testing.T) {
	ready := make(chan struct{})
	relay := NewManager(Options{OnConnected: func(string) { close(ready) }})
	source := httptest.NewTLSServer(http.NotFoundHandler())
	cert := source.TLS.Certificates[0]
	source.Close()

	rawServer, rawClient := net.Pipe()
	defer rawServer.Close()
	defer rawClient.Close()
	listener := &singleConnListener{conn: rawServer, done: make(chan struct{})}
	defer listener.Close()
	server := &http.Server{Handler: relay.Handler()}
	go func() {
		_ = server.Serve(tls.NewListener(listener, &tls.Config{Certificates: []tls.Certificate{cert}}))
	}()
	dialer := websocket.Dialer{
		HandshakeTimeout: time.Second,
		TLSClientConfig:  &tls.Config{InsecureSkipVerify: true},
		NetDialContext:   func(context.Context, string, string) (net.Conn, error) { return rawClient, nil },
	}
	if _, _, err := dialer.Dial("wss://fixture.test"+relay.Path(), nil); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ready:
	case <-time.After(time.Second):
		t.Fatal("relay did not register the connection")
	}

	// The client does not read the server's TLS close alert. Closing the
	// underlying pipe must wake the transport without waiting for that alert.
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- relay.Stop(ctx) }()
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("relay Stop waited for TLS close notification")
	}
}
