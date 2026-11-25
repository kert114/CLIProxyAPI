package home

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"
)

type shutdownBlockedTLSConn struct {
	net.Conn
	started chan struct{}
	release chan struct{}
}

func (c *shutdownBlockedTLSConn) Close() error {
	close(c.started)
	<-c.release
	return nil
}
func (c *shutdownBlockedTLSConn) NetConn() net.Conn { return c.Conn }

type shutdownObservedConn struct {
	net.Conn
	onClose func()
}

func (c *shutdownObservedConn) Close() error {
	c.onClose()
	return c.Conn.Close()
}

func TestHomeTrackedTLSStaysInterruptibleDuringPoolClose(t *testing.T) {
	raw, peer := net.Pipe()
	defer raw.Close()
	defer peer.Close()
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	tls := &shutdownBlockedTLSConn{
		Conn:    &shutdownObservedConn{Conn: raw, onClose: unblock},
		started: make(chan struct{}),
		release: release,
	}
	client := &Client{}
	tracked, err := client.trackedRedisDialer(func(_ context.Context, _, _ string) (net.Conn, error) {
		return tls, nil
	})(t.Context(), "tcp", "fixture")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		_ = tracked.Close()
		close(done)
	}()
	select {
	case <-tls.started:
	case <-time.After(time.Second):
		t.Fatal("pool connection did not enter Close")
	}
	client.Close()
	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Home shutdown lost ownership of a transport still in TLS Close")
	}
}
