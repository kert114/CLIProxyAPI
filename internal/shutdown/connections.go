package shutdown

import (
	"crypto/tls"
	"net"
	"runtime"
	"sync"
	"weak"
)

// CloseTransport interrupts a connection without writing a TLS close alert.
func CloseTransport(conn net.Conn) error {
	for conn != nil {
		wrapped, ok := conn.(interface{ NetConn() net.Conn })
		if !ok {
			return conn.Close()
		}
		conn = wrapped.NetConn()
	}
	return net.ErrClosed
}

// Connections owns the physical connections behind a listener or transport.
type Connections struct {
	mu        sync.Mutex
	active    map[*trackedConn]struct{}
	tlsActive map[weak.Pointer[tls.Conn]]struct{}
	closed    bool
}

// Track tracks a connection until its ordinary Close or until the owner aborts it.
func (c *Connections) Track(conn net.Conn) (net.Conn, error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		_ = CloseTransport(conn)
		return nil, net.ErrClosed
	}
	if c.active == nil {
		c.active = make(map[*trackedConn]struct{})
	}
	tracked := &trackedConn{Conn: conn, owner: c}
	c.active[tracked] = struct{}{}
	c.mu.Unlock()
	return tracked, nil
}

// TrackTLS preserves the concrete TLS connection for net/http ALPN. A weak
// reference lets ordinary transport eviction release it without our help.
func (c *Connections) TrackTLS(conn *tls.Conn) (net.Conn, error) {
	token := weak.Make(conn)
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		_ = CloseTransport(conn)
		return nil, net.ErrClosed
	}
	if c.tlsActive == nil {
		c.tlsActive = make(map[weak.Pointer[tls.Conn]]struct{})
	}
	c.tlsActive[token] = struct{}{}
	c.mu.Unlock()
	runtime.AddCleanup(conn, func(key weak.Pointer[tls.Conn]) {
		c.mu.Lock()
		delete(c.tlsActive, key)
		c.mu.Unlock()
	}, token)
	return conn, nil
}

// Close stops admitting connections and interrupts every tracked transport.
func (c *Connections) Close() {
	c.mu.Lock()
	active, tlsActive := c.active, c.tlsActive
	c.active, c.tlsActive = nil, nil
	c.closed = true
	c.mu.Unlock()
	for conn := range active {
		_ = CloseTransport(conn.Conn)
	}
	for token := range tlsActive {
		if conn := token.Value(); conn != nil {
			_ = CloseTransport(conn)
		}
	}
}

type trackedConn struct {
	net.Conn
	owner *Connections
}

func (c *trackedConn) Close() error {
	err := c.Conn.Close()
	c.owner.mu.Lock()
	delete(c.owner.active, c)
	c.owner.mu.Unlock()
	return err
}

func (c *trackedConn) CloseWrite() error {
	if halfCloser, ok := c.Conn.(interface{ CloseWrite() error }); ok {
		return halfCloser.CloseWrite()
	}
	return nil
}
