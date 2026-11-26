package discovery

import (
	"net"
	"sync"
)

// Listeners owns a service's mDNS responder and probe sockets. It can stop
// admission without waiting for an advertiser or probe to unwind.
type Listeners struct {
	mu     sync.Mutex
	active map[*net.UDPConn]struct{}
	closed bool
}

// Close prevents new binds and closes sockets already in use.
func (l *Listeners) Close() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.closed = true
	for conn := range l.active {
		_ = conn.Close()
	}
	l.active = nil
}

type listenUDPFunc func(string, *net.UDPAddr) (*net.UDPConn, error)

// socketGroup ties the sockets of one responder or probe to its normal cleanup.
func (l *Listeners) socketGroup() (listenUDPFunc, func()) {
	if l == nil {
		return net.ListenUDP, func() {}
	}
	var sockets []*net.UDPConn
	listen := func(network string, addr *net.UDPAddr) (*net.UDPConn, error) {
		l.mu.Lock()
		defer l.mu.Unlock()
		if l.closed {
			return nil, net.ErrClosed
		}
		conn, err := net.ListenUDP(network, addr)
		if err != nil {
			return nil, err
		}
		if l.active == nil {
			l.active = make(map[*net.UDPConn]struct{})
		}
		l.active[conn] = struct{}{}
		sockets = append(sockets, conn)
		return conn, nil
	}
	release := func() {
		l.mu.Lock()
		defer l.mu.Unlock()
		for _, conn := range sockets {
			_ = conn.Close()
			delete(l.active, conn)
		}
	}
	return listen, release
}
