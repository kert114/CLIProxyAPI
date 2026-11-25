package cliproxy

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/pprof"
	"strings"
	"sync"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	log "github.com/sirupsen/logrus"
)

type pprofServer struct {
	mu       sync.Mutex
	server   *http.Server
	listener net.Listener
	addr     string
	enabled  bool
	owner    uint64
	stopped  bool
}

func newPprofServer() *pprofServer {
	return &pprofServer{}
}

func (s *Service) applyPprofConfig(cfg *config.Config) {
	s.applyPprofConfigContext(context.Background(), cfg)
}

func (s *Service) applyPprofConfigContext(ctx context.Context, cfg *config.Config) bool {
	if s == nil || cfg == nil || (ctx != nil && ctx.Err() != nil) {
		return false
	}
	if s.applyPprofConfigContextFn != nil {
		return s.applyPprofConfigContextFn(ctx, cfg)
	}
	s.cfgMu.Lock()
	if s.stopping {
		s.cfgMu.Unlock()
		return false
	}
	if s.pprofServer == nil {
		s.pprofServer = newPprofServer()
	}
	profiler := s.pprofServer
	s.cfgMu.Unlock()
	return profiler.ApplyContext(ctx, cfg)
}

func (s *Service) shutdownPprof(ctx context.Context) error {
	return s.stopPprof(ctx, true)
}

func (s *Service) shutdownPprofLegacy(ctx context.Context) error {
	return s.stopPprof(ctx, false)
}

func (s *Service) stopPprof(ctx context.Context, bounded bool) error {
	if s == nil {
		return nil
	}
	s.cfgMu.RLock()
	profiler := s.pprofServer
	s.cfgMu.RUnlock()
	return profiler.shutdown(ctx, bounded)
}

func (p *pprofServer) Apply(cfg *config.Config) {
	p.ApplyContext(context.Background(), cfg)
}

func (p *pprofServer) ApplyContext(ctx context.Context, cfg *config.Config) bool {
	if p == nil || cfg == nil {
		return false
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if errContext := ctx.Err(); errContext != nil {
		return false
	}
	addr := strings.TrimSpace(cfg.Pprof.Addr)
	if addr == "" {
		addr = config.DefaultPprofAddr
	}
	enabled := cfg.Pprof.Enable

	p.mu.Lock()
	if p.stopped || ctx.Err() != nil {
		p.mu.Unlock()
		return false
	}
	p.owner++
	owner := p.owner
	currentServer, currentListener := p.server, p.listener
	currentAddr := p.addr
	p.addr = addr
	p.enabled = enabled
	if !enabled {
		p.server, p.listener = nil, nil
		p.mu.Unlock()
		if currentListener != nil {
			_ = currentListener.Close()
		}
		if currentServer != nil {
			if errStop := p.stopServerWithContext(ctx, currentServer, currentAddr, "disabled"); errStop != nil {
				return false
			}
		}
		return ctx.Err() == nil
	}
	if currentServer != nil && currentAddr == addr {
		p.mu.Unlock()
		return ctx.Err() == nil
	}
	p.server, p.listener = nil, nil
	p.mu.Unlock()

	if currentListener != nil {
		_ = currentListener.Close()
	}
	if currentServer != nil {
		if errStop := p.stopServerWithContext(ctx, currentServer, currentAddr, "restarted"); errStop != nil {
			return false
		}
	}
	if errContext := ctx.Err(); errContext != nil {
		return false
	}

	startedServer := p.startServer(addr, owner)
	if errContext := ctx.Err(); errContext != nil {
		if startedServer != nil {
			go func() {
				_ = p.stopOwnedServerWithContext(context.Background(), startedServer, addr, "canceled", owner)
			}()
		}
		return false
	}
	return true
}

func (p *pprofServer) Shutdown(ctx context.Context) error {
	return p.shutdown(ctx, true)
}

func (p *pprofServer) shutdown(ctx context.Context, bounded bool) error {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	currentServer, currentListener := p.server, p.listener
	currentAddr := p.addr
	p.owner++
	p.server, p.listener = nil, nil
	p.enabled = false
	p.stopped = bounded
	p.mu.Unlock()

	if currentListener != nil {
		_ = currentListener.Close()
	}
	if currentServer == nil {
		return nil
	}
	return p.stopServerMode(ctx, currentServer, currentAddr, "shutdown", bounded)
}

func (p *pprofServer) startServer(addr string, owner uint64) *http.Server {
	server := &http.Server{
		Addr:              addr,
		Handler:           newPprofMux(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	tcpAddr, errResolve := net.ResolveTCPAddr("tcp", addr)
	if errResolve != nil {
		log.Errorf("pprof server failed on %s: %v", addr, errResolve)
		return nil
	}
	p.mu.Lock()
	if !p.enabled || p.addr != addr || p.owner != owner || p.server != nil {
		p.mu.Unlock()
		return nil
	}
	listener, errListen := net.ListenTCP("tcp", tcpAddr)
	if errListen != nil {
		p.mu.Unlock()
		log.Errorf("pprof server failed on %s: %v", addr, errListen)
		return nil
	}
	p.server, p.listener = server, listener
	p.mu.Unlock()

	go func() {
		log.Infof("pprof server starting on %s", addr)
		if errServe := server.Serve(listener); errServe != nil && !errors.Is(errServe, http.ErrServerClosed) && !errors.Is(errServe, net.ErrClosed) {
			log.Errorf("pprof server failed on %s: %v", addr, errServe)
			p.clearFailedServer(server)
		}
	}()
	return server
}

// clearFailedServer removes a failed physical server even if a same-address
// ApplyContext transferred lifecycle ownership while ListenAndServe was starting.
func (p *pprofServer) clearFailedServer(server *http.Server) {
	if p == nil || server == nil {
		return
	}
	p.mu.Lock()
	if p.server == server {
		p.server, p.listener = nil, nil
	}
	p.mu.Unlock()
}

func (p *pprofServer) stopServer(server *http.Server, addr string, reason string) {
	_ = p.stopServerWithContext(context.Background(), server, addr, reason)
}

func (p *pprofServer) stopOwnedServerWithContext(ctx context.Context, server *http.Server, addr string, reason string, owner uint64) error {
	if p == nil || server == nil {
		return nil
	}
	p.mu.Lock()
	if p.server != server || p.owner != owner {
		p.mu.Unlock()
		return nil
	}
	listener := p.listener
	p.server, p.listener = nil, nil
	p.mu.Unlock()
	if listener != nil {
		_ = listener.Close()
	}
	return p.stopServerWithContext(ctx, server, addr, reason)
}

func (p *pprofServer) stopServerWithContext(ctx context.Context, server *http.Server, addr string, reason string) error {
	return p.stopServerMode(ctx, server, addr, reason, true)
}

func (p *pprofServer) stopServerMode(ctx context.Context, server *http.Server, addr string, reason string, bounded bool) error {
	if server == nil {
		return nil
	}
	stopCtx := ctx
	if stopCtx == nil {
		stopCtx = context.Background()
	}
	stopCtx, cancel := context.WithTimeout(stopCtx, 5*time.Second)
	defer cancel()
	if errStop := server.Shutdown(stopCtx); errStop != nil && !errors.Is(errStop, net.ErrClosed) {
		if bounded {
			if errClose := server.Close(); errClose != nil && !errors.Is(errClose, http.ErrServerClosed) {
				errStop = errors.Join(errStop, errClose)
			}
			go log.Errorf("pprof server stop failed on %s: %v", addr, errStop)
		} else {
			log.Errorf("pprof server stop failed on %s: %v", addr, errStop)
		}
		return errStop
	}
	if bounded {
		go log.Infof("pprof server stopped on %s (%s)", addr, reason)
	} else {
		log.Infof("pprof server stopped on %s (%s)", addr, reason)
	}
	return nil
}

func newPprofMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	mux.Handle("/debug/pprof/allocs", pprof.Handler("allocs"))
	mux.Handle("/debug/pprof/block", pprof.Handler("block"))
	mux.Handle("/debug/pprof/goroutine", pprof.Handler("goroutine"))
	mux.Handle("/debug/pprof/heap", pprof.Handler("heap"))
	mux.Handle("/debug/pprof/mutex", pprof.Handler("mutex"))
	mux.Handle("/debug/pprof/threadcreate", pprof.Handler("threadcreate"))
	return mux
}
