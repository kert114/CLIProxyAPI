package api

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/api/handlers/management"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
)

func TestManagementPanelServesNativeAssetWithoutInjectedLinks(t *testing.T) {
	gin.SetMode(gin.TestMode)
	dir := t.TempDir()
	t.Setenv("MANAGEMENT_STATIC_PATH", dir)
	path := filepath.Join(dir, "management.html")
	const original = "<!doctype html><html><body>management app</body></html>"
	if err := os.WriteFile(path, []byte(original), 0600); err != nil {
		t.Fatal(err)
	}
	s := &Server{cfg: &config.Config{}, engine: gin.New()}
	s.managementRoutesEnabled.Store(true)
	s.engine.GET("/management.html", s.serveManagementControlPanel)
	w := httptest.NewRecorder()
	s.engine.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/management.html", nil))
	if w.Code != http.StatusOK || w.Body.String() != original {
		t.Fatal("management panel changed the native frontend asset")
	}
	unchanged, err := os.ReadFile(path)
	if err != nil || string(unchanged) != original {
		t.Fatal("serving the panel changed the downloaded asset")
	}
}

func TestCursorUsagePageAvailability(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, tc := range []struct {
		name                  string
		enabled, home, hidden bool
		want                  int
	}{
		{"enabled", true, false, false, http.StatusOK},
		{"management disabled", false, false, false, http.StatusNotFound},
		{"home mode", true, true, false, http.StatusNotFound},
		{"panel disabled", true, false, true, http.StatusNotFound},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &config.Config{RemoteManagement: config.RemoteManagement{DisableControlPanel: tc.hidden}}
			cfg.Home.Enabled = tc.home
			s := &Server{cfg: cfg, engine: gin.New()}
			s.managementRoutesEnabled.Store(tc.enabled)
			s.engine.GET("/cursor-usage.html", s.serveCursorUsagePage)
			w := httptest.NewRecorder()
			s.engine.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/cursor-usage.html", nil))
			if w.Code != tc.want {
				t.Fatalf("status = %d, want %d", w.Code, tc.want)
			}
			if tc.want == http.StatusOK && (!strings.Contains(w.Body.String(), "Proxy management key") || w.Header().Get("Cache-Control") != "no-store" || !strings.Contains(w.Header().Get("Content-Security-Policy"), "frame-ancestors 'none'")) {
				t.Fatal("missing usage login or privacy headers")
			}
		})
	}
}

func TestCursorUsageRespectsManagementAvailability(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, home := range []bool{false, true} {
		cfg := &config.Config{RemoteManagement: config.RemoteManagement{SecretKey: "configured"}}
		cfg.Home.Enabled = home
		h := management.NewHandler(cfg, "", nil)
		h.SetLocalPassword("test-password")
		s := &Server{cfg: cfg, engine: gin.New(), mgmt: h}
		s.managementRoutesEnabled.Store(home)
		s.registerManagementRoutes()
		req := httptest.NewRequest(http.MethodGet, "/v8/management/observability/usage/cursor", nil)
		req.RemoteAddr = "127.0.0.1:1234"
		req.Header.Set("Authorization", "Bearer test-password")
		w := httptest.NewRecorder()
		s.engine.ServeHTTP(w, req)
		if w.Code != http.StatusNotFound {
			t.Fatalf("status = %d, want 404 for disabled management or Home mode", w.Code)
		}
	}
}

func TestCursorUsageRequiresManagementKey(t *testing.T) {
	gin.SetMode(gin.TestMode)
	cfg := &config.Config{RemoteManagement: config.RemoteManagement{SecretKey: "configured"}}
	h := management.NewHandler(cfg, "", nil)
	h.SetLocalPassword("test-password")
	s := &Server{cfg: cfg, engine: gin.New(), mgmt: h}
	s.managementRoutesEnabled.Store(true)
	s.registerManagementRoutes()

	req := httptest.NewRequest(http.MethodGet, "/v8/management/observability/usage/cursor", nil)
	req.RemoteAddr = "127.0.0.1:1234"
	w := httptest.NewRecorder()
	s.engine.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401; body = %s", w.Code, w.Body.String())
	}
}

func TestCursorUsageRejectsRemoteClientsEvenWithForwardedLoopback(t *testing.T) {
	gin.SetMode(gin.TestMode)
	cfg := &config.Config{RemoteManagement: config.RemoteManagement{SecretKey: "configured", AllowRemote: true}}
	t.Setenv("MANAGEMENT_PASSWORD", "test-password")
	h := management.NewHandler(cfg, "", nil)
	s := &Server{cfg: cfg, engine: gin.New(), mgmt: h}
	s.managementRoutesEnabled.Store(true)
	s.registerManagementRoutes()
	req := httptest.NewRequest(http.MethodGet, "/v8/management/observability/usage/cursor", nil)
	req.RemoteAddr = "192.0.2.1:1234"
	req.Header.Set("Authorization", "Bearer test-password")
	req.Header.Set("X-Forwarded-For", "127.0.0.1")
	w := httptest.NewRecorder()
	s.engine.ServeHTTP(w, req)
	if w.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403; body = %s", w.Code, w.Body.String())
	}
}
