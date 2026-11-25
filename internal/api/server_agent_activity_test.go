package api

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/MoeForge/moego/ojson"
	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/api/handlers/management"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
)

func TestAgentActivityV8(t *testing.T) {
	gin.SetMode(gin.TestMode)
	fixture, errRead := os.ReadFile("testdata/agent-activity.json")
	if errRead != nil {
		t.Fatal(errRead)
	}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.RequestURI() != "/api/snapshot" || r.Header.Get("Authorization") != "" {
			t.Error("expected a GET of the fixed snapshot path without management credentials")
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		_, _ = w.Write(fixture)
	}))
	defer upstream.Close()
	t.Setenv("AGENT_ACTIVITY_URL", upstream.URL)
	t.Setenv("MANAGEMENT_PASSWORD", "test-management-password")
	cfg := &config.Config{}
	s := &Server{cfg: cfg, engine: gin.New(), mgmt: management.NewHandler(cfg, "", nil)}
	s.managementRoutesEnabled.Store(true)
	s.registerManagementV8Routes()

	for _, tc := range []struct {
		name, remote, key string
		want              int
	}{
		{"local with key", "127.0.0.1:1234", "test-management-password", http.StatusOK},
		{"local without key", "127.0.0.1:1234", "", http.StatusUnauthorized},
		{"remote with key", "192.0.2.2:1234", "test-management-password", http.StatusForbidden},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/v8/management/observability/agent-activity?refresh=1", nil)
			req.RemoteAddr = tc.remote
			req.Header.Set("X-Forwarded-For", "127.0.0.1")
			if tc.key != "" {
				req.Header.Set("Authorization", "Bearer "+tc.key)
			}
			w := httptest.NewRecorder()
			s.engine.ServeHTTP(w, req)
			if w.Code != tc.want {
				t.Fatalf("status=%d want=%d body=%s", w.Code, tc.want, w.Body.String())
			}
			if tc.want == http.StatusOK && (!bytes.Equal(w.Body.Bytes(), fixture) || w.Header().Get("Cache-Control") != "no-store") {
				t.Fatal("successful response must return the upstream snapshot without caching")
			}
			if tc.want == http.StatusForbidden && ojson.GetBytes(w.Body.Bytes(), "code").String() != "agent_activity_local_only" {
				t.Fatal("remote request did not hit the direct loopback check")
			}
		})
	}

	upstream.Close()
	req := httptest.NewRequest(http.MethodGet, "/v8/management/observability/agent-activity", nil)
	req.RemoteAddr = "127.0.0.1:1234"
	req.Header.Set("X-Management-Key", "test-management-password")
	w := httptest.NewRecorder()
	s.engine.ServeHTTP(w, req)
	if w.Code != http.StatusServiceUnavailable || ojson.GetBytes(w.Body.Bytes(), "code").String() != "agent_activity_unavailable" {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
}
