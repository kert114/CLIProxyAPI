package management

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/githubactivity"
)

func TestGitHubDashboardScopeAndLoopback(t *testing.T) {
	gin.SetMode(gin.TestMode)
	upstream := 0
	client := &githubactivity.Client{Run: func(_ context.Context, input []byte) ([]byte, error) {
		upstream++
		if strings.Contains(string(input), "DashboardViewer") {
			return []byte(`{"data":{"viewer":{"id":"viewer-1","login":"tester"}}}`), nil
		}
		return json.Marshal(map[string]any{"data": map[string]any{"viewer": map[string]any{"login": "tester"}, "search": map[string]any{"issueCount": 0, "nodes": []any{}, "pageInfo": map[string]any{"hasNextPage": false}}}})
	}}
	h := &Handler{githubActivityClient: client}
	router := gin.New()
	router.GET("/dashboard", h.GetGitHubDashboard)
	for _, test := range []struct {
		url, remote string
		code        int
	}{{"/dashboard?account=tester&organization=example&section=pulls", "127.0.0.1:1234", 200}, {"/dashboard?account=tester&organization=example&section=pulls", "203.0.113.1:1234", 403}, {"/dashboard?account=tester&organization=example%20org:another&section=pulls", "127.0.0.1:1234", 400}, {"/dashboard?account=tester&section=bad", "127.0.0.1:1234", 400}, {"/dashboard?account=tester&section=statistics&year=9999", "127.0.0.1:1234", 400}, {"/dashboard?account=another&organization=example&section=pulls", "127.0.0.1:1234", 409}} {
		before := upstream
		request := httptest.NewRequest(http.MethodGet, test.url, nil)
		request.RemoteAddr = test.remote
		request.Header.Set("X-Forwarded-For", "127.0.0.1")
		response := httptest.NewRecorder()
		router.ServeHTTP(response, request)
		if response.Code != test.code {
			t.Fatalf("%s got %d: %s", test.url, response.Code, response.Body.String())
		}
		if response.Header().Get("Cache-Control") != "no-store" {
			t.Fatal("private response allowed HTTP caching")
		}
		if (test.code == 400 || test.code == 403) && upstream != before {
			t.Fatal("rejected request reached GitHub")
		}
	}
}
