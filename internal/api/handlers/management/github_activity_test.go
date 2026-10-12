package management

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/githubactivity"
)

func TestGitHubActivityAcceptsCalendarYear(t *testing.T) {
	gin.SetMode(gin.TestMode)
	h := &Handler{githubActivityClient: &githubactivity.Client{Run: func(context.Context, []byte) ([]byte, error) {
		return []byte(`{"data":{"repository":{"nameWithOwner":"local/fork","url":"https://github.com/local/fork","pullRequests":{"totalCount":0,"nodes":[]},"defaultBranchRef":null},"created":{"issueCount":0,"nodes":[]},"merged":{"issueCount":0}}}`), nil
	}}}
	router := gin.New()
	router.GET("/github", h.GetGitHubActivity)
	req := httptest.NewRequest(http.MethodGet, "/github?repository=local/fork&since=2024-01-01T00:00:00Z&until=2025-01-01T00:00:00Z", nil)
	req.RemoteAddr = "127.0.0.1:1234"
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
}

func TestGitHubActivityAcceptsAccountScope(t *testing.T) {
	gin.SetMode(gin.TestMode)
	h := &Handler{githubActivityClient: &githubactivity.Client{Run: func(_ context.Context, input []byte) ([]byte, error) {
		if strings.Contains(string(input), "OrganizationScope") {
			return []byte(`{"data":{"organization":{"login":"60xAI","id":"org-id","url":"https://github.com/60xAI"}}}`), nil
		}
		if strings.Contains(string(input), "AccountSummary") {
			return []byte(`{"data":{"viewer":{"login":"kert114","id":"person","url":"https://github.com/kert114","contributionsCollection":{"pullRequestReviewContributions":{"nodes":[]}}},"created":{"nodes":[]},"merged":{},"issues":{"nodes":[]}}}`), nil
		}
		if strings.Contains(string(input), "AccountRepositories") || strings.Contains(string(input), "OrganizationRepositories") {
			return []byte(`{"data":{"viewer":{"repositories":{"nodes":[],"pageInfo":{"hasNextPage":false}}}}}`), nil
		}
		return []byte(`{"data":{"search":{"nodes":[],"pageInfo":{"hasNextPage":false}}}}`), nil
	}}}
	router := gin.New()
	router.GET("/github", h.GetGitHubActivity)
	for _, scope := range []string{"account=kert114", "account=kert114&organization=60xAI"} {
		req := httptest.NewRequest(http.MethodGet, "/github?"+scope+"&since=2026-01-01T00:00:00Z&until=2027-01-01T00:00:00Z", nil)
		req.RemoteAddr = "127.0.0.1:1234"
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"account":"kert114"`) {
			t.Fatalf("scope=%s status=%d body=%s", scope, w.Code, w.Body.String())
		}
		if strings.Contains(scope, "organization=") && !strings.Contains(w.Body.String(), `"organization":"60xAI"`) {
			t.Fatal("organization missing from response")
		}
	}

}

func TestGitHubActivityRejectsInvalidOrganizationScopes(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	h := &Handler{}
	router.GET("/github", h.GetGitHubActivity)
	for _, scope := range []string{"organization=60xAI", "repository=kert114/fork&account=kert114&organization=60xAI", "account=kert114&organization=60xAI%20org:other"} {
		req := httptest.NewRequest(http.MethodGet, "/github?"+scope+"&since=2026-01-01T00:00:00Z&until=2027-01-01T00:00:00Z", nil)
		req.RemoteAddr = "127.0.0.1:1234"
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("scope=%s status=%d", scope, w.Code)
		}
	}
}
