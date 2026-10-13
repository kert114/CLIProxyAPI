package management

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

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

func TestGitHubActivityCacheRefreshAndLoopbackBoundary(t *testing.T) {
	gin.SetMode(gin.TestMode)
	queries := 0
	cache := githubactivity.NewSnapshotCache(t.TempDir())
	client := &githubactivity.Client{Cache: cache, Run: func(_ context.Context, input []byte) ([]byte, error) {
		if strings.Contains(string(input), "SnapshotViewer") {
			return []byte(`{"data":{"viewer":{"id":"person-id","login":"person"}}}`), nil
		}
		queries++
		payload := map[string]any{"data": map[string]any{"repository": map[string]any{"nameWithOwner": "owner/repo", "url": "https://github.com/owner/repo", "pullRequests": map[string]any{"totalCount": queries, "nodes": []any{}}, "defaultBranchRef": nil}, "created": map[string]any{"nodes": []any{}}, "merged": map[string]any{}}}
		return json.Marshal(payload)
	}}
	h := &Handler{githubActivityClient: client}
	router := gin.New()
	router.GET("/github", h.GetGitHubActivity)
	url := "/github?repository=owner/repo&since=2026-10-12T00:00:00Z&until=2026-10-13T00:00:00Z"
	for index, suffix := range []string{"", "", "&refresh=1"} {
		req := httptest.NewRequest(http.MethodGet, url+suffix, nil)
		req.RemoteAddr = "127.0.0.1:1234"
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		var body githubactivity.Snapshot
		if json.Unmarshal(w.Body.Bytes(), &body) != nil || w.Code != http.StatusOK || w.Header().Get("Cache-Control") != "no-store" {
			t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
		}
		want := 1
		if index == 2 {
			want = 2
		}
		if body.OpenCount != want || queries != want || body.CheckedAt == "" {
			t.Fatalf("refresh request %d snapshot=%+v queries=%d", index, body, queries)
		}
	}
	req := httptest.NewRequest(http.MethodGet, url, nil)
	req.RemoteAddr = "203.0.113.8:1234"
	req.Header.Set("X-Forwarded-For", "127.0.0.1")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusForbidden || queries != 2 {
		t.Fatalf("cached snapshot crossed loopback boundary: status=%d queries=%d", w.Code, queries)
	}
}

func TestGitHubActivityCachedIdentityFailureDoesNotLeakSnapshot(t *testing.T) {
	gin.SetMode(gin.TestMode)
	client := &githubactivity.Client{Cache: githubactivity.NewSnapshotCache(t.TempDir()), Run: func(context.Context, []byte) ([]byte, error) {
		return []byte(`{"errors":[{"message":"private identity error"}]}`), nil
	}}
	h := &Handler{githubActivityClient: client}
	router := gin.New()
	router.GET("/github", h.GetGitHubActivity)
	req := httptest.NewRequest(http.MethodGet, "/github?repository=owner/repo&since="+time.Now().Add(-time.Hour).UTC().Format(time.RFC3339)+"&until="+time.Now().UTC().Format(time.RFC3339), nil)
	req.RemoteAddr = "127.0.0.1:1234"
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusServiceUnavailable || strings.Contains(w.Body.String(), "private identity") || !strings.Contains(w.Body.String(), "github_unavailable") {
		t.Fatalf("identity error leaked or succeeded: status=%d body=%s", w.Code, w.Body.String())
	}
}
