package management

import (
	"context"
	"net/http"
	"net/http/httptest"
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
