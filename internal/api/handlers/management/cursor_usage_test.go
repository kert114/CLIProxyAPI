package management

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/cursorusage"
)

type cursorRoundTripFunc func(*http.Request) (*http.Response, error)

func (f cursorRoundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestGetCursorUsageReturnsSafeSnapshot(t *testing.T) {
	gin.SetMode(gin.TestMode)
	h := &Handler{cursorUsageClient: &cursorusage.Client{
		Credentials: func(context.Context) (cursorusage.Credentials, error) {
			return cursorusage.Credentials{AccessToken: "test-cursor-token"}, nil
		},
		HTTPClient: &http.Client{Transport: cursorRoundTripFunc(func(r *http.Request) (*http.Response, error) {
			body := `{}`
			if strings.HasSuffix(r.URL.Path, "GetCurrentPeriodUsage") {
				body = `{"planUsage":{"totalPercentUsed":25},"accessToken":"never-return-this"}`
			}
			return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}, nil
		})},
	}}
	router := gin.New()
	router.GET("/usage", h.GetCursorUsage)
	req := httptest.NewRequest(http.MethodGet, "/usage", nil)
	req.RemoteAddr = "127.0.0.1:1234"
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"percent_used":25`) || w.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("status=%d body=%s cache=%s", w.Code, w.Body.String(), w.Header().Get("Cache-Control"))
	}
	if strings.Contains(w.Body.String(), "never-return-this") || strings.Contains(w.Body.String(), "test-cursor-token") {
		t.Fatal("Cursor usage API leaked an access token")
	}
}
