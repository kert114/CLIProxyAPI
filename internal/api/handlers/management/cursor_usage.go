package management

import (
	"errors"
	"net"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/cursorusage"
)

func (h *Handler) GetCursorUsage(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	host, _, err := net.SplitHostPort(c.Request.RemoteAddr)
	if err != nil || !net.ParseIP(host).IsLoopback() {
		c.JSON(http.StatusForbidden, gin.H{"error": "Cursor usage is available only on localhost"})
		return
	}
	snapshot, err := h.cursorUsageClient.Fetch(c.Request.Context())
	if err != nil {
		status, code, message := http.StatusBadGateway, "cursor_upstream_unavailable", "Cursor usage could not be fetched. Try refreshing later."
		switch {
		case errors.Is(err, cursorusage.ErrLoginRequired):
			status, code, message = http.StatusServiceUnavailable, "cursor_login_required", "Sign in to Cursor CLI with agent login, then refresh."
		case errors.Is(err, cursorusage.ErrNoUsage):
			status, code, message = http.StatusServiceUnavailable, "cursor_usage_unavailable", "Cursor did not report included usage for this plan. Check the Cursor spending dashboard."
		}
		c.JSON(status, gin.H{"error": message, "code": code})
		return
	}
	c.JSON(http.StatusOK, snapshot)
}
