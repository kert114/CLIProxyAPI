package management

import (
	"net"
	"net/http"
	"os"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/agentactivity"
)

// GetAgentActivity returns the snapshot from the local agent activity dashboard.
func (h *Handler) GetAgentActivity(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	host, _, errAddress := net.SplitHostPort(c.Request.RemoteAddr)
	if errAddress != nil || !net.ParseIP(host).IsLoopback() {
		c.JSON(http.StatusForbidden, gin.H{
			"error":   "agent_activity_local_only",
			"message": "Agent activity is available only on localhost",
		})
		return
	}
	data, errFetch := agentactivity.Fetch(c.Request.Context(), os.Getenv("AGENT_ACTIVITY_URL"))
	if errFetch != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"error":   "agent_activity_unavailable",
			"message": "Agent activity dashboard is unavailable. Make sure agent-activity serve is running.",
		})
		return
	}
	c.Data(http.StatusOK, "application/json", data)
}
