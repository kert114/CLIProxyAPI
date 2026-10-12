package management

import (
	"net"
	"net/http"
	"regexp"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/githubactivity"
)

var githubRepositoryPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9-]{0,38}/[A-Za-z0-9_.-]{1,100}$`)

func (h *Handler) GetGitHubActivity(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	host, _, errAddress := net.SplitHostPort(c.Request.RemoteAddr)
	if errAddress != nil || !net.ParseIP(host).IsLoopback() {
		c.JSON(http.StatusForbidden, gin.H{"error": "GitHub activity is available only on localhost", "code": "github_local_only"})
		return
	}
	repository := c.Query("repository")
	since, errSince := time.Parse(time.RFC3339, c.Query("since"))
	until, errUntil := time.Parse(time.RFC3339, c.Query("until"))
	if !githubRepositoryPattern.MatchString(repository) || errSince != nil || errUntil != nil || !until.After(since) || until.Sub(since) > 367*24*time.Hour {
		c.JSON(http.StatusBadRequest, gin.H{"error": "A repository and a calendar window of up to one calendar year are required", "code": "github_invalid_request"})
		return
	}
	client := h.githubActivityClient
	if client == nil {
		client = githubactivity.NewClient()
	}
	snapshot, errFetch := client.Fetch(c.Request.Context(), repository, since, until)
	if errFetch != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "GitHub activity could not be fetched. Check gh auth status and repository access, then refresh.", "code": "github_unavailable"})
		return
	}
	c.JSON(http.StatusOK, snapshot)
}
