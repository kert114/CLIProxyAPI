package management

import (
	"errors"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/githubactivity"
)

// GetGitHubDashboard separates live PR state from cached contribution history.
func (h *Handler) GetGitHubDashboard(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	host, _, errAddress := net.SplitHostPort(c.Request.RemoteAddr)
	if errAddress != nil || !net.ParseIP(host).IsLoopback() {
		c.JSON(http.StatusForbidden, gin.H{"error": "GitHub activity is available only on localhost", "code": "github_local_only"})
		return
	}
	account, organization, section := c.Query("account"), c.Query("organization"), c.Query("section")
	refresh := c.Query("refresh")
	if !githubAccountPattern.MatchString(account) || (organization != "" && !githubAccountPattern.MatchString(organization)) || (section != "statistics" && section != "pulls") || (refresh != "" && refresh != "0" && refresh != "1") {
		c.JSON(http.StatusBadRequest, gin.H{"error": "A valid account, organization and dashboard section are required", "code": "github_invalid_request"})
		return
	}
	year := time.Now().UTC().Year()
	if value := c.Query("year"); value != "" {
		var errYear error
		year, errYear = strconv.Atoi(value)
		if errYear != nil || year < 1970 || year > time.Now().UTC().Year() {
			c.JSON(http.StatusBadRequest, gin.H{"error": "A valid year up to the current year is required", "code": "github_invalid_request"})
			return
		}
	}
	// The handler owns one cache and one request-coalescing group for all callers.
	h.mu.Lock()
	if h.githubDashboard == nil {
		client := h.githubActivityClient
		if client == nil {
			client = githubactivity.NewClient()
		}
		h.githubDashboard = githubactivity.NewDashboard(client, "")
	}
	dashboard := h.githubDashboard
	h.mu.Unlock()
	var response any
	var errFetch error
	if section == "statistics" {
		response, errFetch = dashboard.Statistics(c.Request.Context(), account, organization, year, refresh == "1")
	} else {
		response, errFetch = dashboard.PullRequests(c.Request.Context(), account, organization, refresh == "1")
	}
	if errors.Is(errFetch, githubactivity.ErrAccountMismatch) {
		c.JSON(http.StatusConflict, gin.H{"error": "The GitHub CLI account does not match the dashboard account", "code": "github_account_mismatch"})
		return
	}
	if errFetch != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "GitHub could not be refreshed. Try again shortly.", "code": "github_unavailable"})
		return
	}
	c.JSON(http.StatusOK, response)
}
