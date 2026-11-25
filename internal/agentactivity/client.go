// Package agentactivity reads snapshots from the local agent activity dashboard.
package agentactivity

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/MoeForge/moego/ojson"
)

const (
	defaultURL       = "http://127.0.0.1:8765"
	maxSnapshotBytes = 1 << 20
)

var snapshotClient = &http.Client{
	// The dashboard can spend up to eight seconds sampling enrolled hosts over SSH.
	Timeout:   10 * time.Second,
	Transport: &http.Transport{}, // Connect directly, ignoring environment proxy settings.
	CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	},
}

func snapshotURL(baseURL string) (string, error) {
	if baseURL = strings.TrimSpace(baseURL); baseURL == "" {
		baseURL = defaultURL
	}
	target, errParse := url.Parse(baseURL)
	if errParse != nil || target.Scheme != "http" || target.User != nil ||
		(target.EscapedPath() != "" && target.EscapedPath() != "/") ||
		target.RawQuery != "" || target.ForceQuery || target.Fragment != "" {
		return "", errors.New("agent activity URL must be a loopback HTTP origin")
	}
	host := target.Hostname()
	if strings.EqualFold(host, "localhost") {
		host = "127.0.0.1"
	}
	ip, errIP := netip.ParseAddr(host)
	if errIP != nil || !ip.IsLoopback() || ip.Zone() != "" {
		return "", errors.New("agent activity URL must use a loopback address")
	}
	port := target.Port()
	if port == "" {
		port = "80"
	}
	portNumber, errPort := strconv.ParseUint(port, 10, 16)
	if errPort != nil || portNumber == 0 {
		return "", errors.New("agent activity URL has an invalid port")
	}
	target.Host = net.JoinHostPort(ip.String(), strconv.FormatUint(portNumber, 10))
	target.Path = "/api/snapshot"
	return target.String(), nil
}

// Fetch returns a bounded JSON snapshot from the dashboard at baseURL.
func Fetch(ctx context.Context, baseURL string) ([]byte, error) {
	target, errURL := snapshotURL(baseURL)
	if errURL != nil {
		return nil, errURL
	}
	req, errRequest := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if errRequest != nil {
		return nil, errRequest
	}
	req.Header.Set("Accept", "application/json")
	resp, errFetch := snapshotClient.Do(req)
	if errFetch != nil {
		return nil, errFetch
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, errors.New("agent activity dashboard did not return a snapshot")
	}

	data, errRead := io.ReadAll(io.LimitReader(resp.Body, maxSnapshotBytes+1))
	if errRead != nil {
		return nil, errRead
	}
	if len(data) > maxSnapshotBytes || !ojson.ValidBytes(data) || !ojson.GetBytesNoCopy(data, "hosts").IsArray() {
		return nil, errors.New("agent activity snapshot is too large or invalid")
	}
	return data, nil
}
