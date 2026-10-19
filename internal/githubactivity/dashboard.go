package githubactivity

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/singleflight"
)

type ContributionDay struct {
	Date          string `json:"date"`
	Contributions int    `json:"contributions"`
}
type Statistics struct {
	Account      string            `json:"account"`
	Organization string            `json:"organization"`
	Year         int               `json:"year"`
	Days         []ContributionDay `json:"days"`
	CheckedAt    time.Time         `json:"checked_at"`
	Stale        bool              `json:"stale"`
	Warning      string            `json:"warning,omitempty"`
	Limited      bool              `json:"limited"`
}
type DashboardPullRequest struct {
	PullRequest
	NodeID string `json:"node_id"`
}
type PullRequests struct {
	Account      string                 `json:"account"`
	Organization string                 `json:"organization"`
	PullRequests []DashboardPullRequest `json:"pull_requests"`
	CheckedAt    time.Time              `json:"checked_at"`
	Stale        bool                   `json:"stale"`
	Truncated    bool                   `json:"truncated"`
	Warning      string                 `json:"warning,omitempty"`
}
type dashboardEntry struct {
	Statistics    *Statistics   `json:"statistics,omitempty"`
	Pulls         *PullRequests `json:"pulls,omitempty"`
	FullCheckedAt time.Time     `json:"full_checked_at"`
}
type Dashboard struct {
	client     *Client
	now        func() time.Time
	path       string
	mu         sync.Mutex
	entries    map[string]dashboardEntry
	retryAfter map[string]time.Time
	flights    singleflight.Group
}

const maxDashboardEntries = 32
const dashboardWarning = "GitHub could not be refreshed. Showing the last saved result."

var ErrAccountMismatch = fmt.Errorf("GitHub CLI is signed in to a different account")

func NewDashboard(client *Client, cachePath string) *Dashboard {
	d := &Dashboard{client: client, now: time.Now, path: cachePath, entries: make(map[string]dashboardEntry), retryAfter: make(map[string]time.Time)}
	if cachePath != "" {
		file, errOpen := os.Open(cachePath)
		if errOpen == nil {
			var disk struct {
				Version int
				Entries map[string]dashboardEntry
			}
			errDecode := json.NewDecoder(io.LimitReader(file, 8<<20)).Decode(&disk)
			if errClose := file.Close(); errClose != nil {
				log.Warn("Could not close GitHub history cache")
			}
			if errDecode == nil && disk.Version == 1 && disk.Entries != nil && len(disk.Entries) <= maxDashboardEntries {
				d.entries = disk.Entries
			} else {
				log.Warn("Ignoring invalid GitHub history cache")
			}
		} else if !os.IsNotExist(errOpen) {
			log.Warn("Could not read GitHub history cache")
		}
	}
	return d
}
func dashboardKey(kind, account, organization string, year int) string {
	return fmt.Sprintf("%s|%s|%s|%d", kind, strings.ToLower(account), strings.ToLower(organization), year)
}
func (d *Dashboard) entry(key string) (dashboardEntry, time.Time) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.entries[key], d.retryAfter[key]
}
func entryTime(e dashboardEntry) time.Time {
	if e.Statistics != nil {
		return e.Statistics.CheckedAt
	}
	if e.Pulls != nil {
		return e.Pulls.CheckedAt
	}
	return time.Time{}
}
func (d *Dashboard) save(key string, entry dashboardEntry) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.entries[key] = entry
	delete(d.retryAfter, key)
	for {
		data, err := json.Marshal(d.entries)
		if err == nil && len(d.entries) <= maxDashboardEntries && len(data) <= (8<<20)-128 {
			break
		}
		oldest := ""
		for k, e := range d.entries {
			if k != key && (oldest == "" || entryTime(e).Before(entryTime(d.entries[oldest]))) {
				oldest = k
			}
		}
		if oldest == "" {
			delete(d.entries, key)
			break
		}
		delete(d.entries, oldest)
		delete(d.retryAfter, oldest)
	}
	if d.path == "" {
		return
	}
	if err := d.persist(); err != nil {
		log.Warn("Could not persist GitHub history cache; memory cache remains available")
	}
}
func (d *Dashboard) persist() error {
	data, errEncode := json.Marshal(struct {
		Version int
		Entries map[string]dashboardEntry
	}{1, d.entries})
	if errEncode != nil {
		return errEncode
	}
	if len(data) > 8<<20 {
		return fmt.Errorf("GitHub cache exceeds size limit")
	}
	dir := filepath.Dir(d.path)
	if errDir := os.MkdirAll(dir, 0700); errDir != nil {
		return errDir
	}
	file, errCreate := os.CreateTemp(dir, ".github-dashboard-*")
	if errCreate != nil {
		return errCreate
	}
	name := file.Name()
	defer func() { _ = os.Remove(name) }()
	if _, errWrite := file.Write(data); errWrite != nil {
		_ = file.Close()
		return errWrite
	}
	if errClose := file.Close(); errClose != nil {
		return errClose
	}
	return os.Rename(name, d.path)
}
func (d *Dashboard) failed(key string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.retryAfter[key] = d.now().Add(30 * time.Second)
	if len(d.retryAfter) > maxDashboardEntries {
		for k := range d.retryAfter {
			if k != key {
				delete(d.retryAfter, k)
				break
			}
		}
	}
}
func fresh(checked, now time.Time, ttl time.Duration, force bool) bool {
	if force {
		ttl = 15 * time.Second
	}
	return !checked.IsZero() && !now.Before(checked) && now.Before(checked.Add(ttl))
}
func cloneStatistics(s Statistics) Statistics {
	s.Days = append([]ContributionDay{}, s.Days...)
	return s
}

func (d *Dashboard) Statistics(ctx context.Context, account, organization string, year int, force bool) (Statistics, error) {
	viewer, errViewer := d.viewer(ctx, account)
	if errViewer != nil {
		return Statistics{}, errViewer
	}
	key := dashboardKey("statistics", account, organization, year) + "|" + viewer.key
	result := d.flights.DoChan(key, func() (any, error) {
		return d.statistics(context.WithoutCancel(ctx), key, account, organization, year, force)
	})
	select {
	case <-ctx.Done():
		return Statistics{}, ctx.Err()
	case answer := <-result:
		if answer.Err != nil {
			return Statistics{}, answer.Err
		}
		result := cloneStatistics(answer.Val.(Statistics))
		result.Limited = viewer.limited
		return result, nil
	}
}
func (d *Dashboard) statistics(ctx context.Context, key, account, organization string, year int, force bool) (Statistics, error) {
	now := d.now().UTC()
	old, retry := d.entry(key)
	ttl := 5 * time.Minute
	if year < now.Year() {
		ttl = 30 * 24 * time.Hour
	}
	if retry.IsZero() && old.Statistics != nil && fresh(old.Statistics.CheckedAt, now, ttl, force) {
		return *old.Statistics, nil
	}
	if now.Before(retry) {
		if old.Statistics != nil {
			s := *old.Statistics
			s.Stale = true
			s.Warning = dashboardWarning
			return s, nil
		}
		return Statistics{}, ErrUnavailable
	}
	start := time.Date(year, 1, 1, 0, 0, 0, 0, time.UTC)
	end := start.AddDate(1, 0, 0)
	if end.After(now) {
		end = now
	}
	full := old.Statistics == nil || year < now.Year() || !now.Before(old.FullCheckedAt.Add(30*24*time.Hour))
	if !full {
		recent := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC).AddDate(0, 0, -35)
		if recent.After(start) {
			start = recent
		}
	}
	days, errFetch := d.fetchCalendar(ctx, account, organization, start, end)
	if errFetch != nil {
		d.failed(key)
		if old.Statistics != nil && errFetch != ErrAccountMismatch {
			s := *old.Statistics
			s.Stale = true
			s.Warning = dashboardWarning
			return s, nil
		}
		return Statistics{}, errFetch
	}
	if !full && old.Statistics != nil {
		past := []ContributionDay{}
		for _, day := range old.Statistics.Days {
			if day.Date < start.Format("2006-01-02") {
				past = append(past, day)
			}
		}
		days = append(past, days...)
	}
	s := Statistics{Account: account, Organization: organization, Year: year, Days: days, CheckedAt: d.now().UTC()}
	fullChecked := old.FullCheckedAt
	if full {
		fullChecked = s.CheckedAt
	}
	d.save(key, dashboardEntry{Statistics: &s, FullCheckedAt: fullChecked})
	return s, nil
}

const dashboardCalendarQuery = `query DashboardCalendar($since:DateTime!,$until:DateTime!,$organizationID:ID) {
 viewer {login contributionsCollection(from:$since,to:$until,organizationID:$organizationID) {
 contributionCalendar {weeks {contributionDays {date contributionCount}}}
 }}
}`

func (d *Dashboard) fetchCalendar(ctx context.Context, account, organization string, start, end time.Time) ([]ContributionDay, error) {
	var scope struct {
		Viewer       *struct{ Login string }
		Organization *struct{ ID, Login string }
	}
	query := `query DashboardScope {viewer {login}}`
	variables := map[string]any{}
	if organization != "" {
		query = `query DashboardScope($organization:String!){viewer{login} organization(login:$organization){id login}}`
		variables["organization"] = organization
	}
	if err := d.client.request(ctx, query, variables, &scope); err != nil {
		return nil, err
	}
	if scope.Viewer == nil || !strings.EqualFold(scope.Viewer.Login, account) {
		return nil, ErrAccountMismatch
	}
	var organizationID any
	if organization != "" {
		if scope.Organization == nil || !strings.EqualFold(scope.Organization.Login, organization) {
			return nil, ErrUnavailable
		}
		organizationID = scope.Organization.ID
	}
	var response struct {
		Viewer *struct {
			Login                   string
			ContributionsCollection struct {
				ContributionCalendar struct {
					Weeks []struct {
						ContributionDays []struct {
							Date              string
							ContributionCount int
						}
					}
				}
			}
		}
	}
	if err := d.client.request(ctx, dashboardCalendarQuery, map[string]any{"since": start.Format(time.RFC3339), "until": end.Add(-time.Second).Format(time.RFC3339), "organizationID": organizationID}, &response); err != nil {
		return nil, err
	}
	if response.Viewer == nil || !strings.EqualFold(response.Viewer.Login, account) {
		return nil, ErrAccountMismatch
	}
	received := map[string]int{}
	for _, week := range response.Viewer.ContributionsCollection.ContributionCalendar.Weeks {
		for _, day := range week.ContributionDays {
			if _, errDate := time.Parse("2006-01-02", day.Date); errDate != nil || day.ContributionCount < 0 {
				return nil, ErrUnavailable
			}
			received[day.Date] = day.ContributionCount
		}
	}
	days := []ContributionDay{}
	for at := start; at.Before(end); at = at.AddDate(0, 0, 1) {
		date := at.Format("2006-01-02")
		count, ok := received[date]
		if !ok {
			return nil, ErrUnavailable
		}
		days = append(days, ContributionDay{Date: date, Contributions: count})
	}
	sort.Slice(days, func(i, j int) bool { return days[i].Date < days[j].Date })
	return days, nil
}

type dashboardViewer struct {
	key     string
	limited bool
}

func (d *Dashboard) viewer(ctx context.Context, account string) (dashboardViewer, error) {
	if errContext := ctx.Err(); errContext != nil {
		return dashboardViewer{}, errContext
	}
	input, _ := json.Marshal(map[string]any{"query": "query DashboardViewer {viewer{id login}}"})
	raw, errRun := d.client.Run(ctx, input)
	if errRun != nil {
		return dashboardViewer{}, ErrUnavailable
	}
	fingerprint := "unknown"
	limited := false
	if bytes.HasPrefix(raw, []byte("HTTP/")) {
		response, errResponse := http.ReadResponse(bufio.NewReader(bytes.NewReader(raw)), nil)
		if errResponse != nil {
			return dashboardViewer{}, ErrUnavailable
		}
		body, errRead := io.ReadAll(io.LimitReader(response.Body, 1<<20))
		if errClose := response.Body.Close(); errClose != nil {
			log.Warn("Could not close GitHub identity metadata")
		}
		if errRead != nil || response.StatusCode != http.StatusOK {
			return dashboardViewer{}, ErrUnavailable
		}
		if _, known := response.Header["X-Oauth-Scopes"]; known {
			scopes := strings.Split(response.Header.Get("X-Oauth-Scopes"), ",")
			private := false
			for i := range scopes {
				scopes[i] = strings.TrimSpace(scopes[i])
				private = private || scopes[i] == "read:user" || scopes[i] == "user"
			}
			sort.Strings(scopes)
			digest := sha256.Sum256([]byte(strings.Join(scopes, ",")))
			fingerprint = hex.EncodeToString(digest[:])
			limited = !private
		}
		raw = body
	}
	var identity struct {
		Errors []json.RawMessage
		Data   struct{ Viewer *struct{ ID, Login string } }
	}
	if json.Unmarshal(raw, &identity) != nil || len(identity.Errors) > 0 || identity.Data.Viewer == nil || identity.Data.Viewer.ID == "" {
		return dashboardViewer{}, ErrUnavailable
	}
	if !strings.EqualFold(identity.Data.Viewer.Login, account) {
		return dashboardViewer{}, ErrAccountMismatch
	}
	return dashboardViewer{key: identity.Data.Viewer.ID + "|" + fingerprint, limited: limited}, nil
}
