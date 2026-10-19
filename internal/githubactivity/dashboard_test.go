package githubactivity

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func calendarRunner(t *testing.T, calls *int, intervals *[][2]string) func(context.Context, []byte) ([]byte, error) {
	t.Helper()
	return func(_ context.Context, input []byte) ([]byte, error) {
		var request struct {
			Query     string
			Variables map[string]any
		}
		if err := json.Unmarshal(input, &request); err != nil {
			t.Fatal(err)
		}
		if strings.Contains(request.Query, "DashboardViewer") {
			return []byte(`{"data":{"viewer":{"id":"viewer-1","login":"tester"}}}`), nil
		}
		*calls++
		if strings.Contains(request.Query, "DashboardScope") {
			return []byte(`{"data":{"viewer":{"login":"tester"},"organization":{"id":"org-1","login":"example"}}}`), nil
		}
		start, _ := time.Parse(time.RFC3339, request.Variables["since"].(string))
		end, _ := time.Parse(time.RFC3339, request.Variables["until"].(string))
		*intervals = append(*intervals, [2]string{start.Format("2006-01-02"), end.Format("2006-01-02")})
		days := []map[string]any{}
		for day := start; !day.After(end); day = day.AddDate(0, 0, 1) {
			count := 0
			if day.Format("2006-01-02") == "2026-01-01" {
				count = 3
			}
			days = append(days, map[string]any{"date": day.Format("2006-01-02"), "contributionCount": count})
		}
		return json.Marshal(map[string]any{"data": map[string]any{"viewer": map[string]any{"login": "tester", "contributionsCollection": map[string]any{"contributionCalendar": map[string]any{"weeks": []any{map[string]any{"contributionDays": days}}}}}}})
	}
}

func TestDashboardStatisticsCachesSuccessfulRead(t *testing.T) {
	calls := 0
	intervals := [][2]string{}
	dashboard := NewDashboard(&Client{Run: calendarRunner(t, &calls, &intervals)}, "")
	dashboard.now = func() time.Time { return time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC) }
	first, err := dashboard.Statistics(context.Background(), "tester", "example", 2026, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Days) != 292 || first.Days[0].Contributions != 3 {
		t.Fatalf("unexpected calendar: %d days", len(first.Days))
	}
	before := calls
	second, err := dashboard.Statistics(context.Background(), "tester", "example", 2026, false)
	if err != nil {
		t.Fatal(err)
	}
	if calls != before {
		t.Fatalf("cached read made %d extra upstream requests", calls-before)
	}
	if second.CheckedAt != first.CheckedAt {
		t.Fatal("cache hit pretended to refresh")
	}
}

func TestDashboardStatisticsRefreshOnlyRecentDates(t *testing.T) {
	calls := 0
	intervals := [][2]string{}
	now := time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)
	d := NewDashboard(&Client{Run: calendarRunner(t, &calls, &intervals)}, "")
	d.now = func() time.Time { return now }
	if _, err := d.Statistics(context.Background(), "tester", "example", 2026, false); err != nil {
		t.Fatal(err)
	}
	now = now.Add(6 * time.Minute)
	next, err := d.Statistics(context.Background(), "tester", "example", 2026, false)
	if err != nil {
		t.Fatal(err)
	}
	if intervals[1][0] != "2026-09-14" {
		t.Fatalf("re-fetched historical dates: %v", intervals[1])
	}
	if len(next.Days) != 292 || next.Days[0].Contributions != 3 {
		t.Fatal("recent refresh lost cached history")
	}
}

func TestDashboardPullsRefreshOpenChecksWithoutHistory(t *testing.T) {
	now := time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)
	refresh := false
	nodesCalled := false
	searches := []string{}
	client := &Client{Run: func(_ context.Context, input []byte) ([]byte, error) {
		var request struct {
			Query     string
			Variables map[string]any
		}
		_ = json.Unmarshal(input, &request)
		if strings.Contains(request.Query, "DashboardViewer") {
			return []byte(`{"data":{"viewer":{"id":"viewer-1","login":"tester"}}}`), nil
		}
		if strings.Contains(request.Query, "history") {
			t.Fatal("PR refresh fetched commit history")
		}
		node := map[string]any{"id": "pr-1", "number": 1, "title": "Repair identities", "url": "https://github.com/example/project/pull/1", "state": "OPEN", "createdAt": "2026-10-10T12:00:00Z", "updatedAt": "2026-10-19T11:00:00Z", "repository": map[string]any{"nameWithOwner": "example/project"}, "commits": map[string]any{"nodes": []any{map[string]any{"commit": map[string]any{"statusCheckRollup": map[string]any{"state": "SUCCESS"}}}}}}
		if strings.Contains(request.Query, "DashboardPullNodes") {
			nodesCalled = true
			node["commits"] = map[string]any{"nodes": []any{map[string]any{"commit": map[string]any{"statusCheckRollup": map[string]any{"state": "FAILURE"}}}}}
			return json.Marshal(map[string]any{"data": map[string]any{"viewer": map[string]any{"login": "tester"}, "nodes": []any{node}}})
		}
		q := request.Variables["search"].(string)
		searches = append(searches, q)
		rows := []any{}
		if !refresh && strings.Contains(q, "is:open") {
			rows = append(rows, node)
		}
		return json.Marshal(map[string]any{"data": map[string]any{"viewer": map[string]any{"login": "tester"}, "search": map[string]any{"issueCount": len(rows), "pageInfo": map[string]any{"hasNextPage": false}, "nodes": rows}}})
	}}
	d := NewDashboard(client, "")
	d.now = func() time.Time { return now }
	initial, err := d.PullRequests(context.Background(), "tester", "example", false)
	if err != nil {
		t.Fatal(err)
	}
	if len(initial.PullRequests) != 1 || initial.PullRequests[0].Checks != "SUCCESS" {
		t.Fatal("missing initial open PR")
	}
	now = now.Add(time.Minute)
	refresh = true
	latest, err := d.PullRequests(context.Background(), "tester", "example", false)
	if err != nil {
		t.Fatal(err)
	}
	if !nodesCalled || latest.PullRequests[0].Checks != "FAILURE" {
		t.Fatal("unchanged updatedAt hid a new failed check")
	}
	if !strings.Contains(searches[len(searches)-1], "updated:>=2026-10-19T11:45:00Z") {
		t.Fatalf("expected incremental search, got %s", searches[len(searches)-1])
	}
}

func TestDashboardStatisticsPersistenceAndOutage(t *testing.T) {
	calls := 0
	intervals := [][2]string{}
	now := time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)
	path := filepath.Join(t.TempDir(), "private", "github.json")
	client := &Client{Run: calendarRunner(t, &calls, &intervals)}
	d := NewDashboard(client, path)
	d.now = func() time.Time { return now }
	first, err := d.Statistics(context.Background(), "tester", "example", 2026, false)
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("cache must be private: %v", err)
	}
	restarted := NewDashboard(client, path)
	restarted.now = func() time.Time { return now }
	before := calls
	cached, err := restarted.Statistics(context.Background(), "tester", "example", 2026, false)
	if err != nil || calls != before || cached.Days[0].Contributions != 3 {
		t.Fatal("restart refetched saved history")
	}
	now = now.Add(6 * time.Minute)
	client.Run = func(_ context.Context, input []byte) ([]byte, error) {
		if strings.Contains(string(input), "DashboardViewer") {
			return []byte(`{"data":{"viewer":{"id":"viewer-1","login":"tester"}}}`), nil
		}
		calls++
		return nil, ErrUnavailable
	}
	stale, err := restarted.Statistics(context.Background(), "tester", "example", 2026, false)
	if err != nil || !stale.Stale || stale.CheckedAt != first.CheckedAt || stale.Days[0].Contributions != 3 {
		t.Fatal("outage erased history or claimed a fresh result")
	}
	before = calls
	_, _ = restarted.Statistics(context.Background(), "tester", "example", 2026, true)
	if calls != before {
		t.Fatal("outage retry bypassed backoff")
	}
}
func TestDashboardStatisticsPeriodicallyReconcilesOlderDates(t *testing.T) {
	calls := 0
	intervals := [][2]string{}
	now := time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)
	d := NewDashboard(&Client{Run: calendarRunner(t, &calls, &intervals)}, "")
	d.now = func() time.Time { return now }
	_, err := d.Statistics(context.Background(), "tester", "example", 2026, false)
	if err != nil {
		t.Fatal(err)
	}
	now = now.AddDate(0, 0, 31)
	_, err = d.Statistics(context.Background(), "tester", "example", 2026, false)
	if err != nil {
		t.Fatal(err)
	}
	if intervals[1][0] != "2026-01-01" {
		t.Fatal("old history was never reconciled")
	}
}
func TestDashboardManualRefreshHasMinimumInterval(t *testing.T) {
	calls := 0
	intervals := [][2]string{}
	now := time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)
	d := NewDashboard(&Client{Run: calendarRunner(t, &calls, &intervals)}, "")
	d.now = func() time.Time { return now }
	_, _ = d.Statistics(context.Background(), "tester", "example", 2026, false)
	before := calls
	_, _ = d.Statistics(context.Background(), "tester", "example", 2026, true)
	if calls != before {
		t.Fatal("rapid manual refresh hit GitHub again")
	}
	now = now.Add(16 * time.Second)
	_, err := d.Statistics(context.Background(), "tester", "example", 2026, true)
	if err != nil || calls == before {
		t.Fatal("manual refresh did not refresh recent data")
	}
}

func TestDashboardCachedDataDoesNotCrossGitHubLogin(t *testing.T) {
	calls := 0
	intervals := [][2]string{}
	signedIn := "tester"
	base := calendarRunner(t, &calls, &intervals)
	client := &Client{Run: func(ctx context.Context, input []byte) ([]byte, error) {
		if strings.Contains(string(input), "DashboardViewer") {
			return json.Marshal(map[string]any{"data": map[string]any{"viewer": map[string]any{"id": "viewer-1", "login": signedIn}}})
		}
		return base(ctx, input)
	}}
	d := NewDashboard(client, "")
	d.now = func() time.Time { return time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC) }
	_, err := d.Statistics(context.Background(), "tester", "example", 2026, false)
	if err != nil {
		t.Fatal(err)
	}
	signedIn = "someone-else"
	_, err = d.Statistics(context.Background(), "tester", "example", 2026, false)
	if err != ErrAccountMismatch {
		t.Fatalf("cached private data crossed login boundary: %v", err)
	}
}

func TestDashboardRecentCalendarReplacementRemovesOldCounts(t *testing.T) {
	calls := 0
	intervals := [][2]string{}
	now := time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)
	first := true
	base := calendarRunner(t, &calls, &intervals)
	client := &Client{Run: func(ctx context.Context, input []byte) ([]byte, error) {
		raw, err := base(ctx, input)
		if strings.Contains(string(input), "DashboardCalendar") {
			var data map[string]any
			_ = json.Unmarshal(raw, &data)
			viewer := data["data"].(map[string]any)["viewer"].(map[string]any)
			calendar := viewer["contributionsCollection"].(map[string]any)["contributionCalendar"].(map[string]any)
			for _, w := range calendar["weeks"].([]any) {
				for _, d := range w.(map[string]any)["contributionDays"].([]any) {
					v := d.(map[string]any)
					if v["date"] == "2026-10-18" {
						if first {
							v["contributionCount"] = 9
						} else {
							v["contributionCount"] = 0
						}
					}
				}
			}
			raw, _ = json.Marshal(data)
		}
		return raw, err
	}}
	d := NewDashboard(client, "")
	d.now = func() time.Time { return now }
	before, err := d.Statistics(context.Background(), "tester", "example", 2026, false)
	if err != nil {
		t.Fatal(err)
	}
	if before.Days[len(before.Days)-2].Contributions != 9 {
		t.Fatal("fixture missing initial contributions")
	}
	first = false
	now = now.Add(6 * time.Minute)
	latest, err := d.Statistics(context.Background(), "tester", "example", 2026, false)
	if err != nil {
		t.Fatal(err)
	}
	if latest.Days[len(latest.Days)-2].Contributions != 0 {
		t.Fatal("refresh appended duplicate counts instead of replacing changed days")
	}
	if latest.Days[0].Contributions != 3 {
		t.Fatal("refresh modified stable history")
	}
}
