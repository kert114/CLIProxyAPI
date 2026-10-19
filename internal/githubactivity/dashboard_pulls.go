package githubactivity

import (
	"context"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"time"
)

const dashboardPRFields = `fragment DashboardPR on PullRequest {
 id number title url state createdAt updatedAt isDraft reviewDecision mergeable author {login}
 repository {nameWithOwner}
 commits(last:1) {nodes {commit {statusCheckRollup {state}}}}
}`
const dashboardPullSearch = `query DashboardPullSearch($search:String!,$cursor:String) {
 viewer {login}
 search(query:$search,type:ISSUE,first:25,after:$cursor) {issueCount pageInfo {hasNextPage endCursor} nodes {...DashboardPR}}
}` + dashboardPRFields
const dashboardPullNodes = `query DashboardPullNodes($ids:[ID!]!){viewer {login} nodes(ids:$ids){...DashboardPR}}` + dashboardPRFields

func (d *Dashboard) PullRequests(ctx context.Context, account, organization string, force bool) (PullRequests, error) {
	viewer, errViewer := d.viewer(ctx, account)
	if errViewer != nil {
		return PullRequests{}, errViewer
	}
	key := dashboardKey("pulls", account, organization, 0) + "|" + viewer.key
	result := d.flights.DoChan(key, func() (any, error) { return d.pulls(context.WithoutCancel(ctx), key, account, organization, force) })
	select {
	case <-ctx.Done():
		return PullRequests{}, ctx.Err()
	case answer := <-result:
		if answer.Err != nil {
			return PullRequests{}, answer.Err
		}
		out := answer.Val.(PullRequests)
		out.PullRequests = append([]DashboardPullRequest{}, out.PullRequests...)
		return out, nil
	}
}
func (d *Dashboard) pulls(ctx context.Context, key, account, organization string, force bool) (PullRequests, error) {
	now := d.now().UTC()
	old, retry := d.entry(key)
	if retry.IsZero() && old.Pulls != nil && fresh(old.Pulls.CheckedAt, now, time.Minute, force) {
		return *old.Pulls, nil
	}
	if now.Before(retry) {
		if old.Pulls != nil {
			out := *old.Pulls
			out.Stale = true
			out.Warning = dashboardWarning
			return out, nil
		}
		return PullRequests{}, ErrUnavailable
	}
	out, err := d.fetchPulls(ctx, account, organization, old, now)
	if err != nil {
		d.failed(key)
		if old.Pulls != nil && err != ErrAccountMismatch {
			stale := *old.Pulls
			stale.Stale = true
			stale.Warning = dashboardWarning
			return stale, nil
		}
		return PullRequests{}, err
	}
	out.CheckedAt = d.now().UTC()
	full := old.FullCheckedAt
	if old.Pulls == nil || !now.Before(full.Add(6*time.Hour)) {
		full = out.CheckedAt
	}
	d.save(key, dashboardEntry{Pulls: &out, FullCheckedAt: full})
	return out, nil
}
func normalizeDashboardPR(pr githubPR, organization string) (DashboardPullRequest, error) {
	result := DashboardPullRequest{PullRequest: normalizePR(pr), NodeID: pr.ID}
	if pr.ID == "" || pr.Number < 1 {
		return result, ErrUnavailable
	}
	parts := strings.Split(result.Repository, "/")
	if len(parts) != 2 || organization != "" && !strings.EqualFold(parts[0], organization) {
		return result, ErrUnavailable
	}
	parsed, err := url.Parse(result.URL)
	if err != nil || parsed.Scheme != "https" || parsed.Host != "github.com" || parsed.User != nil || parsed.Path != fmt.Sprintf("/%s/pull/%d", result.Repository, pr.Number) {
		return result, ErrUnavailable
	}
	if _, errDate := time.Parse(time.RFC3339, result.UpdatedAt); errDate != nil {
		return result, ErrUnavailable
	}
	if result.State != "OPEN" && result.State != "CLOSED" && result.State != "MERGED" {
		return result, ErrUnavailable
	}
	return result, nil
}
func (d *Dashboard) searchPulls(ctx context.Context, account, organization, search string) ([]DashboardPullRequest, bool, error) {
	out := []DashboardPullRequest{}
	var cursor any
	for {
		var response struct {
			Viewer *struct{ Login string }
			Search *struct {
				IssueCount int
				PageInfo   pageInfo
				Nodes      []githubPR
			}
		}
		if err := d.client.request(ctx, dashboardPullSearch, map[string]any{"search": search, "cursor": cursor}, &response); err != nil {
			return nil, false, err
		}
		if response.Viewer == nil || !strings.EqualFold(response.Viewer.Login, account) {
			return nil, false, ErrAccountMismatch
		}
		if response.Search == nil {
			return nil, false, ErrUnavailable
		}
		for _, node := range response.Search.Nodes {
			pr, err := normalizeDashboardPR(node, organization)
			if err != nil {
				return nil, false, err
			}
			out = append(out, pr)
		}
		limit := 1000
		if strings.Contains(search, "is:closed") {
			limit = 25
		}
		if !response.Search.PageInfo.HasNextPage || len(out) >= limit {
			return out, response.Search.IssueCount > len(out), nil
		}
		if response.Search.PageInfo.EndCursor == "" || cursor == response.Search.PageInfo.EndCursor {
			return nil, false, ErrUnavailable
		}
		cursor = response.Search.PageInfo.EndCursor
	}
}
func (d *Dashboard) fetchPulls(ctx context.Context, account, organization string, old dashboardEntry, now time.Time) (PullRequests, error) {
	full := old.Pulls == nil || !now.Before(old.FullCheckedAt.Add(6*time.Hour))
	cutoff := now.AddDate(0, 0, -30)
	rows := map[string]DashboardPullRequest{}
	if old.Pulls != nil {
		for _, pr := range old.Pulls.PullRequests {
			if !full || pr.State != "OPEN" {
				rows[pr.NodeID] = pr
			}
		}
	}
	qualifier := "involves:" + account
	if organization != "" {
		qualifier = "org:" + organization
	}
	queries := []string{}
	if full {
		queries = []string{"is:pr is:open " + qualifier + " sort:updated-desc", "is:pr is:closed " + qualifier + " updated:>=" + cutoff.Format(time.RFC3339) + " sort:updated-desc"}
	} else {
		queries = []string{"is:pr " + qualifier + " updated:>=" + old.Pulls.CheckedAt.Add(-15*time.Minute).Format(time.RFC3339) + " sort:updated-desc"}
	}
	truncated := old.Pulls != nil && old.Pulls.Truncated && !full
	for _, query := range queries {
		prs, partial, err := d.searchPulls(ctx, account, organization, query)
		if err != nil {
			return PullRequests{}, err
		}
		truncated = truncated || partial
		for _, pr := range prs {
			rows[pr.NodeID] = pr
		}
	}
	// Check runs can settle without changing a PR's updatedAt timestamp.
	if !full {
		ids := []string{}
		for id, pr := range rows {
			if pr.State == "OPEN" {
				ids = append(ids, id)
			}
		}
		sort.Strings(ids)
		for at := 0; at < len(ids); at += 50 {
			end := min(at+50, len(ids))
			batch := ids[at:end]
			var response struct {
				Viewer *struct{ Login string }
				Nodes  []*githubPR
			}
			if err := d.client.request(ctx, dashboardPullNodes, map[string]any{"ids": batch}, &response); err != nil {
				return PullRequests{}, err
			}
			if response.Viewer == nil || !strings.EqualFold(response.Viewer.Login, account) {
				return PullRequests{}, ErrAccountMismatch
			}
			if len(response.Nodes) != len(batch) {
				return PullRequests{}, ErrUnavailable
			}
			for i, node := range response.Nodes {
				if node == nil {
					delete(rows, batch[i])
					continue
				}
				pr, err := normalizeDashboardPR(*node, organization)
				if err != nil || pr.NodeID != batch[i] {
					return PullRequests{}, ErrUnavailable
				}
				rows[pr.NodeID] = pr
			}
		}
	}
	out := PullRequests{Account: account, Organization: organization, CheckedAt: now, Truncated: truncated, PullRequests: []DashboardPullRequest{}}
	for _, pr := range rows {
		updated, _ := time.Parse(time.RFC3339, pr.UpdatedAt)
		if pr.State == "OPEN" || !updated.Before(cutoff) {
			out.PullRequests = append(out.PullRequests, pr)
		}
	}
	sort.Slice(out.PullRequests, func(i, j int) bool {
		a, b := out.PullRequests[i], out.PullRequests[j]
		if a.UpdatedAt == b.UpdatedAt {
			return a.URL < b.URL
		}
		return a.UpdatedAt > b.UpdatedAt
	})
	if len(out.PullRequests) > 1000 {
		out.PullRequests = out.PullRequests[:1000]
		out.Truncated = true
	}
	return out, nil
}
