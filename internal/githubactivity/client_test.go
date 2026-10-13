package githubactivity

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestFetchWorkSnapshot(t *testing.T) {
	client := &Client{Run: func(_ context.Context, input []byte) ([]byte, error) {
		if !strings.Contains(string(input), `repo:local/fork is:pr created:`) {
			t.Fatal("missing repository activity filter")
		}
		return []byte(`{"data":{"repository":{"nameWithOwner":"local/fork","url":"https://github.com/local/fork","pullRequests":{"totalCount":1,"nodes":[{"number":7,"title":"Ship dashboard","url":"https://github.com/local/fork/pull/7","isDraft":false,"reviewDecision":"APPROVED","mergeable":"MERGEABLE","author":{"login":"person"},"commits":{"nodes":[{"commit":{"statusCheckRollup":{"state":"SUCCESS"}}}]}}]},"defaultBranchRef":{"name":"main","target":{"history":{"totalCount":2,"nodes":[{"oid":"abc123","messageHeadline":"Add dashboard","url":"https://github.com/local/fork/commit/abc123","committedDate":"2026-10-12T10:00:00Z","author":{"name":"Person"}}]}}}},"created":{"issueCount":3,"nodes":[{"number":7,"title":"Ship dashboard","url":"https://github.com/local/fork/pull/7","state":"OPEN","createdAt":"2026-10-12T10:00:00Z"}]},"merged":{"issueCount":1}}}`), nil
	}}
	got, err := client.Fetch(context.Background(), "local/fork", time.Date(2026, 10, 12, 0, 0, 0, 0, time.UTC), time.Date(2026, 10, 12, 12, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if got.OpenCount != 1 || got.CreatedCount != 3 || got.MergedCount != 1 || got.CommitCount != 2 || got.DefaultBranch != "main" {
		t.Fatalf("unexpected counts: %+v", got)
	}
	if len(got.OpenPRs) != 1 || got.OpenPRs[0].Checks != "SUCCESS" || got.OpenPRs[0].Review != "APPROVED" {
		t.Fatalf("unexpected PRs: %+v", got.OpenPRs)
	}
	if len(got.Commits) != 1 || len(got.CreatedPRs) != 1 {
		t.Fatal("activity links missing")
	}
}

func TestFetchRejectsPartialAndFailedGitHubResponses(t *testing.T) {
	for _, raw := range []string{`{"data":{"repository":null}}`, `{"errors":[{"message":"private-token"}],"data":{"repository":{"nameWithOwner":"local/fork"}}}`, `not-json`} {
		t.Run(raw, func(t *testing.T) {
			client := &Client{Run: func(context.Context, []byte) ([]byte, error) { return []byte(raw), nil }}
			_, err := client.Fetch(context.Background(), "local/fork", time.Now().Add(-24*time.Hour), time.Now())
			if err != ErrUnavailable || strings.Contains(err.Error(), "private-token") {
				t.Fatalf("unsafe or missing failure: %v", err)
			}
		})
	}
}

func TestFetchIncludesPullRequestCreationAndUpdateDates(t *testing.T) {
	client := &Client{Run: func(context.Context, []byte) ([]byte, error) {
		return []byte(`{"data":{"repository":{"nameWithOwner":"owner/repo","pullRequests":{"totalCount":1,"nodes":[{"number":8,"createdAt":"2026-10-12T10:00:00Z","updatedAt":"2026-10-13T11:00:00Z","state":"OPEN"}]},"defaultBranchRef":null},"created":{"nodes":[]},"merged":{}}}`), nil
	}}
	got, err := client.Fetch(context.Background(), "owner/repo", time.Date(2026, 10, 12, 0, 0, 0, 0, time.UTC), time.Date(2026, 10, 14, 0, 0, 0, 0, time.UTC))
	if err != nil || len(got.OpenPRs) != 1 || got.OpenPRs[0].CreatedAt != "2026-10-12T10:00:00Z" || got.OpenPRs[0].UpdatedAt != "2026-10-13T11:00:00Z" {
		t.Fatalf("PR dates missing: snapshot=%+v err=%v", got, err)
	}
}
