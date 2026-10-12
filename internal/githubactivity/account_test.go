package githubactivity

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestFetchAccountIncludesForksAndCollaborations(t *testing.T) {
	client := &Client{Run: func(_ context.Context, input []byte) ([]byte, error) {
		switch {
		case strings.Contains(string(input), "AccountSummary"):
			return []byte(`{"data":{"viewer":{"login":"kert114","id":"person","url":"https://github.com/kert114","contributionsCollection":{"totalPullRequestReviewContributions":2,"pullRequestReviewContributions":{"nodes":[]}}},"created":{"issueCount":3,"nodes":[]},"merged":{"issueCount":2},"issues":{"issueCount":1,"nodes":[]}}}`), nil
		case strings.Contains(string(input), "AccountRepositories"):
			return []byte(`{"data":{"viewer":{"repositories":{"totalCount":2,"pageInfo":{"hasNextPage":false},"nodes":[{"nameWithOwner":"kert114/fork","url":"https://github.com/kert114/fork","isFork":true,"defaultBranchRef":{"name":"main","target":{"history":{"totalCount":4,"nodes":[{"oid":"abc123","url":"https://github.com/kert114/fork/commit/abc123","messageHeadline":"Fork change","committedDate":"2026-10-12T10:00:00Z","author":{"name":"Kert"}}]}}}},{"nameWithOwner":"team/app","url":"https://github.com/team/app","defaultBranchRef":{"name":"main","target":{"history":{"totalCount":6,"nodes":[]}}}}]}}}}`), nil
		default:
			return []byte(`{"data":{"search":{"issueCount":1,"pageInfo":{"hasNextPage":false},"nodes":[{"number":7,"title":"Team change","url":"https://github.com/team/app/pull/7","repository":{"nameWithOwner":"team/app"},"author":{"login":"kert114"},"commits":{"nodes":[{"commit":{"statusCheckRollup":{"state":"SUCCESS"}}}]}}]}}}`), nil
		}
	}}
	got, err := client.FetchAccount(context.Background(), "kert114", time.Date(2026, 10, 12, 0, 0, 0, 0, time.UTC), time.Date(2026, 10, 13, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if got.Account != "kert114" || got.RepositoryCount != 2 || got.CommitCount != 10 || got.CreatedCount != 3 || got.MergedCount != 2 || got.IssueCount != 1 || got.ReviewCount != 2 {
		t.Fatalf("unexpected account totals: %+v", got)
	}
	if got.OpenCount != 1 || len(got.OpenPRs) != 1 || got.OpenPRs[0].Repository != "team/app" || got.OpenPRs[0].Checks != "SUCCESS" {
		t.Fatalf("PR overlap was not deduplicated: %+v", got.OpenPRs)
	}
	if len(got.Commits) != 1 || got.Commits[0].Repository != "kert114/fork" {
		t.Fatalf("fork commit missing: %+v", got.Commits)
	}
}

const emptyAccountSummary = `{"data":{"viewer":{"login":"kert114","id":"person","url":"https://github.com/kert114","contributionsCollection":{"pullRequestReviewContributions":{"nodes":[]}}},"created":{"nodes":[]},"merged":{},"issues":{"nodes":[]}}}`

func TestFetchAccountPaginatesAndKeepsSameNumberAcrossRepositories(t *testing.T) {
	client := &Client{Run: func(_ context.Context, input []byte) ([]byte, error) {
		if strings.Contains(string(input), "AccountSummary") {
			return []byte(emptyAccountSummary), nil
		}
		if strings.Contains(string(input), "AccountRepositories") {
			if strings.Contains(string(input), `"cursor":"next-repository"`) {
				return []byte(`{"data":{"viewer":{"repositories":{"totalCount":2,"pageInfo":{"hasNextPage":false},"nodes":[{"nameWithOwner":"team/app","url":"https://github.com/team/app","defaultBranchRef":null}]}}}}`), nil
			}
			return []byte(`{"data":{"viewer":{"repositories":{"totalCount":2,"pageInfo":{"hasNextPage":true,"endCursor":"next-repository"},"nodes":[{"nameWithOwner":"kert114/fork","url":"https://github.com/kert114/fork","defaultBranchRef":null}]}}}}`), nil
		}
		repo := "team/app"
		if strings.Contains(string(input), "is:open user:") {
			repo = "kert114/fork"
		}
		return []byte(`{"data":{"search":{"issueCount":1,"pageInfo":{"hasNextPage":false},"nodes":[{"number":7,"url":"https://github.com/` + repo + `/pull/7","repository":{"nameWithOwner":"` + repo + `"}}]}}}`), nil
	}}
	got, err := client.FetchAccount(context.Background(), "kert114", time.Now().Add(-24*time.Hour), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Repositories) != 2 || got.OpenCount != 2 || len(got.OpenPRs) != 2 {
		t.Fatalf("missing repositories or PR identities: %+v", got)
	}
}

func TestFetchAccountRejectsWrongViewerAndPartialData(t *testing.T) {
	for _, raw := range []string{strings.Replace(emptyAccountSummary, `"login":"kert114"`, `"login":"another"`, 1), strings.Replace(emptyAccountSummary, `{"data":`, `{"errors":[{"message":"sensitive detail"}],"data":`, 1)} {
		client := &Client{Run: func(context.Context, []byte) ([]byte, error) { return []byte(raw), nil }}
		_, err := client.FetchAccount(context.Background(), "kert114", time.Now().Add(-24*time.Hour), time.Now())
		if err != ErrUnavailable {
			t.Fatalf("expected safe failure, got %v", err)
		}
	}
}

func TestFetchAccountMarksTruncatedSearchAndRejectsStalledCursor(t *testing.T) {
	for _, stalled := range []bool{false, true} {
		client := &Client{Run: func(_ context.Context, input []byte) ([]byte, error) {
			if strings.Contains(string(input), "AccountSummary") {
				return []byte(emptyAccountSummary), nil
			}
			if strings.Contains(string(input), "AccountRepositories") {
				return []byte(`{"data":{"viewer":{"repositories":{"nodes":[],"pageInfo":{"hasNextPage":false}}}}}`), nil
			}
			if stalled {
				return []byte(`{"data":{"search":{"issueCount":200,"nodes":[],"pageInfo":{"hasNextPage":true,"endCursor":"same"}}}}`), nil
			}
			return []byte(`{"data":{"search":{"issueCount":1001,"nodes":[{"url":"https://github.com/team/app/pull/7"}],"pageInfo":{"hasNextPage":false}}}}`), nil
		}}
		got, err := client.FetchAccount(context.Background(), "kert114", time.Now().Add(-24*time.Hour), time.Now())
		if stalled {
			if err != ErrUnavailable {
				t.Fatal("stalled cursor was accepted")
			}
			continue
		}
		if err != nil || !got.OpenTruncated || got.OpenCount != 1 {
			t.Fatalf("expected honest lower bound: %+v, %v", got, err)
		}
	}
}
