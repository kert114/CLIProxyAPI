package githubactivity

import (
	"context"
	"encoding/json"
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

func TestFetchOrganizationScopesEveryActivitySource(t *testing.T) {
	client := &Client{Run: func(_ context.Context, input []byte) ([]byte, error) {
		if strings.Contains(string(input), "OrganizationScope") {
			return []byte(`{"data":{"organization":{"login":"60xAI","id":"org-id","url":"https://github.com/60xAI"}}}`), nil
		}
		if strings.Contains(string(input), "AccountSummary") {
			var request struct{ Variables map[string]any }
			if json.Unmarshal(input, &request) != nil {
				t.Fatal("invalid query payload")
			}
			for _, key := range []string{"created", "merged", "issues"} {
				query, ok := request.Variables[key].(string)
				if !ok || !strings.Contains(query, " org:60xAI ") {
					t.Fatalf("%s search is not organization-scoped", key)
				}
			}
			if request.Variables["organizationID"] != "org-id" {
				t.Fatal("review scope missing")
			}
			return []byte(emptyAccountSummary), nil
		}
		if strings.Contains(string(input), "OrganizationRepositories") {
			if !strings.Contains(string(input), `"organization":"60xAI"`) {
				t.Fatal("inventory organization missing")
			}
			return []byte(`{"data":{"viewer":{"repositories":{"totalCount":1,"pageInfo":{"hasNextPage":false},"nodes":[{"nameWithOwner":"60xAI/app","url":"https://github.com/60xAI/app","defaultBranchRef":null}]}}}}`), nil
		}
		if !strings.Contains(string(input), "is:pr is:open org:60xAI") || strings.Contains(string(input), "involves:") {
			t.Fatal("open PRs must cover the organization only")
		}
		return []byte(`{"data":{"search":{"issueCount":1,"pageInfo":{"hasNextPage":false},"nodes":[{"number":7,"url":"https://github.com/60xAI/app/pull/7","repository":{"nameWithOwner":"60xAI/app"}}]}}}`), nil
	}}
	got, err := client.FetchOrganization(context.Background(), "kert114", "60xai", time.Now().Add(-24*time.Hour), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if got.Organization != "60xAI" || got.URL != "https://github.com/60xAI" || got.RepositoryCount != 1 || got.OpenCount != 1 {
		t.Fatalf("wrong organization scope: %+v", got)
	}
}
