package githubactivity

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

type PullRequest struct {
	Repository string `json:"repository"`
	Number     int    `json:"number"`
	Title      string `json:"title"`
	URL        string `json:"url"`
	Author     string `json:"author"`
	Draft      bool   `json:"draft"`
	Review     string `json:"review"`
	Checks     string `json:"checks"`
	Mergeable  string `json:"mergeable"`
	State      string `json:"state"`
	CreatedAt  string `json:"created_at"`
	UpdatedAt  string `json:"updated_at"`
}
type Commit struct {
	Repository string `json:"repository"`
	SHA        string `json:"sha"`
	Title      string `json:"title"`
	URL        string `json:"url"`
	Date       string `json:"date"`
	Author     string `json:"author"`
}
type Snapshot struct {
	CacheStatus     string               `json:"cache_status,omitempty"`
	RefreshError    bool                 `json:"refresh_error"`
	Organization    string               `json:"organization,omitempty"`
	Account         string               `json:"account,omitempty"`
	RepositoryCount int                  `json:"repository_count"`
	IssueCount      int                  `json:"issue_count"`
	ReviewCount     int                  `json:"review_count"`
	OpenTruncated   bool                 `json:"open_truncated"`
	Repositories    []RepositoryActivity `json:"repositories"`
	Issues          []Issue              `json:"issues"`
	Reviews         []Review             `json:"reviews"`
	Repository      string               `json:"repository"`
	URL             string               `json:"url"`
	DefaultBranch   string               `json:"default_branch"`
	CheckedAt       string               `json:"checked_at"`
	OpenCount       int                  `json:"open_count"`
	CreatedCount    int                  `json:"created_count"`
	MergedCount     int                  `json:"merged_count"`
	CommitCount     int                  `json:"commit_count"`
	OpenPRs         []PullRequest        `json:"open_prs"`
	CreatedPRs      []PullRequest        `json:"created_prs"`
	Commits         []Commit             `json:"commits"`
}
type Client struct {
	Run   func(context.Context, []byte) ([]byte, error)
	Cache *SnapshotCache
}

var ErrUnavailable = errors.New("GitHub activity unavailable")

func NewClient() *Client {
	cacheDirectory := ""
	if directory, errCache := os.UserCacheDir(); errCache == nil {
		cacheDirectory = filepath.Join(directory, "CLIProxyAPI", "github-activity")
	}
	return &Client{Cache: NewSnapshotCache(cacheDirectory), Run: func(ctx context.Context, input []byte) ([]byte, error) {
		cmd := exec.CommandContext(ctx, "gh", "api", "graphql", "--hostname", "github.com", "--input", "-")
		cmd.Stdin = bytes.NewReader(input)
		return cmd.Output()
	}}
}

func (c *Client) FetchCached(ctx context.Context, scope SnapshotScope, force bool) (Snapshot, error) {
	load := func(ctx context.Context) (Snapshot, error) {
		if scope.Organization != "" {
			return c.FetchOrganization(ctx, scope.Account, scope.Organization, scope.Since, scope.Until)
		}
		if scope.Account != "" {
			return c.FetchAccount(ctx, scope.Account, scope.Since, scope.Until)
		}
		return c.Fetch(ctx, scope.Repository, scope.Since, scope.Until)
	}
	if c.Cache == nil {
		return load(ctx)
	}
	var identity struct {
		Viewer *struct{ ID, Login string }
	}
	if c.request(ctx, `query SnapshotViewer {viewer {id login}}`, nil, &identity) != nil || identity.Viewer == nil || identity.Viewer.ID == "" || (scope.Account != "" && !strings.EqualFold(scope.Account, identity.Viewer.Login)) {
		return Snapshot{}, ErrUnavailable
	}
	scope.Viewer = identity.Viewer.ID
	return c.Cache.Fetch(ctx, scope, force, load)
}

const query = `query($owner:String!, $name:String!, $since:GitTimestamp!, $until:GitTimestamp!, $created:String!, $merged:String!) {
 repository(owner:$owner,name:$name) {
  nameWithOwner url
  pullRequests(first:100,states:OPEN,orderBy:{field:UPDATED_AT,direction:DESC}) {
   totalCount nodes { number title url state createdAt updatedAt isDraft reviewDecision mergeable author { login }
    commits(last:1) { nodes { commit { statusCheckRollup { state } } } }
   }
  }
  defaultBranchRef { name target { ... on Commit {
   history(first:100,since:$since,until:$until) { totalCount nodes {
    oid messageHeadline url committedDate author { name }
   } }
  } } }
 }
 created:search(query:$created,type:ISSUE,first:100) { issueCount nodes { ... on PullRequest {
  number title url state createdAt updatedAt isDraft author { login }
 } } }
 merged:search(query:$merged,type:ISSUE,first:1) { issueCount }
}`

type githubPR struct {
	ID                                                                 string
	Repository                                                         struct{ NameWithOwner string }
	Number                                                             int
	Title, URL, ReviewDecision, Mergeable, State, CreatedAt, UpdatedAt string
	IsDraft                                                            bool
	Author                                                             *struct{ Login string }
	Commits                                                            struct {
		Nodes []struct {
			Commit struct{ StatusCheckRollup *struct{ State string } }
		}
	}
}

func normalizePR(pr githubPR) PullRequest {
	out := PullRequest{Number: pr.Number, Title: pr.Title, URL: pr.URL, Draft: pr.IsDraft, Review: pr.ReviewDecision, Mergeable: pr.Mergeable, State: pr.State, CreatedAt: pr.CreatedAt, UpdatedAt: pr.UpdatedAt}
	out.Repository = pr.Repository.NameWithOwner
	if pr.Author != nil {
		out.Author = pr.Author.Login
	}
	if len(pr.Commits.Nodes) > 0 && pr.Commits.Nodes[0].Commit.StatusCheckRollup != nil {
		out.Checks = pr.Commits.Nodes[0].Commit.StatusCheckRollup.State
	}
	return out
}

func (c *Client) Fetch(ctx context.Context, repository string, since, until time.Time) (Snapshot, error) {
	parts := strings.Split(repository, "/")
	if len(parts) != 2 {
		return Snapshot{}, ErrUnavailable
	}
	dateRange := since.UTC().Format(time.RFC3339) + ".." + until.Add(-time.Second).UTC().Format(time.RFC3339)
	input, errEncode := json.Marshal(map[string]any{"query": query, "variables": map[string]string{
		"owner": parts[0], "name": parts[1], "since": since.UTC().Format(time.RFC3339), "until": until.Add(-time.Second).UTC().Format(time.RFC3339),
		"created": "repo:" + repository + " is:pr created:" + dateRange + " sort:created-desc",
		"merged":  "repo:" + repository + " is:pr merged:" + dateRange,
	}})
	if errEncode != nil {
		return Snapshot{}, ErrUnavailable
	}
	raw, errRun := c.Run(ctx, input)
	if errRun != nil {
		return Snapshot{}, ErrUnavailable
	}
	var response struct {
		Errors []json.RawMessage
		Data   struct {
			Repository *struct {
				NameWithOwner, URL string
				PullRequests       struct {
					TotalCount int
					Nodes      []githubPR
				}
				DefaultBranchRef *struct {
					Name   string
					Target struct {
						History struct {
							TotalCount int
							Nodes      []struct {
								OID, MessageHeadline, URL, CommittedDate string
								Author                                   struct{ Name string }
							}
						}
					}
				}
			}
			Created struct {
				IssueCount int
				Nodes      []githubPR
			}
			Merged struct{ IssueCount int }
		}
	}
	if errDecode := json.Unmarshal(raw, &response); errDecode != nil || len(response.Errors) > 0 || response.Data.Repository == nil {
		return Snapshot{}, ErrUnavailable
	}
	repo := response.Data.Repository
	out := Snapshot{Repository: repo.NameWithOwner, URL: repo.URL, CheckedAt: time.Now().UTC().Format(time.RFC3339), OpenCount: repo.PullRequests.TotalCount, CreatedCount: response.Data.Created.IssueCount, MergedCount: response.Data.Merged.IssueCount, OpenPRs: []PullRequest{}, CreatedPRs: []PullRequest{}, Commits: []Commit{}}
	for _, pr := range repo.PullRequests.Nodes {
		out.OpenPRs = append(out.OpenPRs, normalizePR(pr))
	}
	for _, pr := range response.Data.Created.Nodes {
		out.CreatedPRs = append(out.CreatedPRs, normalizePR(pr))
	}
	if branch := repo.DefaultBranchRef; branch != nil {
		out.DefaultBranch = branch.Name
		out.CommitCount = branch.Target.History.TotalCount
		for _, commit := range branch.Target.History.Nodes {
			out.Commits = append(out.Commits, Commit{SHA: commit.OID, Title: commit.MessageHeadline, URL: commit.URL, Date: commit.CommittedDate, Author: commit.Author.Name})
		}
	}
	return out, nil
}
