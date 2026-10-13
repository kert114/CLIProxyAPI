package githubactivity

import (
	"context"
	"encoding/json"
	"sort"
	"strings"
	"time"
)

type RepositoryActivity struct {
	Name          string `json:"name"`
	URL           string `json:"url"`
	DefaultBranch string `json:"default_branch"`
	Fork          bool   `json:"fork"`
	CommitCount   int    `json:"commit_count"`
}
type Issue struct {
	Repository string `json:"repository"`
	Number     int    `json:"number"`
	Title      string `json:"title"`
	URL        string `json:"url"`
	State      string `json:"state"`
	CreatedAt  string `json:"created_at"`
}
type Review struct {
	Repository string `json:"repository"`
	Title      string `json:"title"`
	URL        string `json:"url"`
	Date       string `json:"date"`
}
type pageInfo struct {
	HasNextPage bool
	EndCursor   string
}
type prSearch struct {
	IssueCount int
	Nodes      []githubPR
	PageInfo   pageInfo
}

const accountPRFields = `fragment AccountPR on PullRequest {
 number title url state createdAt updatedAt isDraft reviewDecision mergeable author {login}
 repository {nameWithOwner}
 commits(last:1) {nodes {commit {statusCheckRollup {state}}}}
}`
const accountSummaryQuery = `query AccountSummary($since:DateTime!, $until:DateTime!, $created:String!, $merged:String!, $issues:String!, $organizationID:ID) {
 viewer {login id url contributionsCollection(from:$since,to:$until,organizationID:$organizationID) {
  totalPullRequestReviewContributions
  pullRequestReviewContributions(first:100) {nodes {pullRequestReview {
   url submittedAt pullRequest {title repository {nameWithOwner}}
  }}}
 }}
 created:search(query:$created,type:ISSUE,first:100) {issueCount nodes {...AccountPR}}
 merged:search(query:$merged,type:ISSUE,first:1) {issueCount}
 issues:search(query:$issues,type:ISSUE,first:100) {issueCount nodes {...on Issue {number title url state createdAt repository {nameWithOwner}}}}
}` + accountPRFields
const repositoryInventoryFields = `fragment RepositoryInventory on RepositoryConnection {
 totalCount pageInfo {hasNextPage endCursor} nodes {
  nameWithOwner url isFork defaultBranchRef {name target {...on Commit {
   history(first:10,since:$since,until:$until,author:{id:$authorID}) {
    totalCount nodes {oid messageHeadline url committedDate author {name}}
   }
  }}}
 }
}`
const accountRepositoriesQuery = `query AccountRepositories($authorID:ID!, $since:GitTimestamp!, $until:GitTimestamp!, $cursor:String) {
 viewer {repositories(first:25,after:$cursor,affiliations:[OWNER,COLLABORATOR,ORGANIZATION_MEMBER],ownerAffiliations:[OWNER,COLLABORATOR,ORGANIZATION_MEMBER]) {...RepositoryInventory}}
}` + repositoryInventoryFields
const organizationRepositoriesQuery = `query OrganizationRepositories($authorID:ID!, $since:GitTimestamp!, $until:GitTimestamp!, $cursor:String, $organization:String!) {
 viewer:organization(login:$organization) {repositories(first:25,after:$cursor,ownerAffiliations:[OWNER]) {...RepositoryInventory}}
}` + repositoryInventoryFields
const accountOpenQuery = `query AccountOpen($search:String!, $cursor:String) {
 search(query:$search,type:ISSUE,first:100,after:$cursor) {issueCount pageInfo {hasNextPage endCursor} nodes {...AccountPR}}
}` + accountPRFields

func (c *Client) request(ctx context.Context, query string, variables map[string]any, data any) error {
	input, errEncode := json.Marshal(map[string]any{"query": query, "variables": variables})
	if errEncode != nil {
		return ErrUnavailable
	}
	raw, errRun := c.Run(ctx, input)
	if errRun != nil {
		return ErrUnavailable
	}
	var response struct {
		Errors []json.RawMessage
		Data   json.RawMessage
	}
	if json.Unmarshal(raw, &response) != nil || len(response.Errors) > 0 || len(response.Data) == 0 || string(response.Data) == "null" {
		return ErrUnavailable
	}
	if json.Unmarshal(response.Data, data) != nil {
		return ErrUnavailable
	}
	return nil
}

func (c *Client) FetchAccount(ctx context.Context, account string, since, until time.Time) (Snapshot, error) {
	return c.fetchScope(ctx, account, "", since, until)
}
func (c *Client) FetchOrganization(ctx context.Context, account, organization string, since, until time.Time) (Snapshot, error) {
	return c.fetchScope(ctx, account, organization, since, until)
}
func (c *Client) fetchScope(ctx context.Context, account, organization string, since, until time.Time) (Snapshot, error) {
	var organizationID any
	var organizationURL string
	qualifier := ""
	repositoriesQuery := accountRepositoriesQuery
	if organization != "" {
		var scope struct {
			Organization *struct{ Login, ID, URL string }
		}
		if c.request(ctx, `query OrganizationScope($organization:String!) {organization(login:$organization) {login id url}}`, map[string]any{"organization": organization}, &scope) != nil || scope.Organization == nil || !strings.EqualFold(scope.Organization.Login, organization) {
			return Snapshot{}, ErrUnavailable
		}
		organization = scope.Organization.Login
		organizationID = scope.Organization.ID
		organizationURL = scope.Organization.URL
		qualifier = " org:" + scope.Organization.Login
		repositoriesQuery = organizationRepositoriesQuery
	}

	end := until.Add(-time.Second).UTC().Format(time.RFC3339)
	start := since.UTC().Format(time.RFC3339)
	period := start + ".." + end
	var summary struct {
		Viewer *struct {
			Login, ID, URL          string
			ContributionsCollection struct {
				TotalPullRequestReviewContributions int
				PullRequestReviewContributions      struct {
					Nodes []struct {
						PullRequestReview struct {
							URL, SubmittedAt string
							PullRequest      struct {
								Title      string
								Repository struct{ NameWithOwner string }
							}
						}
					}
				}
			}
		}
		Created prSearch
		Merged  prSearch
		Issues  struct {
			IssueCount int
			Nodes      []struct {
				Number                       int
				Title, URL, State, CreatedAt string
				Repository                   struct{ NameWithOwner string }
			}
		}
	}
	if c.request(ctx, accountSummaryQuery, map[string]any{
		"since": start, "until": end, "organizationID": organizationID,
		"created": "is:pr author:" + account + qualifier + " created:" + period + " sort:created-desc",
		"merged":  "is:pr author:" + account + qualifier + " merged:" + period,
		"issues":  "is:issue author:" + account + qualifier + " created:" + period + " sort:created-desc",
	}, &summary) != nil || summary.Viewer == nil || !strings.EqualFold(summary.Viewer.Login, account) {
		return Snapshot{}, ErrUnavailable
	}
	out := Snapshot{Account: summary.Viewer.Login, URL: summary.Viewer.URL, CheckedAt: time.Now().UTC().Format(time.RFC3339),
		CreatedCount: summary.Created.IssueCount, MergedCount: summary.Merged.IssueCount, IssueCount: summary.Issues.IssueCount,
		ReviewCount: summary.Viewer.ContributionsCollection.TotalPullRequestReviewContributions,
		OpenPRs:     []PullRequest{}, CreatedPRs: []PullRequest{}, Commits: []Commit{}, Repositories: []RepositoryActivity{}, Issues: []Issue{}, Reviews: []Review{}}
	if organization != "" {
		out.Organization = organization
		out.URL = organizationURL
	}
	for _, pr := range summary.Created.Nodes {
		out.CreatedPRs = append(out.CreatedPRs, normalizePR(pr))
	}
	for _, issue := range summary.Issues.Nodes {
		out.Issues = append(out.Issues, Issue{Repository: issue.Repository.NameWithOwner, Number: issue.Number, Title: issue.Title, URL: issue.URL, State: issue.State, CreatedAt: issue.CreatedAt})
	}
	for _, node := range summary.Viewer.ContributionsCollection.PullRequestReviewContributions.Nodes {
		review := node.PullRequestReview
		out.Reviews = append(out.Reviews, Review{Repository: review.PullRequest.Repository.NameWithOwner, Title: review.PullRequest.Title, URL: review.URL, Date: review.SubmittedAt})
	}
	var cursor any
	for {
		var page struct {
			Viewer *struct {
				Repositories struct {
					TotalCount int
					PageInfo   pageInfo
					Nodes      []struct {
						NameWithOwner, URL string
						IsFork             bool
						DefaultBranchRef   *struct {
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
				}
			}
		}
		variables := map[string]any{"authorID": summary.Viewer.ID, "since": start, "until": end, "cursor": cursor}
		if organization != "" {
			variables["organization"] = organization
		}
		if c.request(ctx, repositoriesQuery, variables, &page) != nil || page.Viewer == nil {
			return Snapshot{}, ErrUnavailable
		}
		repos := page.Viewer.Repositories
		out.RepositoryCount = repos.TotalCount
		for _, repo := range repos.Nodes {
			item := RepositoryActivity{Name: repo.NameWithOwner, URL: repo.URL, Fork: repo.IsFork}
			if branch := repo.DefaultBranchRef; branch != nil {
				item.DefaultBranch = branch.Name
				item.CommitCount = branch.Target.History.TotalCount
				out.CommitCount += item.CommitCount
				for _, commit := range branch.Target.History.Nodes {
					out.Commits = append(out.Commits, Commit{Repository: repo.NameWithOwner, SHA: commit.OID, Title: commit.MessageHeadline, URL: commit.URL, Date: commit.CommittedDate, Author: commit.Author.Name})
				}
			}
			out.Repositories = append(out.Repositories, item)
		}
		if !repos.PageInfo.HasNextPage {
			break
		}
		if repos.PageInfo.EndCursor == "" || cursor == repos.PageInfo.EndCursor {
			return Snapshot{}, ErrUnavailable
		}
		cursor = repos.PageInfo.EndCursor
	}
	seen := map[string]bool{}
	filters := []string{"involves:" + account, "user:" + account, "review-requested:" + account}
	if organization != "" {
		filters = []string{"org:" + organization}
	}
	for _, filter := range filters {
		cursor = nil
		fetched := 0
		for {
			var page struct{ Search *prSearch }
			if c.request(ctx, accountOpenQuery, map[string]any{"search": "is:pr is:open " + filter + " sort:updated-desc", "cursor": cursor}, &page) != nil || page.Search == nil {
				return Snapshot{}, ErrUnavailable
			}
			for _, pr := range page.Search.Nodes {
				if !seen[pr.URL] {
					out.OpenPRs = append(out.OpenPRs, normalizePR(pr))
					seen[pr.URL] = true
				}
			}
			fetched += len(page.Search.Nodes)
			if !page.Search.PageInfo.HasNextPage || fetched >= 1000 {
				out.OpenTruncated = out.OpenTruncated || fetched < page.Search.IssueCount
				break
			}
			if page.Search.PageInfo.EndCursor == "" || cursor == page.Search.PageInfo.EndCursor {
				return Snapshot{}, ErrUnavailable
			}
			cursor = page.Search.PageInfo.EndCursor
		}
	}
	out.OpenCount = len(out.OpenPRs)
	sort.Slice(out.OpenPRs, func(i, j int) bool { return out.OpenPRs[i].CreatedAt > out.OpenPRs[j].CreatedAt })
	sort.Slice(out.Commits, func(i, j int) bool { return out.Commits[i].Date > out.Commits[j].Date })
	sort.Slice(out.Repositories, func(i, j int) bool {
		if out.Repositories[i].CommitCount == out.Repositories[j].CommitCount {
			return out.Repositories[i].Name < out.Repositories[j].Name
		}
		return out.Repositories[i].CommitCount > out.Repositories[j].CommitCount
	})
	if len(out.OpenPRs) > 100 {
		out.OpenPRs = out.OpenPRs[:100]
	}
	if len(out.Commits) > 100 {
		out.Commits = out.Commits[:100]
	}
	return out, nil
}
