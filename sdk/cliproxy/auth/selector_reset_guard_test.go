package auth

import (
	"context"
	"net/http"
	"testing"
	"time"

	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
)

func guardTestAuth(id string, weekly time.Time, fiveHourUtilization string, fiveHourReset time.Time, subagentOnly bool) *Auth {
	candidate := resetTestAuth(id, weekly, fiveHourReset)
	if fiveHourUtilization != "" {
		candidate.Quota.Signals[http.CanonicalHeaderKey("Anthropic-Ratelimit-Unified-5h-Utilization")] = fiveHourUtilization
	}
	if subagentOnly {
		candidate.Metadata = map[string]any{"subagent_only": true}
	}
	return candidate
}

func pickReset(t *testing.T, selector Selector, opts cliproxyexecutor.Options, auths []*Auth) string {
	t.Helper()
	got, err := selector.Pick(context.Background(), "claude", "opus", opts, auths)
	if err != nil {
		t.Fatal(err)
	}
	return got.ID
}

var subagentHeaderOpts = cliproxyexecutor.Options{Headers: http.Header{"X-Cliproxy-Session-Role": {"subagent"}}}

func TestResetFirstSubagentOnlyAccounts(t *testing.T) {
	now := time.Now()
	soon, later := now.Add(24*time.Hour), now.Add(48*time.Hour)
	team := guardTestAuth("team", soon, "0.1", now.Add(time.Hour), true)
	max := guardTestAuth("max", later, "0.1", now.Add(time.Hour), false)
	auths := []*Auth{team, max}

	if got := pickReset(t, &ResetFirstSelector{}, cliproxyexecutor.Options{}, auths); got != "max" {
		t.Fatalf("top-level request picked %q, want max", got)
	}
	if got := pickReset(t, &ResetFirstSelector{}, subagentHeaderOpts, auths); got != "team" {
		t.Fatalf("subagent request picked %q, want team", got)
	}
	max.Disabled = true
	if got := pickReset(t, &ResetFirstSelector{}, cliproxyexecutor.Options{}, auths); got != "team" {
		t.Fatalf("top-level request with no other account picked %q, want team", got)
	}
}

func TestResetFirstSubagentOnlyAttributeString(t *testing.T) {
	now := time.Now()
	team := guardTestAuth("team", now.Add(24*time.Hour), "", time.Time{}, false)
	team.Attributes = map[string]string{"subagent_only": "true"}
	max := guardTestAuth("max", now.Add(48*time.Hour), "", time.Time{}, false)
	if got := pickReset(t, &ResetFirstSelector{}, cliproxyexecutor.Options{}, []*Auth{team, max}); got != "max" {
		t.Fatalf("top-level request picked %q, want max", got)
	}
}

func TestResetFirstFiveHourHeadroom(t *testing.T) {
	now := time.Now()
	soon, later := now.Add(24*time.Hour), now.Add(48*time.Hour)
	cases := []struct {
		name  string
		auths []*Auth
		opts  cliproxyexecutor.Options
		want  string
	}{
		{"busy account skipped", []*Auth{
			guardTestAuth("a", soon, "0.85", now.Add(time.Hour), false),
			guardTestAuth("b", later, "0.30", now.Add(time.Hour), false),
		}, cliproxyexecutor.Options{}, "b"},
		{"threshold is inclusive", []*Auth{
			guardTestAuth("a", soon, "0.80", now.Add(time.Hour), false),
			guardTestAuth("b", later, "0.79", now.Add(time.Hour), false),
		}, cliproxyexecutor.Options{}, "b"},
		{"expired five-hour window ignored", []*Auth{
			guardTestAuth("a", soon, "0.95", now.Add(-time.Minute), false),
			guardTestAuth("b", later, "0.30", now.Add(time.Hour), false),
		}, cliproxyexecutor.Options{}, "a"},
		{"all busy keeps reset order", []*Auth{
			guardTestAuth("a", soon, "0.90", now.Add(time.Hour), false),
			guardTestAuth("b", later, "0.95", now.Add(time.Hour), false),
		}, cliproxyexecutor.Options{}, "a"},
		{"busy subagent account spills to other account", []*Auth{
			guardTestAuth("team", soon, "0.85", now.Add(time.Hour), true),
			guardTestAuth("max", later, "0.30", now.Add(time.Hour), false),
		}, subagentHeaderOpts, "max"},
		{"busy main account does not spill top-level chats to subagent account", []*Auth{
			guardTestAuth("team", soon, "0.10", now.Add(time.Hour), true),
			guardTestAuth("max", later, "0.90", now.Add(time.Hour), false),
		}, cliproxyexecutor.Options{}, "max"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := pickReset(t, &ResetFirstSelector{}, tc.opts, tc.auths); got != tc.want {
				t.Fatalf("Pick() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestResetFirstClaudeCodeSubagentUsesSubagentAccount(t *testing.T) {
	disabled := false
	selector := NewSessionAffinitySelectorWithConfig(SessionAffinityConfig{Fallback: &ResetFirstSelector{}, SubagentAffinity: &disabled})
	t.Cleanup(selector.Stop)
	now := time.Now()
	team := guardTestAuth("team", now.Add(24*time.Hour), "0.1", now.Add(time.Hour), true)
	max := guardTestAuth("max", now.Add(48*time.Hour), "0.1", now.Add(time.Hour), false)
	auths := []*Auth{team, max}

	parent := cliproxyexecutor.Options{Headers: http.Header{"X-Claude-Code-Session-Id": {"chat-1"}}}
	if got := pickReset(t, selector, parent, auths); got != "max" {
		t.Fatalf("parent chat picked %q, want max", got)
	}
	child := cliproxyexecutor.Options{Headers: http.Header{
		"X-Claude-Code-Session-Id": {"chat-1"},
		"X-Claude-Code-Agent-Id":   {"reviewer-1"},
	}}
	if got := pickReset(t, selector, child, auths); got != "team" {
		t.Fatalf("subagent picked %q, want team", got)
	}
	if got := pickReset(t, selector, parent, auths); got != "max" {
		t.Fatalf("parent chat moved to %q, want max", got)
	}
}
