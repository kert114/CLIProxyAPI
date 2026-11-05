package auth

import (
	"context"
	"net/http"
	"strconv"
	"testing"
	"time"

	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
)

func newCapSelector(t *testing.T) *SessionAffinitySelector {
	t.Helper()
	disabled := false
	selector := NewSessionAffinitySelectorWithConfig(SessionAffinityConfig{Fallback: &ResetFirstSelector{}, SubagentAffinity: &disabled})
	t.Cleanup(selector.Stop)
	return selector
}

func subagentOpts(agent string) cliproxyexecutor.Options {
	return cliproxyexecutor.Options{Headers: http.Header{
		"X-Claude-Code-Session-Id": {"chat-1"},
		"X-Claude-Code-Agent-Id":   {agent},
	}}
}

func capTestAuths() (*Auth, *Auth) {
	now := time.Now()
	team := guardTestAuth("team", now.Add(24*time.Hour), "0.1", now.Add(time.Hour), true)
	max := guardTestAuth("max", now.Add(48*time.Hour), "0.1", now.Add(time.Hour), false)
	return team, max
}

func TestSubagentOnlyAccountCapsActiveSubagents(t *testing.T) {
	selector := newCapSelector(t)
	team, max := capTestAuths()
	auths := []*Auth{team, max}
	for i := 1; i <= 3; i++ {
		if got := pickReset(t, selector, subagentOpts("agent-"+strconv.Itoa(i)), auths); got != "team" {
			t.Fatalf("subagent %d picked %q, want team", i, got)
		}
	}
	if got := pickReset(t, selector, subagentOpts("agent-4"), auths); got != "max" {
		t.Fatalf("fourth subagent picked %q, want max", got)
	}
	if got := pickReset(t, selector, subagentOpts("agent-1"), auths); got != "team" {
		t.Fatalf("existing subagent moved to %q, want team", got)
	}
}

func TestSubagentCapIgnoresIdleSessions(t *testing.T) {
	selector := newCapSelector(t)
	team, max := capTestAuths()
	auths := []*Auth{team, max}
	for i := 1; i <= 3; i++ {
		pickReset(t, selector, subagentOpts("agent-"+strconv.Itoa(i)), auths)
	}
	selector.subagents.age(subagentActiveWindow + time.Second)
	if got := pickReset(t, selector, subagentOpts("agent-4"), auths); got != "team" {
		t.Fatalf("subagent after idle window picked %q, want team", got)
	}
}

func TestSubagentCapIsConfigurablePerAccount(t *testing.T) {
	selector := newCapSelector(t)
	team, max := capTestAuths()
	team.Metadata["max_subagent_sessions"] = float64(1)
	auths := []*Auth{team, max}
	if got := pickReset(t, selector, subagentOpts("agent-1"), auths); got != "team" {
		t.Fatalf("first subagent picked %q, want team", got)
	}
	if got := pickReset(t, selector, subagentOpts("agent-2"), auths); got != "max" {
		t.Fatalf("second subagent picked %q, want max", got)
	}
}

func TestSubagentCapFullAccountIsLastResort(t *testing.T) {
	selector := newCapSelector(t)
	team, max := capTestAuths()
	auths := []*Auth{team, max}
	for i := 1; i <= 3; i++ {
		pickReset(t, selector, subagentOpts("agent-"+strconv.Itoa(i)), auths)
	}
	max.Disabled = true
	if got := pickReset(t, selector, subagentOpts("agent-4"), auths); got != "team" {
		t.Fatalf("subagent with no other account picked %q, want team", got)
	}
}

func TestSessionRoleHeaderCountsAsSubagent(t *testing.T) {
	selector := newCapSelector(t)
	team, max := capTestAuths()
	auths := []*Auth{team, max}
	thread := func(id string) cliproxyexecutor.Options {
		return cliproxyexecutor.Options{Headers: http.Header{
			"X-Claude-Code-Session-Id": {id},
			"X-Cliproxy-Session-Role":  {"subagent"},
		}}
	}
	for i := 1; i <= 3; i++ {
		if got := pickReset(t, selector, thread("delegated-"+strconv.Itoa(i)), auths); got != "team" {
			t.Fatalf("delegated thread %d picked %q, want team", i, got)
		}
	}
	if got := pickReset(t, selector, thread("delegated-4"), auths); got != "max" {
		t.Fatalf("fourth delegated thread picked %q, want max", got)
	}
	top := cliproxyexecutor.Options{Headers: http.Header{"X-Claude-Code-Session-Id": {"top-level"}}}
	if got := pickReset(t, selector, top, auths); got != "max" {
		t.Fatalf("top-level chat picked %q, want max", got)
	}
}

func TestSubagentCapCountsSessionOnceAcrossModels(t *testing.T) {
	selector := newCapSelector(t)
	team, max := capTestAuths()
	auths := []*Auth{team, max}
	for _, model := range []string{"opus", "haiku"} {
		for i := 1; i <= 2; i++ {
			got, err := selector.Pick(context.Background(), "claude", model, subagentOpts("agent-"+strconv.Itoa(i)), auths)
			if err != nil || got.ID != "team" {
				t.Fatalf("agent-%d on %s picked %v (%v), want team", i, model, got, err)
			}
		}
	}
	if got := pickReset(t, selector, subagentOpts("agent-3"), auths); got != "team" {
		t.Fatalf("third subagent picked %q, want team", got)
	}
}
