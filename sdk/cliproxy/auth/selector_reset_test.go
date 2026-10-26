package auth

import (
	"context"
	"net/http"
	"strconv"
	"testing"
	"time"

	internalconfig "github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	internallogging "github.com/router-for-me/CLIProxyAPI/v8/internal/logging"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
)

func resetTestAuth(id string, weekly, fiveHour time.Time) *Auth {
	candidate := &Auth{ID: id, Provider: "claude"}
	headers := make(http.Header)
	if !weekly.IsZero() {
		headers.Set("Anthropic-Ratelimit-Unified-7d-Reset", strconv.FormatInt(weekly.Unix(), 10))
	}
	if !fiveHour.IsZero() {
		headers.Set("Anthropic-Ratelimit-Unified-5h-Reset", fiveHour.Format(time.RFC3339))
	}
	candidate.Quota.ObserveResponseHeadersForProvider("claude", headers, time.Now())
	return candidate
}

func TestResetFirstSelectorOrdering(t *testing.T) {
	now := time.Now()
	later := now.Add(48 * time.Hour)
	soon := now.Add(24 * time.Hour)
	cases := []struct {
		name  string
		auths []*Auth
		want  string
	}{
		{"weekly beats five-hour", []*Auth{resetTestAuth("a", later, now.Add(time.Hour)), resetTestAuth("b", soon, now.Add(4*time.Hour))}, "b"},
		{"five-hour breaks weekly tie", []*Auth{resetTestAuth("a", soon, now.Add(4*time.Hour)), resetTestAuth("b", soon, now.Add(time.Hour))}, "b"},
		{"known five-hour beats missing", []*Auth{resetTestAuth("a", soon, time.Time{}), resetTestAuth("b", soon, now.Add(time.Hour))}, "b"},
		{"equal resets keep deterministic order", []*Auth{resetTestAuth("b", soon, now.Add(time.Hour)), resetTestAuth("a", soon, now.Add(time.Hour))}, "a"},
		{"disabled account excluded", []*Auth{resetTestAuth("a", later, now.Add(time.Hour)), func() *Auth { a := resetTestAuth("b", soon, now.Add(time.Hour)); a.Disabled = true; return a }()}, "a"},
		{"credential priority preserved", []*Auth{func() *Auth {
			a := resetTestAuth("a", later, now.Add(time.Hour))
			a.Attributes = map[string]string{"priority": "5"}
			return a
		}(), resetTestAuth("b", soon, now.Add(time.Hour))}, "a"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			selector := &ResetFirstSelector{}
			got, err := selector.Pick(context.Background(), "claude", "opus", cliproxyexecutor.Options{}, tc.auths)
			if err != nil {
				t.Fatal(err)
			}
			if got.ID != tc.want {
				t.Fatalf("Pick() = %q, want %q", got.ID, tc.want)
			}
		})
	}
}

func TestResetFirstSelectorUnknownWeeklyUsesRoundRobin(t *testing.T) {
	for _, raw := range []string{"", "invalid", "1", "NaN", "9999999999999999999999999"} {
		t.Run(raw, func(t *testing.T) {
			now := time.Now()
			a := resetTestAuth("a", now.Add(24*time.Hour), now.Add(time.Hour))
			b := resetTestAuth("b", time.Time{}, now.Add(time.Hour))
			b.Quota.Signals[http.CanonicalHeaderKey("Anthropic-Ratelimit-Unified-7d-Reset")] = raw
			selector := &ResetFirstSelector{}
			for i, want := range []string{"a", "b", "a", "b"} {
				got, err := selector.Pick(context.Background(), "claude", "opus", cliproxyexecutor.Options{}, []*Auth{a, b})
				if err != nil {
					t.Fatal(err)
				}
				if got.ID != want {
					t.Fatalf("pick %d = %q, want %q", i, got.ID, want)
				}
			}
		})
	}
}

func TestResetFirstSelectorOtherProvidersRemainRoundRobin(t *testing.T) {
	selector := &ResetFirstSelector{}
	now := time.Now()
	auths := []*Auth{resetTestAuth("a", now.Add(48*time.Hour), now.Add(time.Hour)), resetTestAuth("b", now.Add(24*time.Hour), now.Add(time.Hour))}
	for i, want := range []string{"a", "b", "a"} {
		got, err := selector.Pick(context.Background(), "codex", "gpt", cliproxyexecutor.Options{}, auths)
		if err != nil {
			t.Fatal(err)
		}
		if got.ID != want {
			t.Fatalf("pick %d = %q, want %q", i, got.ID, want)
		}
	}
}

func TestResetFirstAffinityKeepsChatUntilFailover(t *testing.T) {
	selector := NewSessionAffinitySelector(&ResetFirstSelector{})
	t.Cleanup(selector.Stop)
	now := time.Now()
	a := resetTestAuth("a", now.Add(24*time.Hour), now.Add(time.Hour))
	b := resetTestAuth("b", now.Add(48*time.Hour), now.Add(time.Hour))
	c := resetTestAuth("c", now.Add(72*time.Hour), now.Add(time.Hour))
	auths := []*Auth{a, b, c}
	pick := func(session, want string) {
		t.Helper()
		opts := cliproxyexecutor.Options{Headers: http.Header{"X-Claude-Code-Session-Id": {session}}}
		got, err := selector.Pick(context.Background(), "claude", "opus", opts, auths)
		if err != nil {
			t.Fatal(err)
		}
		if got.ID != want {
			t.Fatalf("session %s picked %q, want %q", session, got.ID, want)
		}
	}
	pick("chat-1", "a")
	b.Quota = resetTestAuth("b", now.Add(12*time.Hour), now.Add(time.Hour)).Quota
	pick("chat-1", "a")
	pick("chat-2", "b")
	a.Disabled = true
	pick("chat-1", "b")
	a.Disabled = false
	a.Quota = resetTestAuth("a", now.Add(6*time.Hour), now.Add(time.Hour)).Quota
	pick("chat-1", "b")
	pick("chat-3", "a")
}

func TestResetFirstManagerUsesResolvedAliasAvailability(t *testing.T) {
	const routeModel, targetModel = "reset-route", "reset-target"
	manager := NewManager(nil, &ResetFirstSelector{}, nil)
	manager.RegisterExecutor(&aliasRoutingExecutor{id: "antigravity"})
	manager.SetOAuthModelAlias(map[string][]internalconfig.OAuthModelAlias{
		"antigravity": {{Name: targetModel, Alias: routeModel, Fork: true}},
	})
	candidate := &Auth{ID: "reset-alias-auth", Provider: "antigravity", Status: StatusActive,
		ModelStates: map[string]*ModelState{routeModel: {Unavailable: true, Status: StatusError, NextRetryAfter: time.Now().Add(time.Hour)}},
	}
	if _, err := manager.Register(context.Background(), candidate); err != nil {
		t.Fatal(err)
	}
	registry.GetGlobalRegistry().RegisterClient(candidate.ID, "antigravity", []*registry.ModelInfo{{ID: routeModel}, {ID: targetModel}})
	t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(candidate.ID) })
	got, err := manager.Execute(context.Background(), []string{"antigravity"}, cliproxyexecutor.Request{Model: routeModel}, cliproxyexecutor.Options{})
	if err != nil {
		t.Fatalf("healthy target blocked by unrelated route cooldown: %v", err)
	}
	if string(got.Payload) != targetModel {
		t.Fatalf("response = %q, want %q", got.Payload, targetModel)
	}
}

func TestResetFirstManagerExecutesWeeklyFirst(t *testing.T) {
	const model = "reset-manager-opus"
	manager := NewManager(nil, &ResetFirstSelector{}, nil)
	manager.RegisterExecutor(&mockCustomErrorExecutor{identifier: "claude", executeFn: func(_ context.Context, auth *Auth, _ cliproxyexecutor.Request, _ cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
		return cliproxyexecutor.Response{Payload: []byte(auth.ID)}, nil
	}})
	now := time.Now()
	for _, candidate := range []*Auth{resetTestAuth("reset-execute-a", now.Add(48*time.Hour), now.Add(time.Hour)), resetTestAuth("reset-execute-b", now.Add(24*time.Hour), now.Add(4*time.Hour))} {
		registry.GetGlobalRegistry().RegisterClient(candidate.ID, "claude", []*registry.ModelInfo{{ID: model}})
		t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(candidate.ID) })
		if _, err := manager.Register(context.Background(), candidate); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 3; i++ {
		response, err := manager.Execute(context.Background(), []string{"claude"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
		if err != nil {
			t.Fatal(err)
		}
		if string(response.Payload) != "reset-execute-b" {
			t.Fatalf("request %d used %q, want account reset-execute-b", i, response.Payload)
		}
	}
}

func TestResetFirstManagerLearnsFromResponses(t *testing.T) {
	const model = "reset-learning-opus"
	selector := NewSessionAffinitySelector(&ResetFirstSelector{})
	t.Cleanup(selector.Stop)
	manager := NewManager(nil, selector, nil)
	now := time.Now()
	manager.RegisterExecutor(&mockCustomErrorExecutor{identifier: "claude", executeFn: func(ctx context.Context, auth *Auth, _ cliproxyexecutor.Request, _ cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
		weekly := now.Add(48 * time.Hour)
		if auth.ID == "reset-learning-b" {
			weekly = now.Add(24 * time.Hour)
		}
		internallogging.SetResponseHeaders(ctx, http.Header{"Anthropic-Ratelimit-Unified-7d-Reset": {strconv.FormatInt(weekly.Unix(), 10)}})
		return cliproxyexecutor.Response{Payload: []byte(auth.ID)}, nil
	}})
	for _, id := range []string{"reset-learning-a", "reset-learning-b"} {
		registry.GetGlobalRegistry().RegisterClient(id, "claude", []*registry.ModelInfo{{ID: model}})
		t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(id) })
		if _, err := manager.Register(context.Background(), &Auth{ID: id, Provider: "claude"}); err != nil {
			t.Fatal(err)
		}
	}
	for i, want := range []string{"reset-learning-a", "reset-learning-b", "reset-learning-b"} {
		opts := cliproxyexecutor.Options{Headers: http.Header{"X-Claude-Code-Session-Id": {strconv.Itoa(i)}}}
		response, err := manager.Execute(context.Background(), []string{"claude"}, cliproxyexecutor.Request{Model: model}, opts)
		if err != nil {
			t.Fatal(err)
		}
		if string(response.Payload) != want {
			t.Fatalf("request %d used %q, want %q", i, response.Payload, want)
		}
	}
}

func TestResetFirstMixedProvidersRemainRoundRobin(t *testing.T) {
	now := time.Now()
	a := resetTestAuth("a", now.Add(48*time.Hour), now.Add(time.Hour))
	b := resetTestAuth("b", now.Add(24*time.Hour), now.Add(time.Hour))
	b.Provider = "codex"
	selector := &ResetFirstSelector{}
	for i, want := range []string{"a", "b", "a"} {
		got, err := selector.Pick(context.Background(), "mixed", "model", cliproxyexecutor.Options{}, []*Auth{a, b})
		if err != nil {
			t.Fatal(err)
		}
		if got.ID != want {
			t.Fatalf("pick %d=%q, want %q", i, got.ID, want)
		}
	}
}
