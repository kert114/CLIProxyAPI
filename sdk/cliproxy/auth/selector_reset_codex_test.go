package auth

import (
	"context"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
)

// codexWindow describes one Codex rate-limit window as reported upstream.
type codexWindow struct {
	minutes    int
	usedPct    string
	resetAt    time.Time
	resetAfter time.Duration
}

func codexResetAuth(id string, observedAt time.Time, extra http.Header, windows map[string]codexWindow) *Auth {
	candidate := &Auth{ID: id, Provider: "codex"}
	headers := make(http.Header)
	for name, value := range extra {
		headers[name] = value
	}
	for name, window := range windows {
		prefix := "X-Codex-" + name + "-"
		headers.Set(prefix+"Window-Minutes", strconv.Itoa(window.minutes))
		if window.usedPct != "" {
			headers.Set(prefix+"Used-Percent", window.usedPct)
		}
		if !window.resetAt.IsZero() {
			headers.Set(prefix+"Reset-At", strconv.FormatInt(window.resetAt.Unix(), 10))
		}
		if window.resetAfter > 0 {
			headers.Set(prefix+"Reset-After-Seconds", strconv.FormatInt(int64(window.resetAfter/time.Second), 10))
		}
	}
	candidate.Quota.ObserveResponseHeadersForProvider("codex", headers, observedAt)
	return candidate
}

func pickCodex(t *testing.T, selector Selector, opts cliproxyexecutor.Options, auths []*Auth) string {
	t.Helper()
	got, err := selector.Pick(context.Background(), "codex", "gpt-5", opts, auths)
	if err != nil {
		t.Fatal(err)
	}
	return got.ID
}

const weekMinutes = 7 * 24 * 60

func TestResetFirstCodexPrefersEarlierWeeklyReset(t *testing.T) {
	now := time.Now()
	gmail := codexResetAuth("gmail", now, nil, map[string]codexWindow{
		"Primary": {minutes: weekMinutes, usedPct: "35", resetAt: now.Add(5*24*time.Hour + 14*time.Hour)},
	})
	work := codexResetAuth("work", now, nil, map[string]codexWindow{
		"Primary": {minutes: weekMinutes, usedPct: "80", resetAt: now.Add(15 * time.Hour)},
	})
	selector := &ResetFirstSelector{}
	for i := 0; i < 4; i++ {
		if got := pickCodex(t, selector, cliproxyexecutor.Options{}, []*Auth{gmail, work}); got != "work" {
			t.Fatalf("pick %d = %q, want work (weekly resets in 15h)", i, got)
		}
	}
}

func TestResetFirstCodexTwoWindowsUsesLongestForOrderAndShortForGuard(t *testing.T) {
	now := time.Now()
	// Weekly sits in Secondary here; the window length, not the name, decides.
	early := codexResetAuth("early", now, nil, map[string]codexWindow{
		"Primary":   {minutes: 300, usedPct: "10", resetAt: now.Add(4 * time.Hour)},
		"Secondary": {minutes: weekMinutes, usedPct: "50", resetAt: now.Add(24 * time.Hour)},
	})
	late := codexResetAuth("late", now, nil, map[string]codexWindow{
		"Primary":   {minutes: 300, usedPct: "10", resetAt: now.Add(time.Hour)},
		"Secondary": {minutes: weekMinutes, usedPct: "50", resetAt: now.Add(72 * time.Hour)},
	})
	selector := &ResetFirstSelector{}
	if got := pickCodex(t, selector, cliproxyexecutor.Options{}, []*Auth{late, early}); got != "early" {
		t.Fatalf("picked %q, want early (earlier weekly reset beats earlier five-hour reset)", got)
	}
	early = codexResetAuth("early", now, nil, map[string]codexWindow{
		"Primary":   {minutes: 300, usedPct: "85", resetAt: now.Add(4 * time.Hour)},
		"Secondary": {minutes: weekMinutes, usedPct: "50", resetAt: now.Add(24 * time.Hour)},
	})
	if got := pickCodex(t, &ResetFirstSelector{}, cliproxyexecutor.Options{}, []*Auth{late, early}); got != "late" {
		t.Fatalf("picked %q, want late (early is at 85%% of its five-hour window)", got)
	}
}

func TestResetFirstCodexSingleHighWeeklyUsageDoesNotTripGuard(t *testing.T) {
	now := time.Now()
	nearlyDone := codexResetAuth("nearly-done", now, nil, map[string]codexWindow{
		"Primary": {minutes: weekMinutes, usedPct: "95", resetAt: now.Add(10 * time.Hour)},
	})
	fresh := codexResetAuth("fresh", now, nil, map[string]codexWindow{
		"Primary": {minutes: weekMinutes, usedPct: "5", resetAt: now.Add(100 * time.Hour)},
	})
	if got := pickCodex(t, &ResetFirstSelector{}, cliproxyexecutor.Options{}, []*Auth{fresh, nearlyDone}); got != "nearly-done" {
		t.Fatalf("picked %q, want nearly-done (weekly usage is spent before it resets)", got)
	}
}

func TestResetFirstCodexResetAfterSecondsUsesObservationTime(t *testing.T) {
	now := time.Now()
	a := codexResetAuth("a", now.Add(-2*time.Hour), nil, map[string]codexWindow{
		"Primary": {minutes: weekMinutes, resetAfter: 10 * time.Hour}, // resets in 8h
	})
	b := codexResetAuth("b", now, nil, map[string]codexWindow{
		"Primary": {minutes: weekMinutes, resetAfter: 9 * time.Hour},
	})
	if got := pickCodex(t, &ResetFirstSelector{}, cliproxyexecutor.Options{}, []*Auth{b, a}); got != "a" {
		t.Fatalf("picked %q, want a (observed 2h ago with 10h left)", got)
	}
}

func TestResetFirstCodexIgnoresAdditionalLimitWindows(t *testing.T) {
	now := time.Now()
	extra := http.Header{}
	extra.Set("X-Codex-Bengalfox-Primary-Window-Minutes", strconv.Itoa(weekMinutes*4))
	extra.Set("X-Codex-Bengalfox-Primary-Reset-At", strconv.FormatInt(now.Add(time.Hour).Unix(), 10))
	a := codexResetAuth("a", now, extra, map[string]codexWindow{
		"Primary": {minutes: weekMinutes, resetAt: now.Add(48 * time.Hour)},
	})
	b := codexResetAuth("b", now, nil, map[string]codexWindow{
		"Primary": {minutes: weekMinutes, resetAt: now.Add(24 * time.Hour)},
	})
	if got := pickCodex(t, &ResetFirstSelector{}, cliproxyexecutor.Options{}, []*Auth{a, b}); got != "b" {
		t.Fatalf("picked %q, want b (additional limit windows must not order accounts)", got)
	}
}

func TestResetFirstCodexUnknownWeeklyUsesRoundRobin(t *testing.T) {
	now := time.Now()
	known := codexResetAuth("a", now, nil, map[string]codexWindow{
		"Primary": {minutes: weekMinutes, resetAt: now.Add(24 * time.Hour)},
	})
	expired := codexResetAuth("b", now, nil, map[string]codexWindow{
		"Primary": {minutes: weekMinutes, resetAt: now.Add(-time.Minute)},
	})
	selector := &ResetFirstSelector{}
	for i, want := range []string{"a", "b", "a", "b"} {
		if got := pickCodex(t, selector, cliproxyexecutor.Options{}, []*Auth{known, expired}); got != want {
			t.Fatalf("pick %d = %q, want %q", i, got, want)
		}
	}
}

func TestResetFirstMixedPoolOrdersAcrossProviders(t *testing.T) {
	now := time.Now()
	claude := resetTestAuth("claude", now.Add(48*time.Hour), now.Add(time.Hour))
	codex := codexResetAuth("codex", now, nil, map[string]codexWindow{
		"Primary": {minutes: weekMinutes, resetAt: now.Add(24 * time.Hour)},
	})
	got, err := (&ResetFirstSelector{}).Pick(context.Background(), "mixed", "shared-alias", cliproxyexecutor.Options{}, []*Auth{claude, codex})
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != "codex" {
		t.Fatalf("picked %q, want codex (earlier weekly reset)", got.ID)
	}
}

func TestResetFirstDevinUsesWeeklyThenDailyGuard(t *testing.T) {
	now := time.Now()
	devin := func(id string, weekly time.Duration, dailyRemaining string) *Auth {
		return &Auth{ID: id, Provider: "devin", Quota: QuotaState{ObservedAt: now, Signals: map[string]string{
			"weekly_quota_reset_at":         now.Add(weekly).Format(time.RFC3339),
			"daily_quota_reset_at":          now.Add(6 * time.Hour).Format(time.RFC3339),
			"daily_quota_remaining_percent": dailyRemaining,
		}}}
	}
	got, err := (&ResetFirstSelector{}).Pick(context.Background(), "devin", "swe", cliproxyexecutor.Options{}, []*Auth{devin("late", 72*time.Hour, "90%"), devin("early", 24*time.Hour, "90%")})
	if err != nil || got.ID != "early" {
		t.Fatalf("picked %v (err %v), want early", got, err)
	}
	got, err = (&ResetFirstSelector{}).Pick(context.Background(), "devin", "swe", cliproxyexecutor.Options{}, []*Auth{devin("late", 72*time.Hour, "90%"), devin("early", 24*time.Hour, "10%")})
	if err != nil || got.ID != "late" {
		t.Fatalf("picked %v (err %v), want late (early has 10%% of its daily quota left)", got, err)
	}
}

func TestResetFirstCodexFiveHourOnlySnapshotIsShortWindow(t *testing.T) {
	now := time.Now()
	weekly := codexResetAuth("weekly", now, nil, map[string]codexWindow{
		"Primary": {minutes: weekMinutes, usedPct: "10", resetAt: now.Add(24 * time.Hour)},
	})
	busy := codexResetAuth("busy", now, nil, map[string]codexWindow{
		"Primary": {minutes: 300, usedPct: "85", resetAt: now.Add(time.Hour)},
	})
	selector := &ResetFirstSelector{}
	for i := 0; i < 3; i++ {
		if got := pickCodex(t, selector, cliproxyexecutor.Options{}, []*Auth{busy, weekly}); got != "weekly" {
			t.Fatalf("pick %d = %q, want weekly (busy is at 85%% of a five-hour window)", i, got)
		}
	}
}

func TestResetFirstCodexKeepsWebsocketPreference(t *testing.T) {
	now := time.Now()
	ws := codexResetAuth("ws", now, nil, map[string]codexWindow{
		"Primary": {minutes: weekMinutes, resetAt: now.Add(24 * time.Hour)},
	})
	ws.Attributes = map[string]string{"websockets": "true"}
	httpOnly := codexResetAuth("http", now, nil, map[string]codexWindow{
		"Primary": {minutes: weekMinutes, resetAt: now.Add(15 * time.Hour)},
	})
	ctx := cliproxyexecutor.WithDownstreamWebsocket(context.Background())
	got, err := (&ResetFirstSelector{}).Pick(ctx, "codex", "gpt-5", cliproxyexecutor.Options{}, []*Auth{httpOnly, ws})
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != "ws" {
		t.Fatalf("picked %q, want ws (downstream websocket prefers websocket credentials)", got.ID)
	}
}

func TestResetFirstPoolWithUnparsedProviderKeepsRoundRobin(t *testing.T) {
	now := time.Now()
	claude := guardTestAuth("claude", now.Add(24*time.Hour), "0.85", now.Add(time.Hour), false)
	gemini := &Auth{ID: "gemini", Provider: "gemini"}
	selector := &ResetFirstSelector{}
	for i, want := range []string{"claude", "gemini", "claude", "gemini"} {
		got, err := selector.Pick(context.Background(), "mixed", "shared-alias", cliproxyexecutor.Options{}, []*Auth{claude, gemini})
		if err != nil {
			t.Fatal(err)
		}
		if got.ID != want {
			t.Fatalf("pick %d = %q, want %q", i, got.ID, want)
		}
	}
}

func TestResetFirstUnparsedProviderIgnoresSubagentOnly(t *testing.T) {
	reserved := &Auth{ID: "a", Provider: "gemini", Metadata: map[string]any{"subagent_only": true}}
	other := &Auth{ID: "b", Provider: "gemini"}
	selector := &ResetFirstSelector{}
	for i, want := range []string{"a", "b", "a"} {
		got, err := selector.Pick(context.Background(), "gemini", "gemini-pro", cliproxyexecutor.Options{}, []*Auth{reserved, other})
		if err != nil {
			t.Fatal(err)
		}
		if got.ID != want {
			t.Fatalf("pick %d = %q, want %q", i, got.ID, want)
		}
	}
}

// authIDStreamExecutor streams the selected auth ID so tests can see which credential ran.
type authIDStreamExecutor struct{ mockCustomErrorExecutor }

func (e *authIDStreamExecutor) ExecuteStream(_ context.Context, auth *Auth, _ cliproxyexecutor.Request, _ cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	chunks := make(chan cliproxyexecutor.StreamChunk, 1)
	chunks <- cliproxyexecutor.StreamChunk{Payload: []byte(auth.ID)}
	close(chunks)
	return &cliproxyexecutor.StreamResult{Chunks: chunks}, nil
}

func TestResetFirstManagerStreamKeepsCodexWebsocketPreference(t *testing.T) {
	const model = "reset-ws-codex"
	now := time.Now()
	ws := codexResetAuth("reset-ws-enabled", now, nil, map[string]codexWindow{
		"Primary": {minutes: weekMinutes, resetAt: now.Add(24 * time.Hour)},
	})
	ws.Attributes = map[string]string{"websockets": "true"}
	httpOnly := codexResetAuth("reset-ws-http", now, nil, map[string]codexWindow{
		"Primary": {minutes: weekMinutes, resetAt: now.Add(15 * time.Hour)},
	})
	for name, selector := range map[string]Selector{
		"reset-first":           &ResetFirstSelector{},
		"with session affinity": NewSessionAffinitySelector(&ResetFirstSelector{}),
	} {
		t.Run(name, func(t *testing.T) {
			if affinity, ok := selector.(*SessionAffinitySelector); ok {
				t.Cleanup(affinity.Stop)
			}
			manager := NewManager(nil, selector, nil)
			manager.RegisterExecutor(&authIDStreamExecutor{mockCustomErrorExecutor{identifier: "codex"}})
			for _, candidate := range []*Auth{httpOnly.Clone(), ws.Clone()} {
				registry.GetGlobalRegistry().RegisterClient(candidate.ID, "codex", []*registry.ModelInfo{{ID: model}})
				t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(candidate.ID) })
				if _, err := manager.Register(context.Background(), candidate); err != nil {
					t.Fatal(err)
				}
			}
			ctx := cliproxyexecutor.WithDownstreamWebsocket(context.Background())
			opts := cliproxyexecutor.Options{Headers: http.Header{"Session_id": {"codex-ws-session"}}}
			for i := 0; i < 2; i++ {
				result, err := manager.ExecuteStream(ctx, []string{"codex"}, cliproxyexecutor.Request{Model: model}, opts)
				if err != nil {
					t.Fatal(err)
				}
				var got string
				for chunk := range result.Chunks {
					got += string(chunk.Payload)
				}
				if got != "reset-ws-enabled" {
					t.Fatalf("request %d streamed with %q, want reset-ws-enabled", i, got)
				}
			}
		})
	}
}

func TestResetFirstCodexDailyWindowIsShortWindow(t *testing.T) {
	now := time.Now()
	const dayMinutes = 24 * 60
	busy := codexResetAuth("busy", now, nil, map[string]codexWindow{
		"Primary":   {minutes: dayMinutes, usedPct: "85", resetAt: now.Add(6 * time.Hour)},
		"Secondary": {minutes: weekMinutes, resetAt: now.Add(24 * time.Hour)},
	})
	idle := codexResetAuth("idle", now, nil, map[string]codexWindow{
		"Primary":   {minutes: dayMinutes, usedPct: "10", resetAt: now.Add(6 * time.Hour)},
		"Secondary": {minutes: weekMinutes, resetAt: now.Add(72 * time.Hour)},
	})
	if got := pickCodex(t, &ResetFirstSelector{}, cliproxyexecutor.Options{}, []*Auth{busy, idle}); got != "idle" {
		t.Fatalf("picked %q, want idle (busy is at 85%% of its daily window)", got)
	}
	dailyOnly := codexResetAuth("daily-only", now, nil, map[string]codexWindow{
		"Primary": {minutes: dayMinutes, usedPct: "10", resetAt: now.Add(2 * time.Hour)},
	})
	weekly := codexResetAuth("weekly", now, nil, map[string]codexWindow{
		"Primary": {minutes: weekMinutes, resetAt: now.Add(24 * time.Hour)},
	})
	selector := &ResetFirstSelector{}
	for i, want := range []string{"daily-only", "weekly", "daily-only"} {
		if got := pickCodex(t, selector, cliproxyexecutor.Options{}, []*Auth{dailyOnly, weekly}); got != want {
			t.Fatalf("pick %d = %q, want %q (a daily window must not stand in for a weekly reset)", i, got, want)
		}
	}
}
