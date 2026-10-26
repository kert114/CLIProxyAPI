package cliproxy

import (
	"context"
	"net/http"
	"strconv"
	"testing"
	"time"

	internalconfig "github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
)

func TestResetFirstRoutingPrefersWeeklyReset(t *testing.T) {
	now := time.Now()
	auths := []*coreauth.Auth{{ID: "a", Provider: "claude"}, {ID: "b", Provider: "claude"}}
	for i, candidate := range auths {
		weekly := now.Add(48 * time.Hour)
		fiveHour := now.Add(time.Hour)
		if i == 1 {
			weekly = now.Add(24 * time.Hour)
			fiveHour = now.Add(4 * time.Hour)
		}
		candidate.Quota.ObserveResponseHeadersForProvider("claude", http.Header{
			"Anthropic-Ratelimit-Unified-7d-Reset": {strconv.FormatInt(weekly.Unix(), 10)},
			"Anthropic-Ratelimit-Unified-5h-Reset": {strconv.FormatInt(fiveHour.Unix(), 10)},
		}, now)
	}
	selector := newRoutingSelector(normalizedRoutingRuntimeState(&internalconfig.Config{
		Routing: internalconfig.RoutingConfig{Strategy: "reset-first"},
	}))
	for i := 0; i < 3; i++ {
		got, err := selector.Pick(context.Background(), "claude", "claude-opus", cliproxyexecutor.Options{}, auths)
		if err != nil {
			t.Fatal(err)
		}
		if got.ID != "b" {
			t.Fatalf("pick %d = %q, want b with earlier weekly reset despite later five-hour reset", i, got.ID)
		}
	}
}
