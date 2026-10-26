package auth

import (
	"context"
	"net/http"
	"strconv"
	"strings"
	"time"

	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
)

// ResetFirstSelector consumes Claude accounts in weekly-reset order, then five-hour
// reset order. Missing weekly observations use round-robin so every account can
// acquire a snapshot through normal requests. Other providers retain round-robin.
// Wrap it in SessionAffinitySelector to keep existing conversations on their account.
type ResetFirstSelector struct {
	fallback RoundRobinSelector
}

func (s *ResetFirstSelector) Pick(ctx context.Context, provider, model string, opts cliproxyexecutor.Options, auths []*Auth) (*Auth, error) {
	provider = strings.ToLower(strings.TrimSpace(provider))
	if provider != "claude" && provider != "mixed" {
		return s.fallback.Pick(ctx, provider, model, opts, auths)
	}
	now := time.Now()
	available, err := getSelectorAvailableAuths(ctx, auths, provider, model, now)
	if err != nil {
		return nil, err
	}
	if provider == "mixed" {
		for _, candidate := range available {
			if !strings.EqualFold(strings.TrimSpace(candidate.Provider), "claude") {
				return s.fallback.Pick(ctx, provider, model, opts, available)
			}
		}
	}
	var selected *Auth
	var selectedWeekly, selectedFiveHour time.Time
	for _, candidate := range available {
		weekly := claudeResetTime(candidate.Quota.Signals, "7d", now)
		if weekly.IsZero() {
			return s.fallback.Pick(ctx, provider, model, opts, available)
		}
		fiveHour := claudeResetTime(candidate.Quota.Signals, "5h", now)
		if selected == nil || weekly.Before(selectedWeekly) ||
			(weekly.Equal(selectedWeekly) && !fiveHour.IsZero() && (selectedFiveHour.IsZero() || fiveHour.Before(selectedFiveHour))) {
			selected, selectedWeekly, selectedFiveHour = candidate, weekly, fiveHour
		}
	}
	return selected, nil
}

func claudeResetTime(signals map[string]string, window string, now time.Time) time.Time {
	raw := strings.TrimSpace(signals[http.CanonicalHeaderKey("Anthropic-Ratelimit-Unified-"+window+"-Reset")])
	var reset time.Time
	if seconds, errParse := strconv.ParseInt(raw, 10, 64); errParse == nil {
		reset = time.Unix(seconds, 0)
	} else {
		reset, _ = time.Parse(time.RFC3339, raw)
	}
	if !reset.After(now) {
		return time.Time{}
	}
	return reset
}
