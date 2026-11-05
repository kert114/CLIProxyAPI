package auth

import (
	"context"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
)

const (
	// resetFirstFiveHourLimit is the observed five-hour utilization at which an account
	// stops receiving new work while another account in the same group has headroom.
	resetFirstFiveHourLimit = 0.8
	// subagentRequestMetadataKey marks requests that SessionAffinitySelector identified as subagents.
	subagentRequestMetadataKey = "cliproxy_routing_subagent"
	// SessionRoleHeader lets a client declare a delegated session as a subagent.
	SessionRoleHeader = "X-CLIProxy-Session-Role"
	// AttributeSubagentOnly marks a credential reserved for subagent traffic.
	AttributeSubagentOnly = "subagent_only"
)

// ResetFirstSelector consumes Claude accounts in weekly-reset order, then five-hour
// reset order. Missing weekly observations use round-robin so every account can
// acquire a snapshot through normal requests. Other providers retain round-robin.
// Before ordering, subagent_only accounts are reserved for subagent requests and
// accounts near their five-hour limit are skipped while a peer has headroom.
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
	full, _ := opts.Metadata[subagentFullAuthsMetadataKey].(map[string]bool)
	available = resetFirstCandidates(available, isSubagentRequest(opts), full, now)
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

// resetFirstCandidates narrows available accounts to the first non-empty tier.
// Subagents try subagent_only accounts, then the rest; top-level requests try the
// rest, then subagent_only accounts. Within each group, accounts with five-hour
// headroom come first. Subagents move to any account with headroom before
// pushing a busy subagent_only account into its five-hour limit, and use a
// subagent_only account that reached its session cap only as a last resort.
func resetFirstCandidates(auths []*Auth, subagent bool, full map[string]bool, now time.Time) []*Auth {
	var preferred, other, capped []*Auth
	for _, candidate := range auths {
		if subagent && full[candidate.ID] {
			capped = append(capped, candidate)
		} else if authSubagentOnly(candidate) == subagent {
			preferred = append(preferred, candidate)
		} else {
			other = append(other, candidate)
		}
	}
	tiers := [][]*Auth{withFiveHourHeadroom(preferred, now), preferred, withFiveHourHeadroom(other, now), other}
	if subagent {
		tiers = [][]*Auth{withFiveHourHeadroom(preferred, now), withFiveHourHeadroom(other, now), preferred, other, capped}
	}
	for _, tier := range tiers {
		if len(tier) > 0 {
			return tier
		}
	}
	return auths
}

func withFiveHourHeadroom(auths []*Auth, now time.Time) []*Auth {
	var out []*Auth
	for _, candidate := range auths {
		if claudeFiveHourUtilization(candidate.Quota.Signals, now) < resetFirstFiveHourLimit {
			out = append(out, candidate)
		}
	}
	return out
}

// claudeFiveHourUtilization returns the last observed five-hour utilization (0-1),
// or 0 when the observation is missing or its window has already reset.
func claudeFiveHourUtilization(signals map[string]string, now time.Time) float64 {
	if claudeResetTime(signals, "5h", now).IsZero() {
		return 0
	}
	raw := strings.TrimSpace(signals[http.CanonicalHeaderKey("Anthropic-Ratelimit-Unified-5h-Utilization")])
	utilization, errParse := strconv.ParseFloat(raw, 64)
	if errParse != nil || math.IsNaN(utilization) || math.IsInf(utilization, 0) {
		return 0
	}
	return utilization
}

func authSubagentOnly(auth *Auth) bool {
	if auth == nil {
		return false
	}
	if raw, ok := auth.Attributes[AttributeSubagentOnly]; ok {
		enabled, _ := strconv.ParseBool(strings.TrimSpace(raw))
		return enabled
	}
	switch value := auth.Metadata[AttributeSubagentOnly].(type) {
	case bool:
		return value
	case string:
		enabled, _ := strconv.ParseBool(strings.TrimSpace(value))
		return enabled
	}
	return false
}

func isSubagentRequest(opts cliproxyexecutor.Options) bool {
	if flagged, ok := opts.Metadata[subagentRequestMetadataKey].(bool); ok && flagged {
		return true
	}
	return hasSubagentRoleHeader(opts.Headers)
}

func hasSubagentRoleHeader(headers http.Header) bool {
	return strings.EqualFold(strings.TrimSpace(headers.Get(SessionRoleHeader)), "subagent")
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
