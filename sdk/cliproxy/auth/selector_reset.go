package auth

import (
	"context"
	"net/http"
	"strconv"
	"strings"
	"time"

	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
)

const (
	// resetFirstFiveHourLimit is the observed short-window utilization at which an account
	// stops receiving new work while another account in the same group has headroom.
	resetFirstFiveHourLimit = 0.8
	// subagentRequestMetadataKey marks requests that SessionAffinitySelector identified as subagents.
	subagentRequestMetadataKey = "cliproxy_routing_subagent"
	// SessionRoleHeader lets a client declare a delegated session as a subagent.
	SessionRoleHeader = "X-CLIProxy-Session-Role"
	// AttributeSubagentOnly marks a credential reserved for subagent traffic.
	AttributeSubagentOnly = "subagent_only"
)

// ResetFirstSelector consumes subscription accounts in long-window (weekly) reset
// order, then short-window (for example five-hour) reset order. It reads each
// account's last observed quota windows through observedQuotaWindows, so any
// provider with a window parser takes part. Accounts whose weekly reset is unknown,
// including providers without a parser, use round-robin so every account can acquire
// a snapshot through normal requests. Pools that include a provider without a parser
// keep plain round-robin.
// Before ordering, subagent_only accounts are reserved for subagent requests and
// accounts near their short-window limit are skipped while a peer has headroom.
// Wrap it in SessionAffinitySelector to keep existing conversations on their account.
type ResetFirstSelector struct {
	fallback RoundRobinSelector
}

func (s *ResetFirstSelector) Pick(ctx context.Context, provider, model string, opts cliproxyexecutor.Options, auths []*Auth) (*Auth, error) {
	provider = strings.ToLower(strings.TrimSpace(provider))
	if provider != "mixed" && !supportsQuotaWindows(provider) {
		return s.fallback.Pick(ctx, provider, model, opts, auths)
	}
	now := time.Now()
	available, err := getSelectorAvailableAuths(ctx, auths, provider, model, now)
	if err != nil {
		return nil, err
	}
	for _, candidate := range available {
		if !supportsQuotaWindows(candidate.Provider) {
			return s.fallback.Pick(ctx, provider, model, opts, available)
		}
	}
	available = preferCodexWebsocketAuths(ctx, poolProvider(provider, available), available)
	full, _ := opts.Metadata[subagentFullAuthsMetadataKey].(map[string]bool)
	available = resetFirstCandidates(available, isSubagentRequest(opts), full, now)
	var selected *Auth
	var selectedWindows quotaWindows
	for _, candidate := range available {
		windows := observedQuotaWindows(candidate, now)
		if windows.longReset.IsZero() {
			return s.fallback.Pick(ctx, provider, model, opts, available)
		}
		if selected == nil || windows.longReset.Before(selectedWindows.longReset) ||
			(windows.longReset.Equal(selectedWindows.longReset) && !windows.shortReset.IsZero() &&
				(selectedWindows.shortReset.IsZero() || windows.shortReset.Before(selectedWindows.shortReset))) {
			selected, selectedWindows = candidate, windows
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
		if observedQuotaWindows(candidate, now).shortUsed < resetFirstFiveHourLimit {
			out = append(out, candidate)
		}
	}
	return out
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

// poolProvider returns the single provider behind a "mixed" pool, so provider-specific
// preferences such as the Codex websocket transport still apply when only one provider
// is configured. Pools with several providers keep "mixed".
func poolProvider(provider string, auths []*Auth) string {
	if provider != "mixed" || len(auths) == 0 {
		return provider
	}
	only := strings.ToLower(strings.TrimSpace(auths[0].Provider))
	for _, candidate := range auths[1:] {
		if !strings.EqualFold(strings.TrimSpace(candidate.Provider), only) {
			return provider
		}
	}
	return only
}
