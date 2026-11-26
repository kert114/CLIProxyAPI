package auth

import (
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// quotaWindows is the provider-neutral view of an account's last observed
// subscription windows. Zero times mean the window is unknown or already reset.
type quotaWindows struct {
	// longReset is when the longest window (usually weekly) resets.
	longReset time.Time
	// shortReset is when a shorter window (for example five hours) resets.
	shortReset time.Time
	// shortUsed is the short window's utilization from 0 to 1. It is 0 when unknown.
	shortUsed float64
}

// observedQuotaWindows reads the account's passive quota snapshot using the parser
// for its provider. To route a new subscription provider by reset time, add a case
// that maps its observed signals to quotaWindows. Providers without a parser report
// unknown windows and are routed round-robin.
func observedQuotaWindows(auth *Auth, now time.Time) quotaWindows {
	if auth == nil || len(auth.Quota.Signals) == 0 {
		return quotaWindows{}
	}
	switch strings.ToLower(strings.TrimSpace(auth.Provider)) {
	case "claude":
		return claudeQuotaWindows(auth.Quota.Signals, now)
	case "codex":
		return codexQuotaWindows(auth.Quota, now)
	case "devin":
		return devinQuotaWindows(auth.Quota.Signals, now)
	default:
		return quotaWindows{}
	}
}

func claudeQuotaWindows(signals map[string]string, now time.Time) quotaWindows {
	header := func(name string) string {
		return strings.TrimSpace(signals[http.CanonicalHeaderKey("Anthropic-Ratelimit-Unified-"+name)])
	}
	windows := quotaWindows{
		longReset:  futureTime(parseResetTime(header("7d-Reset")), now),
		shortReset: futureTime(parseResetTime(header("5h-Reset")), now),
	}
	if !windows.shortReset.IsZero() {
		windows.shortUsed = parseFraction(header("5h-Utilization"), 1)
	}
	return windows
}

// codexQuotaWindows reads the base X-Codex-Primary-* and X-Codex-Secondary-*
// windows. Their lengths vary by plan (a Pro plan may report only a weekly window),
// so the longest Window-Minutes value orders accounts and a shorter one feeds the
// short-window guard. Namespaced additional limits are ignored.
func codexQuotaWindows(quota QuotaState, now time.Time) quotaWindows {
	type window struct {
		minutes int64
		reset   time.Time
		used    float64
	}
	var observed []window
	for _, name := range []string{"Primary", "Secondary"} {
		header := func(suffix string) string {
			return strings.TrimSpace(quota.Signals[http.CanonicalHeaderKey("X-Codex-"+name+"-"+suffix)])
		}
		minutes, errParse := strconv.ParseInt(header("Window-Minutes"), 10, 64)
		if errParse != nil || minutes <= 0 {
			continue
		}
		reset := parseResetTime(header("Reset-At"))
		if reset.IsZero() && !quota.ObservedAt.IsZero() {
			if seconds, errAfter := strconv.ParseInt(header("Reset-After-Seconds"), 10, 64); errAfter == nil && seconds >= 0 && seconds <= math.MaxInt32 {
				reset = quota.ObservedAt.Add(time.Duration(seconds) * time.Second)
			}
		}
		observed = append(observed, window{minutes: minutes, reset: futureTime(reset, now), used: parseFraction(header("Used-Percent"), 100)})
	}
	var windows quotaWindows
	var long, short *window
	for i := range observed {
		if long == nil || observed[i].minutes > long.minutes {
			long = &observed[i]
		}
	}
	for i := range observed {
		if &observed[i] != long && observed[i].minutes < long.minutes {
			short = &observed[i]
		}
	}
	if long != nil {
		windows.longReset = long.reset
	}
	if short != nil && !short.reset.IsZero() {
		windows.shortReset, windows.shortUsed = short.reset, short.used
	}
	return windows
}

func devinQuotaWindows(signals map[string]string, now time.Time) quotaWindows {
	windows := quotaWindows{
		longReset:  futureTime(parseResetTime(signals["weekly_quota_reset_at"]), now),
		shortReset: futureTime(parseResetTime(signals["daily_quota_reset_at"]), now),
	}
	if !windows.shortReset.IsZero() {
		raw := strings.TrimSuffix(strings.TrimSpace(signals["daily_quota_remaining_percent"]), "%")
		if remaining, errParse := strconv.ParseFloat(raw, 64); errParse == nil && remaining >= 0 && remaining <= 100 {
			windows.shortUsed = 1 - remaining/100
		}
	}
	return windows
}

// parseResetTime accepts Unix seconds or RFC 3339 and returns zero when invalid.
func parseResetTime(raw string) time.Time {
	raw = strings.TrimSpace(raw)
	if seconds, errParse := strconv.ParseInt(raw, 10, 64); errParse == nil {
		return time.Unix(seconds, 0)
	}
	reset, _ := time.Parse(time.RFC3339, raw)
	return reset
}

func futureTime(t time.Time, now time.Time) time.Time {
	if !t.After(now) {
		return time.Time{}
	}
	return t
}

// parseFraction converts a utilization on the given scale to 0-1, or 0 when invalid.
func parseFraction(raw string, scale float64) float64 {
	value, errParse := strconv.ParseFloat(strings.TrimSpace(raw), 64)
	if errParse != nil || math.IsNaN(value) || math.IsInf(value, 0) {
		return 0
	}
	return value / scale
}
