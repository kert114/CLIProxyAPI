package auth

import (
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	// defaultMaxSubagentSessions caps concurrently active subagent sessions on a subagent_only account.
	defaultMaxSubagentSessions = 3
	// subagentActiveWindow is how recently a subagent session must have made a request to count as active.
	subagentActiveWindow = 5 * time.Minute
	// AttributeMaxSubagentSessions overrides defaultMaxSubagentSessions for one credential.
	AttributeMaxSubagentSessions = "max_subagent_sessions"
	// subagentFullAuthsMetadataKey carries subagent_only account IDs that reached their session cap.
	subagentFullAuthsMetadataKey = "cliproxy_routing_subagent_full_auths"
	// subagentSessionMetadataKey carries the session key recorded after a subagent pick.
	subagentSessionMetadataKey = "cliproxy_routing_subagent_session"
)

type subagentSeen struct {
	authID string
	at     time.Time
}

// subagentLoad tracks which account each recently active subagent session uses.
type subagentLoad struct {
	mu       sync.Mutex
	sessions map[string]subagentSeen
}

func (l *subagentLoad) record(session, authID string, now time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.sessions == nil {
		l.sessions = make(map[string]subagentSeen)
	}
	for key, seen := range l.sessions {
		if now.Sub(seen.at) > subagentActiveWindow {
			delete(l.sessions, key)
		}
	}
	l.sessions[session] = subagentSeen{authID: authID, at: now}
}

// fullAuths returns subagent_only accounts whose other active sessions already reach their cap.
func (l *subagentLoad) fullAuths(auths []*Auth, self string, now time.Time) map[string]bool {
	l.mu.Lock()
	counts := make(map[string]int)
	for key, seen := range l.sessions {
		if key != self && now.Sub(seen.at) <= subagentActiveWindow {
			counts[seen.authID]++
		}
	}
	l.mu.Unlock()
	var full map[string]bool
	for _, candidate := range auths {
		if authSubagentOnly(candidate) && counts[candidate.ID] >= maxSubagentSessions(candidate) {
			if full == nil {
				full = make(map[string]bool)
			}
			full[candidate.ID] = true
		}
	}
	return full
}

// age shifts every recorded activity time into the past; used by tests.
func (l *subagentLoad) age(d time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for key, seen := range l.sessions {
		seen.at = seen.at.Add(-d)
		l.sessions[key] = seen
	}
}

func maxSubagentSessions(auth *Auth) int {
	raw := ""
	if auth.Attributes != nil {
		raw = strings.TrimSpace(auth.Attributes[AttributeMaxSubagentSessions])
	}
	if raw == "" {
		switch value := auth.Metadata[AttributeMaxSubagentSessions].(type) {
		case float64:
			if value >= 1 {
				return int(value)
			}
		case int:
			if value >= 1 {
				return value
			}
		case string:
			raw = strings.TrimSpace(value)
		}
	}
	if parsed, errParse := strconv.Atoi(raw); errParse == nil && parsed >= 1 {
		return parsed
	}
	return defaultMaxSubagentSessions
}
