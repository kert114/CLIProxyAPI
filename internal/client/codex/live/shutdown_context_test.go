package live

import (
	"context"
	"testing"
	"time"
)

func TestHandlerShutdownDeadlineWithActiveSession(t *testing.T) {
	h := &Handler{sessions: newSessionStore()}
	session := h.sessions.put("call-held", liveSession{})
	started, release := make(chan struct{}), make(chan struct{})
	session.resources.add(func() error {
		close(started)
		<-release
		return nil
	})
	defer close(release)

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); h.CloseContext(ctx) }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("session cleanup did not start")
	}
	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("session cleanup exceeded the handler shutdown deadline")
	}
	if _, ok := h.sessions.peek("call-held"); ok {
		t.Fatal("handler retained the closing session")
	}
}
