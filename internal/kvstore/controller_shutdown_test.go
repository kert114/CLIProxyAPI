package kvstore

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func TestShutdownLocalDeadlineWithActiveTransaction(t *testing.T) {
	resetControllerForTest(t)
	if err := ConfigureLocal(true, filepath.Join(t.TempDir(), "fixture.db")); err != nil {
		t.Fatal(err)
	}
	tx, err := localStore.db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- ShutdownLocalContext(ctx) }()
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("local store close returned %v while a transaction was active", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("local store close exceeded the deadline")
	}
}
