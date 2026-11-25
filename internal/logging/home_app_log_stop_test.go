package logging

import (
	"context"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
)

type heldAppLogWrite struct {
	started chan struct{}
	release chan struct{}
	result  chan error
}

func (*heldAppLogWrite) HeartbeatOK() bool { return true }

func (w *heldAppLogWrite) RPushAppLog(ctx context.Context, _ []byte) error {
	close(w.started)
	select {
	case <-ctx.Done():
		w.result <- ctx.Err()
		return ctx.Err()
	case <-w.release:
		return nil
	}
}

func TestHomeAppLogForwarderStopCancelsActiveWrite(t *testing.T) {
	w := &heldAppLogWrite{started: make(chan struct{}), release: make(chan struct{}), result: make(chan error, 1)}
	f := StartHomeAppLogForwarder(1)
	f.bind(w)
	defer close(w.release)
	if err := f.Fire(log.NewEntry(log.StandardLogger())); err != nil {
		t.Fatal(err)
	}
	select {
	case <-w.started:
	case <-time.After(time.Second):
		t.Fatal("forwarder did not start the write")
	}
	done := make(chan struct{})
	go func() { defer close(done); f.Stop() }()
	select {
	case <-done:
		if err := <-w.result; err != context.Canceled {
			t.Fatalf("writer context error = %v, want context.Canceled", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Stop waited for the write instead of canceling it")
	}
}
