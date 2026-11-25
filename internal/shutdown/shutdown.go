// Package shutdown bounds waits for teardown operations.
package shutdown

import "context"

// Wait waits for completion or for the caller's context to end.
func Wait(ctx context.Context, done <-chan struct{}) error {
	select {
	case <-done:
		return nil
	default:
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Run starts a cleanup operation whose API cannot cancel a call in progress.
// If the caller stops waiting, the operation remains responsible for cleanup.
func Run(ctx context.Context, cleanup func() error) error {
	if ctx == nil || ctx.Done() == nil {
		return cleanup()
	}
	result := make(chan error, 1)
	go func() { result <- cleanup() }()
	select {
	case err := <-result:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}
