package shutdown

// RunDetached runs a shutdown callback with no caller left to receive a panic.
func RunDetached(callback func()) {
	go func() {
		defer func() { _ = recover() }()
		callback()
	}()
}
