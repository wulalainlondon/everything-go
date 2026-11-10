package goexec

// Preexisting fake executors must never use the constructor's production data
// directory. Durable tests explicitly assign t.TempDir(); other fakes use the
// per-executor in-memory receipts, isolated from each other and the live store.
func init() { codexDefaultDataDir = func(string) string { return "" } }
