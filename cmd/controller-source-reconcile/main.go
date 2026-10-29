// An operator-only offline maintenance writer. The reviewed coordinator must
// prove the fresh authenticated scope and stop the original host before use.
// This is not a callable native tool or an authority/grant setter.
package main

import (
	"everything-go/internal/session"
	"flag"
	"fmt"
	"os"
)

func main() {
	store := flag.String("session-store", "", "original private store")
	digest := flag.String("expected-sha256", "", "exact after-stop store digest")
	sid := flag.String("session-id", "", "original source")
	thread := flag.String("thread-id", "", "original source thread")
	cfg := flag.Uint64("configuration", 0, "original configuration")
	generation := flag.Uint64("observed-generation", 0, "fresh authenticated scope generation")
	flag.Parse()
	if flag.NArg() != 0 || *store == "" {
		fmt.Fprintln(os.Stderr, "invalid operator reconciliation arguments")
		os.Exit(2)
	}
	if err := session.NewStore(*store).BackfillMissingSourceRevision(*digest, *sid, *thread, *cfg, *generation); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println("ORIGINAL_MISSING_SOURCE_REVISION_RECONCILED_WITH_NORMAL_STORE_WRITER")
}
