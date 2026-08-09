package sourcepolicy

import "testing"

func TestIgnoreCodexSession(t *testing.T) {
	cwdGlobs := []string{"/private/tmp/**", "/tmp/evals"}
	prefixes := []string{"<recommended_plugins>", "[bridge-eval]"}
	cases := []struct {
		cwd, name string
		want      bool
	}{
		{"/private/tmp/run-1", "normal", true},
		{"/tmp/evals/nested", "normal", true},
		{"/Users/test/project", "<recommended_plugins>\nlist", true},
		{"/Users/test/project", "daily work", false},
	}
	for _, tc := range cases {
		if got := IgnoreCodexSession(tc.cwd, tc.name, cwdGlobs, prefixes); got != tc.want {
			t.Fatalf("IgnoreCodexSession(%q, %q)=%v want %v", tc.cwd, tc.name, got, tc.want)
		}
	}
}
