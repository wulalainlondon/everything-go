package search

import (
	"regexp"
	"strings"
)

// Port of bridge/search/_strip_wrappers.py and ingest/display_name.py: remove
// framework-injected XML wrapper blocks and detect framework-noise user lines so
// they neither pollute the FTS index nor become a session display name.

var (
	wrapperClosed = regexp.MustCompile(`(?is)<(system-reminder|local-command-stdout|local-command-stderr|command-name|command-message|command-args|command-output|command-stdout|command-stderr|environment_details|environment_context|local-command-caveat|turn_aborted)>.*?</[^>]+>`)

	// Unclosed wrapper that runs to end-of-block: an opening tag line plus all
	// following non-blank lines. Approximates the Python MULTILINE form.
	wrapperOpenLines = regexp.MustCompile(`(?im)^<(?:system-reminder|local-command-stdout|local-command-stderr|command-name|command-output|local-command-caveat|environment_context|turn_aborted)[^\n]*(?:\n(?:[^\n].*)?)*`)

	manyNewlines = regexp.MustCompile(`\n{3,}`)
)

// stripFrameworkWrappers removes framework wrapper blocks (closed and unclosed)
// and collapses runs of blank lines. Never panics.
func stripFrameworkWrappers(text string) string {
	if text == "" {
		return text
	}
	out := wrapperClosed.ReplaceAllString(text, "")
	out = wrapperOpenLines.ReplaceAllString(out, "")
	out = manyNewlines.ReplaceAllString(out, "\n\n")
	return strings.TrimSpace(out)
}

var noisePrefixes = []*regexp.Regexp{
	regexp.MustCompile(`(?i)^<local-command-caveat>`),
	regexp.MustCompile(`(?i)^This session is being continued from`),
	regexp.MustCompile(`(?i)^#\s*AGENTS\.md\b`),
	regexp.MustCompile(`(?i)^<environment_context>`),
	regexp.MustCompile(`(?i)^<command-name>`),
	regexp.MustCompile(`(?i)^<command-output>`),
	regexp.MustCompile(`(?i)^<local-command-stdout>`),
	regexp.MustCompile(`(?i)^<local-command-stderr>`),
	regexp.MustCompile(`(?i)^<system-reminder>`),
	regexp.MustCompile(`(?i)^Caveat:`),
}

var slashCommands = map[string]bool{
	"/clear": true, "/exit": true, "/compact": true, "/init": true,
	"/help": true, "/quit": true, "/cost": true, "/model": true,
	"/status": true, "/login": true, "/logout": true,
}

// isFrameworkNoise reports whether text looks framework-generated rather than
// user-typed, so it is skipped when choosing a session display name.
func isFrameworkNoise(text string) bool {
	s := strings.TrimSpace(text)
	if s == "" {
		return true
	}
	for _, re := range noisePrefixes {
		if re.MatchString(s) {
			return true
		}
	}
	firstLine := s
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		firstLine = s[:i]
	}
	return slashCommands[strings.TrimSpace(firstLine)]
}

// collapseWS collapses internal whitespace/newlines to single spaces and caps
// the length, matching Python's `' '.join(text.split())[:max]`.
func collapseWS(text string, max int) string {
	collapsed := strings.Join(strings.Fields(text), " ")
	if len([]rune(collapsed)) > max {
		return string([]rune(collapsed)[:max])
	}
	return collapsed
}
