package callbacks

import (
	"strconv"
	"strings"
)

// CompareSpeech compares an original completed transcript with frozen speech.
// Version 2 permits only deterministic, same-value readings of numeric suffixes
// of expected ASCII identifiers. It never normalizes arbitrary spoken numbers.
// prefix means incomplete evidence, not verified content.
func CompareSpeech(expected, observed string, version int) (matched, prefix bool) {
	if len(expected) > 24000 || len(observed) > 24000 {
		return false, false
	}
	want, got := SpokenKey(expected), SpokenKey(observed)
	if version != 2 {
		return want != "" && got == want, got != want && strings.HasPrefix(want, got)
	}
	parts := identifierSpeechParts(expected)
	states := map[int]bool{0: true}
	incomplete := false
	for _, options := range parts {
		next := map[int]bool{}
		for offset := range states {
			suffix := got[offset:]
			for _, option := range options {
				if strings.HasPrefix(suffix, option) {
					next[offset+len(option)] = true
				} else if strings.HasPrefix(option, suffix) {
					incomplete = true
				}
			}
		}
		states = next
	}
	matched = want != "" && states[len(got)]
	return matched, !matched && incomplete
}

func asciiLetter(c byte) bool       { return c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' }
func asciiDigit(c byte) bool        { return c >= '0' && c <= '9' }
func asciiAlphaNumeric(c byte) bool { return asciiLetter(c) || asciiDigit(c) }

func identifierSpeechParts(expected string) [][]string {
	var parts [][]string
	last, count := 0, 0
	literal := func(value string) {
		if key := SpokenKey(value); key != "" {
			parts = append(parts, []string{key})
		}
	}
	for start := 0; start < len(expected); {
		if !asciiAlphaNumeric(expected[start]) {
			start++
			continue
		}
		end := start + 1
		for end < len(expected) && asciiAlphaNumeric(expected[end]) {
			end++
		}
		digit := start
		for digit < end && asciiLetter(expected[digit]) {
			digit++
		}
		valid := digit > start && digit < end && digit-start <= 64 && end-digit <= 6
		for i := digit; i < end; i++ {
			valid = valid && asciiDigit(expected[i])
		}
		// Numbers in signed/decimal/version/fraction/percentage expressions are
		// not identifier suffixes. Do not turn those expressions into aliases.
		for _, adjacent := range []int{start - 1, end} {
			if adjacent < 0 || adjacent >= len(expected) {
				continue
			}
			c := expected[adjacent]
			if strings.ContainsRune("+-/%", rune(c)) {
				valid = false
			}
			if (c == '.' || c == ':') && (adjacent > 0 && asciiDigit(expected[adjacent-1]) || adjacent+1 < len(expected) && asciiDigit(expected[adjacent+1])) {
				valid = false
			}
		}
		if valid {
			count++
			if count > 64 {
				return [][]string{{SpokenKey(expected)}}
			}
			literal(expected[last:digit])
			digits := expected[digit:end]
			options := []string{digits}
			var spoken strings.Builder
			for _, d := range digits {
				spoken.WriteRune([]rune("零一二三四五六七八九")[d-'0'])
			}
			options = append(options, spoken.String())
			n, _ := strconv.Atoi(digits)
			if len(digits) == 1 || digits[0] != '0' {
				if n <= 9999 {
					cardinal := spokenInteger(n, true)
					if cardinal != spoken.String() {
						options = append(options, cardinal)
					}
				}
			}
			parts = append(parts, options)
			last = end
		}
		start = end
	}
	literal(expected[last:])
	return parts
}

// Render the exact expected integer, rather than parsing or rewriting actual
// Chinese speech. Leading-zero identifiers never use this cardinal rendering.
func spokenInteger(n int, leading bool) string {
	digits := []rune("零一二三四五六七八九")
	if n < 10 {
		return string(digits[n])
	}
	if n < 100 {
		prefix := string(digits[n/10])
		if n/10 == 1 && leading {
			prefix = ""
		}
		out := prefix + "十"
		if n%10 != 0 {
			out += string(digits[n%10])
		}
		return out
	}
	unit, label := 100, "百"
	if n >= 1000 {
		unit, label = 1000, "千"
	}
	out := string(digits[n/unit]) + label
	if remainder := n % unit; remainder != 0 {
		if remainder < unit/10 {
			out += "零"
		}
		out += spokenInteger(remainder, false)
	}
	return out
}
