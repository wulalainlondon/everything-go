// Package musicidentity checks a bounded native metadata proof, never fuzzy search.
package musicidentity

import (
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
	"golang.org/x/text/unicode/norm"
	"regexp"
	"strings"
	"unicode"
)

var presentation = regexp.MustCompile(`(?i)\s*(?:\((?:official (?:music )?video|official audio)\)|\[(?:official (?:music )?video|official audio)\])\s*$`)
var separator = regexp.MustCompile(`(?i)\s+(?:&|和|and|·)\s+|\s*、\s*|\s*,\s*`)

func normalized(s string) string {
	s = cases.Lower(language.Und).String(norm.NFKC.String(s))
	return strings.Map(func(r rune) rune {
		if unicode.IsPunct(r) || unicode.IsSymbol(r) || unicode.IsSpace(r) {
			return -1
		}
		return r
	}, s)
}
func title(s string) string {
	return normalized(presentation.ReplaceAllString(strings.TrimSpace(s), ""))
}
func credits(s string) map[string]bool {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	out := map[string]bool{}
	for _, part := range separator.Split(strings.TrimSpace(s), -1) {
		key := normalized(part)
		if key == "" || out[key] {
			return nil
		}
		out[key] = true
	}
	return out
}
func MatchMetadata(expectedTitle, expectedArtist, actualTitle, actualArtist string) bool {
	e, a := credits(expectedArtist), credits(actualArtist)
	if title(expectedTitle) == "" || title(expectedTitle) != title(actualTitle) || e == nil || a == nil || len(e) != len(a) {
		return false
	}
	for k := range e {
		if !a[k] {
			return false
		}
	}
	return true
}
func MetadataChanged(beforeTitle, beforeArtist, actualTitle, actualArtist string) bool {
	return credits(beforeArtist) != nil && credits(actualArtist) != nil && title(beforeTitle) != "" && title(actualTitle) != "" && !MatchMetadata(beforeTitle, beforeArtist, actualTitle, actualArtist)
}
