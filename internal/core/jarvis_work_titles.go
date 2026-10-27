package core

import (
	"everything-go/internal/messagequeue"
	"github.com/rivo/uniseg"
	"strings"
)

// A work title is a display label, never a substitute for the original prompt.
// The mobile contract counts UTF-16 units; a grapheme may contain many runes.
func jarvisDerivedWorkTitle(value, fallback string) string {
	value = strings.TrimSpace(value)
	g := uniseg.NewGraphemes(value)
	var out strings.Builder
	units := 0
	for n := 0; n < 100 && g.Next(); n++ {
		cluster := g.Str()
		width := 0
		for _, r := range cluster {
			width++
			if r > 0xffff {
				width++
			}
		}
		if units+width > 200 {
			break
		}
		units += width
		out.WriteString(cluster)
	}
	if strings.TrimSpace(out.String()) == "" {
		return fallback
	}
	return out.String()
}
func jarvisIntakeWorkTitle(goal string, input intakeInput) string {
	fallback := "未命名工作"
	if len(input.Images) > 0 || len(input.Files) > 0 {
		fallback = "附件工作"
	}
	return jarvisDerivedWorkTitle(goal, fallback)
}

// Legacy metadata remains durable and unchanged. If its raw title violates the
// current wire contract, expose an explicitly marked read-only display label;
// retain the exact original title digest/body/revision rather than enable saving
// the shorter label over the original metadata.
func jarvisWorkTitlePresentation(item messagequeue.WorkItem) messagequeue.WorkItem {
	units := 0
	for _, r := range item.Title {
		units++
		if r > 0xffff {
			units++
		}
	}
	if strings.TrimSpace(item.Title) == "" || units > 200 {
		original := item.Title
		fallback := "未命名事項"
		if item.Kind == "execution" {
			fallback = "未命名工作"
		}
		item.Title = jarvisDerivedWorkTitle(original, fallback)
		item.TitleTruncated = true
		item.TitleHash = jarvisTextHash(original)
		item.DetailTruncated = true
	}
	return item
}
