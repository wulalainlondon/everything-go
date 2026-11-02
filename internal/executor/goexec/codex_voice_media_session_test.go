package goexec

import (
	"everything-go/internal/backend"
	"everything-go/internal/session"
	"testing"
)

func TestMediaSessionCatalogAdvertisesSourcesWithoutCallerOrAdoptionDefaults(t *testing.T) {
	c := &Codex{}
	c.SetSessionControlProvider(mediaCatalogProvider{})
	s := session.NewRegistry().Create("s_catalog", "catalog", t.TempDir(), backend.Codex, "", "", "")
	p := map[string]any{}
	c.applyVoiceMediaTools(s, p)
	tools := p["dynamicTools"].([]map[string]any)[0]["tools"].([]map[string]any)
	by := map[string]map[string]any{}
	for _, v := range tools {
		by[v["name"].(string)] = v
	}
	for _, name := range []string{"play_music", "control", "music_control", "status"} {
		if by[name] == nil {
			t.Fatal("missing source capability", name)
		}
	}
	if by["play_youtube"] != nil {
		t.Fatal("retired source advertised")
	}
	props := by["control"]["inputSchema"].(map[string]any)["properties"].(map[string]any)
	for _, name := range []string{"source", "position_ms", "expected_revision", "adopt_current", "expected_media_id"} {
		if props[name] == nil {
			t.Fatal("missing bounded field", name)
		}
	}
	if props["adopt_current"].(map[string]any)["default"] != nil {
		t.Fatal("implicit adoption")
	}
	music := by["music_control"]["inputSchema"].(map[string]any)["properties"].(map[string]any)["action"].(map[string]any)["enum"].([]string)
	for _, action := range music {
		if action == "volume" {
			t.Fatal("advertised unsupported native Music gain")
		}
	}
	status := by["status"]["inputSchema"].(map[string]any)["properties"].(map[string]any)
	if status["operation"] == nil {
		t.Fatal("cannot observe original operation")
	}
}
