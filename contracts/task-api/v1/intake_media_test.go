package taskcontract

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestMediaOnlyIntakeContractAllowsAttachmentAndRejectsEmptyInput(t *testing.T) {
	c, e := New()
	if e != nil {
		t.Fatal(e)
	}
	for _, kind := range []string{"image", "video", "none", "empty-arrays", "missing-goal", "oversized-images", "extra-caller"} {
		t.Run(kind, func(t *testing.T) {
			in := map[string]any{"action": "submit", "submission_request_id": "r_" + strings.Repeat("a", 32), "target": map[string]any{"instance_id": "i1", "session_id": "s1", "expected_native_thread_id": nil, "expected_config_revision": 0}, "goal": ""}
			switch kind {
			case "image":
				in["images"] = []any{map[string]any{"data": "AQID", "media_type": "image/png"}}
			case "video":
				in["files"] = []any{map[string]any{"attachment_id": "u_" + strings.Repeat("b", 32)}}
			case "empty-arrays":
				in["images"] = []any{}
				in["files"] = []any{}
			case "missing-goal":
				delete(in, "goal")
				in["images"] = []any{map[string]any{"data": "AQID", "media_type": "image/png"}}
			case "oversized-images":
				items := []any{}
				for i := 0; i < 9; i++ {
					items = append(items, map[string]any{"data": "AQID", "media_type": "image/png"})
				}
				in["images"] = items
			case "extra-caller":
				in["goal"] = "ordinary"
				in["caller"] = map[string]string{"kind": "human"}
			}
			raw, _ := json.Marshal(in)
			e := c.Validate(raw, "IntakeInput")
			want := kind == "image" || kind == "video"
			if (e == nil) != want {
				t.Fatal("contract validity", kind, e)
			}
		})
	}
	// A formal create_dispatch still requires an actual goal. Only intake gains media-only semantics.

	var vectors []struct {
		Name  string
		Value map[string]any
	}
	if e = json.Unmarshal(Golden, &vectors); e != nil {
		t.Fatal(e)
	}
	checked := false
	for _, v := range vectors {
		if v.Name == "request-create_dispatch" {
			in := v.Value["input"].(map[string]any)
			raw, _ := json.Marshal(in)
			if e = c.Validate(raw, "CreateInput"); e != nil {
				t.Fatal("original positive create fixture invalid", e)
			}
			in["goal"] = ""
			raw, _ = json.Marshal(in)
			if c.Validate(raw, "CreateInput") == nil {
				t.Fatal("formal create goal requirement weakened")
			}
			checked = true
		}
	}
	if !checked {
		t.Fatal("positive formal create fixture missing")
	}
}
