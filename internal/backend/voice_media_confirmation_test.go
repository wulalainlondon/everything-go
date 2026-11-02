package backend

import (
	"encoding/json"
	"testing"
)

func TestMusicConfirmationDisplayLabelRemainsSeparateFromExactTitle(t *testing.T) {
	var q VoiceMediaRequest
	if e := json.Unmarshal([]byte(`{"url":"https://music.youtube.com/watch?v=yKNxeF4KMsY","title":"Yellow","artist":"Coldplay","display_label":"Coldplay - Yellow"}`), &q); e != nil {
		t.Fatal(e)
	}
	if q.Title != "Yellow" || q.Artist != "Coldplay" || q.DisplayLabel != "Coldplay - Yellow" {
		t.Fatal(q)
	}
	raw, e := json.Marshal(q)
	if e != nil {
		t.Fatal(e)
	}
	var fields map[string]any
	json.Unmarshal(raw, &fields)
	if fields["title"] != "Yellow" || fields["artist"] != "Coldplay" || fields["display_label"] != "Coldplay - Yellow" {
		t.Fatal(fields)
	}
	var old VoiceMediaRequest
	if e = json.Unmarshal([]byte(`{"title":"Coldplay - Yellow","artist":""}`), &old); e != nil || old.DisplayLabel != "" || old.Title != "Coldplay - Yellow" {
		t.Fatal("legacy title was rewritten", old, e)
	}
}
