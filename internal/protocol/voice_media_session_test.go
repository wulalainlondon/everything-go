package protocol

import (
	"encoding/json"
	"testing"
)

func TestMediaSessionWireRequiresActualCountersAndAge(t *testing.T) {
	for _, s := range []string{`{}`, `{"revision":0,"version":1}`, `{"revision":0,"version":1,"observationAgeMs":null}`, `{"revision":null,"version":1,"observationAgeMs":0}`, `{"revision":0,"version":1,"observationAgeMs":0.5}`} {
		var v MediaSessionState
		if json.Unmarshal([]byte(s), &v) == nil {
			t.Fatal("unknown became fresh zero", s)
		}
	}
	var v MediaSessionState
	if e := json.Unmarshal([]byte(`{"revision":0,"version":1,"observationAgeMs":-1}`), &v); e != nil || v.ObservationAgeMS != -1 {
		t.Fatal(v, e)
	}
}
