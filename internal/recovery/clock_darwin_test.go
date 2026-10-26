//go:build darwin

package recovery

import "testing"

func TestSystemContinuousClockIsComparableWithoutExposingBootIdentity(t *testing.T) {
	a, e := SystemClock()
	if e != nil || !a.Valid() {
		t.Fatal("qualified system clock unavailable", e)
	}
	b, e := SystemClock()
	if e != nil {
		t.Fatal(e)
	}
	if _, e = a.Time(b); e != nil {
		t.Fatal("same process system samples incomparable", e)
	}
	if a.Kind != "darwin_continuous_v1" {
		t.Fatal("unexpected clock kind")
	}
}
