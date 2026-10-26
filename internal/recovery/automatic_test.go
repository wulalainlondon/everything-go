package recovery

import (
	"testing"
	"time"
)

func TestAutomaticDelayBudgetsJitterAndRetryAfter(t *testing.T) {
	for i, s := range []int{10, 30, 60, 120, 300} {
		for _, j := range []float64{-1, -.2, 0, .2, 1} {
			d := Delay(i+1, j, 0)
			if d < time.Duration(float64(s)*.8*float64(time.Second)) || d > time.Duration(float64(s)*1.2*float64(time.Second)) {
				t.Fatal(i, j, d)
			}
		}
	}
	if d := Delay(1, -.2, time.Minute); d != time.Minute {
		t.Fatal(d)
	}
}
