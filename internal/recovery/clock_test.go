package recovery

import (
	"errors"
	"testing"
)

func TestContinuousClockPreservesAdmissionEpochAcrossWallChangeAndRestart(t *testing.T) {
	a := ClockAnchor{Boot: "boot", Kind: "continuous", WallMS: 1700000000000, ElapsedMS: 1000}
	for _, tc := range []struct {
		name                string
		elapsed, wall, want int64
	}{
		{"rollback", 31 * 60000, -10 * 60000, 31 * 60000},
		{"sleep-same-boot-restart", 31 * 60000, 21 * 60000, 31 * 60000},
		{"forward-does-not-accelerate", 60000, 3600000, 60000},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := a
			s.ElapsedMS += tc.elapsed
			s.WallMS += tc.wall
			n, e := a.Time(s)
			if e != nil || n != a.ElapsedMS+tc.want {
				t.Fatal(n, e)
			}
		})
	}
}

func TestContinuousClockUnknownBootDomainOrRegressionNeverFallsBackToWall(t *testing.T) {
	a := ClockAnchor{Boot: "boot", Kind: "continuous", WallMS: 1700000000000, ElapsedMS: 1000}
	for _, kind := range []string{"boot", "domain", "regression", "missing"} {
		t.Run(kind, func(t *testing.T) {
			s := a
			s.WallMS += 3600000
			switch kind {
			case "boot":
				s.Boot = "new"
			case "domain":
				s.Kind = "other"
			case "regression":
				s.ElapsedMS--
			case "missing":
				s = ClockAnchor{}
			}
			if _, e := a.Time(s); !errors.Is(e, ErrClock) {
				t.Fatal(e)
			}
		})
	}
}

func TestContinuousClockPublicUTCProjectionDoesNotDriveScheduling(t *testing.T) {
	a := ClockAnchor{Boot: "b", Kind: "continuous", WallMS: 1700000000000, ElapsedMS: 1000}
	if a.PublicWall(11000) != a.WallMS+10000 || a.PublicWall(0) != 0 {
		t.Fatal("display projection lost UTC provenance")
	}
	b := a
	b.WallMS += 120000
	b.ElapsedMS++
	now, e := a.Time(b)
	if e != nil || now != 1001 {
		t.Fatal("wall accelerated private scheduler", now, e)
	}
}
