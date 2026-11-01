package agentactivity

import "testing"

func TestNativeLogicalRoundSurvivesReconnectButObservationProofChanges(t *testing.T) {
	a := nativeTestScope
	b := a
	b.Epoch = "new-boot:2:1"
	old, e := a.LogicalExecution("child", "actual-native-turn")
	if e != nil {
		t.Fatal(e)
	}
	next, e := b.LogicalExecution("child", "actual-native-turn")
	if e != nil || old != next || a == b {
		t.Fatal("reconnect invented a new execution")
	}
	b.Authority = "foreign"
	foreign, _ := b.LogicalExecution("child", "actual-native-turn")
	if foreign == old {
		t.Fatal("authority boundary erased")
	}
	other, _ := a.LogicalExecution("child", "real-next-turn")
	if other == old {
		t.Fatal("actual native next turn collapsed")
	}
	if _, e = a.LogicalExecution("child", ""); e == nil {
		t.Fatal("missing native turn invented a round")
	}
}
