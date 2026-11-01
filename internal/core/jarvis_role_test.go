package core

import (
	"bytes"
	"context"
	"encoding/json"
	contract "everything-go/contracts/task-api/v1"
	"everything-go/internal/backend"
	"everything-go/internal/jarvisrole"
	"everything-go/internal/linebridge"
	"everything-go/internal/partners"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
	"everything-go/internal/sessiondispatch"
	"everything-go/internal/taskapi"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func roleFixture(t *testing.T) (*Hub, func(string, string, any, string) *httptest.ResponseRecorder, *session.Session, partners.Profile) {
	t.Helper()
	h, call := jarvisFixture(t)
	h.cfg.DataDir = t.TempDir()
	var e error
	h.jarvisRoles, e = jarvisrole.Open(t.TempDir(), h.cfg.InstanceID)
	if e != nil {
		t.Fatal(e)
	}
	h.partners, e = partners.Open(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	h.dispatches, e = sessiondispatch.Open(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { h.dispatches.Close() })
	h.dispatches.SetRoleGrantResolver(h.deriveJarvisGrant)
	s := h.registry.Create("s_role_source_original", "Source", h.cfg.RootDir, backend.Codex, "gpt-6.1-sol", "read-only", "native-role-source")
	p, e := h.partners.Save("human", "r_profile_original", 0, partners.Profile{SessionID: s.ID, NativeThreadID: s.ResumeID(), Name: "萬事通", Color: "mint"})
	if e != nil {
		t.Fatal(e)
	}
	return h, call, s, p
}
func roleAssignInput(h *Hub, s *session.Session, p partners.Profile, id string, rev uint64) jarvisRoleRequest {
	return jarvisRoleRequest{Authority: h.cfg.InstanceID, Operation: id, Expected: rev, Target: jarvisrole.Target{h.cfg.InstanceID, s.ID, s.ResumeID(), s.SettingsSnapshot().ConfigRevision}, Partner: p.ID, ProfileRevision: p.Revision}
}
func roleDecode(t *testing.T, w *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var v map[string]any
	if e := json.Unmarshal(w.Body.Bytes(), &v); e != nil {
		t.Fatal(w.Code, w.Body.String(), e)
	}
	return v
}
func TestJarvisRoleHumanAssignmentCandidatesCASAndNoBaseGrantMutation(t *testing.T) {
	h, call, s, p := roleFixture(t)
	ctx := context.Background()
	snap := roleDecode(t, call("POST", "role/snapshot", jarvisRoleRequest{Authority: h.cfg.InstanceID}, "phone"))
	if snap["assignment"] != nil || len(snap["candidates"].([]any)) != 1 {
		t.Fatal(snap)
	}
	in := roleAssignInput(h, s, p, "r_role_assign_original", 0)
	w := call("POST", "role/assign", in, "phone")
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	v := roleDecode(t, w)
	if v["operation"].(map[string]any)["phase"] != "committed" {
		t.Fatal(v)
	}
	base, _ := h.dispatches.BaseGrant(ctx, s.ID)
	effective, _ := h.dispatches.Grant(ctx, s.ID)
	if base.Enabled || !effective.Enabled || !effective.Allows(h.cfg.InstanceID, h.cfg.InstanceID, s.ID) || effective.RoleEpoch != 1 {
		t.Fatal(base, effective)
	}
	old := in
	old.Operation = "r_stale_device_original"
	if w = call("POST", "role/assign", old, "phone"); w.Code != 409 {
		t.Fatal(w.Code, w.Body.String())
	}
	in.Expected = 2
	if w = call("POST", "role/assign", in, "phone"); w.Code != 409 {
		t.Fatal("same key changed body", w.Code)
	}
	if w = call("POST", "role/operation", jarvisRoleRequest{Authority: h.cfg.InstanceID, Operation: in.Operation}, "phone"); w.Code != 200 {
		t.Fatal(w.Code)
	}
	if w = call("POST", "role/assign", old, "foreign"); w.Code != 403 {
		t.Fatal(w.Code)
	}
	revoke := jarvisRoleRequest{Authority: h.cfg.InstanceID, Operation: "r_role_revoke_original", Expected: 1}
	if w = call("POST", "role/revoke", revoke, "phone"); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	effective, _ = h.dispatches.Grant(ctx, s.ID)
	if effective.Enabled {
		t.Fatal("role retained", effective)
	}
	base2, _ := h.dispatches.BaseGrant(ctx, s.ID)
	if base2.Revision != base.Revision || base2.Enabled != base.Enabled {
		t.Fatal("base grants changed")
	}
}
func TestJarvisRoleProfileIdentityCosmeticChangesAndSourceExclusion(t *testing.T) {
	h, call, s, p := roleFixture(t)
	for _, id := range []string{"s_dg_child", "pm_managed"} {
		x := h.registry.Create(id, id, h.cfg.RootDir, backend.Codex, "", "read-only", "native")
		in := roleAssignInput(h, x, p, "r_forbidden_"+id, 0)
		if w := call("POST", "role/assign", in, "phone"); w.Code != 403 {
			t.Fatal(id, w.Code)
		}
	}
	in := roleAssignInput(h, s, p, "r_role_assign_original", 0)
	if w := call("POST", "role/assign", in, "phone"); w.Code != 200 {
		t.Fatal(w.Code)
	}
	p.Name = "新名稱"
	p2, e := h.partners.Save("human", "r_profile_rename", p.Revision, p)
	if e != nil {
		t.Fatal(e)
	}
	if h.SessionRoleEpoch(s) != 1 {
		t.Fatal("cosmetic edit revoked role")
	}
	snap := roleDecode(t, call("POST", "role/snapshot", jarvisRoleRequest{Authority: h.cfg.InstanceID}, "phone"))
	if snap["partner"].(map[string]any)["name"] != p2.Name {
		t.Fatal(snap)
	}
	p2.Hidden = true
	if _, e = h.partners.Save("human", "r_profile_hide", p2.Revision, p2); e != nil {
		t.Fatal(e)
	}
	if h.SessionRoleEpoch(s) != 0 {
		t.Fatal("hidden source retains role")
	}
}
func TestJarvisRoleNativeEpochGrantFencesAndOriginalResultAfterDemotion(t *testing.T) {
	h, call, s, p := roleFixture(t)
	in := roleAssignInput(h, s, p, "r_role_assign_original", 0)
	if w := call("POST", "role/assign", in, "phone"); w.Code != 200 {
		t.Fatal(w.Code)
	}
	target := h.registry.Create("s_role_target_original", "Target", h.cfg.RootDir, backend.Codex, "", "read-only", "native-role-target")
	c := newTestClient(h)
	entered := make(chan struct{})
	finish := make(chan struct{})
	defer close(finish)
	s.SubmitNamed("original-request", func() { close(entered); <-finish })
	<-entered
	defer s.EndTurn()
	_ = c
	caller := backend.SessionControlCaller{Parent: s, RequestID: "original-request", TurnID: "turn-original", ToolCallID: "call-original"}
	if _, e := h.ControlSession(context.Background(), caller, backend.SessionControlRequest{Action: "list_sessions"}); e == nil {
		t.Fatal("pre-assignment native turn inherited role")
	}
	caller.RoleEpoch = 1
	if _, e := h.ControlSession(context.Background(), caller, backend.SessionControlRequest{Action: "list_sessions"}); e != nil {
		t.Fatal(e)
	}
	g, _ := h.dispatches.Grant(backend.WithRoleEpoch(context.Background(), 0), s.ID)
	if g.Enabled {
		t.Fatal("stale task caller inherited")
	}
	r := sessiondispatch.Record{ID: "scd_role_original", RoleEpoch: 1, ParentID: s.ID, ParentThreadID: s.ResumeID(), ParentConfigRevision: s.SettingsSnapshot().ConfigRevision, ParentConfigKnown: true, InstanceID: h.cfg.InstanceID, SessionID: target.ID, ThreadID: target.ResumeID(), ConfigRevision: target.SettingsSnapshot().ConfigRevision, RequestID: "scjob_role_original", OriginRequestID: "request-original", ToolCallID: "original-call", State: "prepared", Mode: "queue", Content: "authorized original", IntentHash: "immutable-original-hash"}
	if _, e := h.dispatches.Create(context.Background(), r); e != nil {
		t.Fatal(e)
	}
	revoke := jarvisRoleRequest{Authority: h.cfg.InstanceID, Operation: "r_role_revoke_original", Expected: 1}
	if w := call("POST", "role/revoke", revoke, "phone"); w.Code != 200 {
		t.Fatal(w.Code)
	}
	if h.jarvisRoleDispatchCurrent(r) {
		t.Fatal("old epoch allowed")
	}
	r = h.submitLocalSessionDispatch(context.Background(), r)
	if r.State != "rejected" || r.Error != "jarvis_role_epoch_retired" {
		t.Fatal(r)
	}
	if _, e := h.ControlSession(context.Background(), caller, backend.SessionControlRequest{Action: "read_dispatch_result", DispatchID: r.ID}); e != nil {
		t.Fatal("original receipt unavailable", e)
	}
	if _, e := h.ControlSession(context.Background(), caller, backend.SessionControlRequest{Action: "get_session_status", SessionID: target.ID}); e == nil {
		t.Fatal("new read retained after revoke")
	}
}
func TestJarvisRoleNeverStartedResidencyAndProviderGenerationPin(t *testing.T) {
	h, call, s, p := roleFixture(t)
	s.SetResumeID("")
	p.NativeThreadID = ""
	var e error
	p, e = h.partners.Save("human", "r_profile_empty_thread", p.Revision, p)
	if e == nil {
		t.Fatal("existing profile thread was silently changed")
	}
	// A separate genuinely never-started source is eligible without creating a native thread.
	s = h.registry.Create("s_never_started_original", "Unstarted", h.cfg.RootDir, backend.Codex, "", "read-only", "")
	p, e = h.partners.Save("human", "r_profile_unstarted", 0, partners.Profile{SessionID: s.ID, Name: "未啟動", Color: "mint"})
	if e != nil {
		t.Fatal(e)
	}
	if w := call("POST", "role/assign", roleAssignInput(h, s, p, "r_role_assign_unstarted", 0), "phone"); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	a := h.currentJarvisRole()
	if a == nil {
		t.Fatal("missing assignment")
	}
	v := h.jarvisResidentSnapshot(a)
	if v.Actual != "pending" {
		t.Fatal(v)
	}
	s.SetResumeID("observed-native-original")
	h.Emit(protocol.NewSessionUUID(s.ID, s.ResumeID()))
	a = h.currentJarvisRole()
	if a == nil || a.Target.Thread != s.ResumeID() || a.Epoch != 1 {
		t.Fatal(a)
	}
	s.SetResumeID("unproven-other-thread")
	if h.currentJarvisRole() != nil {
		t.Fatal("unproven replacement accepted")
	}
}

func TestJarvisRoleDerivedDispatchExecutesOnceWithOriginalTargetPermissions(t *testing.T) {
	h, call, s, p := roleFixture(t)
	if w := call("POST", "role/assign", roleAssignInput(h, s, p, "r_role_assign_original", 0), "phone"); w.Code != 200 {
		t.Fatal(w.Code)
	}
	target := h.registry.Create("s_dispatch_target_original", "Target", h.cfg.RootDir, backend.Codex, "", "read-only", "original-target-native")
	client := newTestClient(h)
	h.voiceClients = map[*Client]*clientVoiceCall{client: {session: s, voiceID: "voice-original"}}
	caller := backend.SessionControlCaller{RoleEpoch: 1, Parent: s, RequestID: "original-voice-request", TurnID: "original-voice-turn", ToolCallID: "original-dispatch-call", VoiceID: "voice-original"}
	fe := h.exec.(*fakeExec)
	effects := make(chan string, 2)
	fe.onSend = func(target *session.Session, id, text string) {
		effects <- target.Snapshot().Sandbox
		fe.sink.Emit(protocol.NewDone(target.ID, id))
	}
	input := backend.SessionControlRequest{Action: "dispatch_to_session", SessionID: target.ID, ExpectedThreadID: target.ResumeID(), ExpectedConfigRevision: target.SettingsSnapshot().ConfigRevision, Content: "human authorized original", Mode: "queue"}
	out, e := h.ControlSession(context.Background(), caller, input)
	if e != nil {
		t.Fatal(e)
	}
	receipt := out.(sessiondispatch.Record)
	if receipt.RoleEpoch != 1 || receipt.Mode != "queue" {
		t.Fatal(receipt)
	}
	select {
	case sandbox := <-effects:
		if sandbox != "read-only" {
			t.Fatal("source permissions leaked", sandbox)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("never dispatched")
	}
	again, e := h.ControlSession(context.Background(), caller, input)
	if e != nil || again.(sessiondispatch.Record).ID != receipt.ID {
		t.Fatal(again, e)
	}
	select {
	case <-effects:
		t.Fatal("duplicate original effect")
	default:
	}
	base, _ := h.dispatches.BaseGrant(context.Background(), s.ID)
	if base.Enabled || base.Revision != 0 {
		t.Fatal("persisted a base grant", base)
	}
}
func TestJarvisRolePolicyAndCredentialChangeRetiresDerivedAuthorityWithoutRewritingBase(t *testing.T) {
	h, call, s, p := roleFixture(t)
	h.cfg.DataDir = t.TempDir()
	if w := call("POST", "role/assign", roleAssignInput(h, s, p, "r_role_assign_original", 0), "phone"); w.Code != 200 {
		t.Fatal(w.Code)
	}
	target := h.registry.Create("s_original_target", "Target", h.cfg.RootDir, backend.Codex, "", "read-only", "native-target")
	r := sessiondispatch.Record{RoleEpoch: 1, ParentID: s.ID, SessionID: target.ID, InstanceID: h.cfg.InstanceID, ThreadID: target.ResumeID()}
	if !h.jarvisRoleDispatchCurrent(r) {
		t.Fatal("current role denied")
	}
	if e := os.WriteFile(filepath.Join(h.cfg.DataDir, "jarvis-role-policy.json"), []byte(`{"excluded_sessions":["s_original_target"]}`), 0600); e != nil {
		t.Fatal(e)
	}
	if h.currentJarvisRole() != nil || h.jarvisRoleDispatchCurrent(r) {
		t.Fatal("policy change kept old authority")
	}
	// A new explicit human designation snapshots the changed operator policy.
	in := roleAssignInput(h, s, p, "r_role_reassign_policy", 1)
	if w := call("POST", "role/assign", in, "phone"); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	grant, _ := h.dispatches.Grant(context.Background(), s.ID)
	if grant.Allows(h.cfg.InstanceID, h.cfg.InstanceID, target.ID) {
		t.Fatal("excluded target granted")
	}
	for _, binding := range h.pairing.DeviceBindings() {
		if binding.DeviceID == "phone" {
			if e := h.pairing.Unclaim(binding.Token); e != nil {
				t.Fatal(e)
			}
		}
	}
	if h.currentJarvisRole() != nil {
		t.Fatal("revoked device kept assignment enabled")
	}
}
func TestJarvisRoleNativeWriterGuardRejectsRetiredQueueAndPreservesAcceptedOriginal(t *testing.T) {
	h, call, s, p := roleFixture(t)
	if w := call("POST", "role/assign", roleAssignInput(h, s, p, "r_role_assign_original", 0), "phone"); w.Code != 200 {
		t.Fatal(w.Code)
	}
	target := h.registry.Create("s_native_guard_target", "Target", h.cfg.RootDir, backend.Codex, "", "read-only", "native-target")
	r := sessiondispatch.Record{ID: "scd_guard_original", RoleEpoch: 1, ParentID: s.ID, ParentThreadID: s.ResumeID(), ParentConfigKnown: true, ParentConfigRevision: s.SettingsSnapshot().ConfigRevision, InstanceID: h.cfg.InstanceID, SessionID: target.ID, ThreadID: target.ResumeID(), ConfigRevision: target.SettingsSnapshot().ConfigRevision, RequestID: "scjob_guard_original", OriginRequestID: "request", ToolCallID: "tool", IntentHash: "immutable", State: "queued", Mode: "queue", Content: "original"}
	if _, e := h.dispatches.Create(context.Background(), r); e != nil {
		t.Fatal(e)
	}
	ctx := h.withJarvisDispatchGuard(context.Background(), target.ID, r.RequestID)
	if e := backend.CheckTaskInputGuard(ctx); e != nil {
		t.Fatal(e)
	}
	if w := call("POST", "role/revoke", jarvisRoleRequest{Authority: h.cfg.InstanceID, Operation: "r_revoke_guard_original", Expected: 1}, "phone"); w.Code != 200 {
		t.Fatal(w.Code)
	}
	if e := backend.CheckTaskInputGuard(ctx); e == nil {
		t.Fatal("retired source reached native writer")
	}
	if !h.jarvisRoleOriginalReceiptReadable(r) {
		t.Fatal("original result relation lost")
	}
}

func TestJarvisRoleFormalIntakeCapturesOriginalEpochAtNativeSend(t *testing.T) {
	h, call, s, p := roleFixture(t)
	if w := call("POST", "role/assign", roleAssignInput(h, s, p, "r_role_assign_original", 0), "phone"); w.Code != 200 {
		t.Fatal(w.Code)
	}
	client := attachmentClient(h, "phone")
	client.ctx = context.Background()
	client.readIdentity.Store(&pairedReadIdentity{token: "jarvis-token", deviceID: "phone"})
	bound, e := h.taskHuman(client)
	if e != nil {
		t.Fatal(e)
	}
	observed := make(chan uint64, 1)
	fe := h.exec.(*fakeExec)
	fe.onSendContext = func(ctx context.Context, s *session.Session, request, content string) {
		observed <- backend.RoleEpoch(ctx)
		fe.sink.Emit(protocol.NewDone(s.ID, request))
	}
	id := "r_" + strings.Repeat("a", 32)
	input, _ := json.Marshal(map[string]any{"action": "submit", "submission_request_id": id, "target": map[string]any{"instance_id": h.cfg.InstanceID, "session_id": s.ID, "expected_native_thread_id": s.ResumeID(), "expected_config_revision": s.SettingsSnapshot().ConfigRevision}, "goal": "人類明示的原工作"})
	raw, _ := json.Marshal(taskapi.Request{Type: "task_api_request", Version: contract.Version, CorrelationID: "role-intake-original", Operation: "intake", IdempotencyKey: id, Input: input})
	response := h.taskService.Execute(context.Background(), bound, raw)
	if !response.OK {
		t.Fatal(response.Error)
	}
	select {
	case epoch := <-observed:
		if epoch != 1 {
			t.Fatal("formal intake lost role witness", epoch)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("formal intake did not execute")
	}

}

func TestJarvisRoleReplacementAndRevokeBlockActualCallStoppingPermitAndAudioLease(t *testing.T) {
	h, call, a, p := roleFixture(t)
	if w := call("POST", "role/assign", roleAssignInput(h, a, p, "r_role_assign_original", 0), "phone"); w.Code != 200 {
		t.Fatal(w.Code)
	}
	b := h.registry.Create("s_new_jarvis_original", "B", h.cfg.RootDir, backend.Codex, "gpt-6.1-sol", "read-only", "b-native")
	bp, e := h.partners.Save("human", "r_b_profile_original", 0, partners.Profile{SessionID: b.ID, NativeThreadID: b.ResumeID(), Name: "B", Color: "mint"})
	if e != nil {
		t.Fatal(e)
	}
	replace := roleAssignInput(h, b, bp, "r_replace_during_call", 1)
	revoke := jarvisRoleRequest{Authority: h.cfg.InstanceID, Operation: "r_revoke_during_call", Expected: 1}
	owner := newTestClient(h)
	h.voiceClients = map[*Client]*clientVoiceCall{owner: {session: a, voiceID: "actual-source-voice"}}
	for _, stopping := range []bool{false, true} {
		h.voiceClients[owner].stopping = stopping
		for path, in := range map[string]any{"role/assign": replace, "role/revoke": revoke} {
			w := call("POST", path, in, "phone")
			if w.Code != 409 {
				t.Fatal(path, stopping, w.Code, w.Body.String())
			}
		}
		rev, current := h.jarvisRoles.Snapshot()
		if rev != 1 || current.Target.Session != a.ID {
			t.Fatal("active source replaced", rev, current)
		}
	}
	delete(h.voiceClients, owner)
	h.voicePermits = map[string]voiceStartPermit{"original-permit": {session: a.ID, expires: time.Now().Add(time.Second)}}
	if w := call("POST", "role/assign", replace, "phone"); w.Code != 409 {
		t.Fatal("prepared transition ignored", w.Code)
	}
	h.voicePermits = nil
	body := map[string]any{"authority_instance_id": h.cfg.InstanceID, "lease_id": "r_original_speech_lease", "speech_id": "speech-original", "target": jarvisrole.Target{h.cfg.InstanceID, a.ID, a.ResumeID(), a.SettingsSnapshot().ConfigRevision}, "expected_role_epoch": 1, "phase": "acquired", "expected_lease_revision": 0}
	if w := call("POST", "role/audio-lease", body, "phone"); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	body["phase"] = "release_unconfirmed"
	body["expected_lease_revision"] = 1
	if w := call("POST", "role/audio-lease", body, "phone"); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	for path, in := range map[string]any{"role/assign": replace, "role/revoke": revoke} {
		if w := call("POST", path, in, "phone"); w.Code != 409 {
			t.Fatal("unknown native release ignored", path, w.Code)
		}
	}
	body["phase"] = "released"
	body["expected_lease_revision"] = 2
	if w := call("POST", "role/audio-lease", body, "phone"); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	if w := call("POST", "role/assign", replace, "phone"); w.Code != 200 {
		t.Fatal("confirmed release did not unblock", w.Code, w.Body.String())
	}
}
func TestJarvisNormalNeverStartedSpeechLeaseDoesNotGrantRoleOrExpireIntoIdle(t *testing.T) {
	h, call, s, p := roleFixture(t)
	s.SetResumeID("")
	body := map[string]any{"authority_instance_id": h.cfg.InstanceID, "lease_id": "r_normal_speech_lease", "speech_id": "speech-normal-original", "target": jarvisrole.Target{h.cfg.InstanceID, s.ID, "", s.SettingsSnapshot().ConfigRevision}, "expected_role_epoch": 0, "phase": "acquired", "expected_lease_revision": 0}
	w := call("POST", "role/audio-lease", body, "phone")
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	if h.currentJarvisRole() != nil {
		t.Fatal("speech lease granted role")
	}
	if !h.jarvisRoles.AudioBusy(s.ID) {
		t.Fatal("missing speech busy lease")
	}
	if w := call("POST", "role/audio-lease/status", map[string]any{"authority_instance_id": h.cfg.InstanceID, "lease_id": "r_normal_speech_lease"}, "phone"); w.Code != 200 {
		t.Fatal(w.Code)
	}
	_ = p
}

func TestJarvisRoleReadinessReflectsConfiguredScopesAndSeparatesNativeHealth(t *testing.T) {
	h, call, s, p := roleFixture(t)
	if w := call("POST", "role/assign", roleAssignInput(h, s, p, "r_role_assign_original", 0), "phone"); w.Code != 200 {
		t.Fatal(w.Code)
	}
	states := func() map[string]map[string]any {
		rows := h.jarvisServiceReadiness(h.currentJarvisRole())
		out := map[string]map[string]any{}
		for _, v := range rows {
			out[v["id"].(string)] = v
		}
		return out
	}
	initial := states()
	for _, id := range []string{"line", "browser", "voice_media"} {
		if initial[id]["state"] != "unavailable" {
			t.Fatal(id, initial[id])
		}
	}
	store, e := linebridge.OpenStore(t.TempDir(), "fixture", bytes.Repeat([]byte{7}, 32))
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { store.Close() })
	account, e := store.CreateAccount(context.Background(), "Original", "device:original-operator", "/fixture/profile", "/fixture/helper")
	if e != nil {
		t.Fatal(e)
	}
	if e = store.BindPrincipal(context.Background(), account.ID, "u"+strings.Repeat("3", 32)); e != nil {
		t.Fatal(e)
	}
	account, _ = store.Account(context.Background(), account.ID)
	service := linebridge.NewService(store, nil, linebridge.Config{SubjectAllowed: h.LineSubjectAllowed, VoiceDispatchSubjectAllowed: h.LineVoiceDispatchSubjectAllowed, VoiceDispatchAccounts: []linebridge.VoiceDispatchAccount{{Account: account.ID, Binding: account.Revision}}})
	h.SetLineService(service)
	t.Cleanup(service.Close)
	c := attachmentClient(h, "phone")
	h.browserControl = &browserControlState{binding: map[string]browserBinding{s.ResumeID(): {client: c, session: s.ID, thread: s.ResumeID(), token: "original-binding"}}, operations: map[string]*browserOperation{}}
	h.unifiedMedia = &unifiedVoiceMedia{}
	h.voiceClients = map[*Client]*clientVoiceCall{c: {session: s, voiceID: "voice-original", mediaCapable: true, mediaSessionCapable: true, externalMusicPlatform: "android"}}
	actual := states()
	if actual["line"]["state"] != "configured" {
		t.Fatal(actual["line"])
	}
	resources := actual["line"]["resources"].([]any)
	if len(resources) != 1 {
		t.Fatal(resources)
	}
	resource := resources[0].(map[string]any)
	if resource["runtime_state"] != "stopped" || resource["health_state"] != "unknown" {
		t.Fatal("fabricated connectivity or started runtime", resource)
	}
	if actual["browser"]["state"] != "selected" || actual["browser"]["native_capability_state"] != "unknown" {
		t.Fatal(actual["browser"])
	}
	if actual["voice_media"]["state"] != "bound" || actual["voice_media"]["native_permission_state"] != "unknown_until_original_native_effect_or_observation" {
		t.Fatal(actual["voice_media"])
	}
	if e = h.jarvisExternalAdmission(backend.SessionControlCaller{Parent: s, RoleEpoch: 1}, false); e == nil {
		t.Fatal("role bypassed independent service scope")
	}
	h.voiceClients = nil
	if w := call("POST", "role/revoke", jarvisRoleRequest{Authority: h.cfg.InstanceID, Operation: "r_revoke_service_role", Expected: 1}, "phone"); w.Code != 200 {
		t.Fatal(w.Code)
	}
	access, e := service.Access(context.Background(), linebridge.Caller{Subject: "session:" + s.ID}, account.ID)
	if e != nil || len(access.Scopes) == 0 {
		t.Fatal("demotion removed original operator scopes", access, e)
	}
}

func TestJarvisAudioOriginalLeaseNarrowRecoveryAfterSourceRetired(t *testing.T) {
	h, call, s, p := roleFixture(t)
	if w := call("POST", "role/assign", roleAssignInput(h, s, p, "r_role_assign_original", 0), "phone"); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	original := jarvisrole.Target{h.cfg.InstanceID, s.ID, s.ResumeID(), s.SettingsSnapshot().ConfigRevision}
	body := map[string]any{"authority_instance_id": h.cfg.InstanceID, "lease_id": "r_retired_source_lease", "speech_id": "speech-retired-source", "target": original, "expected_role_epoch": 1, "phase": "acquired", "expected_lease_revision": 0}
	if w := call("POST", "role/audio-lease", body, "phone"); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	s.SetResumeID("different-native-thread")
	s.SetConfigRevision(original.Revision + 1)
	if e := os.WriteFile(filepath.Join(h.cfg.DataDir, "jarvis-role-policy.json"), []byte(`{"excluded_sessions":["s_role_source_original"]}`), 0600); e != nil {
		t.Fatal(e)
	}
	if h.currentJarvisRole() != nil {
		t.Fatal("retired source retained role")
	}
	status := map[string]any{"authority_instance_id": h.cfg.InstanceID, "lease_id": body["lease_id"]}
	if w := call("POST", "role/audio-lease/status", status, "phone"); w.Code != 200 {
		t.Fatal("original receipt became unreadable", w.Code, w.Body.String())
	}
	body["phase"], body["expected_lease_revision"] = "released", 1
	body["target"] = jarvisrole.Target{h.cfg.InstanceID, s.ID, s.ResumeID(), s.SettingsSnapshot().ConfigRevision}
	if w := call("POST", "role/audio-lease", body, "phone"); w.Code != 409 {
		t.Fatal("new source impersonated old release", w.Code, w.Body.String())
	}
	body["target"] = original
	if w := call("POST", "role/audio-lease", body, "phone"); w.Code != 200 {
		t.Fatal("exact original known release blocked by retired source", w.Code, w.Body.String())
	}
	if h.jarvisRoles.AudioBusy(s.ID) || h.currentJarvisRole() != nil {
		t.Fatal("release restored new authority or retained old busy lease")
	}
}

func TestJarvisRoleResourceExclusionPreservesOrdinaryAudioLease(t *testing.T) {
	for _, kind := range []string{"excluded", "outside_controller_root", "suspended_assignment"} {
		t.Run(kind, func(t *testing.T) {
			h, call, s, p := roleFixture(t)
			if kind == "suspended_assignment" {
				if w := call("POST", "role/assign", roleAssignInput(h, s, p, "r_role_assign_original", 0), "phone"); w.Code != 200 {
					t.Fatal(w.Code, w.Body.String())
				}
			}
			if kind == "outside_controller_root" {
				h.cfg.RootDir = t.TempDir()
			} else if e := os.WriteFile(filepath.Join(h.cfg.DataDir, "jarvis-role-policy.json"), []byte(`{"excluded_sessions":["s_role_source_original"]}`), 0600); e != nil {
				t.Fatal(e)
			}
			if h.jarvisRoleEligible(s) || h.currentJarvisRole() != nil {
				t.Fatal("bundle resource exclusion did not apply")
			}
			body := map[string]any{"authority_instance_id": h.cfg.InstanceID, "lease_id": "r_base_audio_lease_original", "speech_id": "speech-base-original", "target": jarvisrole.Target{h.cfg.InstanceID, s.ID, s.ResumeID(), s.SettingsSnapshot().ConfigRevision}, "expected_role_epoch": 0, "phase": "acquired", "expected_lease_revision": 0}
			if w := call("POST", "role/audio-lease", body, "phone"); w.Code != 200 {
				t.Fatal("role policy revoked base ordinary voice", w.Code, w.Body.String())
			}
			if h.SessionRoleEpoch(s) != 0 || h.currentJarvisRole() != nil {
				t.Fatal("ordinary lease restored suspended role")
			}
		})
	}
}
