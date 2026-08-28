package core

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"everything-go/internal/workitems"
)

func TestWorkAPILoopbackContextAndHumanOnlyDone(t *testing.T) {
	h, _ := newTestHub(t)
	service := attachWorkService(t, h, t.TempDir())
	ctx := context.Background()
	if _, err := service.CreateProject(ctx, workitems.CreateProjectInput{ID: "p1", Name: "Bridge", Context: "Signed release only"}); err != nil {
		t.Fatal(err)
	}
	item, err := service.CreateItem(ctx, workitems.CreateItemInput{ID: "i1", ProjectID: "p1", Title: "Release"})
	if err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/work/v1/items/i1/context", nil)
	req.RemoteAddr = "127.0.0.1:42000"
	response := httptest.NewRecorder()
	h.ServeWorkAPI(response, req)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), "Signed release only") || !strings.Contains(response.Body.String(), "Never mark this item done") {
		t.Fatalf("context response status=%d body=%s", response.Code, response.Body.String())
	}

	body := `{"action":"done","expected_version":` + formatUint(item.Version) + `}`
	req = httptest.NewRequest(http.MethodPost, "/api/work/v1/items/i1/events", strings.NewReader(body))
	req.RemoteAddr = "127.0.0.1:42000"
	response = httptest.NewRecorder()
	h.ServeWorkAPI(response, req)
	if response.Code != http.StatusBadRequest || !strings.Contains(response.Body.String(), "unsupported agent action") {
		t.Fatalf("done response status=%d body=%s", response.Code, response.Body.String())
	}
	current, err := service.GetItem(ctx, item.ID)
	if err != nil || current.Lifecycle == workitems.LifecycleDone {
		t.Fatalf("agent API completed item: %+v err=%v", current, err)
	}
}

func TestWorkAPIRemoteRequiresBearerToken(t *testing.T) {
	h, _ := newTestHub(t)
	attachWorkService(t, h, t.TempDir())
	req := httptest.NewRequest(http.MethodGet, "/api/work/v1/items/missing/context", nil)
	req.RemoteAddr = "100.64.0.2:42000"
	response := httptest.NewRecorder()
	h.ServeWorkAPI(response, req)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("remote response status=%d", response.Code)
	}
}
