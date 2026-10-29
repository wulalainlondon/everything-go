package core

import (
	"net/http/httptest"
	"strings"
)

type overviewHTTPResult struct {
	status int
	body   string
}

func overviewHTTP(h *Hub, path string, body []byte, device string) overviewHTTPResult {
	r := httptest.NewRequest("POST", "/api/jarvis/v1/overview/"+path, strings.NewReader(string(body)))
	r.Header.Set("Authorization", "Bearer jarvis-token")
	r.Header.Set("X-Bridge-Device-ID", device)
	w := httptest.NewRecorder()
	h.ServeJarvisAPI(w, r)
	return overviewHTTPResult{w.Code, w.Body.String()}
}
