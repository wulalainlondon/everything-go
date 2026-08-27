// Package fcm sends Firebase Cloud Messaging push notifications via the HTTP v1
// API, authenticated with a Google service account (the project's
// serviceAccountKey.json). It mirrors the Python bridge's notify_fcm: a
// "task done" push when a turn completes, with a markdown-cleaned summary.
//
// Using the raw HTTP v1 endpoint + golang.org/x/oauth2/google keeps the
// dependency footprint small (no full firebase-admin equivalent) while reusing
// the project's existing credentials, per the user's instruction to share the
// bridge's service token.
package fcm

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
)

const messagingScope = "https://www.googleapis.com/auth/firebase.messaging"

const (
	registryVersion = 1
	legacyDeviceID  = "legacy"
	maxDevices      = 64
)

type deviceRegistration struct {
	Token     string `json:"token"`
	UpdatedAt int64  `json:"updated_at"`
}

type tokenRegistry struct {
	Version int                           `json:"version"`
	Devices map[string]deviceRegistration `json:"devices"`
}

type target struct {
	deviceID string
	token    string
}

// Notifier holds the OAuth2 token source and a per-device token registry.
type Notifier struct {
	projectID    string
	tokenSource  oauth2.TokenSource
	registryPath string
	endpoint     string
	http         *http.Client

	mu      sync.RWMutex
	devices map[string]deviceRegistration
}

// New loads the service account from serviceAccountPath and the persisted
// per-device token registry from registryPath (if present). Returns (nil, err) if the credentials are
// missing or invalid — the caller treats a nil Notifier as "FCM disabled".
func New(serviceAccountPath, registryPath string) (*Notifier, error) {
	data, err := os.ReadFile(serviceAccountPath)
	if err != nil {
		return nil, fmt.Errorf("read service account: %w", err)
	}
	return NewFromBytes(data, registryPath)
}

// NewFromBytes is like New but accepts the service account JSON directly
// (e.g. from an //go:embed directive).
func NewFromBytes(data []byte, registryPath string) (*Notifier, error) {
	var sa struct {
		ProjectID string `json:"project_id"`
	}
	if err := json.Unmarshal(data, &sa); err != nil || sa.ProjectID == "" {
		return nil, fmt.Errorf("service account missing project_id")
	}
	creds, err := google.CredentialsFromJSON(context.Background(), data, messagingScope)
	if err != nil {
		return nil, fmt.Errorf("credentials: %w", err)
	}
	n := &Notifier{
		projectID: sa.ProjectID,
		// Fan-out calls Token concurrently. ReuseTokenSource both caches the
		// OAuth token and serializes refreshes around the underlying source.
		tokenSource:  oauth2.ReuseTokenSource(nil, creds.TokenSource),
		registryPath: registryPath,
		endpoint:     "https://fcm.googleapis.com/v1/projects/" + sa.ProjectID + "/messages:send",
		http:         &http.Client{Timeout: 15 * time.Second},
		devices:      make(map[string]deviceRegistration),
	}
	n.loadRegistry()
	return n, nil
}

// SetToken registers/updates one stable device's token and persists the whole
// registry. Reusing a token under a new device id moves it instead of creating
// duplicate pushes (e.g. after an app reinstall changes the local device id).
func (n *Notifier) SetToken(deviceID, token string) {
	deviceID = strings.TrimSpace(deviceID)
	token = strings.TrimSpace(token)
	if token == "" || deviceID == "" {
		return
	}
	n.mu.Lock()
	for id, registration := range n.devices {
		if id != deviceID && registration.Token == token {
			delete(n.devices, id)
		}
	}
	current, exists := n.devices[deviceID]
	if exists && current.Token == token {
		n.mu.Unlock()
		return
	}
	n.devices[deviceID] = deviceRegistration{Token: token, UpdatedAt: time.Now().UnixMilli()}
	n.enforceCapLocked()
	n.persistLocked()
	n.mu.Unlock()
	log.Printf("[fcm] device token registered device=%s (len=%d)", deviceID, len(token))
}

func (n *Notifier) targets() []target {
	n.mu.RLock()
	defer n.mu.RUnlock()
	out := make([]target, 0, len(n.devices))
	seen := make(map[string]struct{}, len(n.devices))
	for deviceID, registration := range n.devices {
		if registration.Token == "" {
			continue
		}
		if _, duplicate := seen[registration.Token]; duplicate {
			continue
		}
		seen[registration.Token] = struct{}{}
		out = append(out, target{deviceID: deviceID, token: registration.Token})
	}
	return out
}

// NotifyTaskDone sends the turn-complete push. No-op if no device token yet.
func (n *Notifier) NotifyTaskDone(sessionName, lastText, sessionID string) {
	summary := summarize(lastText)
	msg := v1message{}
	msg.Message.Notification.Title = "✓ " + sessionName
	msg.Message.Notification.Body = summary
	msg.Message.Data = map[string]string{"type": "task_done", "session_id": sessionID}
	n.sendAll(msg, "task_done")
}

// NotifyFilePush mirrors notify_fcm_file_push.
func (n *Notifier) NotifyFilePush(fileID, filename string) {
	msg := v1message{}
	msg.Message.Notification.Title = "📎 新檔案"
	msg.Message.Notification.Body = filename
	msg.Message.Data = map[string]string{
		"type": "file_push", "file_id": fileID, "filename": filename, "deep_link": "bridge://inbox",
	}
	n.sendAll(msg, "file_push")
}

// NotifyTunnelURL mirrors push_registry.send_tunnel_fcm_once.
// Sends a data-only (silent) push so the app can update its tunnel URL.
func (n *Notifier) NotifyTunnelURL(wsURL, instanceID string) {
	msg := v1message{}
	msg.Message.Data = map[string]string{
		"type": "tunnel_url", "url": wsURL, "instance_id": instanceID,
	}
	n.sendAll(msg, "tunnel_url")
}

// NotifyFeedNew mirrors feed_ops.notify_fcm_feed_new.
func (n *Notifier) NotifyFeedNew(feedID, title string) {
	msg := v1message{}
	msg.Message.Notification.Title = "新文章"
	msg.Message.Notification.Body = title
	msg.Message.Data = map[string]string{
		"type": "feed_new", "feed_id": feedID, "title": title, "deep_link": "bridge://feed",
	}
	n.sendAll(msg, "feed_new")
}

func (n *Notifier) NotifyWorkAttention(instanceID, workItemID, title string, revision uint64, kind string) {
	msg := v1message{}
	msg.Message.Notification.Title = "工作需要你的注意"
	msg.Message.Notification.Body = title
	msg.Message.Data = map[string]string{
		"type": "work_attention", "authority_instance_id": instanceID,
		"work_item_id": workItemID, "activity_revision": fmt.Sprint(revision),
		"kind": kind, "title": title, "deep_link": "bridge://inbox/work/" + workItemID,
	}
	n.sendAll(msg, "work_attention")
}

type v1message struct {
	Message struct {
		Token        string `json:"token"`
		Notification struct {
			Title string `json:"title"`
			Body  string `json:"body"`
		} `json:"notification"`
		Data map[string]string `json:"data"`
	} `json:"message"`
}

// sendAll fans a notification out concurrently so a slow or invalid device
// cannot delay the remaining devices.
func (n *Notifier) sendAll(msg v1message, kind string) {
	targets := n.targets()
	if len(targets) == 0 {
		return
	}
	var wg sync.WaitGroup
	wg.Add(len(targets))
	for _, dst := range targets {
		dst := dst
		go func() {
			defer wg.Done()
			copy := msg
			copy.Message.Token = dst.token
			n.send(copy, kind, dst)
		}()
	}
	wg.Wait()
}

// send POSTs one device's message with 3 attempts + exponential backoff,
// matching the Python retry policy. A fatal response removes only the token
// that failed; other device registrations remain intact.
func (n *Notifier) send(msg v1message, kind string, dst target) {
	body, _ := json.Marshal(msg)
	for attempt := 0; attempt < 3; attempt++ {
		tok, err := n.tokenSource.Token()
		if err != nil {
			log.Printf("[fcm] oauth token error: %v", err)
			return
		}
		req, _ := http.NewRequest(http.MethodPost, n.endpoint, bytes.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+tok.AccessToken)
		req.Header.Set("Content-Type", "application/json")
		resp, err := n.http.Do(req)
		if err == nil {
			respBody, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.StatusCode == 200 {
				log.Printf("[fcm] %s notification sent device=%s", kind, dst.deviceID)
				return
			}
			if tokenFatal(resp.StatusCode, respBody) {
				log.Printf("[fcm] device token invalid device=%s (%d) — clearing; resp=%s", dst.deviceID, resp.StatusCode, truncate(string(respBody), 300))
				n.invalidate(dst)
				return
			}
			err = fmt.Errorf("status %d: %s", resp.StatusCode, truncate(string(respBody), 200))
		}
		if attempt < 2 {
			time.Sleep(time.Duration(1<<attempt) * time.Second)
			continue
		}
		log.Printf("[fcm] %s send failed device=%s after 3 attempts: %v", kind, dst.deviceID, err)
	}
}

func (n *Notifier) invalidate(dst target) {
	n.mu.Lock()
	current, ok := n.devices[dst.deviceID]
	if ok && current.Token == dst.token {
		delete(n.devices, dst.deviceID)
		n.persistLocked()
	}
	n.mu.Unlock()
}

func (n *Notifier) loadRegistry() {
	if n.registryPath == "" {
		return
	}
	data, err := os.ReadFile(n.registryPath)
	if err == nil {
		var registry tokenRegistry
		if json.Unmarshal(data, &registry) == nil && registry.Devices != nil {
			n.devices = registry.Devices
			before := len(n.devices)
			n.enforceCapLocked()
			if len(n.devices) != before {
				n.persistLocked()
			}
			return
		}
		// Backward compatibility when the constructor still points at the old
		// plain-text file during a rolling upgrade.
		if tok := strings.TrimSpace(string(data)); tok != "" && !strings.HasPrefix(tok, "{") {
			n.devices[legacyDeviceID] = deviceRegistration{Token: tok, UpdatedAt: time.Now().UnixMilli()}
			n.persistLocked()
			return
		}
	}
	legacyPath := filepath.Join(filepath.Dir(n.registryPath), "fcm_token.txt")
	if tok, legacyErr := os.ReadFile(legacyPath); legacyErr == nil {
		if token := strings.TrimSpace(string(tok)); token != "" {
			n.devices[legacyDeviceID] = deviceRegistration{Token: token, UpdatedAt: time.Now().UnixMilli()}
			n.persistLocked()
		}
	}
}

func (n *Notifier) persistLocked() {
	if n.registryPath == "" || os.MkdirAll(filepath.Dir(n.registryPath), 0o700) != nil {
		return
	}
	data, err := json.Marshal(tokenRegistry{Version: registryVersion, Devices: n.devices})
	if err != nil {
		return
	}
	tmp := n.registryPath + ".tmp"
	if os.WriteFile(tmp, data, 0o600) == nil {
		_ = os.Rename(tmp, n.registryPath)
	}
}

func (n *Notifier) enforceCapLocked() {
	for len(n.devices) > maxDevices {
		oldestID := ""
		var oldestAt int64
		for deviceID, registration := range n.devices {
			if oldestID == "" || registration.UpdatedAt < oldestAt {
				oldestID = deviceID
				oldestAt = registration.UpdatedAt
			}
		}
		delete(n.devices, oldestID)
	}
}

// tokenFatal reports whether the FCM error means the device token is permanently
// invalid (so it should be cleared rather than retried).
func tokenFatal(status int, body []byte) bool {
	if status == 404 {
		return true
	}
	s := string(body)
	return strings.Contains(s, "UNREGISTERED") || strings.Contains(s, "INVALID_ARGUMENT")
}

var (
	mdMarks = regexp.MustCompile("[*`#_~>]+")
	mdLinks = regexp.MustCompile(`\[([^\]]+)\]\([^)]+\)`)
	wsRun   = regexp.MustCompile(`\s+`)
	sentEnd = regexp.MustCompile(`[。！？!?.]\s+`)
)

// summarize mirrors notify_fcm's body shaping: take the last non-empty
// paragraph, strip markdown, keep the first sentence, cap at 120 runes.
func summarize(lastText string) string {
	clean := func(s string) string {
		s = mdMarks.ReplaceAllString(s, "")
		s = mdLinks.ReplaceAllString(s, "$1")
		return strings.TrimSpace(wsRun.ReplaceAllString(s, " "))
	}
	var paras []string
	for _, p := range strings.Split(lastText, "\n\n") {
		if strings.TrimSpace(p) != "" {
			paras = append(paras, p)
		}
	}
	source := lastText
	if len(paras) > 0 {
		source = paras[len(paras)-1]
	}
	summary := clean(source)
	if loc := sentEnd.FindStringIndex(summary); loc != nil {
		summary = strings.TrimSpace(summary[:loc[1]])
	}
	if summary == "" {
		summary = clean(lastText)
	}
	if r := []rune(summary); len(r) > 120 {
		summary = strings.TrimRight(string(r[:120]), " ") + "…"
	}
	return summary
}

func truncate(s string, n int) string {
	if len(s) > n {
		return s[:n]
	}
	return s
}
