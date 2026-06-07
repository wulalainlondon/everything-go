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
	"regexp"
	"strings"
	"sync"
	"time"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
)

const messagingScope = "https://www.googleapis.com/auth/firebase.messaging"

// Notifier holds the OAuth2 token source and the current device token.
type Notifier struct {
	projectID   string
	tokenSource oauth2.TokenSource
	tokenPath   string
	endpoint    string
	http        *http.Client

	mu          sync.RWMutex
	deviceToken string
}

// New loads the service account from serviceAccountPath and the persisted device
// token from tokenPath (if present). Returns (nil, err) if the credentials are
// missing or invalid — the caller treats a nil Notifier as "FCM disabled".
func New(serviceAccountPath, tokenPath string) (*Notifier, error) {
	data, err := os.ReadFile(serviceAccountPath)
	if err != nil {
		return nil, fmt.Errorf("read service account: %w", err)
	}
	return NewFromBytes(data, tokenPath)
}

// NewFromBytes is like New but accepts the service account JSON directly
// (e.g. from an //go:embed directive).
func NewFromBytes(data []byte, tokenPath string) (*Notifier, error) {
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
		projectID:   sa.ProjectID,
		tokenSource: creds.TokenSource,
		tokenPath:   tokenPath,
		endpoint:    "https://fcm.googleapis.com/v1/projects/" + sa.ProjectID + "/messages:send",
		http:        &http.Client{Timeout: 15 * time.Second},
	}
	if tok, err := os.ReadFile(tokenPath); err == nil {
		n.deviceToken = strings.TrimSpace(string(tok))
	}
	return n, nil
}

// SetToken registers/updates the device token and persists it (fcm_token cmd).
func (n *Notifier) SetToken(token string) {
	token = strings.TrimSpace(token)
	if token == "" {
		return
	}
	n.mu.Lock()
	n.deviceToken = token
	n.mu.Unlock()
	if n.tokenPath != "" {
		tmp := n.tokenPath + ".tmp"
		if os.WriteFile(tmp, []byte(token), 0o600) == nil {
			_ = os.Rename(tmp, n.tokenPath)
		}
	}
	log.Printf("[fcm] device token registered (len=%d)", len(token))
}

func (n *Notifier) token() string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.deviceToken
}

// NotifyTaskDone sends the turn-complete push. No-op if no device token yet.
func (n *Notifier) NotifyTaskDone(sessionName, lastText, sessionID string) {
	tok := n.token()
	if tok == "" {
		return
	}
	summary := summarize(lastText)
	msg := v1message{}
	msg.Message.Token = tok
	msg.Message.Notification.Title = "✓ " + sessionName
	msg.Message.Notification.Body = summary
	msg.Message.Data = map[string]string{"type": "task_done", "session_id": sessionID}
	n.send(msg, "task_done")
}

// NotifyFilePush mirrors notify_fcm_file_push.
func (n *Notifier) NotifyFilePush(fileID, filename string) {
	tok := n.token()
	if tok == "" {
		return
	}
	msg := v1message{}
	msg.Message.Token = tok
	msg.Message.Notification.Title = "📎 新檔案"
	msg.Message.Notification.Body = filename
	msg.Message.Data = map[string]string{
		"type": "file_push", "file_id": fileID, "filename": filename, "deep_link": "bridge://inbox",
	}
	n.send(msg, "file_push")
}

// NotifyTunnelURL mirrors push_registry.send_tunnel_fcm_once.
// Sends a data-only (silent) push so the app can update its tunnel URL.
func (n *Notifier) NotifyTunnelURL(wsURL, instanceID string) {
	tok := n.token()
	if tok == "" {
		return
	}
	msg := v1message{}
	msg.Message.Token = tok
	msg.Message.Data = map[string]string{
		"type": "tunnel_url", "url": wsURL, "instance_id": instanceID,
	}
	n.send(msg, "tunnel_url")
}

// NotifyFeedNew mirrors feed_ops.notify_fcm_feed_new.
func (n *Notifier) NotifyFeedNew(feedID, title string) {
	tok := n.token()
	if tok == "" {
		return
	}
	msg := v1message{}
	msg.Message.Token = tok
	msg.Message.Notification.Title = "新文章"
	msg.Message.Notification.Body = title
	msg.Message.Data = map[string]string{
		"type": "feed_new", "feed_id": feedID, "title": title, "deep_link": "bridge://feed",
	}
	n.send(msg, "feed_new")
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

// send POSTs the message with 3 attempts + exponential backoff, matching the
// Python retry policy. A 404/UNREGISTERED clears the stored token.
func (n *Notifier) send(msg v1message, kind string) {
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
				log.Printf("[fcm] %s notification sent", kind)
				return
			}
			if tokenFatal(resp.StatusCode, respBody) {
				log.Printf("[fcm] device token invalid (%d) — clearing; resp=%s", resp.StatusCode, truncate(string(respBody), 300))
				n.invalidate()
				return
			}
			err = fmt.Errorf("status %d: %s", resp.StatusCode, truncate(string(respBody), 200))
		}
		if attempt < 2 {
			time.Sleep(time.Duration(1<<attempt) * time.Second)
			continue
		}
		log.Printf("[fcm] %s send failed after 3 attempts: %v", kind, err)
	}
}

func (n *Notifier) invalidate() {
	n.mu.Lock()
	n.deviceToken = ""
	n.mu.Unlock()
	if n.tokenPath != "" {
		_ = os.Remove(n.tokenPath)
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
