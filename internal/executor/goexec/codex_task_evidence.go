package goexec

import "everything-go/internal/session"

func (c *Codex) NativeTurnForRequest(s *session.Session, request string) (string, error) {
	c.historyRequestMu.Lock()
	defer c.historyRequestMu.Unlock()
	r, err := c.loadTurnRequests(s.ResumeID())
	if err != nil {
		return "", err
	}
	for turn, id := range r.Requests {
		if id == request {
			return turn, nil
		}
	}
	return "", nil
}
