package goexec

import (
	"errors"
	"everything-go/internal/session"
)

func (c *Codex) NativeTurnForRequest(s *session.Session, request string) (string, error) {
	c.historyRequestMu.Lock()
	defer c.historyRequestMu.Unlock()
	r, err := c.loadTurnRequests(s.ResumeID())
	if err != nil {
		return "", err
	}
	found := ""
	for turn, id := range r.Requests {
		if id == request {
			if found != "" && found != turn {
				return "", errors.New("ambiguous native request mapping")
			}
			found = turn
		}
	}
	return found, nil
}
