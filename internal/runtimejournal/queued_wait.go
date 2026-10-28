package runtimejournal

// QueuedWait updates only a still-queued exact original head. It cannot create
// a runtime, adopt a request, mark native start, rewind running/terminal progress,
// change queue length or advance read awareness. The queue writer separately
// verifies the same durable command is still queued before calling this method.
func (s *Store) QueuedWait(sessionID, requestID, message string) (View, bool) {
	if sessionID == "" || requestID == "" {
		return View{}, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.records[sessionID]
	if r == nil {
		return View{}, false
	}
	if r.Phase != "queued" || r.ActiveRequestID != requestID || r.Stage != "queued" || r.StageMessage == message {
		return viewLocked(r, ""), false
	}
	r.Revision++
	r.StageMessage = message
	r.StageStartedAt = s.now().UnixMilli()
	r.UpdatedAt = r.StageStartedAt
	s.scheduleSaveLocked()
	return viewLocked(r, ""), true
}
