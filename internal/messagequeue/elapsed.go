package messagequeue

import (
	"database/sql"
	"fmt"
	"sync"
	"time"
)

// Time values retain Go's monotonic component only in the current process.
// Reopen never reconstructs a start from UTC or queue updated_at.
type elapsedClock struct {
	mu     sync.Mutex
	domain string
	starts map[string]time.Time
}
type ElapsedSample struct {
	Authority          string `json:"authority_instance_id"`
	BootGeneration     string `json:"boot_generation"`
	ClockDomain        string `json:"clock_domain"`
	Provenance         string `json:"provenance"`
	Quality            string `json:"quality"`
	UnavailableReason  string `json:"unavailable_reason"`
	SampledServerNowMS int64  `json:"sampled_server_now_ms"`
	ElapsedAtSampleMS  *int64 `json:"elapsed_at_sample_ms"`
	TerminalElapsedMS  *int64 `json:"terminal_elapsed_ms"`
}

func (s *Store) initElapsed() error {
	s.clock = elapsedClock{domain: fmt.Sprintf("go_monotonic_%d", time.Now().UnixNano()), starts: map[string]time.Time{}}
	_, err := s.db.Exec(`CREATE TABLE IF NOT EXISTS task_elapsed(session_id TEXT NOT NULL,request_id TEXT NOT NULL,domain TEXT NOT NULL,provenance TEXT NOT NULL,last_sample_ms INTEGER NOT NULL,terminal_ms INTEGER,PRIMARY KEY(session_id,request_id))`)
	return err
}

// RecordNativeClock is called only for the original correlated live acceptance,
// carrying the executor-captured time. Receipt recovery cannot supply a clock.
func (s *Store) RecordNativeClock(session, request string, started time.Time) error {
	if started.IsZero() {
		return nil
	}
	now := time.Now()
	delta := now.Sub(started)
	// UTC-only timestamps do not carry a comparable original monotonic proof.
	if started == started.Round(0) || delta < 0 {
		return nil
	}
	if _, found, err := s.ExactNativeAcceptance(session, request); err != nil {
		return err
	} else if !found {
		return nil
	}
	s.clock.mu.Lock()
	defer s.clock.mu.Unlock()
	key := session + "\x00" + request
	e, found, err := s.Get(session, request)
	if err != nil {
		return err
	}
	if !found || e.State == Completed || e.State == Cancelled || e.State == Failed || e.State == Steered {
		return nil
	}

	var domain string
	err = s.db.QueryRow(`SELECT domain FROM task_elapsed WHERE session_id=? AND request_id=?`, session, request).Scan(&domain)
	if err == nil {
		return nil
	}
	if err != sql.ErrNoRows {
		return err
	}
	_, err = s.db.Exec(`INSERT INTO task_elapsed VALUES(?,?,?,?,?,NULL)`, session, request, s.clock.domain, "codex_correlated_consumed_response_monotonic_lower_bound", delta.Milliseconds())
	if err == nil {
		s.clock.starts[key] = started
	}
	return err
}
func (s *Store) sealElapsed(session, request string) error {
	s.clock.mu.Lock()
	defer s.clock.mu.Unlock()
	started, ok := s.clock.starts[session+"\x00"+request]
	if !ok {
		return nil
	}
	value := time.Since(started).Milliseconds()
	if value < 0 {
		return fmt.Errorf("invalid monotonic duration")
	}
	_, err := s.db.Exec(`UPDATE task_elapsed SET terminal_ms=?,last_sample_ms=? WHERE session_id=? AND request_id=? AND domain=? AND terminal_ms IS NULL`, value, value, session, request, s.clock.domain)
	if err == nil {
		delete(s.clock.starts, session+"\x00"+request)
	}
	return err
}
func (s *Store) Elapsed(session, request string) (ElapsedSample, error) {
	s.clock.mu.Lock()
	defer s.clock.mu.Unlock()
	out := ElapsedSample{BootGeneration: s.clock.domain, ClockDomain: s.clock.domain, Provenance: "original_message_queue", Quality: "unavailable", UnavailableReason: "native_proof_has_no_comparable_clock", SampledServerNowMS: time.Now().UnixMilli()}
	if _, found, err := s.ExactNativeAcceptance(session, request); err != nil || !found {
		out.UnavailableReason = "native_evidence_conflict_or_unavailable"
		return out, nil
	}
	var domain, provenance string
	var last int64
	var terminal sql.NullInt64
	err := s.db.QueryRow(`SELECT domain,provenance,last_sample_ms,terminal_ms FROM task_elapsed WHERE session_id=? AND request_id=?`, session, request).Scan(&domain, &provenance, &last, &terminal)
	if err == sql.ErrNoRows {
		return out, nil
	}
	if err != nil {
		return out, err
	}
	out.ClockDomain = domain
	out.Provenance = provenance
	if terminal.Valid {
		if terminal.Int64 < 0 {
			out.UnavailableReason = "invalid_duration"
			return out, nil
		}
		value := terminal.Int64
		out.Quality = "estimate"
		out.UnavailableReason = ""
		out.TerminalElapsedMS = &value
		out.ElapsedAtSampleMS = &value
		return out, nil
	}
	entry, found, err := s.Get(session, request)
	if err != nil {
		return out, err
	}
	if found && (entry.State == Completed || entry.State == Failed || entry.State == Cancelled) {
		out.UnavailableReason = "terminal_clock_not_committed"
		return out, nil
	}
	start, ok := s.clock.starts[session+"\x00"+request]
	if domain != s.clock.domain || !ok {
		out.UnavailableReason = "clock_domain_ended"
		return out, nil
	}
	value := time.Since(start).Milliseconds()
	if value < 0 {
		out.UnavailableReason = "invalid_duration"
		return out, nil
	}
	if _, err = s.db.Exec(`UPDATE task_elapsed SET last_sample_ms=? WHERE session_id=? AND request_id=?`, value, session, request); err != nil {
		return out, err
	}
	out.Quality = "estimate"
	out.UnavailableReason = ""
	out.ElapsedAtSampleMS = &value
	return out, nil
}
