package jarvisrole

import (
	"errors"
	"time"
)

// Audio lease is original paired-client observation, separate from model turns.
// Unknown release stays blocking across disconnect/restart; TTL is never proof
// that a microphone was actually released.
type AudioLease struct {
	SpeechID  string `json:"speech_id"`
	ID        string `json:"lease_id"`
	Target    Target `json:"target"`
	Epoch     uint64 `json:"role_epoch"`
	Phase     string `json:"phase"`
	Revision  uint64 `json:"revision"`
	UpdatedAt int64  `json:"updated_at"`
	Actor     string `json:"actor,omitempty"`
	InputHash string `json:"input_hash,omitempty"`
}

func (s *Store) AudioLease(actor, id string) (AudioLease, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.state.AudioLeases[id]
	return v, ok && v.Actor == actor
}
func (s *Store) AudioBusy(session string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, v := range s.state.AudioLeases {
		if v.Target.Session == session && v.Phase != "released" {
			return true
		}
	}
	return false
}
func (s *Store) SetAudioLease(actor string, v AudioLease, expected uint64, hash string, admittedEpoch ...uint64) (AudioLease, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	old, exists := s.state.AudioLeases[v.ID]
	if exists && old.Actor == actor && old.InputHash == hash {
		return old, nil
	}
	if actor == "" || v.ID == "" || v.Target.Authority != s.state.Authority || v.Target.Session == "" {
		return v, errors.New("audio_lease_invalid")
	}
	if exists && (old.Actor != actor || old.Target != v.Target || old.Epoch != v.Epoch || old.SpeechID != v.SpeechID) {
		return v, errors.New("audio_lease_identity_changed")
	}
	if (!exists && expected != 0) || (exists && old.Revision != expected) {
		return v, errors.New("audio_lease_revision_conflict")
	}
	if !exists {
		expectedEpoch := uint64(0)
		if a := s.state.Assignment; a != nil && a.Target.Session == v.Target.Session {
			expectedEpoch = a.Epoch
		}
		// Optional host witness accounts for a stored assignment suspended by
		// current source/pairing/operator policy. Ordinary audio remains epoch0.
		if len(admittedEpoch) > 0 {
			expectedEpoch = admittedEpoch[0]
		}
		if v.Phase != "acquired" || v.Epoch != expectedEpoch {
			return v, errors.New("audio_lease_role_changed")
		}
		for _, other := range s.state.AudioLeases {
			if other.Phase != "released" && other.Target.Session == v.Target.Session {
				return v, errors.New("audio_lease_busy")
			}
		}
	} else {
		valid := v.Phase == "released" || v.Phase == "release_unconfirmed" || (old.Phase == "acquired" && v.Phase == "recording") || (old.Phase == "recording" && v.Phase == "finishing")
		if old.Phase == "released" || !valid {
			return v, errors.New("audio_lease_transition_invalid")
		}
	}
	v.Actor = actor
	v.Revision = expected + 1
	v.InputHash = hash
	v.UpdatedAt = time.Now().UnixMilli()
	n := s.state
	n.AudioLeases = map[string]AudioLease{}
	for k, x := range s.state.AudioLeases {
		n.AudioLeases[k] = x
	}
	n.AudioLeases[v.ID] = v
	if e := s.persist(n); e != nil {
		return v, e
	}
	s.state = n
	return v, nil
}
