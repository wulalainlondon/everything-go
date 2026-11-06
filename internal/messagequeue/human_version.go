package messagequeue

import (
	"database/sql"
	"encoding/json"
	"errors"
	"everything-go/internal/taskapi"
)

// HumanVersion is an exact immutable historical admission witness. Current
// effective content may already have advanced; it is never substituted here.
func (s *Store) HumanVersion(binding HumanBinding, id string, version uint64, wireHash string) (Entry, bool, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return Entry{}, false, err
	}
	defer tx.Rollback()
	var raw []byte
	if err = tx.QueryRow(`SELECT body FROM input_versions WHERE session=? AND id=? AND revision=?`, binding.Session, id, version).Scan(&raw); errors.Is(err, sql.ErrNoRows) {
		return Entry{}, false, nil
	} else if err != nil {
		return Entry{}, false, err
	}
	var original HumanInput
	if json.Unmarshal(raw, &original) != nil || original.Binding != binding || original.WireHash != wireHash || wireHash == "" {
		return Entry{}, false, ErrConflict
	}
	entry, found, err := lookup(tx, binding.Session, id)
	if err != nil || !found {
		return entry, found, err
	}
	var payload []byte
	var content, hash string
	err = tx.QueryRow(`SELECT payload,content,hash FROM unified_input_versions WHERE queue_key=? AND item_id=? AND version=?`, binding.Session, id, version).Scan(&payload, &content, &hash)
	if errors.Is(err, sql.ErrNoRows) {
		return Entry{}, false, nil
	}
	if err != nil {
		return Entry{}, false, err
	}
	if hash != wireHash || taskapi.RawDigest(payload) != wireHash {
		return Entry{}, false, ErrConflict
	}
	entry.Payload = payload
	entry.Content = content
	entry.ContentRevision = version
	entry.EffectiveHash = hash
	return entry, true, nil
}
