package session

import "errors"

var ErrDurableStoreRequired = errors.New("durable session store required")
var ErrCreateConflict = errors.New("immutable session creation conflict")

func (r *Registry) HasDurableStore() bool { return r.store != nil }

// CreateOnlyDurably compares the immutable creation tuple instead of treating
// Registry.Create's existing-ID shortcut as a successful create receipt.
func (r *Registry) CreateOnlyDurably(id, name, cwd, backend, model, sandbox string) (*Session, error) {
	r.mutationMu.Lock()
	defer r.mutationMu.Unlock()
	if r.store == nil {
		return nil, ErrDurableStoreRequired
	}
	s := r.Create(id, name, cwd, backend, model, sandbox, "")
	snap := s.SettingsSnapshot()
	if snap.Name != name || snap.Cwd != cwd || snap.Backend != backend || snap.Model != model || snap.Sandbox != sandbox || snap.ResumeID != "" || snap.ConfigRevision != 0 || snap.Hidden || s.State() == Closed {
		return nil, ErrCreateConflict
	}
	if err := r.store.Save(r.List()); err != nil {
		return nil, err
	}
	return s, nil
}
