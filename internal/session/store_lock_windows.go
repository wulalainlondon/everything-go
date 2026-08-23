//go:build windows

package session

func (st *Store) lock() (func(), error) {
	return func() {}, nil
}
