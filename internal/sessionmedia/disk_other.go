//go:build !darwin && !linux

package sessionmedia

import "errors"

func availableBytes(string) (uint64, error) {
	return 0, errors.New("gallery disk admission unsupported")
}
