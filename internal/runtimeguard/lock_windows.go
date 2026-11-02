//go:build windows

package runtimeguard

import "errors"

func Acquire(string) (func(), error) {
	return nil, errors.New("runtime_data_directory_lock_unsupported")
}
