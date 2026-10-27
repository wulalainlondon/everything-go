package core

import (
	"errors"
	"everything-go/internal/taskapi"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
)

// Snapshot through one confined root and file handle. A renamed/replaced root
// or file cannot be substituted between stat, read and manifest identity.
func readUnifiedDocumentSnapshot(rootPath, relative string, limit int64) ([]byte, string, error) {
	if !filepath.IsLocal(relative) {
		return nil, "", errors.New("document_path_forbidden")
	}
	canonical, e := filepath.EvalSymlinks(rootPath)
	if e != nil {
		return nil, "", e
	}
	root, e := os.OpenRoot(canonical)
	if e != nil {
		return nil, "", e
	}
	defer root.Close()
	rootInfo, e := root.Stat(".")
	if e != nil {
		return nil, "", e
	}
	rootIdentity, e := taskRootInfoIdentity(rootInfo)
	if e != nil {
		return nil, "", e
	}
	parts := strings.Split(filepath.Clean(relative), string(filepath.Separator))
	if len(parts) > 128 {
		return nil, "", errors.New("document_path_depth")
	}
	current := root
	for _, part := range parts[:len(parts)-1] {
		before, e := current.Lstat(part)
		if e != nil || !before.IsDir() || before.Mode()&os.ModeSymlink != 0 {
			return nil, "", errors.New("document_symlink_forbidden")
		}
		next, e := current.OpenRoot(part)
		if e != nil {
			return nil, "", e
		}
		defer next.Close()
		opened, e := next.Stat(".")
		if e != nil || !os.SameFile(before, opened) {
			return nil, "", errors.New("document_source_changed")
		}
		current = next
	}
	name := parts[len(parts)-1]
	before, e := current.Lstat(name)
	if e != nil || !before.Mode().IsRegular() || before.Mode()&os.ModeSymlink != 0 || before.Size() > limit {
		return nil, "", errors.New("document_not_bounded_regular")
	}
	f, e := current.Open(name)
	if e != nil {
		return nil, "", e
	}
	defer f.Close()
	opened, e := f.Stat()
	if e != nil || !os.SameFile(before, opened) {
		return nil, "", errors.New("document_source_changed")
	}
	body, e := io.ReadAll(io.LimitReader(f, limit+1))
	if e != nil || int64(len(body)) > limit {
		return nil, "", errors.New("document_read_failed")
	}
	after, e := f.Stat()
	if e != nil || !os.SameFile(opened, after) || opened.Size() != after.Size() || !opened.ModTime().Equal(after.ModTime()) {
		return nil, "", errors.New("document_source_changed")
	}
	named, e := current.Lstat(name)
	if e != nil || !os.SameFile(named, opened) {
		return nil, "", errors.New("document_source_changed")
	}
	rootNow, e := os.Stat(canonical)
	if e != nil || !os.SameFile(rootInfo, rootNow) {
		return nil, "", errors.New("document_root_changed")
	}
	native := reflect.ValueOf(opened.Sys())
	if native.Kind() == reflect.Pointer {
		native = native.Elem()
	}
	if native.Kind() != reflect.Struct {
		return nil, "", errors.New("document_identity_unsupported")
	}
	dev, ino := native.FieldByName("Dev"), native.FieldByName("Ino")
	if !dev.IsValid() || !ino.IsValid() {
		return nil, "", errors.New("document_identity_unsupported")
	}
	identity := taskapi.Digest([]any{rootIdentity, filepath.ToSlash(relative), fmt.Sprint(dev.Interface()), fmt.Sprint(ino.Interface()), opened.Size(), opened.ModTime().UnixNano()})
	return body, identity, nil
}
