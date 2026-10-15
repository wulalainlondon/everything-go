package main

import (
	"context"
	"everything-go/internal/core"
	"everything-go/internal/filetransfer"
	"everything-go/internal/linebridge"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

func configuredLineService(hub *core.Hub, dataDir, authority string, transfers *filetransfer.Service) (*linebridge.Service, error) {
	entry := os.Getenv("LINE_SERVICE_ENTRY")
	if entry == "" {
		return nil, nil
	}
	helper := os.Getenv("LINEJS_KEYCHAIN_HELPER")
	shared := os.Getenv("LINEJS_SHARED_DIR")
	node := os.Getenv("LINEJS_NODE")
	if node == "" {
		node, _ = exec.LookPath("node")
	}
	if !filepath.IsAbs(entry) || !filepath.IsAbs(helper) || !filepath.IsAbs(shared) || !filepath.IsAbs(node) {
		return nil, linebridge.Fail("line_configuration_invalid")
	}
	dir, err := filepath.Abs(filepath.Join(dataDir, "line"))
	if err != nil {
		return nil, err
	}
	key, err := linebridge.StoreKey(context.Background(), helper, dir)
	if err != nil {
		return nil, err
	}
	defer func() {
		for i := range key {
			key[i] = 0
		}
	}()
	store, err := linebridge.OpenStore(dir, authority, key)
	if err != nil {
		return nil, err
	}
	blocked := map[string]bool{}
	for scope, env := range map[string]string{"messages.send": "LINE_SERVICE_ALLOW_WRITE", "schedule": "LINE_SERVICE_ALLOW_SCHEDULES", "events": "LINE_SERVICE_ALLOW_EVENTS", "resources": "LINE_SERVICE_ALLOW_RESOURCES", "call": "LINEJS_ALLOW_CALLS"} {
		blocked[scope] = os.Getenv(env) != "1"
	}
	config := linebridge.Config{BlockedScopes: blocked, Subjects: hub.LineSubjects, DataDir: dir, KeychainHelper: helper, LegacyDir: os.Getenv("LINE_LEGACY_DATA_DIR"), EventSink: hub.LineEventSink(store), Resources: func(ctx context.Context, c linebridge.Caller, ref string) (linebridge.ResourceInput, error) {
		if strings.HasPrefix(c.Subject, "device:") {
			data, name, mime, err := transfers.ReadCompletedForDevice(strings.TrimPrefix(c.Subject, "device:"), ref, linebridge.MaxResourceBytes)
			if err != nil {
				return linebridge.ResourceInput{}, linebridge.Fail("line_resource_source_invalid")
			}
			return linebridge.ResourceInput{Bytes: data, Name: name, MIME: mime}, nil
		}
		return hub.ResolveLineAttachment(ctx, c, ref)
	}, SubjectAllowed: hub.LineSubjectAllowed}
	service := linebridge.NewService(store, linebridge.ExecFactory{Node: node, Entry: entry, Shared: shared, Manifest: os.Getenv("LINE_SERVICE_MANIFEST")}, config)
	hub.SetLineService(service)
	service.StartScheduler()
	return service, nil
}
