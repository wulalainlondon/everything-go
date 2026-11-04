package core

import (
	"strings"
	"testing"
)

func TestAttachmentUploadNewVideo1GiBGenericFile2GiBBoundary(t *testing.T) {
	if maxVideoUploadBytes != 2147483648 || maxFileUploadBytes != 2147483648 {
		t.Fatal("attachment caps must be exactly 2048 MiB")
	}
	for _, kind := range []string{"video", "file"} {
		t.Run(kind, func(t *testing.T) {
			h, _ := newTestHub(t)
			h.cfg.DataDir = t.TempDir()
			h.registry.Create("large-file", "Boundary", t.TempDir(), "codex", "", "read-only", "")
			c := newTestClient(h)
			c.deviceID = "large-fixture"
			c.uploads = newAttachmentUploads(c, h.cfg.DataDir)
			defer c.uploads.close()
			size := mediaVideoMaxBytes
			name, mime := "boundary.mov", "video/quicktime"
			if kind == "file" {
				size = maxFileUploadBytes
				name, mime = "archive.zip", "application/zip"
			}
			c.uploads.initKind("large-file", "exact-limit", name, mime, size, kind)
			ready := waitForType(t, c, "attachment_upload_ready")
			if ready["protocol_version"] != float64(2) || ready["received_bytes"] != float64(0) {
				t.Fatal(ready)
			}
			c.uploads.initKind("large-file", "over-limit", name, mime, size+1, kind)
			failed := waitForType(t, c, "attachment_upload_error")
			if !strings.Contains(failed["message"].(string), "2048 MB") && !strings.Contains(failed["message"].(string), "1 GiB") {
				t.Fatal(failed)
			}
		})
	}
}
