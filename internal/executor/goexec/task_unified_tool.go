package goexec

import (
	pipeline "everything-go/contracts/task-api/pipeline-v7"
	unified "everything-go/contracts/task-api/unified-v6"
)

// One tool carries a closed versioned operation union. Old one-tool-per-op
// catalogs remain exact; no arbitrary task_* name can call the unified handler.
func taskToolMatches(tool, kind, version, operation string) bool {
	if tool == "task_pipeline" {
		return kind == "task_api_pipeline_request" && version == pipeline.Version
	}
	if tool == "task_unified" {
		return kind == "task_api_unified_request" && version == unified.Version
	}
	return kind != "task_api_pipeline_request" && kind != "task_api_unified_request" && tool == "task_"+operation
}
