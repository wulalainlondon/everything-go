package goexec

import "testing"

func TestTaskPipelineVersionedCatalogKeepsRc7AndRc8Exact(t *testing.T) {
	for _, v := range []string{"1.0.0-rc.7", "1.0.0-rc.8"} {
		if !taskToolMatches("task_pipeline", "task_api_pipeline_request", v, "capabilities") {
			t.Fatal(v)
		}
	}
	for _, c := range [][4]string{{"task_unified", "task_api_pipeline_request", "1.0.0-rc.8", "capabilities"}, {"task_capabilities", "task_api_pipeline_request", "1.0.0-rc.8", "capabilities"}, {"task_pipeline", "task_api_request", "1.0.0-rc.8", "capabilities"}, {"task_pipeline", "task_api_pipeline_request", "1.0.0-rc.9", "capabilities"}} {
		if taskToolMatches(c[0], c[1], c[2], c[3]) {
			t.Fatal("tool/version masquerade", c)
		}
	}
}
