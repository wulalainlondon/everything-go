package search

import "testing"

func TestRecommendedPluginsIsFrameworkNoise(t *testing.T) {
	if !isFrameworkNoise("<recommended_plugins>\nFramework-injected plugin list") {
		t.Fatal("recommended_plugins injection must be treated as framework noise")
	}
}
