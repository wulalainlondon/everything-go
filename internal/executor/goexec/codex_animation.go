package goexec

// Enable the available GIF output mode only on an ordinary worker thread and
// only on a qualified runtime. It doesn't request an image or alter global config.
func (c *Codex) applyAnimationThreadConfig(params map[string]any) {
	c.runtimeMu.Lock()
	diag, _ := c.runtimeDiagnostics["codex"].(map[string]any)
	version, _ := diag["running_version"].(string)
	c.runtimeMu.Unlock()
	if !audioRuntimeVersion(version) {
		return
	}
	config, _ := params["config"].(map[string]any)
	if config == nil {
		config = map[string]any{}
		params["config"] = config
	}
	// Flat dotted overrides preserve unrelated thread settings and native policy.
	config["features.image_generation_animation"] = true
}
