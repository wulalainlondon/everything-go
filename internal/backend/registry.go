package backend

// DefaultRegistry returns the built-in backend/model registry for this bridge
// process. Remote WS is opt-in because it is only valid when the corresponding
// backend transport URL is configured.
func DefaultRegistry(includeRemoteWS bool) []Definition {
	backends := []Definition{
		{
			ID:           Claude,
			Label:        "Claude",
			DefaultModel: "opus",
			Models: []Model{
				{ID: "sonnet", Label: "sonnet"},
				{ID: "opus", Label: "opus"},
				{ID: "opusplan", Label: "opus · plan"},
				{ID: "fable", Label: "fable"},
			},
			Capabilities: Capabilities{
				History: true, Usage: true, Interactions: true, Sandbox: true, Images: true, Files: true,
			},
		},
		{
			ID:           Codex,
			Label:        "Codex",
			DefaultModel: "gpt-5.6-sol",
			Models: []Model{
				{ID: "gpt-5.6-sol", Label: "gpt-5.6-sol"},
				{ID: "gpt-5.6-terra", Label: "gpt-5.6-terra"},
				{ID: "gpt-5.6-luna", Label: "gpt-5.6-luna"},
				{ID: "gpt-5.5", Label: "gpt-5.5"},
				{ID: "gpt-5.4", Label: "gpt-5.4"},
				{ID: "gpt-5.4-mini", Label: "gpt-5.4-mini"},
				{ID: "gpt-5.3-codex-spark", Label: "gpt-5.3-codex-spark"},
			},
			Capabilities: Capabilities{
				History: true, Usage: true, Sandbox: true, Files: true,
			},
		},
		{
			ID:           Ollama,
			Label:        "Ollama",
			DefaultModel: "qwen2.5:7b",
			Models: []Model{
				{ID: "qwen2.5:7b"}, {ID: "llama3.2"}, {ID: "llama3.1"}, {ID: "gemma3"},
				{ID: "qwen3"}, {ID: "mistral"}, {ID: "deepseek-r1"}, {ID: "phi4"},
			},
			Capabilities: Capabilities{},
		},
	}
	if includeRemoteWS {
		backends = append(backends, Definition{
			ID:    RemoteWS,
			Label: "Remote WS",
			Capabilities: Capabilities{
				History: true, Usage: true, Interactions: true, Remote: true,
			},
		})
	}
	return backends
}
