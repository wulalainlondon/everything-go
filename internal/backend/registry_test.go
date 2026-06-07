package backend

import "testing"

func TestDefaultRegistryRemoteWSIsOptIn(t *testing.T) {
	without := DefaultRegistry(false)
	for _, d := range without {
		if d.ID == RemoteWS {
			t.Fatalf("remote-ws should be absent without configuration")
		}
	}

	with := DefaultRegistry(true)
	var remote *Definition
	for i := range with {
		if with[i].ID == RemoteWS {
			remote = &with[i]
		}
	}
	if remote == nil {
		t.Fatalf("remote-ws should be advertised when configured")
	}
	if !remote.Capabilities.Remote || !remote.Capabilities.History || !remote.Capabilities.Interactions {
		t.Fatalf("remote-ws capabilities = %+v", remote.Capabilities)
	}
}

func TestDefaultRegistryOllamaUsesInstalledSmokeModel(t *testing.T) {
	defs := DefaultRegistry(false)
	var ollama *Definition
	for i := range defs {
		if defs[i].ID == Ollama {
			ollama = &defs[i]
		}
	}
	if ollama == nil {
		t.Fatalf("ollama backend missing")
	}
	if ollama.DefaultModel != "qwen2.5:7b" {
		t.Fatalf("ollama default = %q", ollama.DefaultModel)
	}
	if len(ollama.Models) == 0 || ollama.Models[0].ID != "qwen2.5:7b" {
		t.Fatalf("ollama models = %+v", ollama.Models)
	}
}
