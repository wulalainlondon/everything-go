package core

import (
	"everything-go/internal/protocol"
	"everything-go/internal/session"
)

// get_agent_tree returns the subagent hierarchy for a session's latest turn.
// Mirrors system_ops.py's handler: resolve the session's backend, ask it to
// build the tree, send agent_tree back to the requesting client. Read-only.

// agentTreeBuilder is the optional capability a history provider implements to
// reconstruct a session's subagent tree (only the Claude backend does).
type agentTreeBuilder interface {
	BuildAgentTree(resumeID string) (int, []*protocol.AgentNode)
}

func (h *Hub) handleAgentTree(c *Client, sessionID string) {
	s, ok := h.registry.Get(sessionID)
	if !ok {
		return
	}
	resumeID := s.ResumeID()
	if resumeID == "" {
		return
	}
	builder, ok := h.agentTreeBuilderFor(s)
	if !ok {
		return
	}
	total, tree := builder.BuildAgentTree(resumeID)
	c.enqueueEvent(protocol.NewAgentTree(sessionID, resumeID, total, tree))
}

// agentTreeBuilderFor resolves the session's backend provider and type-asserts
// the tree-building capability.
func (h *Hub) agentTreeBuilderFor(s *session.Session) (agentTreeBuilder, bool) {
	hr, ok := h.exec.(historyRouter)
	if !ok {
		return nil, false
	}
	prov, ok := hr.ProviderFor(s)
	if !ok {
		return nil, false
	}
	b, ok := prov.(agentTreeBuilder)
	return b, ok
}
