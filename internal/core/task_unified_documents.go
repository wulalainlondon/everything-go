package core

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	originalcontract "everything-go/contracts/task-api/v1"
	"everything-go/internal/messagequeue"
	"everything-go/internal/taskapi"
	"os"
	"path/filepath"
	"time"
)

func unifiedMutation(c taskapi.VerifiedContext, manager string, req taskapi.Request, check func(context.Context) error) taskapi.QueueMutation {
	return taskapi.QueueMutation{Scope: c.StableScopeID + "\x00" + manager, Generation: c.NamespaceGeneration, Intent: req.IdempotencyKey, Operation: req.Operation, Hash: unifiedHash(req), Revalidate: check}
}
func (h *Hub) unifiedGeneralOperation(ctx context.Context, c taskapi.VerifiedContext, manager string, in unifiedInput, check func(context.Context) error) (any, error) {
	m := taskapi.QueueMutation{Scope: c.StableScopeID + "\x00" + manager, Generation: c.NamespaceGeneration, Intent: in.OriginalKey, Operation: in.OriginalOperation}

	var receipts []taskapi.MutationReceipt
	lookups := []func(context.Context, taskapi.QueueMutation) (taskapi.MutationReceipt, bool, error){h.messageQueue.UnifiedOperation}
	if h.delegations != nil {
		lookups = append(lookups, h.delegations.UnifiedOperation)
	}
	if h.dispatches != nil {
		lookups = append(lookups, h.dispatches.UnifiedOperation)
	}
	for _, lookup := range lookups {
		r, found, e := lookup(ctx, m)
		if e != nil {
			return nil, e
		}
		if found {
			receipts = append(receipts, r)
		}
	}
	for _, j := range h.apiJournals() {
		p, found, e := j.UnifiedFacadeRead(ctx, m)
		if e != nil {
			return nil, e
		}
		if !found {
			continue
		}
		bound, e := taskapi.BindCaller(ctx, apiVerifier{func() (taskapi.VerifiedContext, error) {
			if e := check(ctx); e != nil {
				return c, e
			}
			original := c
			original.TaskSchemaHash = originalcontract.Hash()
			return original, nil
		}}, taskapi.Invocation{})
		if e != nil {
			return nil, e
		}
		r, e := h.recoverUnifiedFacade(ctx, bound, j, m, p)
		if e != nil {
			return nil, e
		}
		receipts = append(receipts, r.(taskapi.MutationReceipt))
	}
	if len(receipts) == 0 {
		return nil, unifiedError("not_found")
	}
	if len(receipts) > 1 {
		return nil, unifiedError("ambiguous_owner")
	}
	if e := check(ctx); e != nil {
		return nil, e
	}
	return receipts[0], nil
}

func (h *Hub) unifiedDocument(ctx context.Context, c taskapi.VerifiedContext, manager string, req taskapi.Request, in unifiedInput, check func(context.Context) error) (result any, err error) {
	m := unifiedMutation(c, manager, req, check)
	// A scoped consumer can ONLY read an exact immutable grant. It never gains the
	// original owner relation/catalog, caller mutation or filesystem permissions.
	if req.Operation == "read_document" && in.GrantID != "" {
		grant, e := h.messageQueue.UnifiedGrant(ctx, in.GrantID)
		if e != nil {
			if errors.Is(e, sql.ErrNoRows) {
				return nil, unifiedError("not_found")
			}
			return nil, e
		}
		var binding unifiedDocumentGrantBinding
		if json.Unmarshal(grant.Consumer, &binding) != nil {
			return nil, unifiedError("permission")
		}
		registerUnifiedDisclosure(ctx, func(ctx context.Context) error {
			latest, e := h.messageQueue.UnifiedGrant(ctx, in.GrantID)
			if e != nil {
				return e
			}
			return h.checkUnifiedDocumentConsumption(ctx, c, manager, latest, binding)
		})
		if grant.Document != in.DocumentID {
			return nil, unifiedError("permission")
		}
		if e = h.checkUnifiedDocumentConsumption(ctx, c, manager, grant, binding); e != nil {
			return nil, e
		}
		d, e := h.messageQueue.UnifiedDocument(ctx, in.DocumentID)
		if e != nil {
			return nil, e
		}
		if taskapi.RawDigest(d.Body) != d.Hash {
			return nil, unifiedError("unknown")
		}
		if d.Hash != in.Hash || in.Locator.SessionID != "" && in.Locator.SessionID != d.OwnerSession || in.Locator.RequestID != "" && in.Locator.RequestID != d.OwnerRequest {
			return nil, unifiedError("scope_changed")
		}
		result, e := unifiedChunk(in.Locator, d.OwnerRequest, d.SealID, d.Hash, d.Body, in)
		if e != nil {
			return nil, e
		}
		again, e := h.messageQueue.UnifiedGrant(ctx, in.GrantID)
		if e != nil {
			return nil, e
		}
		if e = h.checkUnifiedDocumentConsumption(ctx, c, manager, again, binding); e != nil {
			return nil, e
		}
		result.(map[string]any)["document_id"] = d.ID
		result.(map[string]any)["collector_path_identity"] = d.PathIdentity
		return result, nil
	}
	resolved, e := h.resolveUnified(ctx, c, manager, in.Locator)
	if e != nil {
		return nil, e
	}
	defer func() {
		if err == nil {
			if e := resolved.disclosureCurrent(ctx); e != nil {
				result = nil
				err = e
			}
		}
	}()
	if resolved.Parent.SessionID != "" && manager != resolved.Parent.SessionID && c.SourceSessionID != resolved.Parent.SessionID {
		return nil, unifiedError("permission")
	}
	sourceCheck := m.Revalidate
	m.Revalidate = func(ctx context.Context) error {
		if e := sourceCheck(ctx); e != nil {
			return e
		}
		return resolved.disclosureCurrent(ctx)
	}
	if req.Operation != "read_document" {
		if previous, found, e := h.messageQueue.UnifiedOperation(ctx, m); e != nil {
			return nil, e
		} else if found {
			return previous, nil
		}
	}
	if req.Operation == "document_collect" {
		if in.OwnerRequest != resolved.Locator.RequestID {
			return nil, unifiedError("ambiguous_owner")
		}
		owner, found := h.registry.Get(resolved.Locator.SessionID)
		if !found {
			return nil, unifiedError("scope_changed")
		}
		seal, e := h.jarvisResultSeal(ctx, owner, in.OwnerRequest, in.SealID)
		if e != nil {
			return nil, e
		}
		// Only explicit caller-selected paths are collected. Model-reported paths
		// remain untrusted reported metadata. This is a collection-time snapshot,
		// never a claim that the bytes existed at original execution time.
		target, ok := h.registry.Get(resolved.Locator.SessionID)
		if !ok {
			return nil, unifiedError("scope_changed")
		}
		root := target.Cwd()
		path, e := collaborationSourcePath(root, in.Path)
		if e != nil {
			return nil, unifiedError("permission")
		}
		if resolved.Canonical != nil {
			var metadata apiMetadata
			if json.Unmarshal(resolved.Canonical.Metadata, &metadata) == nil && len(metadata.CanonicalRoots) > 0 {
				allowed := false
				for _, allowedRoot := range metadata.CanonicalRoots {
					if pathInsideRoot(path, allowedRoot) {
						allowed = true
					}
				}
				if !allowed {
					return nil, unifiedError("permission")
				}
			}
		}
		body, pathIdentity, e := readUnifiedDocumentSnapshot(root, in.Path, 8*1024*1024)
		if e != nil {
			return nil, e
		}
		if taskapi.RawDigest(body) != in.Hash {
			return nil, unifiedError("stale_revision")
		}
		canonicalRoot, e := filepath.EvalSymlinks(root)
		if e != nil {
			return nil, e
		}
		info, e := os.Stat(canonicalRoot)
		if e != nil {
			return nil, e
		}
		identity, e := taskRootInfoIdentity(info)
		if e != nil {
			return nil, e
		}
		id := "doc_" + taskapi.Digest([]any{resolved.Locator, in.OwnerRequest, seal.ID, in.Hash, in.Path, identity, m.Scope, m.Generation, m.Intent})
		manifest, _ := json.Marshal(map[string]any{"kind": "trusted_collection_snapshot", "owner": resolved.Locator, "owner_request_id": in.OwnerRequest, "seal_id": seal.ID, "final_hash": seal.Hash, "full_hash": in.Hash, "total_bytes": len(body), "relative_path": in.Path, "root_identity": identity, "collected_at_ms": time.Now().UnixMilli(), "execution_time_snapshot": false})
		d := messagequeue.UnifiedDocument{ID: id, OwnerSession: resolved.Locator.SessionID, OwnerRequest: in.OwnerRequest, SealID: seal.ID, Hash: in.Hash, PathIdentity: pathIdentity, Manifest: manifest, Body: body}
		return h.messageQueue.UnifiedDocumentMutation(ctx, m, &d, nil, "", d)
	}
	if req.Operation == "document_revoke" {
		grant, grantErr := h.messageQueue.UnifiedGrant(ctx, in.GrantID)
		if grantErr != nil {
			return nil, grantErr
		}
		if grant.OwnerScope != m.Scope || grant.OwnerGeneration != m.Generation {
			return nil, unifiedError("permission")
		}
		in.DocumentID = grant.Document
	}
	d, e := h.messageQueue.UnifiedDocument(ctx, in.DocumentID)
	if e != nil {
		if errors.Is(e, sql.ErrNoRows) {
			return nil, unifiedError("not_found")
		}
		return nil, e
	}
	if d.OwnerSession != resolved.Locator.SessionID || d.OwnerRequest != resolved.Locator.RequestID {
		return nil, unifiedError("permission")
	}
	switch req.Operation {
	case "document_grant":
		if e = h.unifiedTarget(in.Consumer, false); e != nil {
			return nil, e
		}
		if in.Expires <= time.Now().UnixMilli() || in.Expires > time.Now().Add(24*time.Hour).UnixMilli() {
			return nil, unifiedError("invalid_argument")
		}
		binding, bindingErr := h.newUnifiedDocumentGrantBinding(ctx, c, manager, resolved, in.Consumer)
		if bindingErr != nil {
			return nil, bindingErr
		}
		b, _ := json.Marshal(binding)
		id := "dgr_" + taskapi.Digest([]any{m.Scope, m.Generation, m.Intent, d.ID})
		g := messagequeue.UnifiedDocumentGrant{ID: id, Document: d.ID, OwnerScope: m.Scope, OwnerGeneration: m.Generation, Consumer: b, Purpose: in.Purpose, Expires: in.Expires}
		originalCheck := m.Revalidate
		m.Revalidate = func(ctx context.Context) error {
			if e := originalCheck(ctx); e != nil {
				return e
			}
			return h.checkUnifiedDocumentGrant(ctx, c, in.Consumer.SessionID, g, binding)
		}
		return h.messageQueue.UnifiedDocumentMutation(ctx, m, nil, &g, "", map[string]any{"grant_id": id, "document_id": d.ID, "consumer": in.Consumer, "purpose": in.Purpose, "expires_at": in.Expires, "read_only": true})
	case "document_revoke":
		return h.messageQueue.UnifiedDocumentMutation(ctx, m, nil, nil, in.GrantID, map[string]any{"grant_id": in.GrantID, "revoked": true})
	case "read_document":
		if taskapi.RawDigest(d.Body) != d.Hash {
			return nil, unifiedError("unknown")
		}
		if d.Hash != in.Hash {
			return nil, unifiedError("stale_revision")
		}
		result, e := unifiedChunk(resolved.Locator, d.OwnerRequest, d.SealID, d.Hash, d.Body, in)
		if e == nil {
			result.(map[string]any)["document_id"] = d.ID
			result.(map[string]any)["collector_path_identity"] = d.PathIdentity
		}
		return result, e
	}
	return nil, unifiedError("unsupported")
}
