package core

import (
	"bytes"
	"encoding/json"
	"errors"
	"everything-go/internal/clientproto"
	"everything-go/internal/messagequeue"
	"everything-go/internal/session"
	"io"
)

type humanMediaStage struct {
	Binding         messagequeue.HumanBinding `json:"binding"`
	Request         string                    `json:"request_id"`
	BaseRevision    uint64                    `json:"content_revision"`
	BaseHash        string                    `json:"expected_hash"`
	LeaseID         string                    `json:"lease_id"`
	LeaseGeneration uint64                    `json:"lease_generation"`
}
type humanMediaAdmission struct {
	Binding   messagequeue.HumanBinding   `json:"binding"`
	Operation messagequeue.HumanOperation `json:"operation"`
	Proof     string                      `json:"proof"`
}

// Caller holds the original media lifecycle lock. Only the exact prepared
// operation's SQLite receipt may settle this cross-store admission journal.
func (h *Hub) reconcileHumanMediaAdmission(batch *mediaBatch) error {
	if batch == nil || batch.State != "admitting_unknown" || batch.HumanAdmission == nil {
		return nil
	}
	original := batch.HumanAdmission
	receipt, err := h.messageQueue.ReadHumanOperation(original.Binding, original.Operation.ID, "")
	if err != nil {
		return err
	}
	var operation messagequeue.HumanOperation
	if receipt.Status == "unseen_unknown" {
		return nil
	}
	if json.Unmarshal(receipt.Body, &operation) != nil || mediaHash(operation) != mediaHash(original.Operation) || original.Proof != operation.ReadyProof || original.Proof != batch.BodyHash {
		return errors.New("original media admission receipt changed")
	}
	switch receipt.Status {
	case "rejected_known_none":
		batch.State = "prepared"
	case "committed":
		if receipt.Input == nil || receipt.Input.Binding != original.Binding || receipt.Input.ID != original.Operation.Request {
			return errors.New("original media admission tuple changed")
		}
		entry, found, err := h.messageQueue.HumanVersion(original.Binding, original.Operation.Request, receipt.Input.ContentRevision, receipt.Input.WireHash)
		if err != nil {
			return err
		}
		var payload queuedPayload
		if !found || json.Unmarshal(entry.Payload, &payload) != nil || !mediaQueueBinding(batch, payload) {
			return errors.New("original committed media version unavailable")
		}
		batch.State = "admitted"
	default:
		return errors.New("original media admission unresolved")
	}
	return h.media.save()
}

func (h *Hub) mediaHumanStage(c *Client, s *session.Session, cmd clientproto.Command, requireLive bool) (*humanMediaStage, error) {
	if len(cmd.QueueInput) == 0 {
		return nil, nil
	}
	var envelope struct {
		Binding messagequeue.HumanBinding `json:"binding"`
		Stage   struct {
			Request         string `json:"request_id"`
			BaseRevision    uint64 `json:"content_revision"`
			BaseHash        string `json:"expected_hash"`
			LeaseID         string `json:"lease_id"`
			LeaseGeneration uint64 `json:"lease_generation"`
		} `json:"stage"`
	}
	decoder := json.NewDecoder(bytes.NewReader(cmd.QueueInput))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&envelope) != nil || decoder.Decode(new(any)) != io.EOF {
		return nil, errors.New("invalid staged media")
	}
	stage := &humanMediaStage{Binding: envelope.Binding, Request: envelope.Stage.Request, BaseRevision: envelope.Stage.BaseRevision, BaseHash: envelope.Stage.BaseHash, LeaseID: envelope.Stage.LeaseID, LeaseGeneration: envelope.Stage.LeaseGeneration}
	if stage.Request == "" || stage.Request == cmd.MediaBatchID || stage.LeaseID == "" || stage.LeaseGeneration == 0 || requireLive && (h.humanInputBinding(c, s, stage.Binding, true) != nil || !h.mediaHumanStageLive(stage)) {
		return nil, errors.New("original staged edit changed")
	}
	return stage, nil
}
func (h *Hub) mediaHumanStageLive(stage *humanMediaStage) bool {
	if stage == nil {
		return true
	}
	if h.messageQueue == nil {
		return false
	}
	boot, now := h.messageQueue.HumanClock()
	return h.messageQueue.HumanStageAllowed(stage.Binding, stage.Request, stage.BaseRevision, stage.BaseHash, stage.LeaseID, stage.LeaseGeneration, boot, now)
}
func mediaBusinessID(batch *mediaBatch) string {
	if batch.HumanStage != nil {
		return batch.HumanStage.Request
	}
	return batch.ID
}
func humanStageSealMatches(stage *humanMediaStage, seal *messagequeue.HumanSeal) bool {
	return stage != nil && seal != nil && seal.Binding == stage.Binding && seal.Operation.Kind == "commit_edit" && seal.Operation.Request == stage.Request && seal.Operation.ContentRevision == stage.BaseRevision && seal.Operation.ExpectedHash == stage.BaseHash && seal.Operation.LeaseID == stage.LeaseID && seal.Operation.LeaseGeneration == stage.LeaseGeneration
}
