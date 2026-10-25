package workspace

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// Overview is a read projection, never a new task/approval engine. Counts cover
// the entire authenticated filter; pagination only limits displayed rows.
type OverviewCounts struct {
	NeedsMe         int64 `json:"needs_me"`
	NeedsReply      int64 `json:"needs_reply"`
	NeedsAcceptance int64 `json:"needs_acceptance"`
	Active          int64 `json:"active"`
	Running         int64 `json:"running"`
	Waiting         int64 `json:"waiting"`
	Unknown         int64 `json:"unknown"`
	Unread          int64 `json:"unread"`
}
type OverviewTarget struct {
	ID    string `json:"id"`
	Kind  string `json:"kind"`
	Title string `json:"title"`
}
type OverviewPrimaryItem struct {
	ID        string `json:"id"`
	Kind      string `json:"kind"`
	Title     string `json:"title"`
	Intent    string `json:"intent"`
	RowState  string `json:"row_state"`
	UpdatedAt int64  `json:"updated_at"`
}
type OverviewRoom struct {
	Preview      string               `json:"preview,omitempty"`
	RowState     string               `json:"row_state,omitempty"`
	PrimaryItem  *OverviewPrimaryItem `json:"primary_item,omitempty"`
	Room         Room                 `json:"room"`
	Role         string               `json:"role"`
	LastActivity int64                `json:"last_activity"`
	Counts       OverviewCounts       `json:"counts"`
	Targets      []OverviewTarget     `json:"targets,omitempty"`
}
type OverviewCatalog struct {
	Items      []Workspace `json:"items"`
	HasMore    bool        `json:"has_more"`
	NextCursor string      `json:"next_cursor,omitempty"`
	Coverage   string      `json:"coverage"`
}
type Overview struct {
	SummaryTotals  *OverviewCounts `json:"summary_totals,omitempty"`
	FilteredTotals *OverviewCounts `json:"filtered_totals,omitempty"`
	SummaryScope   string          `json:"summary_scope,omitempty"`
	SchemaVersion  int             `json:"schema_version"`
	Authority      string          `json:"authority"`
	PrincipalID    string          `json:"principal_id"`
	AsOf           int64           `json:"as_of"`
	ScopeRevision  string          `json:"scope_revision"`
	Coverage       string          `json:"coverage"`
	CountsScope    string          `json:"counts_scope"`
	Totals         OverviewCounts  `json:"totals"`
	Rooms          []OverviewRoom  `json:"rooms"`
	Catalog        OverviewCatalog `json:"workspace_catalog"`
	HasMore        bool            `json:"has_more"`
	NextCursor     string          `json:"next_cursor,omitempty"`
}
type overviewInput struct {
	PresentationVersion int    `json:"presentation_version"`
	Sort                string `json:"sort"`
	View                string `json:"view"`
	CatalogCursor       string `json:"catalog_cursor"`
	CatalogLimit        int    `json:"catalog_limit"`
}

// e and wf are aliases shared by overview and the actual acceptance guard.
const taskAcceptancePredicate = `json_extract(e.body,'$.axes.final')='sealed'
 AND COALESCE(json_extract(e.body,'$.final_hash'),'')<>''
 AND COALESCE(json_extract(e.body,'$.axes.qa'),'') NOT IN ('FAIL','pending','unknown')
 AND (COALESCE(json_extract(e.body,'$.workflow_id'),'')='' OR
 (json_extract(wf.body,'$.state')='awaiting_human' AND json_extract(wf.body,'$.latest_draft')=e.id))`

func taskAcceptanceAvailable(ctx context.Context, q sqlReader, room, id string) (bool, error) {
	var yes bool
	err := q.QueryRowContext(ctx, `SELECT COALESCE((`+taskAcceptancePredicate+`),0) FROM entities e LEFT JOIN entities wf ON wf.id=json_extract(e.body,'$.workflow_id') AND wf.room_id=e.room_id AND wf.kind='workflow' WHERE e.id=? AND e.room_id=? AND e.kind='task'`, id, room).Scan(&yes)
	return yes, err
}

func (s *Store) overviewScope(ctx context.Context, q sqlReader, a Actor) (string, error) {
	// Only caller-visible membership/workspace versions contribute. A revoked
	// room, changed ACL/role or rebound identity invalidates every old cursor.
	rows, err := q.QueryContext(ctx, `SELECT r.id,r.acl_revision,m.revision,m.role,r.archived,r.expires_at FROM rooms r JOIN members m ON m.room_id=r.id WHERE m.principal_id=? AND m.active=1 ORDER BY r.id`, a.ID)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	var witness strings.Builder
	for rows.Next() {
		var id, role string
		var acl, rev, expires int64
		var archived bool
		if err = rows.Scan(&id, &acl, &rev, &role, &archived, &expires); err != nil {
			return "", err
		}
		b, _ := json.Marshal([]any{id, acl, rev, role, archived, expires})
		witness.Write(b)
	}
	if err = rows.Err(); err != nil {
		return "", err
	}
	rows.Close()
	wr, err := q.QueryContext(ctx, `SELECT DISTINCT w.id,w.revision FROM workspaces w WHERE w.owner=? OR EXISTS(SELECT 1 FROM rooms r JOIN members m ON m.room_id=r.id WHERE r.workspace_id=w.id AND m.principal_id=? AND m.active=1) ORDER BY w.id`, a.ID, a.ID)
	if err != nil {
		return "", err
	}
	defer wr.Close()
	for wr.Next() {
		var id string
		var rev int64
		if err = wr.Scan(&id, &rev); err != nil {
			return "", err
		}
		b, _ := json.Marshal([]any{id, rev})
		witness.Write(b)
	}
	if err = wr.Err(); err != nil {
		return "", err
	}
	return digest([]byte(witness.String())), nil
}

func (s *Store) overview(ctx context.Context, a Actor, c Command) (Result, error) {
	input := overviewInput{View: "channels", CatalogLimit: 30}
	if len(c.Data) > 0 {
		if err := decode(c.Data, &input); err != nil {
			return Result{}, err
		}
	}
	if input.View == "" {
		input.View = "channels"
	}
	if input.PresentationVersion != 0 && input.PresentationVersion != 2 {
		return Result{}, errors.New("overview_version_invalid")
	}
	compact := input.PresentationVersion == 2
	if input.Sort != "" && input.Sort != "activity" {
		return Result{}, errors.New("overview_sort_invalid")
	}
	switch input.View {
	case "needs_reply", "needs_acceptance":
		if !compact {
			return Result{}, errors.New("overview_filter_invalid")
		}
	case "channels", "needs_me", "active", "unread", "recent", "archived":
	default:
		return Result{}, errors.New("overview_filter_invalid")
	}
	if len(c.Query) > 200 {
		return Result{}, errors.New("overview_query_limit")
	}
	limit := c.Limit
	if limit <= 0 {
		limit = 30
	}
	if limit > 100 {
		limit = 100
	}
	catalogLimit := input.CatalogLimit
	if catalogLimit <= 0 {
		catalogLimit = 30
	}
	if catalogLimit > 100 {
		catalogLimit = 100
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return Result{}, err
	}
	defer tx.Rollback()
	if err = s.actorOK(ctx, tx, a); err != nil {
		return Result{}, err
	}
	scope, err := s.overviewScope(ctx, tx, a)
	if err != nil {
		return Result{}, err
	}
	now := s.now().UnixMilli()
	fingerprintInput := c.WorkspaceID + "\x00" + input.View + "\x00" + c.Query
	if compact {
		fingerprintInput += "\x00presentation=2\x00sort=activity"
	}
	fingerprint := digest([]byte(fingerprintInput))
	parseCursor := func(token, kind string) (cursor, error) {
		v, e := s.decodeCursor(token)
		if e != nil {
			return v, e
		}
		if v.Actor != a.ID || v.Kind != kind || v.Room != c.WorkspaceID || v.Thread != fingerprint || v.Scope != scope || v.Expires < now {
			return v, errors.New("overview_cursor_scope_changed")
		}
		return v, nil
	}
	var page cursor
	if c.Cursor != "" {
		page, err = parseCursor(c.Cursor, "overview")
		if err != nil {
			return Result{}, err
		}
	}
	out := &Overview{SchemaVersion: 1, Authority: s.authority, PrincipalID: a.ID, AsOf: now, ScopeRevision: scope, Coverage: "complete", CountsScope: "authorized_workspace_filter", Rooms: []OverviewRoom{}, Catalog: OverviewCatalog{Items: []Workspace{}, Coverage: "complete"}}
	// Named input values avoid duplicating credentials in SQL or result bodies.
	base := `WITH input AS (SELECT ? AS actor,? AS owner,? AS now),
 visible AS (SELECT r.*,m.role,COALESCE(p.pinned,0) AS pinned,COALESCE(p.revision,0) AS preference_revision FROM rooms r JOIN members m ON m.room_id=r.id LEFT JOIN room_preferences p ON p.room_id=r.id AND p.principal_id=m.principal_id WHERE m.principal_id=(SELECT actor FROM input) AND m.active=1),
 facts AS (SELECT e.*,v.role,v.archived,v.expires_at,
 CASE WHEN e.kind='human_todo' AND v.role IN ('owner','admin','editor','reviewer') AND json_extract(e.body,'$.state')='open' AND json_extract(e.body,'$.assignee')=(SELECT actor FROM input) THEN 1 ELSE 0 END AS reply,
 CASE WHEN v.role IN ('owner','admin','editor','reviewer') AND ((e.kind='human_todo' AND json_extract(e.body,'$.state')='answered' AND (e.created_by=(SELECT actor FROM input) OR v.role IN ('owner','admin')))
 OR (e.kind='task' AND COALESCE(json_extract(e.body,'$.axes.human_acceptance'),'')='unreviewed' AND (` + taskAcceptancePredicate + `))) THEN 1 ELSE 0 END AS acceptance,
 CASE WHEN (SELECT owner FROM input)=1 AND v.role IN ('owner','admin','editor') AND e.kind='task' AND json_extract(e.body,'$.admission')='awaiting_owner' AND json_extract(e.body,'$.manifest.acl_revision')=v.acl_revision AND EXISTS(SELECT 1 FROM members creator WHERE creator.room_id=e.room_id AND creator.principal_id=json_extract(e.body,'$.created_by') AND creator.active=1) THEN 1 ELSE 0 END AS approval,
 CASE WHEN (SELECT owner FROM input)=1 AND v.role IN ('owner','admin','editor','reviewer') AND e.kind='effect' AND json_extract(e.body,'$.state')='draft' AND json_extract(e.body,'$.expires_at')>(SELECT now FROM input) THEN 1 ELSE 0 END AS effect_approval
 FROM entities e JOIN visible v ON v.id=e.room_id LEFT JOIN entities wf ON wf.id=json_extract(e.body,'$.workflow_id') AND wf.room_id=e.room_id AND wf.kind='workflow'),
 stats AS (SELECT room_id,MAX(updated_at) AS activity,
 SUM(CASE WHEN (archived=0 AND (expires_at=0 OR expires_at>(SELECT now FROM input))) AND (reply=1 OR acceptance=1 OR approval=1 OR effect_approval=1) THEN 1 ELSE 0 END) AS needs_me,
 SUM(CASE WHEN archived=0 AND (expires_at=0 OR expires_at>(SELECT now FROM input)) THEN reply ELSE 0 END) AS needs_reply,
 SUM(CASE WHEN archived=0 AND (expires_at=0 OR expires_at>(SELECT now FROM input)) THEN acceptance ELSE 0 END) AS needs_acceptance,
 SUM(CASE WHEN kind='task' AND json_extract(body,'$.axes.execution')='running' THEN 1 ELSE 0 END) AS running,
 SUM(CASE WHEN kind='task' AND json_extract(body,'$.axes.execution')='waiting' THEN 1 ELSE 0 END) AS waiting,
 SUM(CASE WHEN kind='task' AND json_extract(body,'$.axes.execution')='unknown' THEN 1 ELSE 0 END) AS unknown,
 SUM(CASE WHEN kind='message' AND json_extract(body,'$.deleted')=0 AND NOT EXISTS(SELECT 1 FROM read_messages seen WHERE seen.room_id=facts.room_id AND seen.principal_id=(SELECT actor FROM input) AND seen.message_id=facts.id) THEN 1 ELSE 0 END) AS unread
 FROM facts GROUP BY room_id),
 projected AS (SELECT v.*,MAX(v.created_at,COALESCE(stats.activity,v.created_at)) AS last_activity,COALESCE(needs_me,0) AS needs_me,COALESCE(needs_reply,0) AS needs_reply,COALESCE(needs_acceptance,0) AS needs_acceptance,COALESCE(running,0) AS running,COALESCE(waiting,0) AS waiting,COALESCE(unknown,0) AS unknown,COALESCE(unread,0) AS unread FROM visible v LEFT JOIN stats ON stats.room_id=v.id),
 scoped AS (SELECT * FROM projected WHERE (?='' OR workspace_id=?) AND (instr(lower(name||' '||description),lower(?))>0 OR ?='')),
 filtered AS (SELECT * FROM scoped WHERE `
	args := []any{a.ID, a.Owner, now, c.WorkspaceID, c.WorkspaceID, c.Query, c.Query}
	if input.View == "archived" {
		base += `(archived=1 OR (expires_at>0 AND expires_at<=` + fmt.Sprint(now) + `))`
	} else {
		base += `archived=0 AND (expires_at=0 OR expires_at>` + fmt.Sprint(now) + `)`
	}
	switch input.View {
	case "needs_reply":
		base += ` AND needs_reply>0`
	case "needs_acceptance":
		base += ` AND needs_acceptance>0`
	case "needs_me":
		base += ` AND needs_me>0`
	case "active":
		base += ` AND (running+waiting)>0`
	case "unread":
		base += ` AND unread>0`
	}
	base += `) `
	var totals OverviewCounts
	err = tx.QueryRowContext(ctx, base+`SELECT COALESCE(SUM(needs_me),0),COALESCE(SUM(needs_reply),0),COALESCE(SUM(needs_acceptance),0),COALESCE(SUM(running+waiting),0),COALESCE(SUM(running),0),COALESCE(SUM(waiting),0),COALESCE(SUM(unknown),0),COALESCE(SUM(unread),0) FROM filtered`, args...).Scan(&totals.NeedsMe, &totals.NeedsReply, &totals.NeedsAcceptance, &totals.Active, &totals.Running, &totals.Waiting, &totals.Unknown, &totals.Unread)
	if err != nil {
		return Result{}, err
	}
	out.Totals = totals
	if compact {
		out.SchemaVersion = 2
		out.FilteredTotals = &totals
		out.SummaryScope = "authorized_workspace_query_active"
		summary := new(OverviewCounts)
		err = tx.QueryRowContext(ctx, base+`SELECT COALESCE(SUM(needs_me),0),COALESCE(SUM(needs_reply),0),COALESCE(SUM(needs_acceptance),0),COALESCE(SUM(running+waiting),0),COALESCE(SUM(running),0),COALESCE(SUM(waiting),0),COALESCE(SUM(unknown),0),COALESCE(SUM(unread),0) FROM scoped WHERE archived=0 AND (expires_at=0 OR expires_at>(SELECT now FROM input))`, args...).Scan(&summary.NeedsMe, &summary.NeedsReply, &summary.NeedsAcceptance, &summary.Active, &summary.Running, &summary.Waiting, &summary.Unknown, &summary.Unread)
		if err != nil {
			return Result{}, err
		}
		out.SummaryTotals = summary
	}
	query := base + `SELECT id,workspace_id,name,description,owner,revision,acl_revision,archived,expires_at,created_at,project_id,pinned,preference_revision,role,last_activity,needs_me,needs_reply,needs_acceptance,running,waiting,unknown,unread FROM filtered`
	pageArgs := append([]any{}, args...)
	if c.Cursor != "" {
		query += ` WHERE (pinned<? OR (pinned=? AND (last_activity<? OR (last_activity=? AND id>?))))`
		pageArgs = append(pageArgs, page.Pinned, page.Pinned, page.Before, page.Before, page.Key)
	}
	query += ` ORDER BY pinned DESC,last_activity DESC,id LIMIT ?`
	pageArgs = append(pageArgs, limit+1)
	rows, err := tx.QueryContext(ctx, query, pageArgs...)
	if err != nil {
		return Result{}, err
	}
	for rows.Next() {
		var v OverviewRoom
		r := &v.Room
		n := &v.Counts
		if err = rows.Scan(&r.ID, &r.WorkspaceID, &r.Name, &r.Description, &r.Owner, &r.Revision, &r.ACLRevision, &r.Archived, &r.ExpiresAt, &r.CreatedAt, &r.ProjectID, &r.Pinned, &r.PreferenceRevision, &v.Role, &v.LastActivity, &n.NeedsMe, &n.NeedsReply, &n.NeedsAcceptance, &n.Running, &n.Waiting, &n.Unknown, &n.Unread); err != nil {
			rows.Close()
			return Result{}, err
		}
		n.Active = n.Running + n.Waiting
		out.Rooms = append(out.Rooms, v)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return Result{}, err
	}
	makeCursor := func(kind, key string, before int64, pinned bool) (string, error) {
		return s.encodeCursor(cursor{Actor: a.ID, Room: c.WorkspaceID, Kind: kind, Thread: fingerprint, Scope: scope, Key: key, Before: before, Pinned: pinned, Expires: s.now().Add(15 * time.Minute).UnixMilli()})
	}
	if len(out.Rooms) > limit {
		out.HasMore = true
		out.Rooms = out.Rooms[:limit]
		last := out.Rooms[limit-1]
		out.NextCursor, err = makeCursor("overview", last.Room.ID, last.LastActivity, last.Room.Pinned)
		if err != nil {
			return Result{}, err
		}
	}
	// Resolve only a bounded set of real actionable entity links for displayed
	// rows. Full totals above remain independent of this preview limit.
	if len(out.Rooms) > 0 {
		placeholders := strings.TrimSuffix(strings.Repeat("?,", len(out.Rooms)), ",")
		targetArgs := append([]any{}, args...)
		roomIndexes := map[string]int{}
		for i, v := range out.Rooms {
			targetArgs = append(targetArgs, v.Room.ID)
			roomIndexes[v.Room.ID] = i
		}
		targetOrder := "f.updated_at DESC,f.id"
		if compact {
			targetOrder = "CASE WHEN f.reply=1 THEN 0 WHEN f.acceptance=1 THEN 1 ELSE 2 END,f.updated_at DESC,f.id"
		}
		targetQuery := base + `, target_rows AS (
          SELECT f.room_id,f.id,f.kind,substr(COALESCE(json_extract(f.body,'$.title'),''),1,160) AS title,
          f.updated_at,CASE WHEN f.reply=1 THEN 'view_question' WHEN f.acceptance=1 THEN 'view_result' ELSE 'view_approval' END AS intent,
          ROW_NUMBER() OVER(PARTITION BY f.room_id ORDER BY ` + targetOrder + `) AS position
          FROM facts f JOIN filtered v ON v.id=f.room_id
          WHERE v.archived=0 AND (v.expires_at=0 OR v.expires_at>(SELECT now FROM input))
          AND (f.reply=1 OR f.acceptance=1 OR f.approval=1 OR f.effect_approval=1)
          AND f.room_id IN (` + placeholders + `)
        ) SELECT room_id,id,kind,title,updated_at,intent FROM target_rows WHERE position<=3 ORDER BY room_id,position`
		targetRows, e := tx.QueryContext(ctx, targetQuery, targetArgs...)
		if e != nil {
			return Result{}, e
		}
		for targetRows.Next() {
			var room string
			var target OverviewTarget
			var updated int64
			var intent string
			if e = targetRows.Scan(&room, &target.ID, &target.Kind, &target.Title, &updated, &intent); e != nil {
				targetRows.Close()
				return Result{}, e
			}
			i := roomIndexes[room]
			out.Rooms[i].Targets = append(out.Rooms[i].Targets, target)
			if compact && out.Rooms[i].PrimaryItem == nil {
				state := "needs_approval"
				if intent == "view_question" {
					state = "needs_reply"
				} else if intent == "view_result" {
					state = "needs_acceptance"
				}
				out.Rooms[i].PrimaryItem = &OverviewPrimaryItem{ID: target.ID, Kind: target.Kind, Title: target.Title, Intent: intent, RowState: state, UpdatedAt: updated}
			}
		}
		e = targetRows.Err()
		targetRows.Close()
		if e != nil {
			return Result{}, e
		}
	}
	if compact {
		for i := range out.Rooms {
			row := &out.Rooms[i]
			row.Preview = row.Room.Description
			if row.PrimaryItem != nil {
				row.RowState = row.PrimaryItem.RowState
				row.Preview = row.PrimaryItem.Title
			} else if row.Room.Archived || (row.Room.ExpiresAt > 0 && row.Room.ExpiresAt <= now) {
				row.RowState = "archived"
			} else if row.Counts.Running > 0 {
				row.RowState = "running"
			} else if row.Counts.Waiting > 0 {
				row.RowState = "waiting"
			} else if row.Counts.Unknown > 0 {
				row.RowState = "unknown"
			} else {
				row.RowState = "idle"
			}
			row.Preview = string([]rune(row.Preview)[:min(len([]rune(row.Preview)), 160)])
		}
	}
	after := ""
	if input.CatalogCursor != "" {
		v, e := parseCursor(input.CatalogCursor, "overview_catalog")
		if e != nil {
			return Result{}, e
		}
		after = v.Key
	}
	catalog, err := tx.QueryContext(ctx, `SELECT w.id,w.name,w.owner,w.revision FROM workspaces w WHERE w.id>? AND (w.owner=? OR EXISTS(SELECT 1 FROM rooms r JOIN members m ON m.room_id=r.id WHERE r.workspace_id=w.id AND m.principal_id=? AND m.active=1)) ORDER BY w.id LIMIT ?`, after, a.ID, a.ID, catalogLimit+1)
	if err != nil {
		return Result{}, err
	}
	for catalog.Next() {
		var w Workspace
		if err = catalog.Scan(&w.ID, &w.Name, &w.Owner, &w.Revision); err != nil {
			catalog.Close()
			return Result{}, err
		}
		out.Catalog.Items = append(out.Catalog.Items, w)
	}
	err = catalog.Err()
	catalog.Close()
	if err != nil {
		return Result{}, err
	}
	if len(out.Catalog.Items) > catalogLimit {
		out.Catalog.HasMore = true
		out.Catalog.Items = out.Catalog.Items[:catalogLimit]
		out.Catalog.NextCursor, err = makeCursor("overview_catalog", out.Catalog.Items[catalogLimit-1].ID, 0, false)
		if err != nil {
			return Result{}, err
		}
	}
	if err = tx.Commit(); err != nil {
		return Result{}, err
	}
	return Result{Action: "overview", PrincipalID: a.ID, Overview: out}, nil
}
