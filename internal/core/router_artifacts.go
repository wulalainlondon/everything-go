package core

import (
	"strings"
	"time"

	"everything-go/internal/artifacts"
	"everything-go/internal/clientproto"
	rt "everything-go/internal/runtime"
)

func (h *Hub) sendArtifactsList(c *Client, cmd clientproto.Command) {
	limit := cmd.Limit
	if limit <= 0 {
		limit = 100
	}
	var roots []string
	if strings.TrimSpace(cmd.Path) != "" {
		requested := rt.ExpandPath(cmd.Path)
		if h.cfg.RootDir != "" && !pathInsideRoot(requested, h.cfg.RootDir) {
			c.enqueueEvent(h.client.ArtifactsList(nil))
			return
		}
		roots = []string{requested}
	} else {
		roots = artifacts.DefaultRoots(h.cfg.RootDir)
	}
	c.enqueueEvent(h.client.ArtifactsList(artifacts.Scan(roots, limit, h.mediaScan.LocalURL)))
}

func (h *Hub) startYouTubeTask(c *Client, cmd clientproto.Command) {
	taskID, task := artifacts.NewTask(strings.TrimSpace(cmd.URL), cmd.SessionID)
	c.enqueueEvent(h.client.YouTubeTaskStarted(taskID, task))
	items, err := artifacts.DownloadYouTube(cmd.URL, cmd.SessionID, taskID, h.mediaScan.LocalURL)
	if err != nil {
		task.Status = "failed"
		task.UpdatedAt = time.Now().Unix()
		c.enqueueEvent(h.client.YouTubeTaskFailed(taskID, task, err.Error()))
		return
	}
	c.enqueueEvent(h.client.YouTubeTaskDone(taskID, items))
}
