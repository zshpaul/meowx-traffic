package main

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// Each additional binding owns its settings, upload queue, quota queue and
// rate baseline. The original files remain the primary binding.
type extraBinding struct {
	store       *Store
	uploader    *Uploader
	quota       *quotaSync
	rate        nodeRateTracker
	rateStarted time.Time
}

// Creating a binding persists it for the next application start; it never
// changes the running primary binding or switches the proxy group.
func extraBindingRoute(dir string, m *Monitor, primary *Store) http.HandlerFunc {
	var mu sync.Mutex
	create := post(func(w http.ResponseWriter, r *http.Request) {
		var in struct {
			TargetNode string `json:"targetNode"`
			GroupAlias string `json:"groupAlias"`
			NodeAlias  string `json:"nodeAlias"`
			Confirm    bool   `json:"confirm"`
		}
		if json.NewDecoder(r.Body).Decode(&in) != nil || !in.Confirm ||
			strings.TrimSpace(in.TargetNode) != in.TargetNode || in.TargetNode == "" ||
			!validAlias(in.GroupAlias) || !validAlias(in.NodeAlias) || len([]rune(in.NodeAlias)) > 55 {
			http.Error(w, "invalid binding", http.StatusBadRequest)
			return
		}
		mu.Lock()
		defer mu.Unlock()
		if m.uploader == nil || m.quota == nil {
			http.Error(w, "upload unavailable", http.StatusServiceUnavailable)
			return
		}
		m.uploader.sendMu.Lock()
		defer m.uploader.sendMu.Unlock()
		s := primary.Get()
		u := s.Upload
		if !u.Enabled || u.GroupID == "" || u.Binding == "" || u.TokenCipher == "" ||
			in.TargetNode == u.TargetNode {
			http.Error(w, "primary binding or target invalid", http.StatusBadRequest)
			return
		}
		m.uploader.mu.Lock()
		primaryBound := u.Binding == m.uploader.queue.Binding && m.uploader.matchesDestination(u)
		m.uploader.mu.Unlock()
		if !primaryBound {
			http.Error(w, "primary upload binding unavailable", http.StatusConflict)
			return
		}
		m.mu.Lock()
		found := false
		for _, g := range m.latencyGroups {
			if m.connected && g.Endpoint == u.Source && g.Group == u.Group && g.TestURL == u.TargetURL {
				for _, member := range g.Members {
					if member == in.TargetNode {
						found = true
					}
				}
			}
		}
		m.mu.Unlock()
		if !found {
			http.Error(w, "target is not a member of the connected URLTest group", http.StatusBadRequest)
			return
		}
		root := filepath.Join(dir, "bindings")
		if entries, err := os.ReadDir(root); err == nil {
			for _, e := range entries {
				if !e.IsDir() || e.Type()&os.ModeSymlink != 0 {
					continue
				}
				b, err := os.ReadFile(filepath.Join(root, e.Name(), "settings.json"))
				var existing Settings
				if err == nil && json.Unmarshal(b, &existing) == nil &&
					existing.Upload.TargetNode == in.TargetNode {
					http.Error(w, "binding already exists", http.StatusConflict)
					return
				}
			}
		}
		id, err := randomID()
		if err != nil || id == u.GroupID {
			http.Error(w, "cannot create group identity", http.StatusInternalServerError)
			return
		}
		binding, err := randomID()
		if err != nil {
			http.Error(w, "cannot create binding", http.StatusInternalServerError)
			return
		}
		if err := os.MkdirAll(root, 0700); err != nil {
			http.Error(w, "cannot create binding directory", http.StatusInternalServerError)
			return
		}
		path := filepath.Join(root, id)
		if err := os.Mkdir(path, 0700); err != nil {
			http.Error(w, "cannot create binding directory", http.StatusInternalServerError)
			return
		}
		u.GroupID, u.Binding, u.TargetNode = id, binding, in.TargetNode
		u.GroupAlias, u.NodeAlias, u.StartedAt = in.GroupAlias, in.NodeAlias, time.Now().UTC()
		extra := loadStore(path)
		if _, err := extra.Update(func(v *Settings) {
			v.Upload = u
		}); err != nil {
			http.Error(w, "cannot save binding", http.StatusInternalServerError)
			return
		}
		jsonOut(w, map[string]any{"groupId": id, "restartRequired": true})
	})
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			create(w, r)
			return
		}
		type view struct {
			GroupID    string `json:"groupId"`
			GroupAlias string `json:"groupAlias"`
			Pending    int    `json:"pending"`
			Status     string `json:"status"`
			QuotaBound bool   `json:"quotaBound"`
		}
		result := make([]view, 0, len(m.extraBindings))
		for _, binding := range m.extraBindings {
			s := binding.store.Get()
			pending, _, status := binding.uploader.snapshot()
			if !binding.uploader.allowed() {
				status = "主上传已关闭或配置已变更，暂停发送"
			}
			result = append(result, view{s.Upload.GroupID, s.Upload.GroupAlias, pending, status, s.QuotaProfile != ""})
		}
		jsonOut(w, result)
	}
}

func loadExtraBindings(dir, config string, primary *Store) []*extraBinding {
	root := filepath.Join(dir, "bindings")
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil
	}
	type candidate struct {
		dir string
		s   Settings
	}
	var candidates []candidate
	counts := map[string]int{primary.Get().Upload.GroupID: 1}
	for _, entry := range entries {
		if !entry.IsDir() || entry.Type()&os.ModeSymlink != 0 {
			continue
		}
		path := filepath.Join(root, entry.Name())
		data, err := os.ReadFile(filepath.Join(path, "settings.json"))
		if err != nil {
			continue
		}
		var s Settings
		if json.Unmarshal(data, &s) != nil {
			continue
		}
		s, err = s.normalize()
		u := s.Upload
		if err != nil || !u.Enabled || u.GroupID == "" || u.Binding == "" ||
			u.Source == "" || u.Group == "" || u.TargetNode == "" || u.TargetURL == "" ||
			u.TokenCipher == "" || u.StartedAt.IsZero() || !validAlias(u.GroupAlias) ||
			!validAlias(u.NodeAlias) || strings.TrimSpace(u.GroupID) != u.GroupID ||
			(s.QuotaProfile == "" && s.QuotaGroupID != "") ||
			(s.QuotaProfile != "" && s.QuotaGroupID != u.GroupID) {
			continue
		}
		if _, err := uploadURL(u.Endpoint); err != nil {
			continue
		}
		counts[u.GroupID]++
		candidates = append(candidates, candidate{path, s})
	}
	var out []*extraBinding
	for _, c := range candidates {
		if counts[c.s.Upload.GroupID] != 1 {
			continue
		}
		st := loadStore(c.dir)
		u := newUploader(filepath.Join(c.dir, "upload-queue.json"), st)
		u.parent = primary
		// A fresh queue can be bound without modifying any existing pending
		// queue; a mismatched existing binding remains stopped by newUploader.
		if u.status == "" && u.queue.Binding == "" && len(u.queue.Samples) == 0 {
			u.queue.Binding = c.s.Upload.Binding
			u.queue.Destination = c.s.Upload.Endpoint
			u.queue.Credential = credentialFingerprint(c.s.Upload.TokenCipher)
			if u.saveLocked() != nil {
				continue
			}
		}
		q := newQuotaSync(c.dir, config, st, &u.sendMu)
		q.parent = primary
		out = append(out, &extraBinding{
			store:    st,
			uploader: u,
			quota:    q,
		})
	}
	return out
}
