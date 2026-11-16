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

// Migration is explicit because pending records will be sent to the new
// destination. Each durable step can be retried after an interrupted write.
func migrateExtraBindingRoute(m *Monitor, primary *Store) http.HandlerFunc {
	return post(func(w http.ResponseWriter, r *http.Request) {
		var in struct {
			GroupID string `json:"groupId"`
			Confirm bool   `json:"confirm"`
		}
		if json.NewDecoder(r.Body).Decode(&in) != nil || !in.Confirm || in.GroupID == "" || m.uploader == nil {
			http.Error(w, "invalid migration", http.StatusBadRequest)
			return
		}
		m.uploader.sendMu.Lock()
		defer m.uploader.sendMu.Unlock()
		p := primary.Get().Upload
		m.uploader.mu.Lock()
		primaryBound := p.Enabled && p.Binding != "" && p.Binding == m.uploader.queue.Binding && m.uploader.matchesDestination(p)
		m.uploader.mu.Unlock()
		if !primaryBound {
			http.Error(w, "primary upload binding unavailable", http.StatusConflict)
			return
		}
		var b *extraBinding
		for _, candidate := range m.extraBindings {
			if candidate.store.Get().Upload.GroupID == in.GroupID {
				b = candidate
				break
			}
		}
		if b == nil {
			http.Error(w, "binding not found", http.StatusNotFound)
			return
		}
		b.uploader.sendMu.Lock()
		defer b.uploader.sendMu.Unlock()
		s := b.store.Get().Upload
		if !s.Enabled || s.Source != p.Source || s.Group != p.Group || s.TargetURL != p.TargetURL || s.TargetNode == p.TargetNode {
			http.Error(w, "binding scope differs from primary", http.StatusConflict)
			return
		}
		u, q := b.uploader, b.quota
		u.mu.Lock()
		defer u.mu.Unlock()
		q.mu.Lock()
		defer q.mu.Unlock()
		oldCred, newCred := credentialFingerprint(s.TokenCipher), credentialFingerprint(p.TokenCipher)
		validDestination := func(destination, credential string) bool {
			return (destination == s.Endpoint && credential == oldCred) || (destination == p.Endpoint && credential == newCred)
		}
		if u.status == "上传队列无法读取，已暂停" || u.queue.Binding != s.Binding ||
			!validDestination(u.queue.Destination, u.queue.Credential) || q.blocked ||
			(q.disk.Destination != "" && !validDestination(q.disk.Destination, q.disk.Credential)) {
			http.Error(w, "binding queue cannot be safely migrated", http.StatusConflict)
			return
		}
		if q.disk.Destination != p.Endpoint || q.disk.Credential != newCred {
			before := q.disk
			q.disk.Destination, q.disk.Credential, q.disk.Known = p.Endpoint, newCred, false
			if q.disk.Latest.Revision > 0 {
				q.disk.Pending = true
			}
			if q.save() != nil {
				q.disk = before
				http.Error(w, "cannot save quota migration", http.StatusInternalServerError)
				return
			}
		}
		before := u.queue
		u.queue.Destination, u.queue.Credential = p.Endpoint, newCred
		if u.saveLocked() != nil {
			u.queue = before
			http.Error(w, "cannot save queue migration", http.StatusInternalServerError)
			return
		}
		if _, err := b.store.Update(func(v *Settings) {
			v.Upload.Endpoint, v.Upload.TokenCipher = p.Endpoint, p.TokenCipher
		}); err != nil {
			http.Error(w, "cannot save binding migration; retry", http.StatusInternalServerError)
			return
		}
		u.status, u.blocked, u.next, u.backoff = "等待上传", false, time.Time{}, 0
		q.next, q.backoff = time.Time{}, 0
		jsonOut(w, map[string]any{"groupId": s.GroupID})
	})
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
