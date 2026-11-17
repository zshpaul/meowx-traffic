package main

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"
)

func quotaSettingsRoute(q *quotaSync, st *Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if q == nil {
			http.Error(w, "配额同步不可用", http.StatusServiceUnavailable)
			return
		}
		view := func() {
			q.mu.Lock()
			defer q.mu.Unlock()
			s := st.Get()
			jsonOut(w, map[string]any{"profile": s.QuotaProfile, "groupId": s.QuotaGroupID, "pending": q.disk.Pending, "status": q.status})
		}
		if r.Method == http.MethodGet {
			view()
			return
		}
		post(func(w http.ResponseWriter, r *http.Request) {
			var in struct {
				Profile string `json:"profile"`
				Confirm bool   `json:"confirm"`
			}
			if json.NewDecoder(r.Body).Decode(&in) != nil || strings.TrimSpace(in.Profile) != in.Profile || in.Profile == "" || len(in.Profile) > 128 {
				http.Error(w, "订阅档案标识无效", http.StatusBadRequest)
				return
			}
			if q.parent != nil && !in.Confirm {
				http.Error(w, "请确认公开附加节点的订阅用量", http.StatusBadRequest)
				return
			}
			q.sendMu.Lock()
			defer q.sendMu.Unlock()
			q.mu.Lock()
			defer q.mu.Unlock()
			s := st.Get()
			if !s.Upload.Enabled || s.Upload.GroupID == "" {
				http.Error(w, "请先启用探针上传", http.StatusBadRequest)
				return
			}
			if q.parent != nil && q.parent.Get().QuotaProfile == in.Profile {
				http.Error(w, "附加节点不能复用主节点的订阅档案", http.StatusConflict)
				return
			}
			if _, err := readQuota(q.config, in.Profile, s.Upload.GroupID, time.Local); err != nil {
				http.Error(w, "订阅档案不存在、重复或缓存无效", http.StatusBadRequest)
				return
			}
			rebind := q.disk.GroupID != "" && (q.disk.GroupID != s.Upload.GroupID || q.disk.Profile != in.Profile)
			if rebind && q.disk.Pending {
				http.Error(w, "原配额快照尚未确认，不能更改绑定", http.StatusConflict)
				return
			}
			if _, err := st.Update(func(s *Settings) { s.QuotaProfile, s.QuotaGroupID = in.Profile, s.Upload.GroupID }); err != nil {
				http.Error(w, "保存配额绑定失败", http.StatusInternalServerError)
				return
			}
			if rebind {
				before := q.disk
				q.disk = quotaDisk{}
				if err := q.save(); err != nil {
					q.disk = before
					http.Error(w, "保存配额队列失败，请重试绑定", http.StatusInternalServerError)
					return
				}
			}
			q.blocked = false
			q.next = time.Time{}
			jsonOut(w, map[string]string{"status": "saved"})
		})(w, r)
	}
}

// Additional nodes have separate settings and quota queues. Select the
// running binding by its public identity; never route this to the primary.
func extraQuotaSettingsRoute(m *Monitor) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.URL.Query().Get("groupId")
		if id == "" {
			http.Error(w, "请选择附加节点", http.StatusBadRequest)
			return
		}
		for _, b := range m.extraBindings {
			if b.store.Get().Upload.GroupID != id {
				continue
			}
			quotaSettingsRoute(b.quota, b.store)(w, r)
			return
		}
		http.Error(w, "附加节点不存在或尚未重启", http.StatusNotFound)
	}
}
