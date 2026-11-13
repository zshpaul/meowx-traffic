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
			}
			if json.NewDecoder(r.Body).Decode(&in) != nil || strings.TrimSpace(in.Profile) != in.Profile || in.Profile == "" || len(in.Profile) > 128 {
				http.Error(w, "订阅档案标识无效", http.StatusBadRequest)
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
			if _, err := readQuota(q.config, in.Profile, s.Upload.GroupID, time.Local); err != nil {
				http.Error(w, "订阅档案不存在、重复或缓存无效", http.StatusBadRequest)
				return
			}
			if q.disk.GroupID != "" && (q.disk.GroupID != s.Upload.GroupID || q.disk.Profile != in.Profile) {
				http.Error(w, "已有配额绑定，不能自动更改", http.StatusConflict)
				return
			}
			if _, err := st.Update(func(s *Settings) { s.QuotaProfile, s.QuotaGroupID = in.Profile, s.Upload.GroupID }); err != nil {
				http.Error(w, "保存配额绑定失败", http.StatusInternalServerError)
				return
			}
			q.blocked = false
			q.next = time.Time{}
			jsonOut(w, map[string]string{"status": "saved"})
		})(w, r)
	}
}
