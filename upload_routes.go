package main

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

type uploadInput struct {
	Enabled       bool   `json:"enabled"`
	Endpoint      string `json:"endpoint"`
	Source        string `json:"source"`
	Group         string `json:"group"`
	TargetNode    string `json:"targetNode"`
	TargetURL     string `json:"targetUrl"`
	ConfirmNode   bool   `json:"confirmNode"`
	GroupAlias    string `json:"groupAlias"`
	NodeAlias     string `json:"nodeAlias"`
	Token         string `json:"token"`
	ClearToken    bool   `json:"clearToken"`
	PreserveGroup bool   `json:"preserveGroup"`
}

func validAlias(s string) bool {
	if strings.TrimSpace(s) != s || len(s) == 0 || utf8.RuneCountInString(s) > 64 || !utf8.ValidString(s) {
		return false
	}
	for _, r := range s {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}
func uploadSettingsRoute(m *Monitor, st *Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if m.uploader == nil {
			http.Error(w, "上传功能不可用", 503)
			return
		}
		view := func() {
			s := st.Get().Upload
			has := s.TokenCipher != ""
			s.TokenCipher = ""
			pending, dropped, status := m.uploader.snapshot()
			m.mu.Lock()
			groups := append([]LatencyGroup(nil), m.latencyGroups...)
			m.mu.Unlock()
			jsonOut(w, map[string]any{"settings": s, "hasToken": has, "pending": pending, "dropped": dropped, "status": status, "groups": groups})
		}
		if r.Method == "GET" {
			view()
			return
		}
		post(func(w http.ResponseWriter, r *http.Request) {
			var in uploadInput
			if json.NewDecoder(r.Body).Decode(&in) != nil {
				http.Error(w, "请求格式无效", 400)
				return
			}
			in.Endpoint = strings.TrimSpace(in.Endpoint)
			in.GroupAlias = strings.TrimSpace(in.GroupAlias)
			in.NodeAlias = strings.TrimSpace(in.NodeAlias)
			m.uploader.sendMu.Lock()
			defer m.uploader.sendMu.Unlock()
			old := st.Get().Upload
			next := UploadSettings{Enabled: in.Enabled, Endpoint: in.Endpoint, Source: in.Source, Group: in.Group, TargetNode: in.TargetNode, TargetURL: in.TargetURL, Binding: old.Binding, GroupAlias: in.GroupAlias, NodeAlias: in.NodeAlias, GroupID: old.GroupID, TokenCipher: old.TokenCipher, StartedAt: old.StartedAt}
			if !in.Enabled {
				next.Endpoint, next.Source, next.Group = old.Endpoint, old.Source, old.Group
				next.TargetNode, next.TargetURL = old.TargetNode, old.TargetURL
				next.GroupAlias, next.NodeAlias = old.GroupAlias, old.NodeAlias
			}
			if in.Token != "" {
				if len(in.Token) > 1024 || strings.ContainsAny(in.Token, "\r\n") {
					http.Error(w, "上传密钥无效", 400)
					return
				}
				existing, err := decryptToken(old.TokenCipher)
				if err != nil || existing != in.Token {
					cipher, err := encryptToken(in.Token)
					if err != nil {
						http.Error(w, err.Error(), 500)
						return
					}
					next.TokenCipher = cipher
				}
			} else if in.ClearToken {
				next.TokenCipher = ""
			}
			if next.Endpoint != "" {
				if address, err := uploadURL(next.Endpoint); err == nil {
					next.Endpoint = address
				}
			}
			scopeChanged := next.Source != old.Source || next.Group != old.Group || next.TargetNode != old.TargetNode || next.TargetURL != old.TargetURL || next.GroupAlias != old.GroupAlias || next.NodeAlias != old.NodeAlias
			destinationChanged := next.Endpoint != old.Endpoint || next.TokenCipher != old.TokenCipher
			changed := destinationChanged || scopeChanged
			if in.PreserveGroup && (!old.Enabled || !next.Enabled || old.GroupID == "" || scopeChanged || !destinationChanged) {
				http.Error(w, "只能在保持原节点、组和别名时迁移目的地", http.StatusBadRequest)
				return
			}
			if next.Enabled {
				if !in.ConfirmNode || next.TargetNode == "" || next.TargetURL == "" {
					http.Error(w, "请确认当前选中节点确为要公开的节点", 400)
					return
				}
				address, err := uploadURL(next.Endpoint)
				if err != nil {
					http.Error(w, err.Error(), 400)
					return
				}
				next.Endpoint = address
				if !validAlias(next.GroupAlias) || (!validAlias(next.NodeAlias) || utf8.RuneCountInString(next.NodeAlias) > 55) || next.TokenCipher == "" {
					http.Error(w, "启用前请填写公开组别名、节点别名和独立写密钥", 400)
					return
				}
				m.mu.Lock()
				found := false
				for _, g := range m.latencyGroups {
					if g.Endpoint == next.Source && g.Group == next.Group && g.Node == next.TargetNode && g.TestURL == next.TargetURL {
						found = true
						break
					}
				}
				connected := m.connected
				m.mu.Unlock()
				if !found || !connected {
					http.Error(w, "请选择当前连接的自动测速组", 400)
					return
				}
			}
			m.uploader.mu.Lock()
			queueBefore := m.uploader.queue
			if m.uploader.status == "上传队列无法读取，已暂停" {
				m.uploader.mu.Unlock()
				http.Error(w, "上传队列无法读取，已暂停", 500)
				return
			}
			if changed && len(m.uploader.queue.Samples) > 0 {
				m.uploader.mu.Unlock()
				http.Error(w, "队列仍有待发送记录；请先恢复上传清空队列，再更改目标或别名", 409)
				return
			}
			// A scope change also creates a new group ID. Do not strand an
			// unacknowledged quota snapshot under the old group.
			if changed && m.quota != nil {
				m.quota.mu.Lock()
				pending := m.quota.disk.Pending
				m.quota.mu.Unlock()
				if pending {
					m.uploader.mu.Unlock()
					http.Error(w, "订阅用量队列仍待确认；请先恢复上传", 409)
					return
				}
			}
			if (changed && !in.PreserveGroup) || next.GroupID == "" {
				id, err := randomID()
				if err != nil {
					m.uploader.mu.Unlock()
					http.Error(w, "无法生成标识", 500)
					return
				}
				next.GroupID = id
			}
			if next.Enabled && (changed || next.Binding == "" || next.Binding != m.uploader.queue.Binding || !m.uploader.matchesDestination(old)) {
				if len(m.uploader.queue.Samples) > 0 {
					m.uploader.mu.Unlock()
					http.Error(w, "队列包含尚未确认的记录，不能重新绑定", 409)
					return
				}
				binding, err := randomID()
				if err != nil {
					m.uploader.mu.Unlock()
					http.Error(w, "无法生成标识", 500)
					return
				}
				m.uploader.queue.Binding = binding
				m.uploader.queue.Destination = next.Endpoint
				m.uploader.queue.Credential = credentialFingerprint(next.TokenCipher)
				if err = m.uploader.saveLocked(); err != nil {
					m.uploader.queue = queueBefore
					m.uploader.mu.Unlock()
					http.Error(w, "保存上传队列失败", 500)
					return
				}
				next.Binding = binding
			}
			if next.Enabled && (!old.Enabled || (changed && !in.PreserveGroup)) {
				next.StartedAt = time.Now().UTC()
			}
			if _, err := st.Update(func(s *Settings) { s.Upload = next }); err != nil {
				m.uploader.queue = queueBefore
				if m.uploader.saveLocked() != nil {
					m.uploader.status = "上传队列无法读取，已暂停"
				}
				m.uploader.mu.Unlock()
				http.Error(w, "保存上传设置失败", 500)
				return
			}
			m.uploader.next = time.Time{}
			m.uploader.backoff = 0
			m.uploader.blocked = false
			if m.uploader.status == "上传队列目的地或凭据不匹配，已暂停" || m.uploader.status == "上传队列缺少目的地绑定，已暂停" {
				m.uploader.status = "等待上传"
			}
			m.uploader.mu.Unlock()
			view()
		})(w, r)
	}
}
