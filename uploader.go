package main

import (
	"bytes"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
)

const uploadQueueLimit = 1000
const uploadPath = "/api/client-latency/v1/samples"

type UploadSettings struct {
	Enabled     bool      `json:"enabled"`
	Endpoint    string    `json:"endpoint"`
	Source      string    `json:"source"`
	Group       string    `json:"group"`
	TargetNode  string    `json:"targetNode"`
	TargetURL   string    `json:"targetUrl"`
	Binding     string    `json:"binding"`
	GroupAlias  string    `json:"groupAlias"`
	NodeAlias   string    `json:"nodeAlias"`
	GroupID     string    `json:"groupId"`
	TokenCipher string    `json:"tokenCipher,omitempty"`
	StartedAt   time.Time `json:"startedAt,omitempty"`
}
type UploadSample struct {
	ID         string  `json:"id"`
	GroupID    string  `json:"group_id"`
	GroupAlias string  `json:"group_alias"`
	Node       *string `json:"node"`
	MeasuredAt string  `json:"measured_at"`
	ObservedAt string  `json:"observed_at"`
	Status     string  `json:"status"`
	DelayMS    *int    `json:"delay_ms"`
	Segment    string  `json:"segment"`
}
type uploadDisk struct {
	Binding     string               `json:"binding"`
	Destination string               `json:"destination,omitempty"`
	Credential  string               `json:"credential,omitempty"`
	Samples     []UploadSample       `json:"samples"`
	Dropped     int                  `json:"dropped"`
	Seed        string               `json:"seed"`
	Acked       map[string]time.Time `json:"acked"`
}
type Uploader struct {
	sendMu  sync.Mutex
	mu      sync.Mutex
	file    string
	store   *Store
	queue   uploadDisk
	status  string
	next    time.Time
	backoff time.Duration
	blocked bool
	client  *http.Client
}

func randomID() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
func opaqueID(seed string, fields ...string) string {
	h := hmac.New(sha256.New, []byte(seed))
	b, _ := json.Marshal(fields)
	h.Write(b)
	return hex.EncodeToString(h.Sum(nil)[:16])
}
func uploadURL(raw string) (string, error) {
	u, e := url.Parse(strings.TrimSpace(raw))
	if e != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return "", errors.New("请输入探针站点根地址")
	}
	host := strings.ToLower(u.Hostname())
	ip := net.ParseIP(host)
	local := host == "localhost" || (ip != nil && ip.IsLoopback())
	if u.Scheme != "https" && !(u.Scheme == "http" && local) {
		return "", errors.New("探针地址须使用 HTTPS（本机测试除外）")
	}
	return strings.TrimRight(u.String(), "/"), nil
}
func newUploader(file string, st *Store) *Uploader {
	u := &Uploader{file: file, store: st, client: &http.Client{Timeout: 8 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}
	b, e := os.ReadFile(file)
	if e == nil {
		if json.Unmarshal(b, &u.queue) != nil {
			u.status = "上传队列无法读取，已暂停"
			u.queue.Samples = nil
		}
	} else if !os.IsNotExist(e) {
		u.status = "上传队列无法读取，已暂停"
	}
	if u.status == "" && u.queue.Seed == "" {
		seed, err := randomID()
		if err != nil {
			u.status = "上传队列无法读取，已暂停"
		} else {
			u.queue.Seed = seed
			if u.saveLocked() != nil {
				u.status = "上传队列无法读取，已暂停"
			}
		}
	}
	if u.queue.Acked == nil {
		u.queue.Acked = make(map[string]time.Time)
	}
	s := st.Get().Upload
	if u.status == "" && u.queue.Binding != "" && u.queue.Binding == s.Binding && (u.queue.Destination == "" || u.queue.Credential == "") {
		if len(u.queue.Samples) != 0 {
			u.status = "上传队列缺少目的地绑定，已暂停"
		} else {
			u.queue.Destination, u.queue.Credential = s.Endpoint, credentialFingerprint(s.TokenCipher)
			if u.saveLocked() != nil {
				u.status = "上传队列无法读取，已暂停"
			}
		}
	}
	if u.status == "" && u.queue.Binding != "" && len(u.queue.Samples) > 0 && (u.queue.Binding != s.Binding || !u.matchesDestination(s)) {
		u.status = "上传队列目的地或凭据不匹配，已暂停"
	}
	return u
}

func (u *Uploader) matchesDestination(s UploadSettings) bool {
	return u.queue.Destination != "" && u.queue.Destination == s.Endpoint &&
		u.queue.Credential != "" && u.queue.Credential == credentialFingerprint(s.TokenCipher)
}
func (u *Uploader) saveLocked() error {
	b, e := json.Marshal(u.queue)
	if e != nil {
		return e
	}
	if e = os.WriteFile(u.file+".tmp", b, 0600); e != nil {
		return e
	}
	return os.Rename(u.file+".tmp", u.file)
}

// Expired acknowledgements are no longer needed for the server's 30-day window.
// Persist the removal even with an empty upload queue so the cap can recover.
func (u *Uploader) pruneAckedLocked(now time.Time) error {
	expired := make(map[string]time.Time)
	cut := now.Add(-30 * 24 * time.Hour)
	for id, t := range u.queue.Acked {
		if t.Before(cut) {
			expired[id] = t
			delete(u.queue.Acked, id)
		}
	}
	if len(expired) == 0 {
		return nil
	}
	if err := u.saveLocked(); err != nil {
		for id, t := range expired {
			u.queue.Acked[id] = t
		}
		return err
	}
	return nil
}

func (u *Uploader) enqueue(p LatencyPoint) {
	u.mu.Lock()
	defer u.mu.Unlock()
	s := u.store.Get().Upload
	if !s.Enabled || s.Source != p.Endpoint || s.Group != p.Group || s.TargetNode == "" || s.TargetNode != p.Node || s.TargetURL == "" || s.TargetURL != p.TestURL || s.Binding == "" || s.Binding != u.queue.Binding || !u.matchesDestination(s) || !p.Fresh || p.ObservedAt.IsZero() || p.Delay == nil || s.StartedAt.IsZero() || p.ObservedAt.Before(s.StartedAt) || p.Time.Before(time.Now().Add(-30*24*time.Hour)) {
		return
	}
	if u.status == "上传队列无法读取，已暂停" {
		return
	}
	if len(u.queue.Acked) >= 20000 {
		if err := u.pruneAckedLocked(time.Now()); err != nil {
			u.status = "保存上传队列失败"
			return
		}
	}
	id := opaqueID(u.queue.Seed, s.GroupID, p.Endpoint, p.Group, p.Node, p.TestURL, p.Time.UTC().Format(time.RFC3339Nano))
	if _, ok := u.queue.Acked[id]; ok {
		return
	}
	for _, existing := range u.queue.Samples {
		if existing.ID == id {
			return
		}
	}
	if len(u.queue.Samples) >= uploadQueueLimit || len(u.queue.Acked) >= 20000 {
		u.status = "上传队列或确认记录已满，等待处理"
		return
	}
	n := s.NodeAlias + "-" + opaqueID(u.queue.Seed, s.GroupID, p.Node)[:8]
	sample := UploadSample{ID: id, GroupID: s.GroupID, GroupAlias: s.GroupAlias, Node: &n, MeasuredAt: p.Time.UTC().Format(time.RFC3339Nano), ObservedAt: p.ObservedAt.UTC().Format(time.RFC3339Nano), Status: "ok", DelayMS: p.Delay, Segment: opaqueID(u.queue.Seed, s.GroupID, p.Segment)}
	if *p.Delay == 0 {
		sample.Status = "failed"
		sample.DelayMS = nil
	}
	u.queue.Samples = append(u.queue.Samples, sample)
	if e := u.saveLocked(); e != nil {
		u.queue.Samples = u.queue.Samples[:len(u.queue.Samples)-1]
		u.status = "保存上传队列失败"
		return
	}
	if !u.blocked {
		u.status = "等待上传"
	}
}
func (u *Uploader) snapshot() (int, int, string) {
	u.mu.Lock()
	defer u.mu.Unlock()
	return len(u.queue.Samples), u.queue.Dropped, u.status
}
func (u *Uploader) run(stop <-chan struct{}, m *Monitor) {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
			m.mu.Lock()
			points := append([]LatencyPoint(nil), m.disk.Latency...)
			m.mu.Unlock()
			for _, p := range points {
				u.enqueue(p)
			}
			u.flush()
		}
	}
}
func (u *Uploader) fail(msg string, retry bool) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.status = msg
	if retry {
		if u.backoff == 0 {
			u.backoff = 5 * time.Second
		} else {
			u.backoff *= 2
			if u.backoff > 5*time.Minute {
				u.backoff = 5 * time.Minute
			}
		}
		u.next = time.Now().Add(u.backoff)
	} else {
		u.blocked = true
	}
}
func (u *Uploader) flush() {
	u.sendMu.Lock()
	defer u.sendMu.Unlock()
	s := u.store.Get().Upload
	if !s.Enabled {
		return
	}
	u.mu.Lock()
	if s.Binding == "" || s.TargetNode == "" || s.TargetURL == "" || s.Binding != u.queue.Binding || !u.matchesDestination(s) || u.status == "上传队列无法读取，已暂停" {
		u.mu.Unlock()
		return
	}
	if err := u.pruneAckedLocked(time.Now()); err != nil {
		u.status = "保存上传队列失败"
		u.mu.Unlock()
		return
	}
	if u.blocked || time.Now().Before(u.next) || len(u.queue.Samples) == 0 {
		u.mu.Unlock()
		return
	}
	// Old samples cannot be submitted to the v1 endpoint; discard only with visible accounting.
	before := append([]UploadSample(nil), u.queue.Samples...)
	oldDropped := u.queue.Dropped
	kept := u.queue.Samples[:0]
	cut := time.Now().Add(-30 * 24 * time.Hour)
	for _, p := range u.queue.Samples {
		t, e := time.Parse(time.RFC3339Nano, p.MeasuredAt)
		if e == nil && !t.Before(cut) {
			kept = append(kept, p)
		} else {
			u.queue.Dropped++
		}
	}
	u.queue.Samples = kept
	if len(kept) != len(before) {
		if e := u.saveLocked(); e != nil {
			u.queue.Samples = before
			u.queue.Dropped = oldDropped
			u.status = "保存上传队列失败"
			u.mu.Unlock()
			return
		}
		u.status = "过期记录未上传"
	}
	n := len(u.queue.Samples)
	if n > 100 {
		n = 100
	}
	batch := append([]UploadSample(nil), u.queue.Samples[:n]...)
	u.mu.Unlock()
	if len(batch) == 0 {
		return
	}
	token, e := decryptToken(s.TokenCipher)
	if e != nil {
		u.fail("无法读取上传凭据", false)
		return
	}
	base, e := uploadURL(s.Endpoint)
	if e != nil {
		u.fail("上传地址无效", false)
		return
	}
	var body []byte
	for len(batch) > 0 {
		body, _ = json.Marshal(struct {
			Version int            `json:"version"`
			Samples []UploadSample `json:"samples"`
		}{1, batch})
		if len(body) <= 32768 {
			break
		}
		batch = batch[:len(batch)-1]
	}
	if len(batch) == 0 {
		u.fail("上传样本过大", false)
		return
	}
	req, e := http.NewRequest("POST", base+uploadPath, bytes.NewReader(body))
	if e != nil {
		u.fail("上传请求无效", false)
		return
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	resp, e := u.client.Do(req)
	if e != nil {
		u.fail("上传失败，将重试", true)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		u.fail(fmt.Sprintf("上传接口返回 HTTP %d", resp.StatusCode), resp.StatusCode == 429 || resp.StatusCode >= 500)
		return
	}
	var ack struct {
		Version  int      `json:"version"`
		Accepted []string `json:"accepted"`
	}
	if json.NewDecoder(io.LimitReader(resp.Body, 16384)).Decode(&ack) != nil || ack.Version != 1 || len(ack.Accepted) != len(batch) {
		u.fail("上传确认无效，将重试", true)
		return
	}
	ids := map[string]bool{}
	for _, id := range ack.Accepted {
		if ids[id] {
			u.fail("上传确认无效，将重试", true)
			return
		}
		ids[id] = true
	}
	for _, p := range batch {
		if !ids[p.ID] {
			u.fail("上传确认无效，将重试", true)
			return
		}
	}
	u.mu.Lock()
	defer u.mu.Unlock()
	old := append([]UploadSample(nil), u.queue.Samples...)
	for _, p := range batch {
		t, _ := time.Parse(time.RFC3339Nano, p.MeasuredAt)
		u.queue.Acked[p.ID] = t
	}
	filtered := u.queue.Samples[:0]
	for _, p := range u.queue.Samples {
		if !ids[p.ID] {
			filtered = append(filtered, p)
		}
	}
	u.queue.Samples = filtered
	if e = u.saveLocked(); e != nil {
		u.queue.Samples = old
		for _, p := range batch {
			delete(u.queue.Acked, p.ID)
		}
		u.status = "保存上传队列失败"
		return
	}
	u.status = "上传成功"
	u.backoff = 0
	u.next = time.Time{}
}
