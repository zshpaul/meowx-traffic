package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"time"
)

const quotaPath = "/api/client-quota/v1/snapshot"
const maxSafeInteger = int64(9007199254740991)

func credentialFingerprint(cipher string) string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(cipher)))
}

type quotaSnapshot struct {
	Version  int     `json:"version"`
	GroupID  string  `json:"group_id"`
	Revision int64   `json:"revision"`
	Updated  string  `json:"updated_at"`
	Upload   *int64  `json:"upload_bytes"`
	Download *int64  `json:"download_bytes"`
	Total    *int64  `json:"total_bytes"`
	Expires  *string `json:"expires_at"`
}

type quotaDisk struct {
	Profile     string        `json:"profile"`
	GroupID     string        `json:"group_id"`
	Latest      quotaSnapshot `json:"latest"`
	Pending     bool          `json:"pending"`
	Known       bool          `json:"known"`
	Destination string        `json:"destination,omitempty"`
	Credential  string        `json:"credential,omitempty"`
}

// readQuota uses only the exact stored profile id, never the current profile.
func readQuota(path, profile, group string, loc *time.Location) (quotaSnapshot, error) {
	var out quotaSnapshot
	b, err := os.ReadFile(path)
	if err != nil {
		return out, err
	}
	var data struct {
		Profiles []struct {
			ID      string `json:"id"`
			Updated string `json:"lastUpdateDate"`
			Info    *struct {
				Upload   *int64 `json:"upload"`
				Download *int64 `json:"download"`
				Total    *int64 `json:"total"`
				Expire   *int64 `json:"expire"`
			} `json:"subscriptionInfo"`
		} `json:"profiles"`
	}
	if err := json.Unmarshal(b, &data); err != nil {
		return out, err
	}
	count := 0
	for _, p := range data.Profiles {
		if p.ID != profile {
			continue
		}
		count++
		if p.Info == nil || (p.Info.Upload == nil && p.Info.Download == nil && p.Info.Total == nil) {
			return out, errors.New("quota missing")
		}
		t, err := time.Parse(time.RFC3339Nano, p.Updated)
		if err != nil {
			t, err = time.ParseInLocation("2006-01-02T15:04:05.999999999", p.Updated, loc)
		}
		if err != nil {
			return out, errors.New("cache time missing or invalid")
		}
		for _, v := range []*int64{p.Info.Upload, p.Info.Download, p.Info.Total, p.Info.Expire} {
			if v != nil && (*v < 0 || *v > maxSafeInteger) {
				return out, errors.New("quota value invalid")
			}
		}
		out = quotaSnapshot{Version: 1, GroupID: group, Updated: t.UTC().Format(time.RFC3339Nano), Upload: p.Info.Upload, Download: p.Info.Download, Total: p.Info.Total}
		if p.Info.Expire != nil && *p.Info.Expire > 0 {
			t := time.Unix(*p.Info.Expire, 0).UTC()
			if t.Year() < 1 || t.Year() > 9999 {
				return quotaSnapshot{}, errors.New("expiry invalid")
			}
			expiry := t.Format(time.RFC3339)
			out.Expires = &expiry
		}
	}
	if count != 1 {
		return quotaSnapshot{}, errors.New("profile missing or ambiguous")
	}
	return out, nil
}

type quotaSync struct {
	mu      sync.Mutex
	sendMu  *sync.Mutex
	store   *Store
	path    string
	config  string
	disk    quotaDisk
	status  string
	blocked bool
	next    time.Time
	backoff time.Duration
	client  *http.Client
}

func newQuotaSync(dir, config string, st *Store, sendMu *sync.Mutex) *quotaSync {
	q := &quotaSync{store: st, sendMu: sendMu, path: filepath.Join(dir, "quota-queue.json"), config: config, client: &http.Client{Timeout: 8 * time.Second, Transport: &http.Transport{Proxy: nil}, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}
	b, err := os.ReadFile(q.path)
	if err == nil {
		if json.Unmarshal(b, &q.disk) != nil {
			q.blocked, q.status = true, "配额队列无法读取"
		}
	} else if !os.IsNotExist(err) {
		q.blocked, q.status = true, "配额队列无法读取"
	}
	return q
}

func (q *quotaSync) save() error {
	b, err := json.Marshal(q.disk)
	if err != nil {
		return err
	}
	if err = os.WriteFile(q.path+".tmp", b, 0600); err != nil {
		return err
	}
	return os.Rename(q.path+".tmp", q.path)
}

func (q *quotaSync) tick() {
	q.sendMu.Lock()
	defer q.sendMu.Unlock()
	q.mu.Lock()
	defer q.mu.Unlock()
	s := q.store.Get()
	if q.blocked || !s.Upload.Enabled || s.QuotaProfile == "" || s.QuotaGroupID == "" || s.Upload.GroupID != s.QuotaGroupID {
		return
	}
	base, err := uploadURL(s.Upload.Endpoint)
	if err != nil || (base != "http://127.0.0.1:8796" && !strings.HasPrefix(base, "https://")) {
		q.status = "配额同步需要 HTTPS 或本机探针"
		return
	}
	if q.disk.GroupID != "" && (q.disk.GroupID != s.QuotaGroupID || q.disk.Profile != s.QuotaProfile) {
		q.status = "配额绑定已变化，需重新配置"
		return
	}
	credential := credentialFingerprint(s.Upload.TokenCipher)
	if q.disk.Destination != base || q.disk.Credential != credential {
		if q.disk.Destination != "" && q.disk.Pending {
			q.status = "配额队列尚未确认，不能更改目的地或凭据"
			return
		}
		before := q.disk
		if q.disk.Destination != "" && q.disk.Latest.Revision > 0 {
			q.disk.Pending, q.disk.Known = true, false
		}
		q.disk.Destination, q.disk.Credential = base, credential
		if q.save() != nil {
			q.disk = before
			q.status = "配额绑定保存失败"
			return
		}
	}
	value, err := readQuota(q.config, s.QuotaProfile, s.QuotaGroupID, time.Local)
	if err != nil {
		q.status = "订阅缓存不可用"
	}
	prev := q.disk.Latest
	compare := prev
	compare.Revision = 0
	if err == nil && !reflect.DeepEqual(compare, value) {
		if prev.Revision >= maxSafeInteger {
			q.blocked, q.status = true, "配额版本已满"
			return
		}
		before := q.disk
		value.Revision = prev.Revision + 1
		q.disk = quotaDisk{Profile: s.QuotaProfile, GroupID: s.QuotaGroupID, Latest: value, Pending: true, Known: before.Known, Destination: before.Destination, Credential: before.Credential}
		if q.save() != nil {
			q.disk = before
			q.status = "配额队列保存失败"
			return
		}
	}
	if !q.disk.Pending || time.Now().Before(q.next) {
		return
	}
	if !q.disk.Known {
		q.reconcile(base, s)
		if !q.disk.Known {
			return
		}
	}
	secret, err := decryptToken(s.Upload.TokenCipher)
	if err != nil {
		q.blocked, q.status = true, "配额凭据不可用"
		return
	}
	body, _ := json.Marshal(q.disk.Latest)
	req, _ := http.NewRequest("POST", base+quotaPath, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+secret)
	resp, err := q.client.Do(req)
	if err != nil {
		q.retry("配额同步等待重试")
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		if resp.StatusCode == 429 || resp.StatusCode >= 500 {
			q.retry("配额同步等待重试")
		} else {
			q.blocked, q.status = true, fmt.Sprintf("配额同步暂停：HTTP %d", resp.StatusCode)
		}
		return
	}
	var ack struct {
		Version     int    `json:"version"`
		GroupID     string `json:"group_id"`
		Revision    int64  `json:"revision"`
		Disposition string `json:"disposition"`
	}
	if json.NewDecoder(io.LimitReader(resp.Body, 4096)).Decode(&ack) != nil || ack.Version != 1 || ack.GroupID != s.QuotaGroupID || ack.Revision != q.disk.Latest.Revision || (ack.Disposition != "stored" && ack.Disposition != "duplicate" && ack.Disposition != "stale") {
		q.retry("配额确认无效")
		return
	}
	if ack.Disposition == "stale" {
		q.reconcile(base, s)
		return
	}
	q.disk.Pending = false
	if q.save() != nil {
		q.disk.Pending = true
		q.retry("配额确认保存失败")
		return
	}
	q.status, q.backoff, q.next = "配额同步成功", 0, time.Time{}
}

func (q *quotaSync) reconcile(base string, s Settings) {
	r, err := q.client.Get(base + quotaPath + "?group_id=" + url.QueryEscape(s.QuotaGroupID))
	if err != nil {
		q.retry("配额版本对齐失败")
		return
	}
	defer r.Body.Close()
	var result struct {
		Version  int            `json:"version"`
		Snapshot *quotaSnapshot `json:"snapshot"`
	}
	if r.StatusCode != 200 || json.NewDecoder(io.LimitReader(r.Body, 4096)).Decode(&result) != nil || result.Version != 1 {
		q.retry("配额版本对齐失败")
		return
	}
	old := q.disk
	if result.Snapshot != nil {
		if result.Snapshot.GroupID != s.QuotaGroupID || result.Snapshot.Revision < 1 || result.Snapshot.Revision >= maxSafeInteger {
			q.retry("配额版本对齐失败")
			return
		}
		if result.Snapshot.Revision >= q.disk.Latest.Revision {
			q.disk.Latest.Revision = result.Snapshot.Revision + 1
		}
	}
	q.disk.Known = true
	if q.save() != nil {
		q.disk = old
		q.retry("配额队列保存失败")
		return
	}
	q.next = time.Time{}
}

func (q *quotaSync) retry(message string) {
	q.status = message
	if q.backoff == 0 {
		q.backoff = 5 * time.Second
	} else {
		q.backoff *= 2
		if q.backoff > 5*time.Minute {
			q.backoff = 5 * time.Minute
		}
	}
	q.next = time.Now().Add(q.backoff)
}

func (q *quotaSync) run(stop <-chan struct{}) {
	t := time.NewTicker(10 * time.Second)
	defer t.Stop()
	for {
		q.tick()
		select {
		case <-stop:
			return
		case <-t.C:
		}
	}
}
