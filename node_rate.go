package main

import (
	"bytes"
	"encoding/json"
	"math"
	"net/http"
	"reflect"
	"time"
)

const ratePath = "/api/client-rate/v1/snapshot"

type nodeRateSnapshot struct {
	Version  int    `json:"version"`
	GroupID  string `json:"group_id"`
	Observed string `json:"observed_at"`
	Status   string `json:"status"`
	Upload   *int64 `json:"upload_bps"`
	Download *int64 `json:"download_bps"`
}

type rateCounter struct {
	upload, download int64
	chains           []string
}

type nodeRateTracker struct {
	source, target string
	at             time.Time
	previous       map[string]rateCounter
	snapshot       nodeRateSnapshot
}

func (r *nodeRateTracker) unavailable(now time.Time, group string) {
	r.at = time.Time{}
	r.previous = nil
	r.snapshot = nodeRateSnapshot{Version: 1, GroupID: group, Observed: now.UTC().Format("2006-01-02T15:04:05.000Z"), Status: "unknown"}
}

func containsNode(chain []string, target string) bool {
	for _, name := range chain {
		if name == target {
			return true
		}
	}
	return false
}

func (r *nodeRateTracker) observe(source, target, group string, connections []Connection, now time.Time) {
	if source == "" || target == "" || group == "" || connections == nil {
		r.unavailable(now, group)
		return
	}
	current := make(map[string]rateCounter, len(connections))
	valid := true
	for _, c := range connections {
		id := identity(c)
		if c.ID == "" || c.Start == "" || len(c.Chains) == 0 || c.Upload < 0 || c.Download < 0 {
			valid = false
			break
		}
		if _, exists := current[id]; exists {
			valid = false
			break
		}
		current[id] = rateCounter{c.Upload, c.Download, append([]string(nil), c.Chains...)}
	}
	if !valid {
		r.unavailable(now, group)
		return
	}
	if r.source != source || r.target != target {
		r.at, r.previous = time.Time{}, nil
	}
	dt := now.Sub(r.at).Seconds()
	available := !r.at.IsZero() && dt > 0 && dt <= 10
	var up, down int64
	if available {
		for _, c := range connections {
			if !containsNode(c.Chains, target) {
				continue
			}
			old, existed := r.previous[identity(c)]
			var u, d int64
			if existed {
				// A path change cannot be attributed to either endpoint safely.
				if !reflect.DeepEqual(old.chains, c.Chains) || c.Upload < old.upload || c.Download < old.download {
					available = false
					break
				}
				u, d = c.Upload-old.upload, c.Download-old.download
			} else {
				started, err := time.Parse(time.RFC3339Nano, c.Start)
				if err != nil || started.Before(r.at) || started.After(now) {
					available = false
					break
				}
				u, d = c.Upload, c.Download
			}
			if u > maxSafeInteger-up || d > maxSafeInteger-down {
				available = false
				break
			}
			up, down = up+u, down+d
		}
	}
	r.source, r.target, r.at, r.previous = source, target, now, current
	r.snapshot = nodeRateSnapshot{Version: 1, GroupID: group, Observed: now.UTC().Format("2006-01-02T15:04:05.000Z"), Status: "unknown"}
	if available {
		ur, dr := math.Round(float64(up)/dt), math.Round(float64(down)/dt)
		if ur <= float64(maxSafeInteger) && dr <= float64(maxSafeInteger) {
			u, d := int64(ur), int64(dr)
			r.snapshot.Status, r.snapshot.Upload, r.snapshot.Download = "ok", &u, &d
		}
	}
}

func (m *Monitor) observeNodeRate(base string, s Snapshot, now time.Time, generation uint64) {
	settings := m.settings().Upload
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.generation != generation {
		return
	}
	if !settings.Enabled || settings.Source != base {
		m.nodeRate.unavailable(now, settings.GroupID)
		return
	}
	m.nodeRate.observe(base, settings.TargetNode, settings.GroupID, s.Connections, now)
}

func (m *Monitor) clearNodeRate(now time.Time) {
	m.nodeRate.unavailable(now, "")
}

func (u *Uploader) sendNodeRate(m *Monitor) {
	u.sendMu.Lock()
	defer u.sendMu.Unlock()
	s := u.store.Get().Upload
	base, err := uploadURL(s.Endpoint)
	if !s.Enabled || err != nil || s.GroupID == "" || s.TargetNode == "" {
		return
	}
	u.mu.Lock()
	bound := s.Binding != "" && s.Binding == u.queue.Binding && u.matchesDestination(s)
	u.mu.Unlock()
	if !bound {
		return
	}
	m.mu.Lock()
	sample := m.nodeRate.snapshot
	observed, parseErr := time.Parse(time.RFC3339Nano, sample.Observed)
	if !m.connected || m.nodeRate.source != s.Source || m.nodeRate.target != s.TargetNode || sample.GroupID != s.GroupID || parseErr != nil || time.Since(observed) > 10*time.Second || time.Since(observed) < -5*time.Second {
		sample = nodeRateSnapshot{Version: 1, GroupID: s.GroupID, Observed: time.Now().UTC().Format("2006-01-02T15:04:05.000Z"), Status: "unknown"}
	}
	m.mu.Unlock()
	secret, err := decryptToken(s.TokenCipher)
	if err != nil {
		return
	}
	body, err := json.Marshal(sample)
	if err != nil {
		return
	}
	req, err := http.NewRequest(http.MethodPost, base+ratePath, bytes.NewReader(body))
	if err != nil {
		return
	}
	req.Header.Set("Authorization", "Bearer "+secret)
	req.Header.Set("Content-Type", "application/json")
	// Rate is a live snapshot: a failure is dropped, not queued or replayed.
	if resp, err := u.client.Do(req); err == nil {
		resp.Body.Close()
	}
}

func (u *Uploader) runNodeRate(stop <-chan struct{}, m *Monitor) {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
			u.sendNodeRate(m)
		}
	}
}
