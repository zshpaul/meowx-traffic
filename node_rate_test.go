package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

func TestNodeRateTracksOnlyExactConnectionPaths(t *testing.T) {
	t0 := time.Date(2026, 11, 13, 0, 0, 0, 0, time.UTC)
	start := t0.Add(-time.Minute).Format(time.RFC3339Nano)
	conn := func(id string, chain []string, up, down int64) Connection {
		return Connection{ID: id, Start: start, Chains: chain, Upload: up, Download: down}
	}
	r := nodeRateTracker{}
	mixed := []Connection{
		conn("target", []string{"BWH-US", "auto"}, 100, 200),
		conn("direct", []string{"DIRECT"}, 1000, 2000),
		conn("other", []string{"BWH-US-extra", "auto"}, 500, 500),
	}
	r.observe("controller", "BWH-US", "group", mixed, t0)
	if r.snapshot.Status != "unknown" {
		t.Fatal("first sample needs a baseline")
	}
	mixed[0].Upload, mixed[0].Download = 140, 280
	mixed[1].Upload, mixed[2].Download = 9000, 9000
	r.observe("controller", "BWH-US", "group", mixed, t0.Add(2*time.Second))
	if r.snapshot.Status != "ok" || *r.snapshot.Upload != 20 || *r.snapshot.Download != 40 {
		t.Fatalf("mixed path result: %+v", r.snapshot)
	}
	// The auto group's current selection does not affect an existing path.
	mixed[0].Upload += 10
	r.observe("controller", "BWH-US", "group", mixed, t0.Add(3*time.Second))
	if r.snapshot.Status != "ok" || *r.snapshot.Upload != 10 {
		t.Fatal("existing target connection lost after selection change")
	}
	mixed[2].Chains = []string{"BWH-US", "auto"}
	r.observe("controller", "BWH-US", "group", mixed, t0.Add(4*time.Second))
	if r.snapshot.Status != "unknown" {
		t.Fatal("changed path must not be attributed")
	}
	r.observe("controller", "BWH-US", "group", []Connection{}, t0.Add(5*time.Second))
	if r.snapshot.Status != "ok" || *r.snapshot.Upload != 0 || *r.snapshot.Download != 0 {
		t.Fatal("empty valid snapshot must be zero")
	}
}

func TestNodeRateIsolatedProbe(t *testing.T) {
	endpoint := os.Getenv("MEOW_RATE_TEST_ENDPOINT")
	if endpoint == "" {
		t.Skip("isolated probe not requested")
	}
	u, st, _ := uploadFixture(t, endpoint)
	cipher, err := encryptToken(os.Getenv("MEOW_RATE_TEST_TOKEN"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = st.Update(func(s *Settings) { s.Upload.TokenCipher = cipher })
	if err != nil {
		t.Fatal(err)
	}
	u.queue.Credential = credentialFingerprint(cipher)
	now := time.Now().UTC()
	up, down := int64(83), int64(127)
	m := &Monitor{connected: true}
	m.nodeRate = nodeRateTracker{source: "private-controller", target: "private-node", snapshot: nodeRateSnapshot{
		Version: 1, GroupID: "group_123", Observed: now.Format("2006-01-02T15:04:05.000Z"), Status: "ok", Upload: &up, Download: &down,
	}}
	u.sendNodeRate(m)
	resp, err := http.Get(endpoint + ratePath + "?group_id=group_123")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var result struct {
		Snapshot nodeRateSnapshot `json:"snapshot"`
		Expires  int              `json:"expires_in_ms"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	if result.Snapshot.Observed != m.nodeRate.snapshot.Observed || result.Snapshot.Status != "ok" ||
		result.Snapshot.Upload == nil || *result.Snapshot.Upload != up || result.Snapshot.Download == nil ||
		*result.Snapshot.Download != down || result.Expires <= 0 || result.Expires > 30000 {
		t.Fatalf("isolated probe did not store current sample: %+v", result)
	}
}

func TestNodeRateBaselinesAndFailures(t *testing.T) {
	t0 := time.Date(2026, 11, 13, 0, 0, 0, 0, time.UTC)
	r := nodeRateTracker{}
	c := Connection{ID: "one", Start: t0.Add(-time.Minute).Format(time.RFC3339Nano), Chains: []string{"node"}, Upload: 10}
	r.observe("source", "node", "group", []Connection{c}, t0)
	c.Upload = 5
	r.observe("source", "node", "group", []Connection{c}, t0.Add(time.Second))
	if r.snapshot.Status != "unknown" {
		t.Fatal("counter reset must be unknown")
	}
	r.observe("source", "node", "group", nil, t0.Add(2*time.Second))
	r.observe("source", "node", "group", []Connection{c}, t0.Add(3*time.Second))
	if r.snapshot.Status != "unknown" {
		t.Fatal("reconnection needs a baseline")
	}
	r.observe("other source", "node", "group", []Connection{c}, t0.Add(4*time.Second))
	if r.snapshot.Status != "unknown" {
		t.Fatal("new controller needs a baseline")
	}
	r.observe("other source", "node", "group", []Connection{c}, t0.Add(20*time.Second))
	if r.snapshot.Status != "unknown" {
		t.Fatal("long gap must be unknown")
	}
	newConn := Connection{ID: "new", Start: t0.Add(20500 * time.Millisecond).Format(time.RFC3339Nano), Chains: []string{"node"}, Download: 30}
	r.observe("other source", "node", "group", []Connection{c, newConn}, t0.Add(21*time.Second))
	if r.snapshot.Status != "ok" || *r.snapshot.Download != 30 {
		t.Fatal("new connection count was not attributed")
	}
	r.observe("other source", "node", "group", []Connection{{ID: "bad", Start: c.Start}}, t0.Add(22*time.Second))
	if r.snapshot.Status != "unknown" {
		t.Fatal("missing chain must be unknown")
	}
}

func TestNodeRateSendPrivacyAndNoRetry(t *testing.T) {
	var payloads []nodeRateSnapshot
	var raw []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != ratePath || r.Header.Get("Authorization") != "Bearer local-test-only" {
			t.Error("unexpected request")
		}
		var fields map[string]json.RawMessage
		if err := json.NewDecoder(r.Body).Decode(&fields); err != nil {
			t.Fatal(err)
		}
		b, _ := json.Marshal(fields)
		raw = append(raw, string(b))
		var v nodeRateSnapshot
		_ = json.Unmarshal(b, &v)
		payloads = append(payloads, v)
		if len(fields) != 6 {
			t.Errorf("unexpected fields: %s", b)
		}
		http.Error(w, "temporary", http.StatusServiceUnavailable)
	}))
	defer srv.Close()
	u, _, _ := uploadFixture(t, srv.URL)
	now := time.Now().UTC()
	up, down := int64(42), int64(84)
	m := &Monitor{connected: true}
	m.nodeRate = nodeRateTracker{source: "private-controller", target: "private-node", snapshot: nodeRateSnapshot{
		Version: 1, GroupID: "group_123", Observed: now.Format("2006-01-02T15:04:05.000Z"), Status: "ok", Upload: &up, Download: &down,
	}}
	u.sendNodeRate(m)
	m.connected = false
	u.sendNodeRate(m)
	if len(payloads) != 2 || payloads[0].Status != "ok" || payloads[1].Status != "unknown" || payloads[1].Upload != nil || payloads[1].Download != nil {
		t.Fatalf("unexpected snapshots: %+v", payloads)
	}
	if strings.Contains(strings.Join(raw, ""), "private-") || len(u.queue.Samples) != 0 {
		t.Fatal("private data or live rate was persisted")
	}
}

func TestNodeRateDoesNotFollowRedirect(t *testing.T) {
	redirected := false
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		redirected = true
	}))
	defer other.Close()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, other.URL, http.StatusTemporaryRedirect)
	}))
	defer server.Close()
	u, _, _ := uploadFixture(t, server.URL)
	m := &Monitor{}
	u.sendNodeRate(m)
	if redirected {
		t.Fatal("authenticated request followed a redirect")
	}
}
