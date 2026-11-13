package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func uploadFixture(t *testing.T, endpoint string) (*Uploader, *Store, LatencyPoint) {
	t.Helper()
	dir := t.TempDir()
	st := loadStore(dir)
	cipher, err := encryptToken("local-test-only")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	s := UploadSettings{Enabled: true, Endpoint: endpoint, Source: "private-controller", Group: "private-group", TargetNode: "private-node", TargetURL: "private-test-url", Binding: "binding_1", GroupID: "group_123", GroupAlias: "Demo group", NodeAlias: "Demo node", TokenCipher: cipher, StartedAt: now.Add(-time.Minute)}
	if _, err = st.Update(func(v *Settings) { v.Upload = s }); err != nil {
		t.Fatal(err)
	}
	value := 42
	u := newUploader(filepath.Join(dir, "queue.json"), st)
	u.queue.Binding = s.Binding
	u.queue.Destination = s.Endpoint
	u.queue.Credential = credentialFingerprint(s.TokenCipher)
	if err := u.saveLocked(); err != nil {
		t.Fatal(err)
	}
	return u, st, LatencyPoint{Endpoint: s.Source, Group: s.Group, Node: "private-node", TestURL: "private-test-url", Time: now.Add(-time.Second), ObservedAt: now, Delay: &value, Segment: "internal-segment", Fresh: true}
}

func TestUploaderPrunesExpiredAcknowledgementsWithEmptyQueue(t *testing.T) {
	u, st, point := uploadFixture(t, "http://127.0.0.1:8796")
	expired := time.Now().Add(-31 * 24 * time.Hour)
	for i := 0; i < 20000; i++ {
		u.queue.Acked[fmt.Sprintf("expired-%d", i)] = expired
	}
	if err := u.saveLocked(); err != nil {
		t.Fatal(err)
	}
	u.flush()
	if len(u.queue.Acked) != 0 {
		t.Fatal("empty queue did not prune expired acknowledgements")
	}
	u = newUploader(u.file, st)
	if len(u.queue.Acked) != 0 {
		t.Fatal("pruned acknowledgements returned after restart")
	}
	u.enqueue(point)
	if len(u.queue.Samples) != 1 {
		t.Fatal("upload did not recover after pruning")
	}
}

func TestUploaderAcceptsSamplesWithManyUnexpiredAcknowledgements(t *testing.T) {
	u, st, point := uploadFixture(t, "http://127.0.0.1:8796")
	for i := 0; i < 20000; i++ {
		u.queue.Acked[fmt.Sprintf("recent-%d", i)] = time.Now()
	}
	u.enqueue(point)
	u.enqueue(point)
	if len(u.queue.Samples) != 1 {
		t.Fatal("recent confirmations blocked a new sample or deduplication failed")
	}
	u = newUploader(u.file, st)
	u.enqueue(point)
	if len(u.queue.Samples) != 1 || len(u.queue.Acked) != 20000 {
		t.Fatal("recent confirmations or pending sample were lost after restart")
	}
}
func TestUploaderRetryPersistenceAndPrivacy(t *testing.T) {
	var first []byte
	count := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		count++
		if r.Header.Get("Authorization") != "Bearer local-test-only" {
			t.Error("authorization missing")
		}
		for _, secret := range []string{"private-controller", "private-group", "private-node", "private-test-url", "local-test-only"} {
			if strings.Contains(string(b), secret) {
				t.Errorf("body exposes %s", secret)
			}
		}
		if count == 1 {
			first = append([]byte(nil), b...)
			http.Error(w, "temporary", 503)
			return
		}
		if string(first) != string(b) {
			t.Error("retry changed payload")
		}
		var v struct {
			Version int            `json:"version"`
			Samples []UploadSample `json:"samples"`
		}
		if json.Unmarshal(b, &v) != nil || len(v.Samples) != 1 {
			t.Error("bad batch")
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"version": 1, "accepted": []string{v.Samples[0].ID}})
	}))
	defer srv.Close()
	u, st, p := uploadFixture(t, srv.URL)
	u.enqueue(p)
	u.enqueue(p)
	if len(u.queue.Samples) != 1 {
		t.Fatal("duplicate queue entry")
	}
	u.flush()
	if len(u.queue.Samples) != 1 {
		t.Fatal("removed before confirmation")
	}
	u = newUploader(u.file, st)
	u.flush()
	if len(u.queue.Samples) != 0 || count != 2 {
		t.Fatal("restart did not send")
	}
	u = newUploader(u.file, st)
	u.enqueue(p)
	if len(u.queue.Samples) != 0 {
		t.Fatal("confirmed sample requeued")
	}
	p.Node = "different-private-node"
	p.Time = p.Time.Add(time.Second)
	p.Segment = "new-segment"
	u.enqueue(p)
	if len(u.queue.Samples) != 0 {
		t.Fatal("different node uploaded")
	}
	p.Node = "private-node"
	p.TestURL = "different-url"
	u.enqueue(p)
	if len(u.queue.Samples) != 0 {
		t.Fatal("different test URL uploaded")
	}
	p.TestURL = "private-test-url"
	p.Fresh = false
	u.enqueue(p)
	if len(u.queue.Samples) != 0 {
		t.Fatal("stale sample uploaded")
	}
}
func TestUploaderRedirectDoesNotLeak(t *testing.T) {
	received := false
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { received = true }))
	defer target.Close()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, target.URL, 302) }))
	defer srv.Close()
	u, _, p := uploadFixture(t, srv.URL)
	u.enqueue(p)
	u.flush()
	if received || len(u.queue.Samples) != 1 || !u.blocked {
		t.Fatal("redirect followed or queue not retained")
	}
}
func TestUploaderLocalWorker(t *testing.T) {
	endpoint := os.Getenv("MEOWX_UPLOAD_LOCAL_TEST")
	if endpoint == "" {
		t.Skip("explicit isolated local Worker test only")
	}
	if endpoint != "http://127.0.0.1:8796" {
		t.Fatal("only local test Worker allowed")
	}

	u, st, p := uploadFixture(t, endpoint)
	id, err := randomID()
	if err != nil {
		t.Fatal(err)
	}
	_, err = st.Update(func(s *Settings) { s.Upload.GroupID = id; s.Upload.GroupAlias = "Synthetic BWH group" })
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	p.Time = now.Add(-3 * time.Minute)
	u.enqueue(p)
	p.Time = now.Add(-2 * time.Minute)
	value := 55
	p.Delay = &value
	u.enqueue(p)
	p.Time = now.Add(-time.Minute)
	p.Node = "another-private-node"
	p.Segment = "switched-segment"
	value2 := 68
	p.Delay = &value2
	u.enqueue(p)
	if len(u.queue.Samples) != 2 {
		t.Fatal("mixed-node filtering failed")
	}
	p.Time = now
	p.Node = "private-node"
	p.Segment = "return-segment"
	u.enqueue(p)
	if len(u.queue.Samples) != 3 || u.queue.Samples[0].Segment == u.queue.Samples[2].Segment {
		t.Fatal("return segment missing")
	}
	u.flush()
	if len(u.queue.Samples) != 0 {
		t.Fatalf("worker did not confirm: %s", u.status)
	}

}

func TestUploaderIncompleteAckAndExpired(t *testing.T) {
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		json.NewEncoder(w).Encode(map[string]any{"version": 1, "accepted": []string{}})
	}))
	defer srv.Close()
	u, _, p := uploadFixture(t, srv.URL)
	u.enqueue(p)
	u.flush()
	if calls != 1 || len(u.queue.Samples) != 1 {
		t.Fatal("partial ack removed sample")
	}
	u.next = time.Time{}
	u.queue.Samples[0].MeasuredAt = time.Now().Add(-31 * 24 * time.Hour).UTC().Format(time.RFC3339Nano)
	u.flush()
	if calls != 1 || len(u.queue.Samples) != 0 || u.queue.Dropped != 1 {
		t.Fatal("expired sample sent or not accounted")
	}
}
func TestUploaderSaveFailureAndDisabled(t *testing.T) {
	u, st, p := uploadFixture(t, "http://127.0.0.1:8796")
	u.file = filepath.Join(t.TempDir(), "missing", "queue.json")
	u.enqueue(p)
	if len(u.queue.Samples) != 0 {
		t.Fatal("failed save kept sample in memory")
	}
	u.file = filepath.Join(t.TempDir(), "queue.json")
	u.enqueue(p)
	if len(u.queue.Samples) != 1 {
		t.Fatal("sample not recovered")
	}
	_, err := st.Update(func(s *Settings) { s.Upload.Enabled = false })
	if err != nil {
		t.Fatal(err)
	}
	p.Time = p.Time.Add(time.Second)
	u.enqueue(p)
	if len(u.queue.Samples) != 1 {
		t.Fatal("disabled collection enqueued")
	}
}
func TestUploadSettingsDoNotExposeToken(t *testing.T) {
	u, st, _ := uploadFixture(t, "http://127.0.0.1:8796")
	m := mon()
	m.uploader = u
	r := httptest.NewRequest("GET", "/api/upload-settings", nil)
	w := httptest.NewRecorder()
	uploadSettingsRoute(m, st)(w, r)
	text := w.Body.String()
	if strings.Contains(text, "local-test-only") || strings.Contains(text, st.Get().Upload.TokenCipher) {
		t.Fatal("secret exposed")
	}
	if !strings.Contains(text, `"hasToken":true`) {
		t.Fatal("token presence missing")
	}
}

func TestUploadBindingRejectsLegacyAndProtectsPending(t *testing.T) {
	u, st, p := uploadFixture(t, "http://127.0.0.1:8796")
	// A legacy entry has no binding and must never be sent, including after restart.
	u.enqueue(p)
	if len(u.queue.Samples) != 1 {
		t.Fatal("fixture")
	}
	u.queue.Binding = ""
	if err := u.saveLocked(); err != nil {
		t.Fatal(err)
	}
	u = newUploader(u.file, st)
	u.flush()
	if len(u.queue.Samples) != 1 {
		t.Fatal("legacy queue sent")
	}
	m := mon()
	m.uploader = u
	m.connected = true
	m.latencyGroups = []LatencyGroup{{Endpoint: p.Endpoint, Group: p.Group, Node: p.Node, TestURL: p.TestURL}}
	input := uploadInput{Enabled: true, Endpoint: st.Get().Upload.Endpoint, Source: p.Endpoint, Group: p.Group, TargetNode: p.Node, TargetURL: p.TestURL, ConfirmNode: true, GroupAlias: "Demo group", NodeAlias: "Demo node"}
	send := func(v uploadInput) *httptest.ResponseRecorder {
		b, _ := json.Marshal(v)
		r := httptest.NewRequest("POST", "/api/upload-settings", strings.NewReader(string(b)))
		w := httptest.NewRecorder()
		r.Header.Set("Content-Type", "application/json")
		uploadSettingsRoute(m, st)(w, r)
		return w
	}
	w := send(input)
	if w.Code != 409 {
		t.Fatalf("unverified pending queue must be retained: %d", w.Code)
	}
	if len(u.queue.Samples) != 1 || u.queue.Dropped != 0 {
		t.Fatal("unverified queue was discarded")
	}
	// Isolated fixture is cleared only to continue testing the new binding.
	u.queue.Samples = nil
	if err := u.saveLocked(); err != nil {
		t.Fatal(err)
	}
	if w = send(input); w.Code != 200 {
		t.Fatalf("empty rebind status %d", w.Code)
	}
	p.ObservedAt = time.Now().Add(time.Second)
	p.Time = time.Now()
	u.enqueue(p)
	if len(u.queue.Samples) != 1 {
		t.Fatal("confirmed sample missing")
	}
	// Different binding must not relabel a pending sample.
	m.latencyGroups[0].Node = "another-node"
	input.TargetNode = "another-node"
	if w = send(input); w.Code != 409 {
		t.Fatalf("changed pending binding: %d", w.Code)
	}
	if st.Get().Upload.TargetNode != p.Node || len(u.queue.Samples) != 1 {
		t.Fatal("pending binding mutated")
	}
	// Disabling is allowed even when current selected node changed.
	input.Enabled = false
	input.TargetNode = ""
	input.Source = ""
	input.Group = ""
	input.TargetURL = ""
	if w = send(input); w.Code != 200 || st.Get().Upload.Enabled {
		t.Fatal("failed to disable")
	}
}

func TestDisableWaitsForInflightUpload(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	done := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		<-release
		var body struct {
			Samples []UploadSample `json:"samples"`
		}
		json.NewDecoder(r.Body).Decode(&body)
		json.NewEncoder(w).Encode(map[string]any{"version": 1, "accepted": []string{body.Samples[0].ID}})
	}))
	defer srv.Close()
	u, st, p := uploadFixture(t, srv.URL)
	u.enqueue(p)
	go func() { u.flush(); close(done) }()
	<-started
	m := mon()
	m.uploader = u
	disabled := make(chan int, 1)
	go func() {
		b, _ := json.Marshal(uploadInput{Enabled: false})
		r := httptest.NewRequest("POST", "/api/upload-settings", strings.NewReader(string(b)))
		r.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		uploadSettingsRoute(m, st)(w, r)
		disabled <- w.Code
	}()
	select {
	case <-disabled:
		t.Fatal("disabled before in-flight request completed")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	<-done
	if code := <-disabled; code != 200 {
		t.Fatalf("disable failed %d", code)
	}
	p.Time = p.Time.Add(time.Second)
	u.enqueue(p)
	if len(u.queue.Samples) != 0 {
		t.Fatal("disabled upload enqueued")
	}
}

func TestDestinationMigrationPreservesIdentityAndPending(t *testing.T) {
	u, st, p := uploadFixture(t, "http://127.0.0.1:8796")
	m := mon()
	m.uploader = u
	m.connected = true
	m.latencyGroups = []LatencyGroup{{Endpoint: p.Endpoint, Group: p.Group, Node: p.Node, TestURL: p.TestURL}}
	q := newQuotaSync(t.TempDir(), "", st, &u.sendMu)
	m.quota = q
	input := uploadInput{Enabled: true, Endpoint: "https://probe.example", Source: p.Endpoint, Group: p.Group, TargetNode: p.Node, TargetURL: p.TestURL, ConfirmNode: true, GroupAlias: "Demo group", NodeAlias: "Demo node", Token: "new-secret", PreserveGroup: true}
	send := func() int {
		b, _ := json.Marshal(input)
		r := httptest.NewRequest("POST", "/api/upload-settings", strings.NewReader(string(b)))
		r.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		uploadSettingsRoute(m, st)(w, r)
		return w.Code
	}
	u.enqueue(p)
	if code := send(); code != 409 || len(u.queue.Samples) != 1 {
		t.Fatalf("destination changed with unconfirmed latency: code=%d pending=%d", code, len(u.queue.Samples))
	}
	u.queue.Samples = nil
	q.disk.Pending = true
	if code := send(); code != 409 {
		t.Fatal("destination changed with unconfirmed quota")
	}
	input.Endpoint, input.Token, input.PreserveGroup = st.Get().Upload.Endpoint, "", false
	input.GroupAlias = "Changed alias"
	if code := send(); code != 409 || st.Get().Upload.GroupAlias != "Demo group" || !q.disk.Pending {
		t.Fatal("scope change stranded an unconfirmed quota snapshot")
	}
	input.Endpoint, input.Token, input.PreserveGroup = "https://probe.example", "new-secret", true
	input.GroupAlias = "Demo group"
	q.disk.Pending = false
	oldID, oldBinding, oldStart := st.Get().Upload.GroupID, st.Get().Upload.Binding, st.Get().Upload.StartedAt
	if code := send(); code != 200 {
		t.Fatalf("migration rejected: %d", code)
	}
	got := st.Get().Upload
	if got.GroupID != oldID || got.Binding == oldBinding || got.Endpoint != input.Endpoint || !got.StartedAt.Equal(oldStart) {
		t.Fatal("migration did not preserve identity and rotate binding")
	}
	if token, err := decryptToken(got.TokenCipher); err != nil || token != "new-secret" {
		t.Fatal("new credential not protected")
	}
	p.Time = p.Time.Add(time.Millisecond)
	u.enqueue(p)
	if len(u.queue.Samples) != 1 {
		t.Fatal("a previously observed but unqueued sample was lost at cutover")
	}
}

func TestPendingDestinationBindingSurvivesRestart(t *testing.T) {
	u, st, p := uploadFixture(t, "http://127.0.0.1:8796")
	u.enqueue(p)
	if len(u.queue.Samples) != 1 {
		t.Fatal("missing sample")
	}
	_, err := st.Update(func(s *Settings) { s.Upload.Endpoint = "https://wrong.example" })
	if err != nil {
		t.Fatal(err)
	}
	u = newUploader(u.file, st)
	u.client.Transport = quotaTransport(func(*http.Request) (*http.Response, error) {
		t.Fatal("pending sample sent with a different destination")
		return nil, nil
	})
	u.flush()
	if len(u.queue.Samples) != 1 || u.status != "上传队列目的地或凭据不匹配，已暂停" {
		t.Fatal("pending sample was lost or mismatch was hidden")
	}
}
