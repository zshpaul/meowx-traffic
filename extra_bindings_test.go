package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func writeExtraSettings(t *testing.T, root, name string, s Settings) {
	t.Helper()
	dir := filepath.Join(root, "bindings", name)
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	b, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "settings.json"), b, 0600); err != nil {
		t.Fatal(err)
	}
}

func TestCreateExtraBindingRequiresMembershipAndLeavesQuotaUnbound(t *testing.T) {
	u, st, point := uploadFixture(t, "http://127.0.0.1:8796")
	root := t.TempDir()
	m := &Monitor{connected: true, uploader: u, quota: &quotaSync{}}
	m.latencyGroups = []LatencyGroup{{
		Endpoint: point.Endpoint, Group: point.Group, Node: point.Node, TestURL: point.TestURL,
		Members: []string{point.Node, "second-node"},
	}}
	route := extraBindingRoute(root, m, st)
	request := func(body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		route(w, req)
		return w
	}
	bad := request(`{"targetNode":"unknown","groupAlias":"second","nodeAlias":"second","confirm":true}`)
	if bad.Code != http.StatusBadRequest {
		t.Fatalf("accepted nonmember: %d", bad.Code)
	}
	good := request(`{"targetNode":"second-node","groupAlias":"second","nodeAlias":"second","confirm":true,"profile":"untrusted-profile"}`)
	if good.Code != http.StatusOK {
		t.Fatalf("could not create binding: %d %s", good.Code, good.Body.String())
	}
	var result struct {
		GroupID string `json:"groupId"`
	}
	if err := json.Unmarshal(good.Body.Bytes(), &result); err != nil || result.GroupID == "" ||
		result.GroupID == st.Get().Upload.GroupID {
		t.Fatal("invalid independent identity")
	}
	loaded := loadExtraBindings(root, "unused", st)
	if len(loaded) != 1 || loaded[0].store.Get().Upload.GroupID != result.GroupID ||
		loaded[0].store.Get().QuotaProfile != "" || loaded[0].store.Get().QuotaGroupID != "" {
		t.Fatal("binding did not persist independently with quota disabled")
	}
	again := request(`{"targetNode":"second-node","groupAlias":"second","nodeAlias":"second","confirm":true}`)
	if again.Code != http.StatusConflict {
		t.Fatalf("duplicate binding accepted: %d", again.Code)
	}
	if _, err := st.Update(func(v *Settings) { v.Upload.Group = "another-group" }); err != nil {
		t.Fatal(err)
	}
	m.latencyGroups[0].Group = "another-group"
	other := request(`{"targetNode":"second-node","groupAlias":"other","nodeAlias":"other","confirm":true}`)
	if other.Code != http.StatusOK {
		t.Fatalf("same node in a different group was rejected: %d", other.Code)
	}
}

func TestExtraBindingKeepsPrimaryQueueAndOnlyAcceptsItsNode(t *testing.T) {
	primary, st, point := uploadFixture(t, "http://127.0.0.1:8796")
	primary.enqueue(point)
	before, err := os.ReadFile(primary.file)
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	s := st.Get()
	s.Upload.GroupID = "independent-group"
	s.Upload.Binding = "independent-binding"
	s.Upload.TargetNode = "second-node"
	s.Upload.GroupAlias = "second"
	s.Upload.NodeAlias = "second"
	s.QuotaProfile = "second-profile"
	s.QuotaGroupID = s.Upload.GroupID
	writeExtraSettings(t, root, "second", s)

	extras := loadExtraBindings(root, "unused-config", st)
	if len(extras) != 1 {
		t.Fatalf("got %d extra bindings", len(extras))
	}
	extras[0].uploader.enqueue(point)
	if len(extras[0].uploader.queue.Samples) != 0 {
		t.Fatal("primary node sample attributed to extra binding")
	}
	point.Node = s.Upload.TargetNode
	point.Time = time.Now().UTC()
	point.ObservedAt = point.Time
	extras[0].uploader.enqueue(point)
	if len(extras[0].uploader.queue.Samples) != 1 {
		t.Fatal("extra node sample was not queued")
	}
	reloaded := loadExtraBindings(root, "unused-config", st)
	if len(reloaded) != 1 || reloaded[0].store.Get().Upload.GroupID != s.Upload.GroupID ||
		len(reloaded[0].uploader.queue.Samples) != 1 {
		t.Fatal("extra identity or pending sample did not survive restart")
	}
	if _, err := st.Update(func(v *Settings) { v.Upload.Enabled = false }); err != nil {
		t.Fatal(err)
	}
	point.Time = point.Time.Add(time.Second)
	reloaded[0].uploader.enqueue(point)
	if len(reloaded[0].uploader.queue.Samples) != 1 {
		t.Fatal("extra binding accepted samples after primary upload was disabled")
	}
	if _, err := st.Update(func(v *Settings) {
		v.Upload.Enabled = true
		v.Upload.Endpoint = "https://different.example"
	}); err != nil {
		t.Fatal(err)
	}
	reloaded[0].uploader.client.Transport = quotaTransport(func(*http.Request) (*http.Response, error) {
		t.Error("extra upload used an outdated destination or credential")
		return nil, nil
	})
	reloaded[0].uploader.enqueue(point)
	reloaded[0].uploader.flush()
	if len(reloaded[0].uploader.queue.Samples) != 1 {
		t.Fatal("extra binding accepted samples after primary destination changed")
	}
	if _, err := st.Update(func(v *Settings) {
		v.Upload.Endpoint = s.Upload.Endpoint
		v.Upload.TokenCipher = "different-credential"
	}); err != nil {
		t.Fatal(err)
	}
	reloaded[0].uploader.flush()
	if len(reloaded[0].uploader.queue.Samples) != 1 {
		t.Fatal("extra pending sample lost after primary credential changed")
	}
	if _, err := st.Update(func(v *Settings) {
		v.Upload.TokenCipher = s.Upload.TokenCipher
		v.Upload.Group = "different-group"
	}); err != nil {
		t.Fatal(err)
	}
	reloaded[0].uploader.flush()
	if len(reloaded[0].uploader.queue.Samples) != 1 {
		t.Fatal("extra pending sample lost after primary group changed")
	}
	if _, err := st.Update(func(v *Settings) {
		v.Upload.Endpoint = s.Upload.Endpoint
		v.Upload.TokenCipher = s.Upload.TokenCipher
		v.Upload.Group = s.Upload.Group
		v.Upload.StartedAt = point.ObservedAt.Add(time.Second)
	}); err != nil {
		t.Fatal(err)
	}
	reloaded[0].uploader.enqueue(point)
	if len(reloaded[0].uploader.queue.Samples) != 1 {
		t.Fatal("extra binding accepted a sample observed before the primary restart")
	}
	point.Time = point.ObservedAt
	point.ObservedAt = point.ObservedAt.Add(2 * time.Second)
	reloaded[0].uploader.enqueue(point)
	if len(reloaded[0].uploader.queue.Samples) != 1 {
		t.Fatal("extra binding accepted a pre-enable measurement observed afterward")
	}
	after, err := os.ReadFile(primary.file)
	if err != nil || string(before) != string(after) {
		t.Fatal("primary pending queue changed")
	}
}

func TestExtraBindingsRejectDuplicateGroupIDs(t *testing.T) {
	_, st, _ := uploadFixture(t, "http://127.0.0.1:8796")
	root := t.TempDir()
	s := st.Get()
	s.QuotaProfile = "profile"
	s.QuotaGroupID = s.Upload.GroupID
	writeExtraSettings(t, root, "collision", s)
	if got := loadExtraBindings(root, "unused", st); len(got) != 0 {
		t.Fatal("accepted a binding with the primary group ID")
	}
	s.Upload.GroupID = "duplicate"
	s.QuotaGroupID = "duplicate"
	writeExtraSettings(t, root, "first", s)
	writeExtraSettings(t, root, "second", s)
	if got := loadExtraBindings(root, "unused", st); len(got) != 0 {
		t.Fatal("accepted duplicate extra group IDs")
	}
}

func TestExtraRatesUseSeparateNodeBaselines(t *testing.T) {
	primary, st, _ := uploadFixture(t, "http://127.0.0.1:8796")
	root := t.TempDir()
	s := st.Get()
	s.Upload.GroupID = "second-group"
	s.Upload.Binding = "second-binding"
	s.Upload.TargetNode = "second-node"
	s.QuotaProfile, s.QuotaGroupID = "", ""
	writeExtraSettings(t, root, "second", s)
	extras := loadExtraBindings(root, "unused", st)
	if len(extras) != 1 {
		t.Fatal("missing extra binding")
	}
	m := &Monitor{store: st, uploader: primary, extraBindings: extras}
	t0 := time.Now()
	start := t0.Add(-time.Minute).Format(time.RFC3339Nano)
	conns := []Connection{
		{ID: "one", Start: start, Chains: []string{"private-node"}, Upload: 10, Download: 20},
		{ID: "two", Start: start, Chains: []string{"second-node"}, Upload: 100, Download: 200},
	}
	m.observeNodeRate("private-controller", Snapshot{Connections: conns}, t0, 0)
	conns[0].Upload += 10
	conns[1].Upload += 30
	m.observeNodeRate("private-controller", Snapshot{Connections: conns}, t0.Add(time.Second), 0)
	if m.nodeRate.snapshot.Status != "ok" || *m.nodeRate.snapshot.Upload != 10 ||
		extras[0].rate.snapshot.Status != "ok" || *extras[0].rate.snapshot.Upload != 30 {
		t.Fatal("rates were not isolated by exact node")
	}
	if _, err := st.Update(func(v *Settings) { v.Upload.Enabled = false }); err != nil {
		t.Fatal(err)
	}
	conns[1].Upload += 200
	m.observeNodeRate("private-controller", Snapshot{Connections: conns}, t0.Add(2*time.Second), 0)
	if _, err := st.Update(func(v *Settings) {
		v.Upload.Enabled = true
		v.Upload.StartedAt = t0.Add(3 * time.Second)
	}); err != nil {
		t.Fatal(err)
	}
	conns[1].Upload += 100
	m.observeNodeRate("private-controller", Snapshot{Connections: conns}, t0.Add(3*time.Second), 0)
	if extras[0].rate.snapshot.Status != "unknown" {
		t.Fatal("rate across disabled interval was attributed")
	}
	conns[1].Upload += 7
	m.observeNodeRate("private-controller", Snapshot{Connections: conns}, t0.Add(4*time.Second), 0)
	if extras[0].rate.snapshot.Status != "ok" || *extras[0].rate.snapshot.Upload != 7 {
		t.Fatal("extra rate did not recover after a new baseline")
	}
}

func TestDisablingPrimaryWaitsForExtraAndPreservesPending(t *testing.T) {
	started, release, finished := make(chan struct{}), make(chan struct{}), make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		<-release
		http.Error(w, "retry", http.StatusServiceUnavailable)
	}))
	defer srv.Close()
	primary, st, point := uploadFixture(t, srv.URL)
	root := t.TempDir()
	s := st.Get()
	s.Upload.GroupID, s.Upload.Binding, s.Upload.TargetNode = "second-group", "second-binding", "second-node"
	writeExtraSettings(t, root, "second", s)
	extras := loadExtraBindings(root, "unused", st)
	if len(extras) != 1 {
		t.Fatal("missing extra binding")
	}
	point.Node = "second-node"
	extras[0].uploader.enqueue(point)
	go func() { extras[0].uploader.flush(); close(finished) }()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("extra upload did not start")
	}
	m := mon()
	m.uploader, m.extraBindings = primary, extras
	disabled := make(chan int, 1)
	go func() {
		req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"enabled":false}`))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		uploadSettingsRoute(m, st)(w, req)
		disabled <- w.Code
	}()
	select {
	case <-disabled:
		close(release)
		t.Fatal("disable returned while extra upload was in flight")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	<-finished
	if code := <-disabled; code != http.StatusOK {
		t.Fatalf("disable returned %d", code)
	}
	if len(extras[0].uploader.queue.Samples) != 1 {
		t.Fatal("disable lost the extra pending sample")
	}
	point.Time = point.Time.Add(time.Second)
	extras[0].uploader.enqueue(point)
	if len(extras[0].uploader.queue.Samples) != 1 {
		t.Fatal("extra queue grew after disable")
	}
}

func TestExtraQuotaDoesNotSendAfterPrimaryDestinationChange(t *testing.T) {
	_, st, _ := uploadFixture(t, "http://127.0.0.1:8796")
	root := t.TempDir()
	s := st.Get()
	s.Upload.GroupID, s.Upload.Binding, s.Upload.TargetNode = "second-group", "second-binding", "second-node"
	s.QuotaProfile, s.QuotaGroupID = "second-profile", s.Upload.GroupID
	writeExtraSettings(t, root, "second", s)
	extras := loadExtraBindings(root, "unused", st)
	if len(extras) != 1 {
		t.Fatal("missing extra binding")
	}
	q := extras[0].quota
	q.disk = quotaDisk{Profile: s.QuotaProfile, GroupID: s.QuotaGroupID, Pending: true, Known: true}
	q.client.Transport = quotaTransport(func(*http.Request) (*http.Response, error) {
		t.Error("quota sent to outdated destination")
		return nil, nil
	})
	if _, err := st.Update(func(v *Settings) { v.Upload.Endpoint = "https://different.example" }); err != nil {
		t.Fatal(err)
	}
	q.tick()
	if !q.disk.Pending {
		t.Fatal("pending quota lost after primary destination changed")
	}
}

func TestExtraMigrationPreservesPendingAndRecoversInterruptedWrite(t *testing.T) {
	primary, st, point := uploadFixture(t, "http://127.0.0.1:8796")
	root := t.TempDir()
	s := st.Get()
	s.Upload.GroupID, s.Upload.Binding, s.Upload.TargetNode = "second-group", "second-binding", "second-node"
	s.QuotaProfile, s.QuotaGroupID = "second-profile", s.Upload.GroupID
	writeExtraSettings(t, root, "second", s)
	extras := loadExtraBindings(root, "unused", st)
	if len(extras) != 1 {
		t.Fatal("missing extra binding")
	}
	point.Node = "second-node"
	extras[0].uploader.enqueue(point)
	u, q := extras[0].uploader, extras[0].quota
	if len(u.queue.Samples) != 1 {
		t.Fatal("missing pending sample")
	}
	seed, pending := u.queue.Seed, u.queue.Samples[0].ID
	q.disk = quotaDisk{Profile: s.QuotaProfile, GroupID: s.QuotaGroupID, Destination: s.Upload.Endpoint,
		Credential: credentialFingerprint(s.Upload.TokenCipher), Pending: true, Known: true,
		Latest: quotaSnapshot{Revision: 7}}
	if err := q.save(); err != nil {
		t.Fatal(err)
	}
	cipher, err := encryptToken("rotated-test-token")
	if err != nil {
		t.Fatal(err)
	}
	_, err = st.Update(func(v *Settings) {
		v.Upload.Endpoint = "https://new.example"
		v.Upload.TokenCipher = cipher
	})
	if err != nil {
		t.Fatal(err)
	}
	primary.queue.Destination = st.Get().Upload.Endpoint
	primary.queue.Credential = credentialFingerprint(cipher)
	if err := primary.saveLocked(); err != nil {
		t.Fatal(err)
	}
	m := &Monitor{uploader: primary, quota: &quotaSync{}, extraBindings: extras, connected: true}
	route := migrateExtraBindingRoute(m, st)
	call := func(body string) int {
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		route(w, req)
		return w.Code
	}
	if call(`{"groupId":"second-group"}`) != http.StatusBadRequest {
		t.Fatal("migration without confirmation succeeded")
	}
	// Simulate a crash after the queue write but before settings are updated.
	if err := os.WriteFile(extras[0].migrationMarker(), nil, 0600); err != nil {
		t.Fatal(err)
	}
	u.queue.Destination, u.queue.Credential = st.Get().Upload.Endpoint, credentialFingerprint(cipher)
	if err := u.saveLocked(); err != nil {
		t.Fatal(err)
	}
	current := st.Get().Upload
	m.latencyGroups = []LatencyGroup{{Endpoint: current.Source, Group: current.Group, Node: current.TargetNode, TestURL: current.TargetURL}}
	body, err := json.Marshal(uploadInput{Enabled: true, Endpoint: "https://third.example", Source: current.Source,
		Group: current.Group, TargetNode: current.TargetNode, TargetURL: current.TargetURL, ConfirmNode: true,
		GroupAlias: current.GroupAlias, NodeAlias: current.NodeAlias})
	if err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(string(body)))
	req.Header.Set("Content-Type", "application/json")
	uploadSettingsRoute(m, st)(w, req)
	if w.Code != http.StatusConflict || st.Get().Upload.Endpoint != current.Endpoint {
		t.Fatalf("primary changed during interrupted migration: %d", w.Code)
	}
	beforeCancel, err := os.ReadFile(u.file)
	if err != nil {
		t.Fatal(err)
	}
	cancelReq := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"groupId":"second-group","confirm":true}`))
	cancelReq.Header.Set("Content-Type", "application/json")
	cancel := httptest.NewRecorder()
	cancelExtraMigrationRoute(m)(cancel, cancelReq)
	afterCancel, err := os.ReadFile(u.file)
	if cancel.Code != http.StatusOK || err != nil || string(beforeCancel) != string(afterCancel) {
		t.Fatal("cancellation changed the queued data")
	}
	if err := os.WriteFile(extras[0].migrationMarker(), nil, 0600); err != nil {
		t.Fatal(err)
	}
	m.extraBindings = loadExtraBindings(root, "unused", st)
	if len(m.extraBindings) != 1 {
		t.Fatal("interrupted binding was not loaded")
	}
	if call(`{"groupId":"second-group","confirm":true}`) != http.StatusOK {
		t.Fatal("could not resume migration")
	}
	reloaded := loadExtraBindings(root, "unused", st)
	if len(reloaded) != 1 {
		t.Fatal("migrated binding did not reload")
	}
	got, quota := reloaded[0].uploader, reloaded[0].quota
	if got.queue.Seed != seed || len(got.queue.Samples) != 1 || got.queue.Samples[0].ID != pending ||
		!got.matchesDestination(reloaded[0].store.Get().Upload) || !got.allowed() {
		t.Fatal("migration lost identity or pending sample")
	}
	if !quota.disk.Pending || quota.disk.Known || quota.disk.Latest.Revision != 7 ||
		quota.disk.Destination != st.Get().Upload.Endpoint {
		t.Fatal("quota migration did not preserve pending data for reconciliation")
	}
	if _, err := os.Stat(reloaded[0].migrationMarker()); !os.IsNotExist(err) {
		t.Fatal("completed migration marker remains")
	}
}

func TestExtraQueueFailureDoesNotTrapPrimarySettings(t *testing.T) {
	primary, st, point := uploadFixture(t, "http://127.0.0.1:8796")
	root := t.TempDir()
	s := st.Get()
	s.Upload.GroupID, s.Upload.Binding, s.Upload.TargetNode = "second-group", "second-binding", "second-node"
	writeExtraSettings(t, root, "second", s)
	extras := loadExtraBindings(root, "unused", st)
	if len(extras) != 1 {
		t.Fatal("missing extra")
	}
	extras[0].uploader.status = "上传队列无法读取，已暂停"
	extras[0].quota.blocked = true
	m := &Monitor{uploader: primary, quota: &quotaSync{}, extraBindings: extras, connected: true}
	m.latencyGroups = []LatencyGroup{{Endpoint: point.Endpoint, Group: point.Group, Node: point.Node, TestURL: point.TestURL}}
	current := st.Get().Upload
	body, err := json.Marshal(uploadInput{Enabled: true, Endpoint: "https://new.example", Source: current.Source,
		Group: current.Group, TargetNode: current.TargetNode, TargetURL: current.TargetURL, ConfirmNode: true,
		GroupAlias: current.GroupAlias, NodeAlias: current.NodeAlias})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(string(body)))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	uploadSettingsRoute(m, st)(w, req)
	if w.Code != http.StatusOK || st.Get().Upload.Endpoint != "https://new.example" {
		t.Fatalf("extra failure trapped primary: %d", w.Code)
	}
}

func TestExtraBindingEndToEndKeepsBWHAndReportsOnlySelectedNode(t *testing.T) {
	received := make(chan []UploadSample, 2)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Samples []UploadSample `json:"samples"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || len(body.Samples) == 0 {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		received <- body.Samples
		ids := make([]string, 0, len(body.Samples))
		for _, sample := range body.Samples {
			ids = append(ids, sample.ID)
		}
		json.NewEncoder(w).Encode(map[string]any{"version": 1, "accepted": ids})
	}))
	defer srv.Close()
	primary, st, point := uploadFixture(t, srv.URL)
	primary.enqueue(point)
	primarySeed, pendingID := primary.queue.Seed, primary.queue.Samples[0].ID
	root := t.TempDir()
	m := &Monitor{connected: true, base: point.Endpoint, uploader: primary, quota: &quotaSync{}, disk: newDisk()}
	m.latencyGroups = []LatencyGroup{{Endpoint: point.Endpoint, Group: point.Group, Node: point.Node,
		TestURL: point.TestURL, Members: []string{point.Node, "second-node"}}}
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"targetNode":"second-node","groupAlias":"second","nodeAlias":"second","confirm":true}`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	extraBindingRoute(root, m, st)(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("create: %d %s", w.Code, w.Body.String())
	}
	var result struct {
		GroupID string `json:"groupId"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	m.extraBindings = loadExtraBindings(root, "unused", st)
	if len(m.extraBindings) != 1 || m.extraBindings[0].store.Get().Upload.GroupID != result.GroupID {
		t.Fatal("persisted binding not restored")
	}
	now := time.Now().UTC().Add(time.Second)
	value := 42
	proxies := map[string]latencyProxy{
		point.Group: {Type: "URLTest", Now: point.Node, TestURL: point.TestURL, All: []string{point.Node, "second-node"}},
		point.Node:  {Type: "Vmess", Extra: map[string]proxyState{point.TestURL: {History: []delayResult{{Time: now, Delay: &value}}}}},
		"second-node": {Type: "Vmess", Extra: map[string]proxyState{point.TestURL: {}}},
	}
	m.ingestLatency(point.Endpoint, proxies, nil, now)
	if len(m.extraBindings[0].uploader.queue.Samples) != 0 {
		t.Fatal("BWH measurement was attributed to second node")
	}
	proxies[point.Group] = latencyProxy{Type: "URLTest", Now: "second-node", TestURL: point.TestURL, All: []string{point.Node, "second-node"}}
	proxies["second-node"] = latencyProxy{Type: "Vmess", Extra: map[string]proxyState{point.TestURL: {History: []delayResult{{Time: now.Add(time.Second), Delay: &value}}}}}
	m.ingestLatency(point.Endpoint, proxies, nil, now.Add(time.Second))
	m.extraBindings[0].uploader.flush()
	select {
	case samples := <-received:
		if len(samples) != 1 || samples[0].GroupID != result.GroupID || samples[0].DelayMS == nil || *samples[0].DelayMS != value {
			t.Fatal("extra sample did not retain its identity and real delay")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no extra sample sent")
	}
	originalPending := false
	for _, sample := range primary.queue.Samples {
		if sample.ID == pendingID && sample.GroupID == st.Get().Upload.GroupID {
			originalPending = true
		}
	}
	if primary.queue.Seed != primarySeed || !originalPending {
		t.Fatal("primary seed or original pending sample was lost")
	}
}
