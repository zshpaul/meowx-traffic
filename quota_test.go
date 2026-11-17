package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type quotaTransport func(*http.Request) (*http.Response, error)

func (f quotaTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestExtraQuotaBindingIsIndependentAndSendsRawValues(t *testing.T) {
	primary, st, _ := uploadFixture(t, "http://127.0.0.1:8796")
	if _, err := st.Update(func(s *Settings) {
		s.QuotaProfile, s.QuotaGroupID = "bwh-profile", s.Upload.GroupID
	}); err != nil {
		t.Fatal(err)
	}
	primarySettings, err := os.ReadFile(st.file)
	if err != nil {
		t.Fatal(err)
	}
	primaryQueue, err := os.ReadFile(primary.file)
	if err != nil {
		t.Fatal(err)
	}
	primaryQuota := newQuotaSync(filepath.Dir(st.file), "", st, &primary.sendMu)
	primaryQuota.disk = quotaDisk{Profile: "bwh-profile", GroupID: st.Get().Upload.GroupID, Pending: true, Latest: quotaSnapshot{GroupID: st.Get().Upload.GroupID, Revision: 7}}
	if err := primaryQuota.save(); err != nil {
		t.Fatal(err)
	}
	primaryQuotaQueue, err := os.ReadFile(primaryQuota.path)
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	config := filepath.Join(root, "config.json")
	if err := os.WriteFile(config, []byte(`{"profiles":[{"id":"dmit-profile","lastUpdateDate":"2026-11-17T00:00:00Z","subscriptionInfo":{"upload":10,"download":20,"total":214748364800}}]}`), 0600); err != nil {
		t.Fatal(err)
	}
	extraSettings := st.Get()
	extraSettings.Upload.GroupID, extraSettings.Upload.Binding, extraSettings.Upload.TargetNode = "dmit-group", "dmit-binding", "dmit-node"
	extraSettings.QuotaProfile, extraSettings.QuotaGroupID = "", ""
	writeExtraSettings(t, root, "dmit", extraSettings)
	extras := loadExtraBindings(root, config, st)
	if len(extras) != 1 {
		t.Fatal("missing extra binding")
	}
	m := &Monitor{uploader: primary, extraBindings: extras}
	m.quota = newQuotaSync(filepath.Dir(st.file), config, st, &primary.sendMu)
	route := extraQuotaSettingsRoute(m)
	call := func(group, body string) int {
		w := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodPost, "/?groupId="+group, strings.NewReader(body))
		r.Header.Set("Content-Type", "application/json")
		route(w, r)
		return w.Code
	}
	if call("dmit-group", `{"profile":"dmit-profile"}`) != http.StatusBadRequest ||
		call("dmit-group", `{"profile":"bwh-profile","confirm":true}`) != http.StatusConflict ||
		call(st.Get().Upload.GroupID, `{"profile":"dmit-profile","confirm":true}`) != http.StatusNotFound {
		t.Fatal("unconfirmed, primary profile, or primary group was accepted")
	}
	if code := call("dmit-group", `{"profile":"dmit-profile","confirm":true}`); code != http.StatusOK {
		t.Fatalf("bind failed: %d", code)
	}
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"profile":"dmit-profile"}`))
	req.Header.Set("Content-Type", "application/json")
	primaryQuotaSettingsRoute(m, st)(w, req)
	if w.Code != http.StatusConflict {
		t.Fatal("primary accepted a profile already bound to an additional node")
	}
	q := extras[0].quota
	var sent quotaSnapshot
	q.client.Transport = quotaTransport(func(r *http.Request) (*http.Response, error) {
		if r.Method == http.MethodGet {
			return quotaResponse(200, `{"version":1,"snapshot":null}`), nil
		}
		if err := json.NewDecoder(r.Body).Decode(&sent); err != nil {
			t.Fatal(err)
		}
		return quotaResponse(200, `{"version":1,"group_id":"dmit-group","revision":1,"disposition":"stored"}`), nil
	})
	q.tick()
	if sent.GroupID != "dmit-group" || sent.Upload == nil || *sent.Upload != 10 ||
		sent.Download == nil || *sent.Download != 20 || sent.Total == nil || *sent.Total != 214748364800 || q.disk.Pending {
		t.Fatal("extra quota did not send the raw values and acknowledge them")
	}
	reloaded := loadExtraBindings(root, config, st)
	if len(reloaded) != 1 || reloaded[0].store.Get().QuotaProfile != "dmit-profile" ||
		reloaded[0].store.Get().QuotaGroupID != "dmit-group" || reloaded[0].quota.disk.Pending {
		t.Fatal("extra quota binding or queue did not survive restart")
	}
	afterSettings, err := os.ReadFile(st.file)
	if err != nil || string(afterSettings) != string(primarySettings) {
		t.Fatal("primary settings changed")
	}
	afterQueue, err := os.ReadFile(primary.file)
	if err != nil || string(afterQueue) != string(primaryQueue) {
		t.Fatal("primary upload queue changed")
	}
	afterQuotaQueue, err := os.ReadFile(primaryQuota.path)
	if err != nil || string(afterQuotaQueue) != string(primaryQuotaQueue) {
		t.Fatal("primary pending quota queue changed")
	}
}

func TestConcurrentQuotaBindingsAndUnreadableQueue(t *testing.T) {
	u, st, _ := uploadFixture(t, "http://127.0.0.1:8796")
	root := t.TempDir()
	config := filepath.Join(root, "config.json")
	if err := os.WriteFile(config, []byte(`{"profiles":[{"id":"shared","lastUpdateDate":"2026-11-17T00:00:00Z","subscriptionInfo":{"upload":1,"download":2,"total":100}},{"id":"distinct","lastUpdateDate":"2026-11-17T00:00:00Z","subscriptionInfo":{"upload":3,"download":4,"total":100}}]}`), 0600); err != nil {
		t.Fatal(err)
	}
	s := st.Get()
	s.Upload.GroupID, s.Upload.Binding = "extra-group", "extra-binding"
	writeExtraSettings(t, root, "extra", s)
	extras := loadExtraBindings(root, config, st)
	m := &Monitor{quota: newQuotaSync(filepath.Dir(st.file), config, st, &u.sendMu), extraBindings: extras}
	results := make(chan int, 2)
	start := make(chan struct{})
	for _, handler := range []http.HandlerFunc{primaryQuotaSettingsRoute(m, st), extraQuotaSettingsRoute(m)} {
		go func(h http.HandlerFunc) {
			<-start
			w := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/?groupId=extra-group", strings.NewReader(`{"profile":"shared","confirm":true}`))
			req.Header.Set("Content-Type", "application/json")
			h(w, req)
			results <- w.Code
		}(handler)
	}
	close(start)
	a, b := <-results, <-results
	if !((a == http.StatusOK && b == http.StatusConflict) || (b == http.StatusOK && a == http.StatusConflict)) {
		t.Fatalf("concurrent bindings: %d, %d", a, b)
	}
	queue := filepath.Join(root, "bindings", "extra", "quota-queue.json")
	corrupt := []byte(`{"pending":`)
	if err := os.WriteFile(queue, corrupt, 0600); err != nil {
		t.Fatal(err)
	}
	extras = loadExtraBindings(root, config, st)
	m.extraBindings = extras
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/?groupId=extra-group", strings.NewReader(`{"profile":"distinct","confirm":true}`))
	req.Header.Set("Content-Type", "application/json")
	extraQuotaSettingsRoute(m)(w, req)
	extras[0].quota.tick()
	after, err := os.ReadFile(queue)
	if w.Code != http.StatusConflict || err != nil || string(after) != string(corrupt) {
		t.Fatal("unreadable pending queue was not preserved")
	}
	restarted := loadExtraBindings(root, config, st)
	if len(restarted) != 1 || !restarted[0].quota.loadError {
		t.Fatal("unreadable queue protection did not survive restart")
	}
	afterRestart, err := os.ReadFile(queue)
	if err != nil || string(afterRestart) != string(corrupt) {
		t.Fatal("unreadable queue changed after restart")
	}
}

func TestQuotaExplicitRebindPreservesPendingAndRestarts(t *testing.T) {
	dir := t.TempDir()
	config := filepath.Join(dir, "config.json")
	if err := os.WriteFile(config, []byte(`{"profiles":[{"id":"fixture","lastUpdateDate":"2026-11-13T00:00:00Z","subscriptionInfo":{"upload":10,"download":20,"total":100}}]}`), 0600); err != nil {
		t.Fatal(err)
	}
	st := loadStore(dir)
	cipher, err := encryptToken("fixture-only")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Update(func(s *Settings) {
		s.QuotaProfile, s.QuotaGroupID = "fixture", "old_group"
		s.Upload = UploadSettings{Enabled: true, Endpoint: "http://127.0.0.1:8796", GroupID: "new_group", TokenCipher: cipher}
	}); err != nil {
		t.Fatal(err)
	}
	u := newUploader(filepath.Join(dir, "upload.json"), st)
	q := newQuotaSync(dir, config, st, &u.sendMu)
	q.disk = quotaDisk{Profile: "fixture", GroupID: "old_group", Pending: true, Latest: quotaSnapshot{GroupID: "old_group", Revision: 7}}
	if err := q.save(); err != nil {
		t.Fatal(err)
	}
	bind := func() int {
		w := httptest.NewRecorder()
		r := httptest.NewRequest("POST", "/api/quota-settings", strings.NewReader(`{"profile":"fixture"}`))
		r.Header.Set("Content-Type", "application/json")
		quotaSettingsRoute(q, st)(w, r)
		return w.Code
	}
	if bind() != http.StatusConflict || !q.disk.Pending || st.Get().QuotaGroupID != "old_group" {
		t.Fatal("pending old snapshot was not preserved")
	}
	q.disk.Pending = false
	if err := q.save(); err != nil {
		t.Fatal(err)
	}
	if code := bind(); code != http.StatusOK || st.Get().QuotaGroupID != "new_group" {
		t.Fatalf("acknowledged binding could not be changed: %d", code)
	}
	q = newQuotaSync(dir, config, st, &u.sendMu)
	q.client.Transport = quotaTransport(func(r *http.Request) (*http.Response, error) {
		if r.Method == "GET" {
			return quotaResponse(200, `{"version":1,"snapshot":null}`), nil
		}
		var sent quotaSnapshot
		if err := json.NewDecoder(r.Body).Decode(&sent); err != nil || sent.GroupID != "new_group" || sent.Revision != 1 {
			t.Error("new group snapshot is invalid")
		}
		return quotaResponse(200, `{"version":1,"group_id":"new_group","revision":1,"disposition":"stored"}`), nil
	})
	q.tick()
	if q.disk.Pending || q.disk.GroupID != "new_group" || q.disk.Latest.Revision != 1 {
		t.Fatalf("new group did not sync after restart: %+v", q.disk)
	}
}
func quotaResponse(code int, body string) *http.Response {
	return &http.Response{StatusCode: code, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}
}

// Optional cross-process integration uses an isolated probe package and data directory.
func TestQuotaLocalProbeIntegration(t *testing.T) {
	pkg := os.Getenv("QUOTA_PROBE_PACKAGE")
	if pkg == "" {
		t.Skip("no isolated probe package supplied")
	}
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	listener.Close()
	dir := t.TempDir()
	cmd := exec.Command(filepath.Join(pkg, "node.exe"), filepath.Join(pkg, "server.mjs"), "--data-dir", filepath.Join(dir, "db"), "--port", fmt.Sprint(port))
	pipe, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { pipe.Close(); cmd.Wait() }()
	if _, err = io.WriteString(pipe, `{"token":"fixture-only"}`+"\n"); err != nil {
		t.Fatal(err)
	}
	base := fmt.Sprintf("http://127.0.0.1:%d", port)
	ready := false
	for i := 0; i < 50; i++ {
		resp, e := http.Get(base + "/api/local/health")
		if e == nil {
			resp.Body.Close()
			if resp.StatusCode == 200 {
				ready = true
				break
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !ready {
		t.Fatal("isolated probe did not start")
	}
	config := filepath.Join(dir, "config.json")
	write := func(n int) {
		t.Helper()
		err := os.WriteFile(config, []byte(fmt.Sprintf(`{"profiles":[{"id":"bwh","lastUpdateDate":"2026-11-13T11:24:30+08:00","subscriptionInfo":{"upload":%d,"download":20,"total":100}}]}`, n)), 0600)
		if err != nil {
			t.Fatal(err)
		}
	}
	write(10)
	st := loadStore(dir)
	cipher, err := encryptToken("fixture-only")
	if err != nil {
		t.Fatal(err)
	}
	st.Update(func(s *Settings) {
		s.QuotaProfile, s.QuotaGroupID = "bwh", "fixture_group"
		s.Upload = UploadSettings{Enabled: true, Endpoint: "http://127.0.0.1:8796", GroupID: "fixture_group", TokenCipher: cipher}
	})
	u := newUploader(filepath.Join(dir, "upload.json"), st)
	q := newQuotaSync(dir, config, st, &u.sendMu)
	offline := false
	transport := quotaTransport(func(r *http.Request) (*http.Response, error) {
		if offline {
			return nil, errors.New("fixture offline")
		}
		r.URL.Host = fmt.Sprintf("127.0.0.1:%d", port)
		return http.DefaultTransport.RoundTrip(r)
	})
	q.client.Transport = transport
	q.tick()
	if q.disk.Pending {
		t.Fatalf("initial snapshot not acknowledged: %s", q.status)
	}
	offline = true
	write(1)
	q.tick()
	if !q.disk.Pending || q.disk.Latest.Revision != 2 {
		t.Fatalf("offline reset not retained: %s", q.status)
	}
	q = newQuotaSync(dir, config, st, &u.sendMu)
	offline = false
	q.client.Transport = transport
	q.tick()
	if q.disk.Pending || q.disk.Latest.Revision != 2 {
		t.Fatalf("restarted reset not acknowledged: %s", q.status)
	}
	resp, err := http.Get(base + quotaPath + "?group_id=fixture_group")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var result struct {
		Snapshot quotaSnapshot `json:"snapshot"`
	}
	if err = json.NewDecoder(resp.Body).Decode(&result); err != nil || result.Snapshot.Revision != 2 || result.Snapshot.Upload == nil || *result.Snapshot.Upload != 1 {
		t.Fatalf("incorrect public snapshot: %+v %v", result, err)
	}
}

func TestQuotaCacheBindingAndSnapshotRecovery(t *testing.T) {
	dir := t.TempDir()
	config := filepath.Join(dir, "meow.json")
	write := func(data string) {
		t.Helper()
		if err := os.WriteFile(config, []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
	}
	write(`{"profiles":[{"id":"other","subscriptionInfo":{"upload":999}},{"id":"bwh","lastUpdateDate":"2026-11-13T11:24:30.585690","subscriptionInfo":{"upload":10,"download":20,"total":100,"expire":0}}]}`)
	v, err := readQuota(config, "bwh", "g", time.FixedZone("HK", 8*3600))
	if err != nil || v.Updated != "2026-11-13T03:24:30.58569Z" || *v.Upload != 10 || v.Expires != nil {
		t.Fatalf("cache read: %+v %v", v, err)
	}
	st := loadStore(dir)
	cipher, err := encryptToken("test-token")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = st.Update(func(s *Settings) {
		s.QuotaProfile, s.QuotaGroupID = "bwh", "g"
		s.Upload = UploadSettings{Enabled: true, Endpoint: "http://127.0.0.1:8796", GroupID: "g", TokenCipher: cipher}
	}); err != nil {
		t.Fatal(err)
	}
	u := newUploader(filepath.Join(dir, "upload.json"), st)
	q := newQuotaSync(dir, config, st, &u.sendMu)
	posts := 0
	var sent quotaSnapshot
	transport := quotaTransport(func(r *http.Request) (*http.Response, error) {
		if r.Method == "GET" {
			return quotaResponse(200, `{"version":1,"snapshot":null}`), nil
		}
		if r.Header.Get("Authorization") != "Bearer test-token" {
			t.Error("missing token")
		}
		posts++
		if err := json.NewDecoder(r.Body).Decode(&sent); err != nil {
			t.Fatal(err)
		}
		if posts == 1 {
			return quotaResponse(503, `{}`), nil
		}
		return quotaResponse(200, `{"version":1,"group_id":"g","revision":2,"disposition":"stored"}`), nil
	})
	q.client.Transport = transport
	q.tick()
	if posts != 1 || !q.disk.Pending || q.disk.Latest.Revision != 1 {
		t.Fatal("offline snapshot was not retained")
	}
	// The same cache time can contain a legitimate reset and must get a new revision.
	write(`{"profiles":[{"id":"bwh","lastUpdateDate":"2026-11-13T11:24:30.585690","subscriptionInfo":{"upload":1,"download":2,"total":100,"expire":0}}]}`)
	q.tick()
	if q.disk.Latest.Revision != 2 || *q.disk.Latest.Upload != 1 {
		t.Fatal("same-time reset was not queued")
	}
	q = newQuotaSync(dir, config, st, &u.sendMu)
	q.client.Transport = transport
	q.tick()
	if posts != 2 || q.disk.Pending || sent.Revision != 2 || *sent.Upload != 1 {
		t.Fatalf("restart did not deliver latest snapshot: %+v", q.disk)
	}
	write(`{"profiles":[{"id":"bwh","lastUpdateDate":"2026-11-13T11:24:30","subscriptionInfo":{"upload":1}},{"id":"bwh","lastUpdateDate":"2026-11-13T11:24:30","subscriptionInfo":{"upload":2}}]}`)
	if _, err = readQuota(config, "bwh", "g", time.UTC); err == nil {
		t.Fatal("duplicate identity accepted")
	}
	if _, err = readQuota(config, "missing", "g", time.UTC); err == nil {
		t.Fatal("missing identity accepted")
	}
	write(`{"profiles":[{"id":"bwh","lastUpdateDate":"bad","subscriptionInfo":{"upload":10,"total":100}}]}`)
	if _, err = readQuota(config, "bwh", "g", time.UTC); err == nil {
		t.Fatal("invalid cache time accepted")
	}
	write(`{"profiles":[{"id":"bwh","lastUpdateDate":"2026-11-13T11:24:30","subscriptionInfo":{"upload":9007199254740992}}]}`)
	if _, err = readQuota(config, "bwh", "g", time.UTC); err == nil {
		t.Fatal("unsafe integer accepted")
	}
}

func TestQuotaStaleRebaseAndConflictKeepsPending(t *testing.T) {
	dir := t.TempDir()
	config := filepath.Join(dir, "config.json")
	if err := os.WriteFile(config, []byte(`{"profiles":[{"id":"bwh","lastUpdateDate":"2026-11-13T11:24:30+08:00","subscriptionInfo":{"upload":0,"download":1,"total":2}}]}`), 0600); err != nil {
		t.Fatal(err)
	}
	st := loadStore(dir)
	cipher, err := encryptToken("test-token")
	if err != nil {
		t.Fatal(err)
	}
	st.Update(func(s *Settings) {
		s.QuotaProfile, s.QuotaGroupID = "bwh", "g"
		s.Upload = UploadSettings{Enabled: true, Endpoint: "http://127.0.0.1:8796", GroupID: "g", TokenCipher: cipher}
	})
	u := newUploader(filepath.Join(dir, "upload.json"), st)
	q := newQuotaSync(dir, config, st, &u.sendMu)
	post := 0
	q.client.Transport = quotaTransport(func(r *http.Request) (*http.Response, error) {
		if r.Method == "GET" {
			if post == 0 {
				return quotaResponse(200, `{"version":1,"snapshot":null}`), nil
			}
			return quotaResponse(200, `{"version":1,"snapshot":{"version":1,"group_id":"g","revision":8}}`), nil
		}
		post++
		if post == 1 {
			return quotaResponse(200, `{"version":1,"group_id":"g","revision":1,"disposition":"stale"}`), nil
		}
		return quotaResponse(409, `{}`), nil
	})
	q.tick()
	if q.disk.Latest.Revision != 9 || !q.disk.Pending {
		t.Fatalf("stale did not rebase: %+v", q.disk)
	}
	q.tick()
	if !q.blocked || !q.disk.Pending || q.disk.Latest.Revision != 9 {
		t.Fatal("conflict must pause with the same pending snapshot")
	}
	restarted := newQuotaSync(dir, config, st, &u.sendMu)
	if !restarted.disk.Pending || restarted.disk.Latest.Revision != 9 {
		t.Fatal("pending conflict did not survive restart")
	}
}

func TestQuotaHTTPSDestinationBindingAndRedirect(t *testing.T) {
	dir := t.TempDir()
	config := filepath.Join(dir, "config.json")
	if err := os.WriteFile(config, []byte(`{"profiles":[{"id":"bwh","lastUpdateDate":"2026-11-13T11:24:30+08:00","subscriptionInfo":{"upload":1,"total":2}}]}`), 0600); err != nil {
		t.Fatal(err)
	}
	redirected := false
	target := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { redirected = true }))
	defer target.Close()
	source := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			w.Write([]byte(`{"version":1,"snapshot":null}`))
			return
		}
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}))
	defer source.Close()
	st := loadStore(dir)
	cipher, err := encryptToken("fixture-token")
	if err != nil {
		t.Fatal(err)
	}
	st.Update(func(s *Settings) {
		s.QuotaProfile, s.QuotaGroupID = "bwh", "g"
		s.Upload = UploadSettings{Enabled: true, Endpoint: source.URL, GroupID: "g", TokenCipher: cipher}
	})
	u := newUploader(filepath.Join(dir, "upload.json"), st)
	q := newQuotaSync(dir, config, st, &u.sendMu)
	q.client = source.Client()
	q.client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	q.tick()
	if redirected || !q.disk.Pending || !q.blocked {
		t.Fatal("redirect must not be followed or acknowledged")
	}
	if q.disk.Destination != source.URL || q.disk.Credential == "" {
		t.Fatal("snapshot was not bound to destination and credential")
	}
	st.Update(func(s *Settings) { s.Upload.Endpoint = target.URL })
	q.blocked = false
	q.tick()
	if redirected || q.disk.Destination != source.URL || !q.disk.Pending {
		t.Fatal("pending snapshot moved to another destination")
	}
}
