package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLatencyCollectionPersistenceAndRetention(t *testing.T) {
	now := time.Date(2026, 11, 13, 12, 0, 0, 0, time.UTC)
	m := mon()
	m.connected = true
	m.base = "http://controller"
	m.retention = 2
	value := 42
	failed := 0
	proxies := map[string]latencyProxy{
		"group": {Type: "URLTest", Now: "node", TestURL: "https://test"},
		"node":  {Extra: map[string]proxyState{"https://test": {History: []delayResult{{Time: now.Add(-time.Minute), Delay: &value}}}}},
	}
	m.ingestLatency(m.base, proxies, nil, now)
	m.ingestLatency(m.base, proxies, nil, now)
	if len(m.disk.Latency) != 1 {
		t.Fatalf("duplicate result: %d", len(m.disk.Latency))
	}
	node := proxies["node"]
	node.Extra["https://test"] = proxyState{History: []delayResult{{Time: now, Delay: &failed}}}
	proxies["node"] = node
	m.ingestLatency(m.base, proxies, nil, now)
	if len(m.disk.Latency) != 2 || *m.disk.Latency[1].Delay != 0 {
		t.Fatal("failed test was lost")
	}
	m.file = filepath.Join(t.TempDir(), "history.json")
	if err := m.save(); err != nil {
		t.Fatal(err)
	}
	reloaded := mon()
	reloaded.file = m.file
	if err := reloaded.load(); err != nil {
		t.Fatal(err)
	}
	if len(reloaded.disk.Latency) != 2 {
		t.Fatal("latency history not restored")
	}
	reloaded.retention = 1
	reloaded.prune(now.AddDate(0, 0, 1))
	if len(reloaded.disk.Latency) != 0 {
		t.Fatal("retention did not remove old points")
	}
}

func TestLatencyDoesNotInventMissingOrAmbiguousResult(t *testing.T) {
	now := time.Now()
	value, other := 20, 30
	m := mon()
	m.connected = true
	m.base = "http://controller"
	proxies := map[string]latencyProxy{"group": {Type: "URLTest", Now: "node", TestURL: "url"}, "node": {Extra: map[string]proxyState{"url": {History: []delayResult{{Time: now, Delay: &value}}}}}}
	providers := map[string]struct {
		Proxies []latencyProxy `json:"proxies"`
	}{"p": {Proxies: []latencyProxy{{Name: "node", Extra: map[string]proxyState{"url": {History: []delayResult{{Time: now, Delay: &other}}}}}}}}
	m.ingestLatency(m.base, proxies, providers, now)
	if len(m.disk.Latency) != 0 || len(m.latencyGroups) != 1 {
		t.Fatal("ambiguous result recorded")
	}
	delete(proxies, "node")
	m.ingestLatency(m.base, proxies, nil, now)
	if len(m.disk.Latency) != 0 {
		t.Fatal("missing result recorded")
	}
}

func TestLiveLatencyReadOnly(t *testing.T) {
	if os.Getenv("MEOWX_LATENCY_LIVE_TEST") != "1" {
		t.Skip("opt-in read-only local controller")
	}
	st := loadStore(filepath.Join(os.Getenv("LOCALAPPDATA"), "MeowX Traffic"))
	base, _, err := resolve(st.Get())
	if err != nil {
		t.Fatal("controller settings unavailable")
	}
	m := mon()
	m.store = st
	m.base = base
	m.connected = true
	m.pollLatency()
	if m.latencyError != "" {
		t.Fatal("read-only latency request failed")
	}
	if len(m.latencyGroups) == 0 || len(m.disk.Latency) == 0 {
		t.Fatal("no URLTest group with usable selected-node result")
	}
	t.Logf("discovered groups: %d; recorded existing results: %d", len(m.latencyGroups), len(m.disk.Latency))
}

func TestLatencySegmentsOnSwitchAndDisconnect(t *testing.T) {
	now := time.Now()
	a, b := 25, 35
	m := mon()
	m.connected = true
	m.base = "http://controller"
	proxies := map[string]latencyProxy{"g": {Type: "URLTest", Now: "a", TestURL: "url"}, "a": {Extra: map[string]proxyState{"url": {History: []delayResult{{Time: now, Delay: &a}}}}}, "b": {Extra: map[string]proxyState{"url": {History: []delayResult{{Time: now.Add(time.Second), Delay: &b}}}}}}
	m.ingestLatency(m.base, proxies, nil, now.Add(time.Second))
	g := proxies["g"]
	g.Now = "b"
	proxies["g"] = g
	m.ingestLatency(m.base, proxies, nil, now.Add(2*time.Second))
	node := proxies["a"]
	node.Extra["url"] = proxyState{History: []delayResult{{Time: now.Add(2 * time.Second), Delay: &a}}}
	proxies["a"] = node
	g.Now = "a"
	proxies["g"] = g
	m.ingestLatency(m.base, proxies, nil, now.Add(3*time.Second))
	if len(m.disk.Latency) != 3 || m.disk.Latency[0].Segment == m.disk.Latency[2].Segment {
		t.Fatal("switch back did not start a new segment")
	}
	m.rebase()
	m.connected = true
	node.Extra["url"] = proxyState{History: []delayResult{{Time: now.Add(4 * time.Second), Delay: &a}}}
	proxies["a"] = node
	m.ingestLatency(m.base, proxies, nil, now.Add(5*time.Second))
	if len(m.disk.Latency) != 4 || m.disk.Latency[2].Segment == m.disk.Latency[3].Segment {
		t.Fatal("reconnect did not start a new segment")
	}
}

func TestLatencySameTimestampDistinctGroupsAndReload(t *testing.T) {
	now := time.Now()
	value := 31
	m := mon()
	m.connected = true
	m.base = "http://controller"
	m.file = filepath.Join(t.TempDir(), "history.json")
	proxies := map[string]latencyProxy{"first": {Type: "URLTest", Now: "n", TestURL: "url"}, "second": {Type: "URLTest", Now: "n", TestURL: "url"}, "n": {Extra: map[string]proxyState{"url": {History: []delayResult{{Time: now, Delay: &value}}}}}}
	m.ingestLatency(m.base, proxies, nil, now.Add(time.Second))
	if len(m.disk.Latency) != 2 {
		t.Fatal("same measurement must be attributed once per observed group")
	}
	if err := m.save(); err != nil {
		t.Fatal(err)
	}
	restored := mon()
	restored.file = m.file
	restored.connected = true
	restored.base = m.base
	if err := restored.load(); err != nil {
		t.Fatal(err)
	}
	restored.ingestLatency(restored.base, proxies, nil, now.Add(2*time.Second))
	if len(restored.disk.Latency) != 2 {
		t.Fatal("restart duplicated existing samples")
	}
}

func TestLatencyStaleAndMissingLatest(t *testing.T) {
	now := time.Now()
	value := 30
	m := mon()
	m.connected = true
	m.base = "http://controller"
	proxies := map[string]latencyProxy{"g": {Type: "URLTest", Now: "n", TestURL: "url"}, "n": {Extra: map[string]proxyState{"url": {History: []delayResult{{Time: now.Add(-time.Minute), Delay: &value}}}}}}
	m.ingestLatency(m.base, proxies, nil, now)
	if len(m.disk.Latency) != 1 || m.disk.Latency[0].Fresh {
		t.Fatal("pre-selection result must be isolated")
	}
	n := proxies["n"]
	n.Extra["url"] = proxyState{History: []delayResult{{Time: now.Add(-time.Minute), Delay: &value}, {Time: now.Add(time.Second), Delay: nil}}}
	proxies["n"] = n
	m.ingestLatency(m.base, proxies, nil, now.Add(2*time.Second))
	if len(m.disk.Latency) != 1 {
		t.Fatal("missing latest result must not reuse an earlier one")
	}
	n.Extra["url"] = proxyState{History: []delayResult{{Time: now.Add(time.Second), Delay: &value}}}
	proxies["n"] = n
	m.ingestLatency(m.base, proxies, nil, now.Add(2*time.Second))
	if len(m.disk.Latency) != 2 || !m.disk.Latency[1].Fresh {
		t.Fatal("new measurement should be connectable")
	}
}

func TestLatencyControllerChangeRejectsOldResponse(t *testing.T) {
	now := time.Now()
	value := 31
	m := mon()
	m.connected = true
	m.base = "http://old"
	proxies := map[string]latencyProxy{"g": {Type: "URLTest", Now: "n", TestURL: "url"}, "n": {Extra: map[string]proxyState{"url": {History: []delayResult{{Time: now, Delay: &value}}}}}}
	m.ingestLatency(m.base, proxies, nil, now)
	m.rebase()
	m.base = "http://new"
	m.connected = true
	m.ingestLatency("http://old", proxies, nil, now.Add(time.Second))
	if len(m.disk.Latency) != 1 {
		t.Fatal("old controller result accepted after switch")
	}
	m.ingestLatency("http://new", proxies, nil, now.Add(2*time.Second))
	if len(m.disk.Latency) != 2 {
		t.Fatal("new controller result not recorded independently")
	}
}

func TestLatencyInFlightResultRejectedAfterSameEndpointRebase(t *testing.T) {
	now := time.Now()
	value := 9
	m := mon()
	m.connected = true
	m.base = "http://controller"
	proxies := map[string]latencyProxy{"g": {Type: "URLTest", Now: "n", TestURL: "url"}, "n": {Extra: map[string]proxyState{"url": {History: []delayResult{{Time: now, Delay: &value}}}}}}
	generation := m.generation
	m.rebase()
	m.connected = true
	m.ingestLatencyGeneration(m.base, proxies, nil, now, generation)
	if len(m.disk.Latency) != 0 {
		t.Fatal("old request accepted after settings changed at same endpoint")
	}
}

func TestBoundNodeHistoryUnselectedFailureRecoveryAndRestart(t *testing.T) {
	u, st, _ := uploadFixture(t, "http://127.0.0.1:8796")
	s := st.Get().Upload
	_, parent, _ := uploadFixture(t, s.Endpoint)
	if _, err := parent.Update(func(v *Settings) {
		v.Upload = s
		v.Upload.TargetNode = "other"
	}); err != nil {
		t.Fatal(err)
	}
	u.parent = parent
	m := mon()
	m.connected, m.base = true, s.Source
	m.extraBindings = []*extraBinding{{store: st, uploader: u}}
	now := time.Now().UTC()
	success, failed, recovered := 40, 0, 55
	proxies := map[string]latencyProxy{
		s.Group:      {Type: "URLTest", Now: "other", TestURL: s.TargetURL, All: []string{"other", s.TargetNode}},
		"other":      {Type: "Vmess"},
		s.TargetNode: {Type: "Vmess", Extra: map[string]proxyState{s.TargetURL: {History: []delayResult{{Time: now.Add(-time.Second), Delay: &success}}}}},
	}
	m.ingestLatency(m.base, proxies, nil, now)
	if len(u.queue.Samples) != 0 {
		t.Fatal("pre-observation history uploaded")
	}
	node := proxies[s.TargetNode]
	node.Extra[s.TargetURL] = proxyState{History: []delayResult{
		{Time: now.Add(time.Second), Delay: &success},
		{Time: now.Add(2 * time.Second), Delay: &failed},
		{Time: now.Add(3 * time.Second), Delay: &recovered},
	}}
	proxies[s.TargetNode] = node
	m.ingestLatency(m.base, proxies, nil, now.Add(4*time.Second))
	m.ingestLatency(m.base, proxies, nil, now.Add(5*time.Second))
	if len(u.queue.Samples) != 3 || u.queue.Samples[0].Status != "ok" ||
		u.queue.Samples[1].Status != "failed" || u.queue.Samples[1].DelayMS != nil ||
		u.queue.Samples[2].Status != "ok" {
		t.Fatalf("incorrect success/failure/recovery samples: %+v", u.queue.Samples)
	}
	restarted := newUploader(u.file, st)
	restarted.parent = parent
	m.extraBindings[0].uploader = restarted
	m.boundLatency = nil
	m.ingestLatency(m.base, proxies, nil, now.Add(6*time.Second))
	if len(restarted.queue.Samples) != 3 {
		t.Fatal("restart duplicated old history")
	}
}

func TestBoundNodeRejectsAmbiguousIdentityAndWrongURL(t *testing.T) {
	u, st, _ := uploadFixture(t, "http://127.0.0.1:8796")
	s := st.Get().Upload
	m := mon()
	m.connected, m.base, m.uploader = true, s.Source, u
	now := time.Now().UTC()
	value := 0
	proxies := map[string]latencyProxy{
		s.Group: {Type: "URLTest", Now: "other", TestURL: s.TargetURL, All: []string{"other", s.TargetNode}},
		s.TargetNode: {Type: "Vmess", Extra: map[string]proxyState{"wrong-url": {
			History: []delayResult{{Time: now.Add(time.Second), Delay: &value}},
		}}},
	}
	m.ingestLatency(m.base, proxies, nil, now)
	n := proxies[s.TargetNode]
	n.Extra[s.TargetURL] = proxyState{History: []delayResult{{Time: now.Add(time.Second), Delay: &value}}}
	proxies[s.TargetNode] = n
	providers := map[string]struct {
		Proxies []latencyProxy `json:"proxies"`
	}{"conflict": {Proxies: []latencyProxy{{Name: s.TargetNode, Type: "Vmess"}}}}
	m.ingestLatency(m.base, proxies, providers, now.Add(2*time.Second))
	n.Type = "URLTest"
	proxies[s.TargetNode] = n
	m.ingestLatency(m.base, proxies, nil, now.Add(3*time.Second))
	if len(u.queue.Samples) != 0 {
		t.Fatal("wrong URL, ambiguous node or group history uploaded")
	}
}
