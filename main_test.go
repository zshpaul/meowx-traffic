package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestSaveReload(t *testing.T) {
	m := mon()
	m.file = filepath.Join(t.TempDir(), "history.json")
	m.disk.Rows["a"] = &Row{App: "test", Down: 42}
	if err := m.save(); err != nil {
		t.Fatal(err)
	}
	m.disk.Rows["a"].Down = 65
	if err := m.save(); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(m.file)
	if err != nil {
		t.Fatal(err)
	}
	var d Disk
	if json.Unmarshal(b, &d) != nil || d.Rows["a"].Down != 65 {
		t.Fatal("save/replace failed")
	}
}

func TestLiveMeowX(t *testing.T) {
	if os.Getenv("MEOWX_LIVE_TEST") != "1" {
		t.Skip("opt-in local integration")
	}
	base, secret, err := endpoint()
	if err != nil {
		t.Fatal(err)
	}
	var version map[string]any
	if err = get(base, secret, "/version", &version); err != nil {
		t.Fatal(err)
	}
	var s Snapshot
	if err = get(base, secret, "/connections", &s); err != nil {
		t.Fatal(err)
	}
	if err = validate(s); err != nil {
		t.Fatal(err)
	}
	m := mon()
	m.ingest(s, time.Now())
	time.Sleep(1200 * time.Millisecond)
	if err = get(base, secret, "/connections", &s); err != nil {
		t.Fatal(err)
	}
	m.ingest(s, time.Now())
	t.Logf("version=%q; active=%d; groups=%d; deltaUp=%d; deltaDown=%d", version["version"], len(s.Connections), len(m.disk.Rows), m.disk.CoreUp, m.disk.CoreDown)
}

func snap(u, d int64, cs ...Connection) Snapshot {
	return Snapshot{UploadTotal: &u, DownloadTotal: &d, Connections: cs}
}
func mon() *Monitor { return &Monitor{disk: Disk{Rows: map[string]*Row{}}} }
func TestBaselineDeltaResetAndReconnect(t *testing.T) {
	m := mon()
	now := time.Now()
	c := Connection{ID: "a", Start: now.Add(-time.Hour).Format(time.RFC3339Nano), Upload: 100, Download: 300, Chains: []string{"DIRECT"}}
	m.ingest(snap(1000, 3000, c), now)
	if len(m.disk.Rows) != 0 {
		t.Fatal("baseline must not import old traffic")
	}
	c.Upload += 20
	c.Download += 50
	m.ingest(snap(1020, 3050, c), now.Add(time.Second))
	if len(m.disk.Rows) != 1 {
		t.Fatal("expected exactly one attributed row")
	}
	for _, r := range m.disk.Rows {
		if r.Up != 20 || r.Down != 50 || r.Route != "直连" {
			t.Fatal(r)
		}
	}
	m.ingest(snap(1, 1), now.Add(2*time.Second))
	if m.disk.Gaps != 1 || m.disk.CoreDown != 50 {
		t.Fatal("core reset counted")
	}
	m.ingest(snap(2000, 8000, c), now.Add(time.Minute))
	if m.disk.Gaps != 2 || m.disk.CoreDown != 50 {
		t.Fatal("gap counted")
	}
}
func TestNewConnectionAndShortConnectionResidual(t *testing.T) {
	m := mon()
	now := time.Now()
	m.ingest(snap(0, 0), now)
	c := Connection{ID: "new", Start: now.Add(time.Millisecond).Format(time.RFC3339Nano), Download: 40, Chains: []string{"US", "selector"}}
	m.ingest(snap(0, 100, c), now.Add(time.Second))
	var total int64
	for _, r := range m.disk.Rows {
		total += r.Down
		if r.Route != "代理" {
			t.Fatal(r)
		}
	}
	if total != 40 || m.disk.CoreDown-total != 60 {
		t.Fatal("unobserved bytes must not be attributed")
	}
}
func TestCapabilityValidation(t *testing.T) {
	if validate(Snapshot{}) == nil {
		t.Fatal("accepted invalid controller")
	}
	if validate(snap(0, 0)) != nil {
		t.Fatal("empty connections is valid")
	}
	if validate(snap(-1, 0)) == nil {
		t.Fatal("negative counter")
	}
}
