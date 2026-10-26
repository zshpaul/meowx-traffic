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

func TestControllerURL(t *testing.T) {
	for in, want := range map[string]string{"127.0.0.1:9090": "http://127.0.0.1:9090", " http://localhost:9097/ ": "http://localhost:9097", "https://h:1/api": "https://h:1/api"} {
		if got, err := controllerURL(in); err != nil || got != want {
			t.Fatalf("%q: %q %v", in, got, err)
		}
	}
	for _, in := range []string{"", "ftp://x:1", "http://", "http://x:1/?a=1"} {
		if _, err := controllerURL(in); err == nil {
			t.Fatalf("accepted %q", in)
		}
	}
	if _, err := (Settings{RetentionDays: 4000}).normalize(); err == nil {
		t.Fatal("accepted retention")
	}
	if _, err := (Settings{Theme: "blue"}).normalize(); err == nil {
		t.Fatal("accepted theme")
	}
}

func TestStoreMigratesAndPersists(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "desktop.json"), []byte(`{"Silent":false}`), 0600)
	st := loadStore(dir)
	if s := st.Get(); s.Silent || s.RetentionDays != 30 || s.Controller != "auto" {
		t.Fatal(s)
	}
	called := 0
	st.OnChange(func(Settings) { called++ })
	if _, err := st.Update(func(s *Settings) { s.RetentionDays = 7; s.Controller = "manual"; s.Address = "127.0.0.1:9090"; s.Secret = "x" }); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Update(func(s *Settings) { s.RetentionDays = -1 }); err == nil || st.Get().RetentionDays != 7 {
		t.Fatal("invalid update applied")
	}
	s := loadStore(dir).Get()
	if called != 1 || s.RetentionDays != 7 || s.Address != "http://127.0.0.1:9090" || s.Secret != "x" || s.Silent {
		t.Fatal(s, called)
	}
}

func TestLoadMarksOldRowsWithoutHour(t *testing.T) {
	m := mon()
	m.file = filepath.Join(t.TempDir(), "history.json")
	os.WriteFile(m.file, []byte(`{"Since":"2026-10-20T00:00:00Z","Rows":{"k":{"Day":"2026-10-20","App":"a","Down":5}}}`), 0600)
	if err := m.load(); err != nil {
		t.Fatal(err)
	}
	if m.disk.Version != dataVersion || m.disk.Rows["k"].Hour != -1 {
		t.Fatal(m.disk.Rows["k"])
	}
	os.WriteFile(m.file, []byte(`not json`), 0600)
	if m.load() == nil {
		t.Fatal("accepted broken history")
	}
}

func TestHourConnsAndRetention(t *testing.T) {
	m := mon()
	now := time.Date(2026, 10, 26, 14, 30, 0, 0, time.Local)
	m.disk.Rows["old"] = &Row{Day: "2026-10-20", Down: 1}
	m.retention = 3
	m.ingest(snap(0, 0), now)
	if m.disk.Rows["old"] != nil {
		t.Fatal("retention not applied")
	}
	c := Connection{ID: "c", Start: now.Add(time.Millisecond).Format(time.RFC3339Nano), Download: 10, Chains: []string{"DIRECT"}}
	m.ingest(snap(0, 10, c), now.Add(time.Second))
	c.Download = 30
	m.ingest(snap(0, 30, c), now.Add(2*time.Second))
	if len(m.disk.Rows) != 1 {
		t.Fatal(m.disk.Rows)
	}
	for _, r := range m.disk.Rows {
		if r.Hour != 14 || r.Conns != 1 || r.Down != 30 {
			t.Fatal(r)
		}
	}
}

func TestRoutes(t *testing.T) {
	core := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer s" {
			w.WriteHeader(401)
			return
		}
		switch r.URL.Path {
		case "/connections":
			w.Write([]byte(`{"uploadTotal":1,"downloadTotal":2,"connections":[]}`))
		case "/version":
			w.Write([]byte(`{"version":"v1.19"}`))
		}
	}))
	defer core.Close()
	dir := t.TempDir()
	st := loadStore(dir)
	m := mon()
	m.file = filepath.Join(dir, "history.json")
	m.store = st
	m.disk.Rows["a"] = &Row{Day: "2026-10-26", Down: 9}
	mux := http.NewServeMux()
	routes(mux, "/t/", dir, m, st)
	srv := httptest.NewServer(mux)
	defer srv.Close()
	send := func(method, path, ct, body string) (int, map[string]any) {
		req, _ := http.NewRequest(method, srv.URL+"/t/"+path, strings.NewReader(body))
		if ct != "" {
			req.Header.Set("Content-Type", ct)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var out map[string]any
		json.NewDecoder(resp.Body).Decode(&out)
		return resp.StatusCode, out
	}
	if code, out := send("GET", "api/history", "", ""); code != 200 || len(out["rows"].([]any)) != 1 {
		t.Fatal(code, out)
	}
	if code, _ := send("POST", "api/clear", "text/plain", "{}"); code != 405 {
		t.Fatal("clear without JSON content type", code)
	}
	if code, out := send("POST", "api/test", "application/json", `{"controller":"manual","address":"`+core.URL+`","secret":"s"}`); code != 200 || out["ok"] != true || out["version"] != "v1.19" {
		t.Fatal(code, out)
	}
	if _, out := send("POST", "api/test", "application/json", `{"controller":"manual","address":"`+core.URL+`","secret":"bad"}`); out["ok"] != false {
		t.Fatal(out)
	}
	if code, _ := send("POST", "api/clear", "application/json", "{}"); code != 200 || len(m.disk.Rows) != 0 {
		t.Fatal("clear failed")
	}
	if _, err := os.Stat(m.file); err != nil {
		t.Fatal("clear not saved")
	}
}
