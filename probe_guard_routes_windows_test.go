//go:build windows

package main

import (
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestProbeGuardStopPersists(t *testing.T) {
	dir := t.TempDir()
	r := httptest.NewRequest("POST", "/", strings.NewReader(`{"action":"stop"}`))
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	probeGuardRoute(dir)(w, r)
	if w.Code != 200 {
		t.Fatal(w.Code)
	}
	if _, e := os.Stat(filepath.Join(dir, "probe", "guard.stop")); e != nil {
		t.Fatal(e)
	}
	w = httptest.NewRecorder()
	probeGuardRoute(dir)(w, httptest.NewRequest("GET", "/", nil))
	if !strings.Contains(w.Body.String(), `"state":"stopped"`) {
		t.Fatal("stop not reported")
	}
}
