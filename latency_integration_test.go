//go:build integration

package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

// TestSyntheticProbeIntegration is run only against an isolated loopback
// candidate Probe. It requires an operator to inspect each announced stage.
func TestSyntheticProbeIntegration(t *testing.T) {
	endpoint, token := os.Getenv("SYNTHETIC_PROBE_URL"), os.Getenv("SYNTHETIC_PROBE_TOKEN")
	if !strings.HasPrefix(endpoint, "http://127.0.0.1:") || token == "" {
		t.Fatal("isolated loopback Probe and synthetic token required")
	}
	const testURL = "https://synthetic.invalid/generate_204"
	const dmitID = "f2f12d63d35852638ee68073d60e2028"
	history := []delayResult{}
	bwhHistory := []delayResult{}
	controller := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/proxies":
			json.NewEncoder(w).Encode(map[string]any{"proxies": map[string]any{
				"synthetic-group": map[string]any{"type": "URLTest", "now": "synthetic-BWH", "testUrl": testURL, "all": []string{"synthetic-BWH", "synthetic-DMIT"}},
				"synthetic-BWH": map[string]any{"type": "Vmess", "extra": map[string]any{testURL: map[string]any{"history": bwhHistory}}},
				"synthetic-DMIT": map[string]any{"type": "Vmess", "extra": map[string]any{testURL: map[string]any{"history": history}}},
			}})
		case "/providers/proxies":
			json.NewEncoder(w).Encode(map[string]any{"providers": map[string]any{}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer controller.Close()
	primary, st, _ := uploadFixture(t, endpoint)
	cipher, err := encryptToken(token)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now().UTC().Add(-time.Minute)
	_, err = st.Update(func(v *Settings) {
		v.Controller, v.Address = "manual", controller.URL
		v.Upload.Source, v.Upload.Group, v.Upload.TargetNode, v.Upload.TargetURL = controller.URL, "synthetic-group", "synthetic-BWH", testURL
		v.Upload.TokenCipher, v.Upload.StartedAt = cipher, start
		v.Upload.GroupID, v.Upload.GroupAlias, v.Upload.NodeAlias = "8a0929c0a3e3ad68ff8be667470b45ca", "Synthetic BWH", "Synthetic BWH"
	})
	if err != nil {
		t.Fatal(err)
	}
	primary.queue.Credential = credentialFingerprint(cipher)
	if err := primary.saveLocked(); err != nil {
		t.Fatal(err)
	}
	extra, extraStore, _ := uploadFixture(t, endpoint)
	_, err = extraStore.Update(func(v *Settings) {
		v.Upload = st.Get().Upload
		v.Upload.GroupID, v.Upload.GroupAlias, v.Upload.NodeAlias = dmitID, "Synthetic DMIT", "Synthetic DMIT"
		v.Upload.Binding, v.Upload.TargetNode = "synthetic-dmit-binding", "synthetic-DMIT"
	})
	if err != nil {
		t.Fatal(err)
	}
	extra.parent = st
	extra.queue.Binding, extra.queue.Credential = "synthetic-dmit-binding", credentialFingerprint(cipher)
	if err := extra.saveLocked(); err != nil {
		t.Fatal(err)
	}
	m := mon()
	m.connected, m.base, m.store, m.uploader = true, controller.URL, st, primary
	m.extraBindings = []*extraBinding{{store: extraStore, uploader: extra}}
	m.pollLatency()
	if m.latencyError != "" {
		t.Fatal(m.latencyError)
	}
	wait := bufio.NewReader(os.Stdin)
	stage := func(name string) {
		t.Helper()
		fmt.Printf("SYNTHETIC_STAGE %s\n", name)
		if _, err := wait.ReadString('\n'); err != nil {
			t.Fatal(err)
		}
	}
	record := func(value int) {
		t.Helper()
		time.Sleep(5 * time.Millisecond)
		measured := time.Now().UTC()
		history = append(history, delayResult{Time: measured, Delay: &value})
		m.pollLatency()
		if m.latencyError != "" {
			t.Fatal(m.latencyError)
		}
		fmt.Printf("SYNTHETIC_SAMPLE %s %d\n", measured.Format(time.RFC3339Nano), value)
	}
	// BWH is selected and remains a separate binding throughout.
	bwh := 33
	bwhHistory = []delayResult{{Time: time.Now().UTC(), Delay: &bwh}}
	record(120)
	primary.flush()
	extra.flush()
	if len(extra.queue.Samples) != 0 || len(primary.queue.Samples) != 0 {
		t.Fatal("synthetic success was not acknowledged")
	}
	stage("success")
	// Retain one failure across an uploader restart before sending it.
	record(0)
	if len(extra.queue.Samples) != 1 {
		t.Fatal("synthetic failure was not queued")
	}
	extra = newUploader(extra.file, extraStore)
	extra.parent = st
	m.extraBindings[0].uploader = extra
	extra.flush()
	if len(extra.queue.Samples) != 0 {
		t.Fatal("synthetic failure was not acknowledged after restart")
	}
	for i := 0; i < 9; i++ {
		record(0)
		extra.flush()
		if len(extra.queue.Samples) != 0 {
			t.Fatal("synthetic failure was not acknowledged")
		}
	}
	stage("ten-failures")
	record(160)
	extra.flush()
	if len(extra.queue.Samples) != 0 {
		t.Fatal("synthetic recovery was not acknowledged")
	}
	stage("recovery")
}
