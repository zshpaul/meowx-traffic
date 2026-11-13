package main

import (
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"time"
)

type probeGuardStatus struct {
	State   string    `json:"state"`
	Updated time.Time `json:"updatedAt"`
	PID     int       `json:"pid,omitempty"`
}

func probeGuardRoute(dir string) http.HandlerFunc {
	folder := filepath.Join(dir, "probe")
	stop := filepath.Join(folder, "guard.stop")
	statefile := filepath.Join(folder, "guard-status.json")
	view := func(w http.ResponseWriter) {
		var state probeGuardStatus
		if b, e := os.ReadFile(statefile); e == nil {
			_ = json.Unmarshal(b, &state)
		}
		if _, e := os.Stat(stop); e == nil {
			state.State = "stopped"
		} else if state.Updated.IsZero() || time.Since(state.Updated) > 20*time.Second {
			state.State = "not-running"
		}
		jsonOut(w, state)
	}
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			view(w)
			return
		}
		post(func(w http.ResponseWriter, r *http.Request) {
			var in struct {
				Action string `json:"action"`
			}
			if json.NewDecoder(r.Body).Decode(&in) != nil {
				http.Error(w, "请求格式无效", 400)
				return
			}
			switch in.Action {
			case "stop":
				if e := os.MkdirAll(folder, 0700); e != nil {
					http.Error(w, "无法停止探针", 500)
					return
				}
				if e := os.WriteFile(stop, []byte("stop\n"), 0600); e != nil {
					http.Error(w, "无法停止探针", 500)
					return
				}
			case "start":
				if e := os.Remove(stop); e != nil && !errors.Is(e, os.ErrNotExist) {
					http.Error(w, "无法启动探针", 500)
					return
				}
				cmd := exec.Command("schtasks.exe", "/Run", "/TN", "MeowX Traffic Probe Guard")
				cmd.SysProcAttr = &syscall.SysProcAttr{HideWindow: true, CreationFlags: 0x08000000}
				if e := cmd.Run(); e != nil {
					http.Error(w, "无法启动后台任务，请检查是否已安装", 500)
					return
				}
			default:
				http.Error(w, "未知操作", 400)
				return
			}
			view(w)
		})(w, r)
	}
}
