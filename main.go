package main

import (
	"crypto/rand"
	"embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/egoist/mygo"
)

//go:embed web/* assets/icon.png
var assets embed.FS

type Connection struct {
	ID          string   `json:"id"`
	Start       string   `json:"start"`
	Upload      int64    `json:"upload"`
	Download    int64    `json:"download"`
	Chains      []string `json:"chains"`
	Rule        string   `json:"rule"`
	RulePayload string   `json:"rulePayload"`
	Metadata    struct {
		Process       string `json:"process"`
		ProcessPath   string `json:"processPath"`
		Host          string `json:"host"`
		DestinationIP string `json:"destinationIP"`
	} `json:"metadata"`
}
type Snapshot struct {
	UploadTotal   *int64       `json:"uploadTotal"`
	DownloadTotal *int64       `json:"downloadTotal"`
	Connections   []Connection `json:"connections"`
}
type Row struct {
	Day   string
	App   string
	Path  string
	Host  string
	Route string
	Node  string
	Up    int64
	Down  int64
}
type Disk struct {
	Since    time.Time
	Rows     map[string]*Row
	CoreUp   int64
	CoreDown int64
	Gaps     int
}
type Monitor struct {
	mu        sync.Mutex
	disk      Disk
	prev      *Snapshot
	last      time.Time
	status    string
	saveError string
	version   string
	live      []Connection
	upRate    float64
	downRate  float64
	trend     [][3]float64
	file      string
}

func validate(s Snapshot) error {
	if s.UploadTotal == nil || s.DownloadTotal == nil || *s.UploadTotal < 0 || *s.DownloadTotal < 0 {
		return fmt.Errorf("接口缺少有效的流量计数")
	}
	for _, c := range s.Connections {
		if c.ID == "" || c.Upload < 0 || c.Download < 0 {
			return fmt.Errorf("连接计数格式无效")
		}
	}
	return nil
}
func identity(c Connection) string { return c.ID + "|" + c.Start }
func route(c Connection) string {
	if len(c.Chains) == 0 {
		return "未知"
	}
	switch strings.ToUpper(c.Chains[0]) {
	case "DIRECT":
		return "直连"
	case "REJECT", "REJECT-DROP":
		return "拒绝"
	default:
		return "代理"
	}
}
func (m *Monitor) ingest(s Snapshot, now time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.live = s.Connections
	m.upRate = 0
	m.downRate = 0
	if m.prev != nil && now.Sub(m.last) < 10*time.Second && *s.UploadTotal >= *m.prev.UploadTotal && *s.DownloadTotal >= *m.prev.DownloadTotal {
		dt := now.Sub(m.last).Seconds()
		du, dd := *s.UploadTotal-*m.prev.UploadTotal, *s.DownloadTotal-*m.prev.DownloadTotal
		m.disk.CoreUp += du
		m.disk.CoreDown += dd
		if dt > 0 {
			m.upRate = float64(du) / dt
			m.downRate = float64(dd) / dt
		}
		old := map[string]Connection{}
		for _, c := range m.prev.Connections {
			old[identity(c)] = c
		}
		for _, c := range s.Connections {
			u, d := c.Upload, c.Download
			if p, ok := old[identity(c)]; ok {
				u -= p.Upload
				d -= p.Download
				if u < 0 || d < 0 {
					continue
				}
			} else {
				start, err := time.Parse(time.RFC3339Nano, c.Start)
				if err != nil || start.Before(m.last) {
					continue
				}
			}
			if u == 0 && d == 0 {
				continue
			}
			app := c.Metadata.Process
			if app == "" {
				app = "未识别进程"
			}
			host := c.Metadata.Host
			if host == "" {
				host = c.Metadata.DestinationIP
			}
			node := ""
			if len(c.Chains) > 0 {
				node = c.Chains[0]
			}
			r := Row{Day: now.Format("2006-01-02"), App: app, Path: c.Metadata.ProcessPath, Host: host, Route: route(c), Node: node}
			kb, _ := json.Marshal([]string{r.Day, r.App, r.Path, r.Host, r.Route, r.Node})
			k := string(kb)
			if m.disk.Rows[k] == nil {
				m.disk.Rows[k] = &r
			}
			m.disk.Rows[k].Up += u
			m.disk.Rows[k].Down += d
		}
	} else if m.prev != nil {
		m.disk.Gaps++
	}
	m.prev = &s
	m.last = now
	m.trend = append(m.trend, [3]float64{float64(now.Unix()), m.upRate, m.downRate})
	if len(m.trend) > 120 {
		m.trend = m.trend[len(m.trend)-120:]
	}
	cut := now.AddDate(0, 0, -30).Format("2006-01-02")
	for k, r := range m.disk.Rows {
		if r.Day < cut {
			delete(m.disk.Rows, k)
		}
	}
}
func endpoint() (string, string, error) {
	p := filepath.Join(os.Getenv("APPDATA"), "com.miaomiaowux", "MeowX", "config.json")
	b, err := os.ReadFile(p)
	if err != nil {
		return "", "", fmt.Errorf("无法读取 MeowX 配置")
	}
	var conf struct {
		Patch struct {
			Address string `json:"external-controller"`
			Secret  string `json:"secret"`
		} `json:"patchClashConfig"`
	}
	if json.Unmarshal(b, &conf) != nil {
		return "", "", fmt.Errorf("MeowX 配置格式无效")
	}
	host, _, err := net.SplitHostPort(conf.Patch.Address)
	if err != nil || (host != "127.0.0.1" && host != "localhost" && host != "::1") {
		return "", "", fmt.Errorf("请先在 MeowX 开启本机 external-controller")
	}
	return "http://" + conf.Patch.Address, conf.Patch.Secret, nil
}

var client = &http.Client{Timeout: 4 * time.Second, Transport: &http.Transport{Proxy: nil}, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}

func get(base, secret, path string, out any) error {
	req, _ := http.NewRequest("GET", base+path, nil)
	req.Header.Set("Authorization", "Bearer "+secret)
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("MeowX 接口不可达，请确认内核已启动")
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("接口 HTTP %d；401 表示密钥不匹配", resp.StatusCode)
	}
	return json.NewDecoder(io.LimitReader(resp.Body, 16<<20)).Decode(out)
}
func (m *Monitor) poll() {
	base, secret, err := endpoint()
	if err == nil {
		var s Snapshot
		requestStart := time.Now()
		err = get(base, secret, "/connections", &s)
		if err == nil {
			err = validate(s)
		}
		if err == nil {
			m.ingest(s, requestStart)
			m.mu.Lock()
			m.status = "已连接 MeowX · 每秒采集"
			m.mu.Unlock()
			return
		}
	}
	m.mu.Lock()
	m.status = err.Error()
	m.upRate = 0
	m.downRate = 0
	m.live = nil
	m.mu.Unlock()
}
func (m *Monitor) save() error {
	m.mu.Lock()
	b, err := json.Marshal(m.disk)
	m.mu.Unlock()
	if err != nil {
		return err
	}
	if err = os.WriteFile(m.file+".tmp", b, 0600); err != nil {
		return err
	}
	return os.Rename(m.file+".tmp", m.file)
}
func main() {
	app := mygo.App
	app.SetName("MeowX Traffic")
	app.SetVersion("0.2.0")
	if !app.RequestSingleInstanceLock() {
		return
	}
	dir := filepath.Join(os.Getenv("LOCALAPPDATA"), "MeowX Traffic")
	if err := os.MkdirAll(dir, 0700); err != nil {
		panic(err)
	}
	m := &Monitor{file: filepath.Join(dir, "history.json"), disk: Disk{Since: time.Now(), Rows: map[string]*Row{}}, status: "连接中", live: []Connection{}, trend: [][3]float64{}}
	if b, err := os.ReadFile(m.file); err == nil {
		if json.Unmarshal(b, &m.disk) != nil || m.disk.Rows == nil {
			mygo.Dialog.Error("历史记录读取失败", "为保护历史数据，程序没有覆盖原文件："+m.file)
			return
		}
	} else if !os.IsNotExist(err) {
		mygo.Dialog.Error("历史记录读取失败", "已停止启动，避免覆盖数据："+err.Error())
		return
	}
	b := make([]byte, 24)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	token := hex.EncodeToString(b)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	root := "/" + token + "/"
	mux := http.NewServeMux()
	mux.HandleFunc(root+"api", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			http.Error(w, "Method not allowed", 405)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		m.mu.Lock()
		defer m.mu.Unlock()
		status := m.status
		if m.saveError != "" {
			status = "历史未保存：" + m.saveError
		}
		json.NewEncoder(w).Encode(map[string]any{"history": m.disk, "status": status, "last": m.last, "live": m.live, "upRate": m.upRate, "downRate": m.downRate, "trend": m.trend})
	})
	mux.HandleFunc(root, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != root {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Referrer-Policy", "no-referrer")
		b, _ := assets.ReadFile("web/index.html")
		w.Write(b)
	})
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 3 * time.Second}
	go srv.Serve(ln)
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		m.poll()
		t := time.NewTicker(time.Second)
		defer t.Stop()
		n := 0
		for {
			select {
			case <-stop:
				return
			case <-t.C:
				m.poll()
				n++
				if n%5 == 0 {
					saveErr := m.save()
					m.mu.Lock()
					m.saveError = ""
					if saveErr != nil {
						m.saveError = saveErr.Error()
					}
					m.mu.Unlock()
				}
			}
		}
	}()
	desktop("http://"+ln.Addr().String()+root, dir)
	go func() {
		time.Sleep(8 * time.Second)
		if !app.IsReady() {
			buf := make([]byte, 1<<20)
			n := runtime.Stack(buf, true)
			os.WriteFile(filepath.Join(dir, "startup-diagnostic.log"), buf[:n], 0600)
		}
	}()
	err = app.Run()
	close(stop)
	<-done
	if saveErr := m.save(); saveErr != nil {
		os.WriteFile(filepath.Join(dir, "save-error.log"), []byte(saveErr.Error()), 0600)
		mygo.Dialog.Error("流量历史保存失败", saveErr.Error())
	}
	srv.Close()
	if err != nil {
		os.WriteFile(filepath.Join(dir, "error.log"), []byte(err.Error()), 0600)
	}
}
