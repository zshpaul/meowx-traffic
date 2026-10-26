package main

import (
	"crypto/rand"
	"embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"mime"
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

const version = "0.3.0"

// dataVersion 2 adds the hour to every row; older rows get Hour -1.
const dataVersion = 2

type Connection struct {
	ID          string   `json:"id"`
	Start       string   `json:"start"`
	Upload      int64    `json:"upload"`
	Download    int64    `json:"download"`
	Chains      []string `json:"chains"`
	Rule        string   `json:"rule"`
	RulePayload string   `json:"rulePayload"`
	Metadata    struct {
		Network         string          `json:"network"`
		Process         string          `json:"process"`
		ProcessPath     string          `json:"processPath"`
		Host            string          `json:"host"`
		DestinationIP   string          `json:"destinationIP"`
		DestinationPort json.RawMessage `json:"destinationPort"`
	} `json:"metadata"`
}
type Snapshot struct {
	UploadTotal   *int64       `json:"uploadTotal"`
	DownloadTotal *int64       `json:"downloadTotal"`
	Connections   []Connection `json:"connections"`
}
type Row struct {
	Day   string
	Hour  int
	App   string
	Path  string
	Host  string
	Route string
	Node  string
	Up    int64
	Down  int64
	Conns int64
}
type Disk struct {
	Version  int
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
	connected bool
	status    string
	saveError string
	version   string
	base      string
	polls     int
	live      []Connection
	upRate    float64
	downRate  float64
	trend     [][3]float64
	counted   map[string]bool
	retention int
	store     *Store
	file      string
}

func newDisk() Disk {
	return Disk{Version: dataVersion, Since: time.Now(), Rows: map[string]*Row{}}
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
	if m.counted == nil {
		m.counted = map[string]bool{}
	}
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
			r := Row{Day: now.Format("2006-01-02"), Hour: now.Hour(), App: app, Path: c.Metadata.ProcessPath, Host: host, Route: route(c), Node: node}
			kb, _ := json.Marshal([]any{r.Day, r.Hour, r.App, r.Path, r.Host, r.Route, r.Node})
			k := string(kb)
			if m.disk.Rows[k] == nil {
				m.disk.Rows[k] = &r
			}
			m.disk.Rows[k].Up += u
			m.disk.Rows[k].Down += d
			// A connection counts once, in the row of its first attributed bytes.
			if id := identity(c); !m.counted[id] {
				m.counted[id] = true
				m.disk.Rows[k].Conns++
			}
		}
	} else if m.prev != nil {
		m.disk.Gaps++
	}
	present := map[string]bool{}
	for _, c := range s.Connections {
		if id := identity(c); m.counted[id] {
			present[id] = true
		}
	}
	m.counted = present
	m.prev = &s
	m.last = now
	m.trend = append(m.trend, [3]float64{float64(now.Unix()), m.upRate, m.downRate})
	if len(m.trend) > 300 {
		m.trend = m.trend[len(m.trend)-300:]
	}
	m.prune(now)
}

// prune drops rows older than the retention. Caller holds m.mu.
func (m *Monitor) prune(now time.Time) {
	days := m.retention
	if days <= 0 {
		days = 30
	}
	cut := now.AddDate(0, 0, -(days - 1)).Format("2006-01-02")
	for k, r := range m.disk.Rows {
		if r.Day < cut {
			delete(m.disk.Rows, k)
		}
	}
}

// endpoint reads the controller of MeowX's own config.
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

// resolve gives the controller the settings choose.
func resolve(s Settings) (string, string, error) {
	if s.Controller == "manual" {
		base, err := controllerURL(s.Address)
		return base, s.Secret, err
	}
	return endpoint()
}

var client = &http.Client{Timeout: 4 * time.Second, Transport: &http.Transport{Proxy: nil}, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}

func get(base, secret, path string, out any) error {
	req, err := http.NewRequest("GET", base+path, nil)
	if err != nil {
		return fmt.Errorf("控制接口地址无效")
	}
	if secret != "" {
		req.Header.Set("Authorization", "Bearer "+secret)
	}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("MeowX 接口不可达，请确认内核已启动")
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("接口 HTTP %d；401 表示密钥不匹配", resp.StatusCode)
	}
	if err = json.NewDecoder(io.LimitReader(resp.Body, 16<<20)).Decode(out); err != nil {
		return fmt.Errorf("不是 Clash 控制接口")
	}
	return nil
}

// probe checks that a controller answers like Clash and returns its version.
func probe(base, secret string) (string, error) {
	var s Snapshot
	if err := get(base, secret, "/connections", &s); err != nil {
		return "", err
	}
	if err := validate(s); err != nil {
		return "", fmt.Errorf("不是 Clash 控制接口：%v", err)
	}
	var v struct {
		Version string `json:"version"`
	}
	_ = get(base, secret, "/version", &v)
	return v.Version, nil
}

func (m *Monitor) settings() Settings {
	if m.store == nil {
		return defaultSettings()
	}
	return m.store.Get()
}

func (m *Monitor) poll() {
	base, secret, err := resolve(m.settings())
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
			need := m.version == "" && m.polls%30 == 0
			m.polls++
			m.mu.Unlock()
			ver := ""
			if need {
				var v struct {
					Version string `json:"version"`
				}
				if get(base, secret, "/version", &v) == nil {
					ver = v.Version
				}
			}
			m.mu.Lock()
			m.connected = true
			m.base = base
			m.status = "已连接 MeowX · 每秒采集"
			if ver != "" {
				m.version = ver
			}
			m.mu.Unlock()
			return
		}
	}
	m.mu.Lock()
	m.connected = false
	m.base = base
	m.version = ""
	m.polls = 0
	m.status = err.Error()
	m.upRate = 0
	m.downRate = 0
	m.live = nil
	m.mu.Unlock()
}

// rebase forgets the last snapshot, e.g. when the controller changes, so the
// totals of another core are not counted as traffic.
func (m *Monitor) rebase() {
	m.mu.Lock()
	m.prev = nil
	m.counted = nil
	m.version = ""
	m.polls = 0
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

// load reads the history and moves older data to the current format.
func (m *Monitor) load() error {
	b, err := os.ReadFile(m.file)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	var d Disk
	if err = json.Unmarshal(b, &d); err != nil || d.Rows == nil {
		return fmt.Errorf("历史文件格式无效：%s", m.file)
	}
	if d.Version < 2 {
		for _, r := range d.Rows {
			r.Hour = -1
		}
	}
	d.Version = dataVersion
	m.disk = d
	return nil
}

func jsonOut(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	json.NewEncoder(w).Encode(v)
}

// post accepts only JSON POSTs: a page elsewhere can't send those here
// without a CORS preflight, which this server never grants.
func post(fn func(w http.ResponseWriter, r *http.Request)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ct, _, _ := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if r.Method != "POST" || ct != "application/json" {
			http.Error(w, "Method not allowed", 405)
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, 1<<16)
		fn(w, r)
	}
}

func getOnly(fn func(w http.ResponseWriter, r *http.Request)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			http.Error(w, "Method not allowed", 405)
			return
		}
		fn(w, r)
	}
}

// settingsView is what the page sees of the settings: never a secret.
func settingsView(st *Store, dir string) map[string]any {
	s := st.Get()
	hasSecret := s.Secret != ""
	s.Secret = ""
	auto, _, autoErr := endpoint()
	autoError := ""
	if autoErr != nil {
		autoError = autoErr.Error()
	}
	return map[string]any{"settings": s, "hasSecret": hasSecret, "autostart": mygo.App.OpenAtLogin(), "dataDir": dir, "version": version, "autoEndpoint": auto, "autoError": autoError}
}

type settingsInput struct {
	Settings
	ClearSecret bool  `json:"clearSecret"`
	Autostart   *bool `json:"autostart"`
}

func routes(mux *http.ServeMux, root, dir string, m *Monitor, st *Store) {
	mux.HandleFunc(root+"api/live", getOnly(func(w http.ResponseWriter, r *http.Request) {
		m.mu.Lock()
		defer m.mu.Unlock()
		var up, down int64
		if m.prev != nil && m.connected {
			up, down = *m.prev.UploadTotal, *m.prev.DownloadTotal
		}
		jsonOut(w, map[string]any{"status": m.status, "connected": m.connected, "version": m.version, "endpoint": m.base, "saveError": m.saveError, "last": m.last, "live": m.live, "upRate": m.upRate, "downRate": m.downRate, "trend": m.trend, "uploadTotal": up, "downloadTotal": down})
	}))
	mux.HandleFunc(root+"api/history", getOnly(func(w http.ResponseWriter, r *http.Request) {
		m.mu.Lock()
		rows := make([]Row, 0, len(m.disk.Rows))
		for _, r := range m.disk.Rows {
			rows = append(rows, *r)
		}
		out := map[string]any{"since": m.disk.Since, "coreUp": m.disk.CoreUp, "coreDown": m.disk.CoreDown, "gaps": m.disk.Gaps, "rows": rows, "retentionDays": m.retention}
		m.mu.Unlock()
		jsonOut(w, out)
	}))
	mux.HandleFunc(root+"api/settings", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			jsonOut(w, settingsView(st, dir))
			return
		}
		post(func(w http.ResponseWriter, r *http.Request) {
			var in settingsInput
			if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
				http.Error(w, "请求格式无效", 400)
				return
			}
			if _, err := in.Settings.normalize(); err != nil {
				http.Error(w, err.Error(), 400)
				return
			}
			before := st.Get()
			if in.Autostart != nil && *in.Autostart != mygo.App.OpenAtLogin() {
				if err := mygo.App.SetOpenAtLogin(*in.Autostart); err != nil {
					http.Error(w, "开机自启设置失败："+err.Error(), 500)
					return
				}
			}
			next, err := st.Update(func(s *Settings) {
				secret := s.Secret
				*s = in.Settings
				if in.Secret == "" && !in.ClearSecret {
					s.Secret = secret
				}
			})
			if err != nil {
				http.Error(w, err.Error(), 400)
				return
			}
			if next.Controller != before.Controller || next.Address != before.Address || next.Secret != before.Secret {
				m.rebase()
			}
			jsonOut(w, settingsView(st, dir))
		})(w, r)
	})
	mux.HandleFunc(root+"api/test", post(func(w http.ResponseWriter, r *http.Request) {
		var in Settings
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			http.Error(w, "请求格式无效", 400)
			return
		}
		if in.Controller == "manual" && in.Secret == "" {
			in.Secret = st.Get().Secret
		}
		base, secret, err := resolve(in)
		ver := ""
		if err == nil {
			ver, err = probe(base, secret)
		}
		out := map[string]any{"ok": err == nil, "endpoint": base, "version": ver}
		if err != nil {
			out["error"] = err.Error()
		}
		jsonOut(w, out)
	}))
	mux.HandleFunc(root+"api/clear", post(func(w http.ResponseWriter, r *http.Request) {
		m.mu.Lock()
		m.disk = newDisk()
		m.counted = nil
		m.mu.Unlock()
		if err := m.save(); err != nil {
			http.Error(w, "已清空，但保存失败："+err.Error(), 500)
			return
		}
		jsonOut(w, map[string]any{"ok": true})
	}))
	mux.HandleFunc(root+"api/open-dir", post(func(w http.ResponseWriter, r *http.Request) {
		if err := mygo.Shell.OpenPath(dir); err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		jsonOut(w, map[string]any{"ok": true})
	}))
	mux.HandleFunc(root+"icon.png", getOnly(func(w http.ResponseWriter, r *http.Request) {
		b, _ := assets.ReadFile("assets/icon.png")
		w.Header().Set("Content-Type", "image/png")
		w.Write(b)
	}))
	mux.HandleFunc(root, getOnly(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != root {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Referrer-Policy", "no-referrer")
		w.Header().Set("Cache-Control", "no-store")
		b, _ := assets.ReadFile("web/index.html")
		w.Write(b)
	}))
}

func main() {
	app := mygo.App
	app.SetName("MeowX Traffic")
	app.SetVersion(version)
	if !app.RequestSingleInstanceLock() {
		return
	}
	dir := filepath.Join(os.Getenv("LOCALAPPDATA"), "MeowX Traffic")
	if err := os.MkdirAll(dir, 0700); err != nil {
		panic(err)
	}
	st := loadStore(dir)
	m := &Monitor{file: filepath.Join(dir, "history.json"), disk: newDisk(), status: "连接中", live: []Connection{}, trend: [][3]float64{}, store: st, retention: st.Get().RetentionDays}
	if err := m.load(); err != nil {
		mygo.Dialog.Error("历史记录读取失败", "为保护历史数据，程序没有覆盖原文件："+err.Error())
		return
	}
	mygo.Theme.SetSource(mygo.ThemeSource(st.Get().Theme))
	st.OnChange(func(s Settings) {
		mygo.Theme.SetSource(mygo.ThemeSource(s.Theme))
		m.mu.Lock()
		m.retention = s.RetentionDays
		m.prune(time.Now())
		m.mu.Unlock()
	})
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
	routes(mux, root, dir, m, st)
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
	desktop("http://"+ln.Addr().String()+root, st)
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
