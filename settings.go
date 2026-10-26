package main

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// Settings are the user's choices, kept in settings.json next to the history.
type Settings struct {
	Controller    string `json:"controller"` // "auto" reads MeowX's config, "manual" uses Address/Secret
	Address       string `json:"address"`
	Secret        string `json:"secret,omitempty"`
	RetentionDays int    `json:"retentionDays"`
	Theme         string `json:"theme"` // system | light | dark
	Silent        bool   `json:"silent"`
}

func defaultSettings() Settings {
	return Settings{Controller: "auto", RetentionDays: 30, Theme: "system", Silent: true}
}

// normalize checks a settings value and fills what is missing.
func (s Settings) normalize() (Settings, error) {
	if s.Controller == "" {
		s.Controller = "auto"
	}
	if s.Controller != "auto" && s.Controller != "manual" {
		return s, fmt.Errorf("未知的连接方式")
	}
	s.Address = strings.TrimSpace(s.Address)
	if s.Controller == "manual" {
		addr, err := controllerURL(s.Address)
		if err != nil {
			return s, err
		}
		s.Address = addr
	}
	if s.RetentionDays == 0 {
		s.RetentionDays = 30
	}
	if s.RetentionDays < 1 || s.RetentionDays > 3650 {
		return s, fmt.Errorf("保留天数需在 1 到 3650 之间")
	}
	switch s.Theme {
	case "":
		s.Theme = "system"
	case "system", "light", "dark":
	default:
		return s, fmt.Errorf("未知的主题")
	}
	return s, nil
}

// controllerURL accepts "127.0.0.1:9090" or a full http(s) URL.
func controllerURL(s string) (string, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return "", fmt.Errorf("请填写控制接口地址")
	}
	if !strings.Contains(s, "://") {
		s = "http://" + s
	}
	u, err := url.Parse(s)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.RawQuery != "" || u.Fragment != "" {
		return "", fmt.Errorf("控制接口地址格式无效，例如 http://127.0.0.1:9090")
	}
	return strings.TrimRight(u.Scheme+"://"+u.Host+u.Path, "/"), nil
}

type Store struct {
	mu       sync.Mutex
	file     string
	s        Settings
	onChange []func(Settings)
}

// loadStore reads settings.json; the first time it takes the silent-start
// choice of the older desktop.json.
func loadStore(dir string) *Store {
	st := &Store{file: filepath.Join(dir, "settings.json"), s: defaultSettings()}
	if b, err := os.ReadFile(st.file); err == nil {
		s := defaultSettings()
		if json.Unmarshal(b, &s) == nil {
			if n, err := s.normalize(); err == nil {
				st.s = n
			}
		}
	} else if b, err := os.ReadFile(filepath.Join(dir, "desktop.json")); err == nil {
		old := struct{ Silent bool }{true}
		if json.Unmarshal(b, &old) == nil {
			st.s.Silent = old.Silent
		}
	}
	return st
}

func (st *Store) Get() Settings {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.s
}

// Update applies fn to a copy, validates and saves it, then tells listeners.
func (st *Store) Update(fn func(*Settings)) (Settings, error) {
	st.mu.Lock()
	next := st.s
	fn(&next)
	next, err := next.normalize()
	if err == nil {
		var b []byte
		b, _ = json.MarshalIndent(next, "", "  ")
		if err = os.WriteFile(st.file+".tmp", b, 0600); err == nil {
			err = os.Rename(st.file+".tmp", st.file)
		}
	}
	if err != nil {
		st.mu.Unlock()
		return st.Get(), err
	}
	st.s = next
	listeners := append([]func(Settings){}, st.onChange...)
	st.mu.Unlock()
	for _, fn := range listeners {
		fn(next)
	}
	return next, nil
}

func (st *Store) OnChange(fn func(Settings)) {
	st.mu.Lock()
	st.onChange = append(st.onChange, fn)
	st.mu.Unlock()
}
