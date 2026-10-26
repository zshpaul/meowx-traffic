package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"slices"

	"github.com/egoist/mygo"
)

func desktop(url, dir string) {
	app := mygo.App
	var win *mygo.Window
	show := func() {
		if win == nil || win.IsDestroyed() {
			win = mygo.NewWindow(mygo.WindowOptions{Title: "MeowX Traffic", Width: 1240, Height: 850, MinWidth: 760, MinHeight: 580, URL: url})
		}
		icon, _ := assets.ReadFile("assets/icon.png")
		_ = win.SetIcon(icon)
		win.Show()
		win.Restore()
		win.Focus()
	}
	app.OnSecondInstance(func([]string, string) { show() })
	app.OnWindowAllClosed(func() {})
	app.WhenReady(func() {
		prefFile := filepath.Join(dir, "desktop.json")
		pref := struct{ Silent bool }{true}
		if b, err := os.ReadFile(prefFile); err == nil {
			_ = json.Unmarshal(b, &pref)
		}
		if slices.Contains(os.Args, "--enable-autostart") {
			if err := app.SetOpenAtLogin(true); err != nil {
				mygo.Dialog.Error("开机自启设置失败", err.Error())
			}
		}
		var menu *mygo.Menu
		refresh := func() {}
		menu = mygo.NewMenu([]*mygo.MenuItem{
			{Label: "打开统计窗口", Click: func(*mygo.MenuItem, *mygo.Window) { show() }},
			{ID: "autostart", Label: "开机自启", Click: func(*mygo.MenuItem, *mygo.Window) {
				if err := app.SetOpenAtLogin(!app.OpenAtLogin()); err != nil {
					mygo.Dialog.Error("设置失败", err.Error())
				}
				refresh()
			}},
			{ID: "silent", Label: "静默启动", Click: func(*mygo.MenuItem, *mygo.Window) {
				next := pref
				next.Silent = !pref.Silent
				b, _ := json.Marshal(next)
				if err := os.WriteFile(prefFile, b, 0600); err != nil {
					mygo.Dialog.Error("设置失败", err.Error())
					return
				}
				pref = next
				refresh()
			}},
			mygo.Separator(),
			{Label: "退出并保存", Click: func(*mygo.MenuItem, *mygo.Window) { app.Quit() }},
		})
		refresh = func() {
			label := func(s string, on bool) string {
				if on {
					return s + "：已开启"
				}
				return s + "：已关闭"
			}
			menu.ItemByID("autostart").SetLabel(label("开机自启", app.OpenAtLogin()))
			menu.ItemByID("silent").SetLabel(label("静默启动", pref.Silent))
		}
		refresh()
		icon, _ := assets.ReadFile("assets/icon.png")
		tray, err := mygo.NewTray(mygo.TrayOptions{Icon: icon, ToolTip: "MeowX Traffic · 后台采集", Menu: menu})
		if err != nil {
			mygo.Dialog.Error("托盘创建失败", "将显示窗口，避免程序隐藏后无法操作："+err.Error())
			show()
			return
		}
		tray.OnClick(show)
		if slices.Contains(os.Args, "--show") || (!pref.Silent && !slices.Contains(os.Args, "--silent")) {
			show()
		}
	})
}
