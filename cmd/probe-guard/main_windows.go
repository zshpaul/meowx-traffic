//go:build windows

package main

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"
	"unsafe"
)

var kernel = syscall.NewLazyDLL("kernel32.dll")
var iphlp = syscall.NewLazyDLL("iphlpapi.dll")
var crypt = syscall.NewLazyDLL("crypt32.dll")
var createMutex = kernel.NewProc("CreateMutexW")
var createJob = kernel.NewProc("CreateJobObjectW")
var setJob = kernel.NewProc("SetInformationJobObject")
var assignJob = kernel.NewProc("AssignProcessToJobObject")
var openProcess = kernel.NewProc("OpenProcess")
var closeHandle = kernel.NewProc("CloseHandle")
var tcpTable = iphlp.NewProc("GetExtendedTcpTable")
var unprotect = crypt.NewProc("CryptUnprotectData")
var localFree = kernel.NewProc("LocalFree")

type blob struct {
	Size uint32
	Data *byte
}
type basicLimits struct {
	PerProcessUserTimeLimit, PerJobUserTimeLimit int64
	LimitFlags                                   uint32
	MinimumWorkingSetSize, MaximumWorkingSetSize uintptr
	ActiveProcessLimit                           uint32
	Affinity                                     uintptr
	PriorityClass, SchedulingClass               uint32
}
type ioCounters struct{ ReadOperationCount, WriteOperationCount, OtherOperationCount, ReadTransferCount, WriteTransferCount, OtherTransferCount uint64 }
type extendedLimits struct {
	Basic                                                                        basicLimits
	IO                                                                           ioCounters
	ProcessMemoryLimit, JobMemoryLimit, PeakProcessMemoryUsed, PeakJobMemoryUsed uintptr
}

func singleton() (syscall.Handle, error) { return lockNamed(`Local\MeowXTrafficProbeGuardian`) }
func lockNamed(value string) (syscall.Handle, error) {
	name, _ := syscall.UTF16PtrFromString(value)
	h, _, e := createMutex.Call(0, 0, uintptr(unsafe.Pointer(name)))
	if h == 0 {
		return 0, e
	}
	if errors.Is(e, syscall.Errno(183)) {
		closeHandle.Call(h)
		return 0, errors.New("already running")
	}
	return syscall.Handle(h), nil
}
func job() (syscall.Handle, error) {
	h, _, e := createJob.Call(0, 0)
	if h == 0 {
		return 0, e
	}
	v := extendedLimits{}
	v.Basic.LimitFlags = 0x2000
	r, _, e := setJob.Call(h, 9, uintptr(unsafe.Pointer(&v)), unsafe.Sizeof(v))
	if r == 0 {
		closeHandle.Call(h)
		return 0, e
	}
	return syscall.Handle(h), nil
}
func assign(j syscall.Handle, pid int) error {
	p, _, e := openProcess.Call(0x0100|0x0001, 0, uintptr(pid))
	if p == 0 {
		return e
	}
	defer closeHandle.Call(p)
	r, _, e := assignJob.Call(uintptr(j), p)
	if r == 0 {
		return e
	}
	return nil
}
func owner(port uint16) (uint32, error) {
	var n uint32
	tcpTable.Call(0, uintptr(unsafe.Pointer(&n)), 0, 2, 3, 0)
	if n == 0 || n > 4<<20 {
		return 0, errors.New("port query failed")
	}
	buf := make([]byte, n)
	r, _, _ := tcpTable.Call(uintptr(unsafe.Pointer(&buf[0])), uintptr(unsafe.Pointer(&n)), 0, 2, 3, 0)
	if r != 0 {
		return 0, errors.New("port query failed")
	}
	count := int(binary.LittleEndian.Uint32(buf))
	if 4+count*24 > len(buf) {
		return 0, errors.New("port table malformed")
	}
	for i := 0; i < count; i++ {
		b := buf[4+i*24:]
		v := binary.LittleEndian.Uint32(b[8:])
		p := uint16(v>>8) | uint16(v<<8)
		if p == port {
			return binary.LittleEndian.Uint32(b[20:]), nil
		}
	}
	return 0, nil
}
func token(path string) (string, error) {
	b, e := os.ReadFile(path)
	if e != nil {
		return "", e
	}
	var s struct {
		Upload struct {
			TokenCipher string `json:"tokenCipher"`
		} `json:"upload"`
	}
	if json.Unmarshal(b, &s) != nil {
		return "", errors.New("settings unreadable")
	}
	raw, e := base64.StdEncoding.DecodeString(s.Upload.TokenCipher)
	if e != nil || len(raw) == 0 {
		return "", errors.New("token missing")
	}
	src := blob{uint32(len(raw)), &raw[0]}
	var dst blob
	r, _, _ := unprotect.Call(uintptr(unsafe.Pointer(&src)), 0, 0, 0, 0, 1, uintptr(unsafe.Pointer(&dst)))
	if r == 0 {
		return "", errors.New("token unavailable")
	}
	defer localFree.Call(uintptr(unsafe.Pointer(dst.Data)))
	return string(unsafe.Slice(dst.Data, int(dst.Size))), nil
}

type status struct {
	State   string    `json:"state"`
	Updated time.Time `json:"updatedAt"`
	PID     int       `json:"pid,omitempty"`
}
type guard struct {
	data, program string
	job           syscall.Handle
	http          *http.Client
}

func (g *guard) report(state string, pid int) {
	v := status{state, time.Now().UTC(), pid}
	b, _ := json.Marshal(v)
	tmp := filepath.Join(g.data, "guard-status.json.tmp")
	if os.WriteFile(tmp, b, 0600) == nil {
		_ = os.Rename(tmp, filepath.Join(g.data, "guard-status.json"))
	}
	log := filepath.Join(g.data, "guard.log")
	if i, e := os.Stat(log); e == nil && i.Size() > 65536 {
		_ = os.WriteFile(log, nil, 0600)
	}
	f, e := os.OpenFile(log, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if e == nil {
		fmt.Fprintf(f, "%s %s\n", time.Now().UTC().Format(time.RFC3339), state)
		f.Close()
	}
}
func (g *guard) stopped() bool { _, e := os.Stat(filepath.Join(g.data, "guard.stop")); return e == nil }
func (g *guard) healthy(pid int) bool {
	owner, e := owner(8796)
	if e != nil || int(owner) != pid {
		return false
	}
	r, e := g.http.Get("http://127.0.0.1:8796/api/local/health")
	if e != nil {
		return false
	}
	defer r.Body.Close()
	var result struct {
		Service string `json:"service"`
		Version int    `json:"version"`
	}
	return r.StatusCode == 200 && json.NewDecoder(io.LimitReader(r.Body, 4096)).Decode(&result) == nil &&
		result.Service == "meowx-client-latency" && result.Version == 1
}
func (g *guard) runChild() (bool, error) {
	if p, e := owner(8796); e != nil {
		return false, e
	} else if p != 0 {
		g.report("port-in-use", 0)
		return false, errors.New("port occupied")
	}
	secret, e := token(filepath.Join(filepath.Dir(g.data), "settings.json"))
	if e != nil {
		return false, e
	}
	node := filepath.Join(g.program, "node.exe")
	entry := filepath.Join(g.program, "server.mjs")
	cmd := exec.Command(node, entry, "--data-dir", g.data)
	cmd.Dir = g.program
	cmd.SysProcAttr = &syscall.SysProcAttr{HideWindow: true, CreationFlags: 0x08000000}
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	stdin, e := cmd.StdinPipe()
	if e != nil {
		return false, e
	}
	if e = cmd.Start(); e != nil {
		stdin.Close()
		return false, e
	}
	if e = assign(g.job, cmd.Process.Pid); e != nil {
		cmd.Process.Kill()
		cmd.Wait()
		stdin.Close()
		return false, e
	}
	encoded, _ := json.Marshal(map[string]string{"token": secret})
	secret = ""
	if _, e = stdin.Write(append(encoded, '\n')); e != nil {
		stdin.Close()
		cmd.Process.Kill()
		cmd.Wait()
		return false, e
	}
	for i := range encoded {
		encoded[i] = 0
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	started := time.Now()
	failures := 0
	g.report("starting", cmd.Process.Pid)
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case e := <-done:
			stdin.Close()
			if g.stopped() {
				g.report("stopped", 0)
				return true, nil
			}
			_ = e
			return false, errors.New("probe exited unexpectedly")
		case <-ticker.C:
			if g.stopped() {
				g.report("stopping", cmd.Process.Pid)
				stdin.Close()
				select {
				case <-done:
				case <-time.After(10 * time.Second):
					cmd.Process.Kill()
					<-done
				}
				g.report("stopped", 0)
				return true, nil
			}
			if g.healthy(cmd.Process.Pid) {
				failures = 0
				g.report("running", cmd.Process.Pid)
			} else if time.Since(started) > 20*time.Second {
				failures++
				g.report("unhealthy", cmd.Process.Pid)
				if failures >= 3 {
					stdin.Close()
					cmd.Process.Kill()
					<-done
					return false, errors.New("probe health failed")
				}
			}
		}
	}
}
func main() {
	h, e := singleton()
	if e != nil {
		return
	}
	defer closeHandle.Call(uintptr(h))
	j, e := job()
	if e != nil {
		os.Exit(1)
	}
	defer closeHandle.Call(uintptr(j))
	local := os.Getenv("LOCALAPPDATA")
	if local == "" {
		os.Exit(1)
	}
	g := guard{data: filepath.Join(local, "MeowX Traffic", "probe"), program: filepath.Join(local, "Programs", "MeowX Traffic", "probe"), job: j, http: &http.Client{Timeout: 2 * time.Second, Transport: &http.Transport{Proxy: nil}, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}
	if os.MkdirAll(g.data, 0700) != nil {
		os.Exit(1)
	}
	delay := 5 * time.Second
	for {
		if g.stopped() {
			g.report("stopped", 0)
			return
		}
		normal, err := g.runChild()
		if normal {
			return
		}
		message := "retrying"
		if err != nil && strings.Contains(err.Error(), "port occupied") {
			message = "port-in-use"
		}
		g.report(message, 0)
		for i := 0; i < int(delay/time.Second); i++ {
			if g.stopped() {
				g.report("stopped", 0)
				return
			}
			if i > 0 && i%10 == 0 {
				g.report(message, 0)
			}
			time.Sleep(time.Second)
		}
		if delay < time.Minute {
			delay *= 2
			if delay > time.Minute {
				delay = time.Minute
			}
		}
	}
}
