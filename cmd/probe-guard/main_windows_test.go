//go:build windows

package main

import (
	"fmt"
	"net"
	"os"
	"testing"
)

func TestPortOwnerAndSingleton(t *testing.T) {
	l, e := net.Listen("tcp4", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	defer l.Close()
	p := uint16(l.Addr().(*net.TCPAddr).Port)
	pid, e := owner(p)
	if e != nil || pid != uint32(os.Getpid()) {
		t.Fatalf("port ownership: pid=%d err=%v", pid, e)
	}
	name := fmt.Sprintf(`Local\MeowXTrafficProbeTest-%d`, os.Getpid())
	h, e := lockNamed(name)
	if e != nil {
		t.Fatal(e)
	}
	defer closeHandle.Call(uintptr(h))
	h2, e := lockNamed(name)
	if e == nil {
		closeHandle.Call(uintptr(h2))
		t.Fatal("second guardian not rejected")
	}
	j, e := job()
	if e != nil {
		t.Fatal(e)
	}
	closeHandle.Call(uintptr(j))
}
