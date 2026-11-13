package main

import (
	"encoding/base64"
	"errors"
	"syscall"
	"unsafe"
)

type dpapiBlob struct {
	Size uint32
	Data *byte
}

var crypt32 = syscall.NewLazyDLL("crypt32.dll")
var kernel32 = syscall.NewLazyDLL("kernel32.dll")
var protectData = crypt32.NewProc("CryptProtectData")
var unprotectData = crypt32.NewProc("CryptUnprotectData")
var freeData = kernel32.NewProc("LocalFree")

func cryptToken(in []byte, protect bool) ([]byte, error) {
	if len(in) == 0 {
		return nil, errors.New("empty token")
	}
	src := dpapiBlob{uint32(len(in)), &in[0]}
	var dst dpapiBlob
	proc := unprotectData
	if protect {
		proc = protectData
	}
	r, _, _ := proc.Call(uintptr(unsafe.Pointer(&src)), 0, 0, 0, 0, 1, uintptr(unsafe.Pointer(&dst)))
	if r == 0 {
		return nil, errors.New("本机凭据保护失败")
	}
	defer freeData.Call(uintptr(unsafe.Pointer(dst.Data)))
	return append([]byte(nil), unsafe.Slice(dst.Data, int(dst.Size))...), nil
}
func encryptToken(s string) (string, error) {
	b, e := cryptToken([]byte(s), true)
	if e != nil {
		return "", e
	}
	return base64.StdEncoding.EncodeToString(b), nil
}
func decryptToken(s string) (string, error) {
	b, e := base64.StdEncoding.DecodeString(s)
	if e != nil {
		return "", errors.New("无法读取上传凭据")
	}
	b, e = cryptToken(b, false)
	if e != nil {
		return "", e
	}
	return string(b), nil
}
