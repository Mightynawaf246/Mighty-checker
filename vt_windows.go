//go:build windows

package main

import (
	"syscall"
	"unsafe"
)

// enableVT turns on virtual terminal processing on Windows so ANSI colors
// render in cmd.exe. Windows Terminal does not need it, but enabling it there
// is harmless. Uses only the standard library, via kernel32.dll.
func enableVT() {
	const enableVTProcessing = 0x0004
	const stdOutputHandle = ^uintptr(10) // (DWORD)-11

	k := syscall.NewLazyDLL("kernel32.dll")
	getStdHandle := k.NewProc("GetStdHandle")
	getConsoleMode := k.NewProc("GetConsoleMode")
	setConsoleMode := k.NewProc("SetConsoleMode")

	h, _, _ := getStdHandle.Call(stdOutputHandle)
	if h == 0 || h == uintptr(syscall.InvalidHandle) {
		return
	}

	var mode uint32
	if r, _, _ := getConsoleMode.Call(h, uintptr(unsafe.Pointer(&mode))); r == 0 {
		return
	}
	setConsoleMode.Call(h, uintptr(mode|enableVTProcessing))
}
