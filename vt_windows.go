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

// terminalWidth reports the console width in columns, or 0 when it cannot be
// determined. cmd.exe defaults to 80, which the status line comfortably
// exceeds, so knowing this is what keeps it on one line.
func terminalWidth() int {
	type coord struct{ X, Y int16 }
	type smallRect struct{ Left, Top, Right, Bottom int16 }
	type screenBufferInfo struct {
		Size              coord
		CursorPosition    coord
		Attributes        uint16
		Window            smallRect
		MaximumWindowSize coord
	}

	const stdOutputHandle = ^uintptr(10) // (DWORD)-11

	k := syscall.NewLazyDLL("kernel32.dll")
	getStdHandle := k.NewProc("GetStdHandle")
	getInfo := k.NewProc("GetConsoleScreenBufferInfo")

	h, _, _ := getStdHandle.Call(stdOutputHandle)
	if h == 0 || h == uintptr(syscall.InvalidHandle) {
		return 0
	}

	var info screenBufferInfo
	if r, _, _ := getInfo.Call(h, uintptr(unsafe.Pointer(&info))); r == 0 {
		return 0
	}
	w := int(info.Window.Right) - int(info.Window.Left) + 1
	if w < 1 {
		return 0
	}
	return w
}
