//go:build !windows

package main

import (
	"os"
	"syscall"
	"unsafe"
)

// enableVT is a no-op outside Windows: Unix terminals speak ANSI natively.
func enableVT() {}

// terminalWidth reports the console width in columns, or 0 when it cannot be
// determined.
func terminalWidth() int {
	var ws struct{ Row, Col, Xpixel, Ypixel uint16 }
	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, os.Stdout.Fd(),
		uintptr(syscall.TIOCGWINSZ), uintptr(unsafe.Pointer(&ws)))
	if errno != 0 {
		return 0
	}
	return int(ws.Col)
}
