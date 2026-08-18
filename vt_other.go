//go:build !windows

package main

// enableVT is a no-op outside Windows: Unix terminals speak ANSI natively.
func enableVT() {}
