package main

import (
	"fmt"
	"math/rand/v2"
	"strings"
	"testing"
)

// Every input that reaches this tool from outside it - a proxy line the user
// pasted, a body the network returned, a name from a file - must be survivable.
// parseProxy and interpret both eat untrusted input: one from a file the user
// pasted, the other from whatever the network returns. Neither may panic.
func TestNoPanicOnHostileInput(t *testing.T) {
	alphabet := []rune("abc:/@%[].0123456789-_\\\"'{}\x00\n\t \x7f€😀+&?=#|~^")

	var lines []string
	// Deliberate edge cases first.
	lines = append(lines,
		"", " ", ":", "::", ":::", "@", "@@", "://", "http://", "http://@",
		"http://:@", "[", "]", "[]", "[]:", "[::1]", "[::1]:", "::1:80",
		"a:b:c:d:e", "%", "%%", "%zz", "%00", "http://a:b@c:d",
		"host:99999", "host:-1", "host:0", "host: 80 ", strings.Repeat("a", 10000),
		strings.Repeat(":", 500), "socks5://"+strings.Repeat("x", 5000),
	)
	// Then random noise.
	for i := 0; i < 20000; i++ {
		n := rand.IntN(40)
		b := make([]rune, n)
		for j := range b {
			b[j] = alphabet[rand.IntN(len(alphabet))]
		}
		lines = append(lines, string(b))
	}

	parsed := 0
	for _, l := range lines {
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("parseProxy panicked on %q: %v", l, r)
				}
			}()
			if p, err := parseProxy(l); err == nil {
				parsed++
				// Anything that parses must produce usable, non-panicking values.
				_ = p.String()
				_ = p.key()
				_ = p.Addr()
				if p.Port < 1 || p.Port > 65535 {
					t.Fatalf("parseProxy(%q) yielded port %d", l, p.Port)
				}
				if p.Host == "" {
					t.Fatalf("parseProxy(%q) yielded an empty host", l)
				}
			}
		}()
	}
	fmt.Printf("  parseProxy: %d inputs, %d parsed, 0 panics\n", len(lines), parsed)

	// interpret sees whatever the network returns.
	bodies := []string{"", "{", "}", "[", "null", "\x00\x00", strings.Repeat("a", 100000),
		`{"status":`, `{"status":"`, `"errors":[`, `"errors":[{`, `{"errors":`,
		strings.Repeat(`{"status":"success"}`, 1000)}
	for i := 0; i < 5000; i++ {
		n := rand.IntN(60)
		b := make([]rune, n)
		for j := range b {
			b[j] = alphabet[rand.IntN(len(alphabet))]
		}
		bodies = append(bodies, string(b))
	}
	for _, code := range []int{0, 200, 204, 301, 400, 403, 429, 500, 503, 999, -1} {
		for _, b := range bodies {
			func() {
				defer func() {
					if r := recover(); r != nil {
						t.Fatalf("interpret(%d, %q) panicked: %v", code, b, r)
					}
				}()
				interpret(code, b)
			}()
		}
	}
	fmt.Printf("  interpret : %d bodies x %d codes, 0 panics\n", len(bodies), 11)

	// And the username validator.
	for _, l := range lines {
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("validUsername panicked on %q: %v", l, r)
				}
			}()
			validUsername(l)
		}()
	}
	fmt.Printf("  validUsername: clean\n")
}
