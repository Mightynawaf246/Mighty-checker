package main

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// --------------------------------------------------- fake SOCKS5 test server

// fakeSOCKS5 runs an in-process SOCKS5 server. When wantUser is non-empty it
// requires username/password auth and verifies it.
func fakeSOCKS5(t *testing.T, wantUser, wantPass string) string {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { ln.Close() })

	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go serveSOCKS5(c, wantUser, wantPass)
		}
	}()

	return ln.Addr().String()
}

func serveSOCKS5(c net.Conn, wantUser, wantPass string) {
	defer c.Close()

	head := make([]byte, 2)
	if _, err := io.ReadFull(c, head); err != nil {
		return
	}
	if head[0] != 0x05 {
		return
	}
	methods := make([]byte, int(head[1]))
	if _, err := io.ReadFull(c, methods); err != nil {
		return
	}

	if wantUser != "" {
		// Demand username/password authentication.
		if _, err := c.Write([]byte{0x05, 0x02}); err != nil {
			return
		}
		vh := make([]byte, 2)
		if _, err := io.ReadFull(c, vh); err != nil {
			return
		}
		// The sub-negotiation version must be exactly 0x01.
		if vh[0] != 0x01 {
			c.Write([]byte{0x01, 0x01})
			return
		}
		user := make([]byte, int(vh[1]))
		if _, err := io.ReadFull(c, user); err != nil {
			return
		}
		pl := make([]byte, 1)
		if _, err := io.ReadFull(c, pl); err != nil {
			return
		}
		pass := make([]byte, int(pl[0]))
		if _, err := io.ReadFull(c, pass); err != nil {
			return
		}
		if string(user) != wantUser || string(pass) != wantPass {
			c.Write([]byte{0x01, 0x01})
			return
		}
		if _, err := c.Write([]byte{0x01, 0x00}); err != nil {
			return
		}
	} else {
		if _, err := c.Write([]byte{0x05, 0x00}); err != nil {
			return
		}
	}

	req := make([]byte, 4)
	if _, err := io.ReadFull(c, req); err != nil {
		return
	}
	if req[0] != 0x05 || req[1] != 0x01 {
		return
	}

	var host string
	switch req[3] {
	case 0x01:
		b := make([]byte, 4)
		if _, err := io.ReadFull(c, b); err != nil {
			return
		}
		host = net.IP(b).String()
	case 0x03:
		l := make([]byte, 1)
		if _, err := io.ReadFull(c, l); err != nil {
			return
		}
		b := make([]byte, int(l[0]))
		if _, err := io.ReadFull(c, b); err != nil {
			return
		}
		host = string(b)
	case 0x04:
		b := make([]byte, 16)
		if _, err := io.ReadFull(c, b); err != nil {
			return
		}
		host = net.IP(b).String()
	default:
		return
	}

	pb := make([]byte, 2)
	if _, err := io.ReadFull(c, pb); err != nil {
		return
	}
	port := binary.BigEndian.Uint16(pb)

	up, err := net.Dial("tcp", net.JoinHostPort(host, strconv.Itoa(int(port))))
	if err != nil {
		c.Write([]byte{0x05, 0x05, 0x00, 0x01, 0, 0, 0, 0, 0, 0})
		return
	}
	defer up.Close()

	// Success reply with a dummy bound address.
	if _, err := c.Write([]byte{0x05, 0x00, 0x00, 0x01, 127, 0, 0, 1, 0x00, 0x50}); err != nil {
		return
	}

	pipe(c, up)
}

// --------------------------------------------------- fake SOCKS4 test server

// fakeSOCKS4 runs an in-process SOCKS4/4a server and records the USERID sent.
func fakeSOCKS4(t *testing.T, gotUserID *string, mu *sync.Mutex) string {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { ln.Close() })

	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go serveSOCKS4(c, gotUserID, mu)
		}
	}()

	return ln.Addr().String()
}

func serveSOCKS4(c net.Conn, gotUserID *string, mu *sync.Mutex) {
	defer c.Close()

	head := make([]byte, 8)
	if _, err := io.ReadFull(c, head); err != nil {
		return
	}
	if head[0] != 0x04 || head[1] != 0x01 {
		return
	}
	port := binary.BigEndian.Uint16(head[2:4])
	ip := net.IPv4(head[4], head[5], head[6], head[7])

	br := newByteReader(c)

	userID, err := br.readUntilNull()
	if err != nil {
		return
	}
	if gotUserID != nil {
		mu.Lock()
		*gotUserID = userID
		mu.Unlock()
	}

	host := ip.String()
	// 0.0.0.x with a nonzero x means SOCKS4a: the hostname follows.
	if head[4] == 0 && head[5] == 0 && head[6] == 0 && head[7] != 0 {
		h, err := br.readUntilNull()
		if err != nil {
			return
		}
		host = h
	}

	up, err := net.Dial("tcp", net.JoinHostPort(host, strconv.Itoa(int(port))))
	if err != nil {
		c.Write([]byte{0x00, 0x5B, 0, 0, 0, 0, 0, 0})
		return
	}
	defer up.Close()

	// VN in the reply is zero, not 0x04.
	if _, err := c.Write([]byte{0x00, 0x5A, 0, 0, 0, 0, 0, 0}); err != nil {
		return
	}

	pipe(c, up)
}

type byteReader struct {
	c   net.Conn
	buf []byte
}

func newByteReader(c net.Conn) *byteReader { return &byteReader{c: c} }

func (b *byteReader) readUntilNull() (string, error) {
	var sb strings.Builder
	one := make([]byte, 1)
	for i := 0; i < 512; i++ {
		if _, err := io.ReadFull(b.c, one); err != nil {
			return "", err
		}
		if one[0] == 0x00 {
			return sb.String(), nil
		}
		sb.WriteByte(one[0])
	}
	return "", fmt.Errorf("no null terminator")
}

// pipe relays bytes both ways. When one direction ends we close both sides so
// EOF reaches the other; without that the opposite direction blocks on read
// forever and the client never sees the end of the response.
func pipe(a, b net.Conn) {
	done := make(chan struct{}, 2)
	go func() { io.Copy(a, b); done <- struct{}{} }()
	go func() { io.Copy(b, a); done <- struct{}{} }()
	<-done
	a.Close()
	b.Close()
	<-done
}

// ------------------------------------------------------------------------ tests

func TestSOCKS5NoAuth(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "hello-from-backend")
	}))
	defer backend.Close()

	proxyAddr := fakeSOCKS5(t, "", "")
	host, port, err := splitHostPort(strings.TrimPrefix(backend.URL, "http://"))
	if err != nil {
		t.Fatalf("split backend: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := dialSOCKS5(ctx, proxyAddr, "", "", host, port, false)
	if err != nil {
		t.Fatalf("dialSOCKS5: %v", err)
	}
	defer conn.Close()

	assertHTTPOverConn(t, conn, host, port, "hello-from-backend")
}

func TestSOCKS5UserPassAuth(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "authed-ok")
	}))
	defer backend.Close()

	proxyAddr := fakeSOCKS5(t, "bob", "s3cr3t")
	host, port, _ := splitHostPort(strings.TrimPrefix(backend.URL, "http://"))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := dialSOCKS5(ctx, proxyAddr, "bob", "s3cr3t", host, port, false)
	if err != nil {
		t.Fatalf("dialSOCKS5 with auth: %v", err)
	}
	defer conn.Close()

	assertHTTPOverConn(t, conn, host, port, "authed-ok")
}

func TestSOCKS5WrongPasswordRejected(t *testing.T) {
	proxyAddr := fakeSOCKS5(t, "bob", "s3cr3t")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := dialSOCKS5(ctx, proxyAddr, "bob", "wrong", "example.com", 80, false)
	if err == nil {
		t.Fatal("expected authentication failure, got nil error")
	}
	if !strings.Contains(err.Error(), "authentication rejected") {
		t.Fatalf("expected auth rejection, got: %v", err)
	}
}

func TestSOCKS4aDial(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "socks4-ok")
	}))
	defer backend.Close()

	var mu sync.Mutex
	var gotUserID string
	proxyAddr := fakeSOCKS4(t, &gotUserID, &mu)

	host, port, _ := splitHostPort(strings.TrimPrefix(backend.URL, "http://"))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// The host here is a literal IP, so it is sent directly with no need for 4a.
	conn, err := dialSOCKS4(ctx, proxyAddr, "myuser", host, port, true)
	if err != nil {
		t.Fatalf("dialSOCKS4: %v", err)
	}
	defer conn.Close()

	assertHTTPOverConn(t, conn, host, port, "socks4-ok")

	mu.Lock()
	defer mu.Unlock()
	if gotUserID != "myuser" {
		t.Fatalf("USERID field: want %q, got %q", "myuser", gotUserID)
	}
}

func TestSOCKS4aHostnameIsSentToProxy(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "resolved-by-proxy")
	}))
	defer backend.Close()

	var mu sync.Mutex
	var gotUserID string
	proxyAddr := fakeSOCKS4(t, &gotUserID, &mu)

	_, port, _ := splitHostPort(strings.TrimPrefix(backend.URL, "http://"))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// "localhost" is a hostname, so it must take the SOCKS4a path and reach the proxy.
	conn, err := dialSOCKS4(ctx, proxyAddr, "", "localhost", port, true)
	if err != nil {
		t.Fatalf("dialSOCKS4 (4a path): %v", err)
	}
	defer conn.Close()

	assertHTTPOverConn(t, conn, "localhost", port, "resolved-by-proxy")
}

// TestTLSOverSOCKS5 verifies the most important design claim: http.Transport
// automatically layers TLS over the tunnel returned by DialContext when the
// target is https. That is exactly what happens talking to Instagram via SOCKS.
func TestTLSOverSOCKS5(t *testing.T) {
	backend := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "tls-through-socks")
	}))
	defer backend.Close()

	proxyAddr := fakeSOCKS5(t, "", "")

	tr := &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			host, port, err := splitHostPort(addr)
			if err != nil {
				return nil, err
			}
			return dialSOCKS5(ctx, proxyAddr, "", "", host, port, false)
		},
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	client := &http.Client{Transport: tr, Timeout: 5 * time.Second}

	resp, err := client.Get(backend.URL)
	if err != nil {
		t.Fatalf("GET over TLS-through-SOCKS5: %v", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if string(body) != "tls-through-socks" {
		t.Fatalf("body: want %q, got %q", "tls-through-socks", string(body))
	}
}

// TestClientCacheThroughSOCKS5 exercises the full path through clientCache.
func TestClientCacheThroughSOCKS5(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "via-cache")
	}))
	defer backend.Close()

	proxyAddr := fakeSOCKS5(t, "", "")
	spec, err := parseProxy("socks5://" + proxyAddr)
	if err != nil {
		t.Fatalf("parseProxy: %v", err)
	}

	cache := newClientCache(5 * time.Second)
	client, err := cache.clientFor(spec)
	if err != nil {
		t.Fatalf("clientFor: %v", err)
	}

	resp, err := client.Get(backend.URL)
	if err != nil {
		t.Fatalf("GET through cached socks5 client: %v", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if string(body) != "via-cache" {
		t.Fatalf("body: want %q, got %q", "via-cache", string(body))
	}

	// A second call with the same spec must return the very same client.
	again, err := cache.clientFor(spec)
	if err != nil {
		t.Fatalf("clientFor second call: %v", err)
	}
	if again != client {
		t.Fatal("clientCache returned a different client for the same proxy")
	}
}

// assertHTTPOverConn performs a simple GET over a ready connection and checks the body.
func assertHTTPOverConn(t *testing.T, conn net.Conn, host string, port int, want string) {
	t.Helper()

	conn.SetDeadline(time.Now().Add(5 * time.Second))
	req := fmt.Sprintf("GET / HTTP/1.1\r\nHost: %s\r\nConnection: close\r\n\r\n",
		net.JoinHostPort(host, strconv.Itoa(port)))
	if _, err := conn.Write([]byte(req)); err != nil {
		t.Fatalf("write request over tunnel: %v", err)
	}

	raw, err := io.ReadAll(conn)
	if err != nil {
		t.Fatalf("read response over tunnel: %v", err)
	}
	if !strings.Contains(string(raw), want) {
		t.Fatalf("tunnelled response missing %q; got:\n%s", want, string(raw))
	}
}
