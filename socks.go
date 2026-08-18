package main

// Hand-written SOCKS implementations using only the standard library.
//
// Why: golang.org/x/net/proxy supports SOCKS5 only and has no SOCKS4 at all,
// and its current release forces a Go toolchain upgrade. Since every proxy type
// must be supported, both layers are written by hand and the tool stays
// dependency-free.
//
// References: RFC 1928 (SOCKS5), RFC 1929 (username/password auth), and the
// original SOCKS4/4a specification.

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strconv"
	"time"
)

// ------------------------------------------------------------------ SOCKS5

const (
	socks5Version = 0x05
	socks5CmdConn = 0x01

	socks5AuthNone     = 0x00
	socks5AuthUserPass = 0x02
	socks5AuthNoAccept = 0xFF

	socks5AuthSubVersion = 0x01 // sub-negotiation version, NOT 0x05

	socks5AtypIPv4   = 0x01
	socks5AtypDomain = 0x03
	socks5AtypIPv6   = 0x04
)

// SOCKS5 reply codes as defined in RFC 1928.
var socks5Replies = map[byte]string{
	0x00: "succeeded",
	0x01: "general SOCKS server failure",
	0x02: "connection not allowed by ruleset",
	0x03: "network unreachable",
	0x04: "host unreachable",
	0x05: "connection refused",
	0x06: "TTL expired",
	0x07: "command not supported",
	0x08: "address type not supported",
}

// dialSOCKS5 opens a tunnel to the target through a SOCKS5 proxy and returns
// the ready connection.
//
// When resolveLocally is true the hostname is resolved locally and sent as an
// IP address (socks5 behavior); otherwise the hostname itself is sent for the
// proxy to resolve (socks5h behavior).
func dialSOCKS5(ctx context.Context, proxyAddr, user, pass, targetHost string, targetPort int, resolveLocally bool) (net.Conn, error) {
	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", proxyAddr)
	if err != nil {
		return nil, fmt.Errorf("socks5: cannot reach proxy %s: %w", proxyAddr, err)
	}

	// Close the connection on every failure path; hand it over only on full success.
	ok := false
	defer func() {
		if !ok {
			conn.Close()
		}
	}()

	if dl, has := ctx.Deadline(); has {
		if err := conn.SetDeadline(dl); err != nil {
			return nil, fmt.Errorf("socks5: set deadline: %w", err)
		}
	}

	// 1) Greeting: version plus the auth methods we support.
	methods := []byte{socks5AuthNone}
	if user != "" || pass != "" {
		methods = []byte{socks5AuthNone, socks5AuthUserPass}
	}
	greeting := make([]byte, 0, 2+len(methods))
	greeting = append(greeting, socks5Version, byte(len(methods)))
	greeting = append(greeting, methods...)
	if _, err := conn.Write(greeting); err != nil {
		return nil, fmt.Errorf("socks5: write greeting: %w", err)
	}

	sel := make([]byte, 2)
	if _, err := io.ReadFull(conn, sel); err != nil {
		return nil, fmt.Errorf("socks5: read method selection: %w", err)
	}
	if sel[0] != socks5Version {
		return nil, fmt.Errorf("socks5: bad version 0x%02x in method selection", sel[0])
	}

	switch sel[1] {
	case socks5AuthNone:
		// No authentication required.
	case socks5AuthUserPass:
		if user == "" && pass == "" {
			return nil, fmt.Errorf("socks5: proxy demands username/password but none supplied")
		}
		if len(user) > 255 || len(pass) > 255 {
			return nil, fmt.Errorf("socks5: username/password exceeds 255 bytes")
		}
		// RFC 1929: the version here is 0x01, not 0x05.
		req := make([]byte, 0, 3+len(user)+len(pass))
		req = append(req, socks5AuthSubVersion, byte(len(user)))
		req = append(req, user...)
		req = append(req, byte(len(pass)))
		req = append(req, pass...)
		if _, err := conn.Write(req); err != nil {
			return nil, fmt.Errorf("socks5: write auth: %w", err)
		}
		ar := make([]byte, 2)
		if _, err := io.ReadFull(conn, ar); err != nil {
			return nil, fmt.Errorf("socks5: read auth reply: %w", err)
		}
		// The sub-negotiation reply version must be 0x01 (RFC 1929).
		if ar[0] != socks5AuthSubVersion {
			return nil, fmt.Errorf("socks5: bad auth reply version 0x%02x", ar[0])
		}
		if ar[1] != 0x00 {
			return nil, fmt.Errorf("socks5: authentication rejected (status 0x%02x)", ar[1])
		}
	case socks5AuthNoAccept:
		return nil, fmt.Errorf("socks5: proxy accepted none of our auth methods")
	default:
		return nil, fmt.Errorf("socks5: unsupported auth method 0x%02x", sel[1])
	}

	// 2) CONNECT request.
	if targetPort < 1 || targetPort > 65535 {
		return nil, fmt.Errorf("socks5: invalid target port %d", targetPort)
	}
	req := []byte{socks5Version, socks5CmdConn, 0x00}

	host := targetHost
	if resolveLocally {
		if net.ParseIP(host) == nil {
			ips, err := net.DefaultResolver.LookupIPAddr(ctx, host)
			if err != nil {
				return nil, fmt.Errorf("socks5: resolve %s locally: %w", host, err)
			}
			if len(ips) == 0 {
				return nil, fmt.Errorf("socks5: no addresses for %s", host)
			}
			host = ips[0].IP.String()
		}
	}

	if ip := net.ParseIP(host); ip != nil {
		if v4 := ip.To4(); v4 != nil {
			req = append(req, socks5AtypIPv4)
			req = append(req, v4...)
		} else {
			req = append(req, socks5AtypIPv6)
			req = append(req, ip.To16()...)
		}
	} else {
		if len(host) > 255 {
			return nil, fmt.Errorf("socks5: hostname too long (%d bytes)", len(host))
		}
		req = append(req, socks5AtypDomain, byte(len(host)))
		req = append(req, host...)
	}
	req = binary.BigEndian.AppendUint16(req, uint16(targetPort))

	if _, err := conn.Write(req); err != nil {
		return nil, fmt.Errorf("socks5: write connect: %w", err)
	}

	// 3) Read the reply. The bound address must be consumed in full, otherwise
	//    the byte stream is left misaligned for the TLS data that follows.
	head := make([]byte, 4)
	if _, err := io.ReadFull(conn, head); err != nil {
		return nil, fmt.Errorf("socks5: read connect reply: %w", err)
	}
	if head[0] != socks5Version {
		return nil, fmt.Errorf("socks5: bad version 0x%02x in connect reply", head[0])
	}
	if head[1] != 0x00 {
		msg, known := socks5Replies[head[1]]
		if !known {
			msg = "unknown error"
		}
		return nil, fmt.Errorf("socks5: connect refused: %s (0x%02x)", msg, head[1])
	}

	var boundLen int
	switch head[3] {
	case socks5AtypIPv4:
		boundLen = 4
	case socks5AtypIPv6:
		boundLen = 16
	case socks5AtypDomain:
		l := make([]byte, 1)
		if _, err := io.ReadFull(conn, l); err != nil {
			return nil, fmt.Errorf("socks5: read bound domain length: %w", err)
		}
		boundLen = int(l[0])
	default:
		return nil, fmt.Errorf("socks5: unknown address type 0x%02x in reply", head[3])
	}
	if _, err := io.ReadFull(conn, make([]byte, boundLen+2)); err != nil {
		return nil, fmt.Errorf("socks5: read bound address: %w", err)
	}

	// Clear the absolute deadline so it does not kill the request that follows.
	if err := conn.SetDeadline(time.Time{}); err != nil {
		return nil, fmt.Errorf("socks5: clear deadline: %w", err)
	}

	ok = true
	return conn, nil
}

// ------------------------------------------------------------------ SOCKS4

const (
	socks4Version = 0x04
	socks4CmdConn = 0x01

	socks4Granted = 0x5A
)

var socks4Replies = map[byte]string{
	0x5A: "request granted",
	0x5B: "request rejected or failed",
	0x5C: "request failed: client not running identd",
	0x5D: "request failed: identd could not confirm user ID",
}

// dialSOCKS4 opens a tunnel through a SOCKS4 or SOCKS4a proxy.
//
// SOCKS4 has no authentication mechanism; the USERID field is the only place a
// username fits, and passwords are simply not part of the protocol.
//
// With use4a the hostname is sent for the proxy to resolve (SOCKS4a);
// otherwise it is resolved locally to IPv4. The protocol has no IPv6 support.
func dialSOCKS4(ctx context.Context, proxyAddr, userID, targetHost string, targetPort int, use4a bool) (net.Conn, error) {
	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", proxyAddr)
	if err != nil {
		return nil, fmt.Errorf("socks4: cannot reach proxy %s: %w", proxyAddr, err)
	}

	ok := false
	defer func() {
		if !ok {
			conn.Close()
		}
	}()

	if dl, has := ctx.Deadline(); has {
		if err := conn.SetDeadline(dl); err != nil {
			return nil, fmt.Errorf("socks4: set deadline: %w", err)
		}
	}

	if targetPort < 1 || targetPort > 65535 {
		return nil, fmt.Errorf("socks4: invalid target port %d", targetPort)
	}

	var dstIP net.IP
	sendHostname := false

	if ip := net.ParseIP(targetHost); ip != nil {
		v4 := ip.To4()
		if v4 == nil {
			return nil, fmt.Errorf("socks4: protocol is IPv4-only, cannot reach %s", targetHost)
		}
		dstIP = v4
	} else if use4a {
		// SOCKS4a: an address of 0.0.0.x with a nonzero x tells the proxy that the
		// hostname follows after the USERID field.
		dstIP = net.IPv4(0, 0, 0, 1).To4()
		sendHostname = true
	} else {
		ips, err := net.DefaultResolver.LookupIPAddr(ctx, targetHost)
		if err != nil {
			return nil, fmt.Errorf("socks4: resolve %s locally: %w", targetHost, err)
		}
		for _, cand := range ips {
			if v4 := cand.IP.To4(); v4 != nil {
				dstIP = v4
				break
			}
		}
		if dstIP == nil {
			return nil, fmt.Errorf("socks4: no IPv4 address for %s (protocol is IPv4-only)", targetHost)
		}
	}

	req := make([]byte, 0, 9+len(userID)+len(targetHost)+1)
	req = append(req, socks4Version, socks4CmdConn)
	req = binary.BigEndian.AppendUint16(req, uint16(targetPort))
	req = append(req, dstIP...)
	req = append(req, userID...)
	req = append(req, 0x00)
	if sendHostname {
		if len(targetHost) > 255 {
			return nil, fmt.Errorf("socks4a: hostname too long (%d bytes)", len(targetHost))
		}
		req = append(req, targetHost...)
		req = append(req, 0x00)
	}

	if _, err := conn.Write(req); err != nil {
		return nil, fmt.Errorf("socks4: write connect: %w", err)
	}

	// The reply is eight bytes: VN, CD, port, address.
	// Note: VN in the reply must be 0x00, not 0x04.
	reply := make([]byte, 8)
	if _, err := io.ReadFull(conn, reply); err != nil {
		return nil, fmt.Errorf("socks4: read connect reply: %w", err)
	}
	if reply[0] != 0x00 {
		return nil, fmt.Errorf("socks4: bad reply version 0x%02x", reply[0])
	}
	if reply[1] != socks4Granted {
		msg, known := socks4Replies[reply[1]]
		if !known {
			msg = "unknown error"
		}
		return nil, fmt.Errorf("socks4: connect refused: %s (0x%02x)", msg, reply[1])
	}

	if err := conn.SetDeadline(time.Time{}); err != nil {
		return nil, fmt.Errorf("socks4: clear deadline: %w", err)
	}

	ok = true
	return conn, nil
}

// splitHostPort splits "host:port" and returns the host and a numeric port.
func splitHostPort(addr string) (string, int, error) {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return "", 0, err
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return "", 0, fmt.Errorf("invalid port %q: %w", portStr, err)
	}
	return host, port, nil
}
