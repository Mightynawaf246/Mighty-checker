package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// Does a request built for an HTTP proxy actually leave through that proxy?
// This is the path the v3.15.0 dial change touched, so it needs proving rather
// than assuming.
func TestRequestActuallyTraversesTheHTTPProxy(t *testing.T) {
	var endpointHits, proxyHits atomic.Int64

	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		endpointHits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"data":{"xfb_validate":{"status":"SUCCESS"}},"errors":null}`))
	}))
	defer endpoint.Close()

	// A forwarding proxy that counts what passes through it and never chains
	// to anything from the environment.
	direct := &http.Transport{}
	eu, _ := url.Parse(endpoint.URL)
	endpointHost := eu.Host
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		proxyHits.Add(1)
		// A real proxy resolves the host it is asked for. This one is told
		// "www.instagram.com" - which is right, and is what the tool must send -
		// so it resolves that name to the stand-in endpoint.
		//
		// The request line carries the host from req.Host, not from the URL the
		// client built, so pointing graphqlURL at a local address is not enough
		// on its own to keep a proxied request local. That is correct
		// behaviour: the endpoint has to be addressed by its real name for any
		// proxy in the world to route it.
		if r.Host == "www.instagram.com" {
			r.URL.Host = endpointHost
		}
		r.RequestURI = ""
		resp, err := direct.RoundTrip(r)
		if err != nil {
			http.Error(w, err.Error(), 502)
			return
		}
		defer resp.Body.Close()
		for k, v := range resp.Header {
			w.Header()[k] = v
		}
		w.WriteHeader(resp.StatusCode)
		buf := make([]byte, 4096)
		for {
			n, err := resp.Body.Read(buf)
			if n > 0 {
				w.Write(buf[:n])
			}
			if err != nil {
				break
			}
		}
	}))
	defer proxy.Close()

	// Deliberately the REAL url: a proxied request is addressed by hostname,
	// and the proxy above is what turns that name into the stand-in server. No
	// packet leaves the machine.
	old := graphqlURL
	graphqlURL = "http://www.instagram.com/api/graphql"
	defer func() { graphqlURL = old }()

	pu, _ := url.Parse(proxy.URL)
	spec, err := parseProxy("http://" + pu.Host)
	if err != nil {
		t.Fatal(err)
	}

	cache := newClientCacheFor(10*time.Second, 8, 1)
	client, err := cache.clientFor(spec)
	if err != nil {
		t.Fatal(err)
	}

	tr := client.Transport.(*http.Transport)
	if tr.Proxy == nil {
		t.Fatal("tr.Proxy is nil - no proxy would be used at all")
	}
	if tr.DialContext == nil {
		t.Fatal("tr.DialContext is nil - the dial would inherit no timeout")
	}

	resp, err := checkOnce(context.Background(), client, "somebody")
	if err != nil {
		t.Fatalf("checkOnce through the proxy failed: %v", err)
	}
	if resp.code != 200 {
		t.Fatalf("HTTP %d through the proxy, body %q", resp.code, resp.body)
	}
	if status, _ := interpret(resp.code, resp.body); status != statusAvailable {
		t.Errorf("classified %s, want available (body %q)", status, resp.body)
	}
	if proxyHits.Load() == 0 {
		t.Error("the request never went through the proxy")
	}
	if endpointHits.Load() == 0 {
		t.Error("the request never reached the endpoint")
	}
	if !strings.Contains(resp.body, "SUCCESS") {
		t.Errorf("body came back as %q", resp.body)
	}
}
