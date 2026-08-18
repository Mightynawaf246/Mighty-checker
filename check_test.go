package main

import (
	"context"
	"io"
	"strings"
	"testing"
)

func TestInterpret(t *testing.T) {
	cases := []struct {
		name      string
		code      int
		body      string
		want      string
		retryable bool
	}{
		// The endpoint's real contract: SUCCESS means available,
		// VALIDATION_ERROR means taken.
		{"status SUCCESS", 200,
			`{"data":{"xfb_caa_registration_field_validation":{"status":"SUCCESS"}}}`,
			statusAvailable, false},
		{"status VALIDATION_ERROR", 200,
			`{"data":{"xfb_caa_registration_field_validation":{"status":"VALIDATION_ERROR"}}}`,
			statusTaken, false},
		// Case must not matter, since the body is lowercased before matching.
		{"lowercase success", 200, `{"status":"success"}`, statusAvailable, false},
		{"lowercase validation_error", 200, `{"status":"validation_error"}`, statusTaken, false},
		// With both signals present, taken must win: a false available is the
		// costliest possible mistake.
		{"both signals prefers taken", 200,
			`{"status":"SUCCESS","error":"VALIDATION_ERROR"}`, statusTaken, false},

		// Availability is declared only on an explicit validation signal.
		{"valid true", 200, `{"data":{"xfb_field_validation":{"is_valid":true}}}`, statusAvailable, false},
		{"valid false", 200, `{"data":{"xfb_field_validation":{"is_valid":false}}}`, statusTaken, false},
		{"taken marker", 200, `{"errors":[{"message":"username_is_taken"}]}`, statusTaken, false},
		{"taken phrase", 200, `{"message":"This username isn't available."}`, statusTaken, false},

		// The key change: a 200 containing "data" without a validation signal is
		// no longer reported as available.
		{"data without validity", 200, `{"data":{"something":"else"}}`, statusUnknown, false},

		{"empty body", 200, ``, statusUnknown, false},
		{"errors array", 200, `{"errors":[{"message":"something else"}]}`, statusUnknown, false},
		{"html on 200", 200, `<html><body>Please wait</body></html>`, statusUnknown, false},

		// Throttling codes: unknown and retryable.
		{"429 rate limited", 429, `{"data":{"xfb_field_validation":{"is_valid":true}}}`, statusUnknown, true},
		{"503", 503, ``, statusUnknown, true},
		{"502", 502, `whatever`, statusUnknown, true},

		// Any other non-200 code: unknown, not retryable.
		{"403 not retryable", 403, `{"data":{"xfb_field_validation":{"is_valid":true}}}`, statusUnknown, false},
		{"302 redirect", 302, ``, statusUnknown, false},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, retry := interpret(c.code, c.body)
			if got != c.want {
				t.Errorf("interpret(%d, %q) status = %s, want %s", c.code, c.body, got, c.want)
			}
			if retry != c.retryable {
				t.Errorf("interpret(%d, %q) retryable = %v, want %v", c.code, c.body, retry, c.retryable)
			}
		})
	}
}

// The worst possible bug: reporting "available" for an ambiguous response. It
// must never happen without an explicit signal, whatever the HTTP status.
func TestInterpretNeverGuessesAvailable(t *testing.T) {
	suspicious := []struct {
		code int
		body string
	}{
		{200, `<html>429 Too Many Requests</html>`},
		{200, `login required`},
		{200, `{"status":"fail"}`},
		{200, `checkpoint_required`},
		{200, `{"data":{"xfb_field_validation":{}}}`}, // "data" without is_valid
		{429, `{"data":{"xfb_field_validation":{"is_valid":true}}}`},
		{403, `{"data":{"xfb_field_validation":{"is_valid":true}}}`},
		{500, `{"is_valid":true}`},
		// A bare unquoted word must not be read as the SUCCESS status.
		{200, `the operation was a success`},
		// SUCCESS on a throttled or non-200 response is still not trustworthy.
		{429, `{"status":"SUCCESS"}`},
		{403, `{"status":"SUCCESS"}`},
	}
	for _, c := range suspicious {
		if got, _ := interpret(c.code, c.body); got == statusAvailable {
			t.Errorf("interpret(%d, %q) must not be available", c.code, c.body)
		}
	}
}

func TestValidUsername(t *testing.T) {
	valid := []string{"cristiano", "a", "user_name", "user.name", "abc123", strings.Repeat("a", 30)}
	for _, u := range valid {
		if !validUsername(u) {
			t.Errorf("validUsername(%q) should be true", u)
		}
	}

	invalid := []string{
		"",
		strings.Repeat("a", 31),
		"has space",
		"has&amp",    // would corrupt the form-urlencoded body
		"has=equals", // would corrupt the form-urlencoded body
		"has#hash",
		"unicodé",
		"emoji😀",
	}
	for _, u := range invalid {
		if validUsername(u) {
			t.Errorf("validUsername(%q) should be false", u)
		}
	}
}

func TestBuildRequestShape(t *testing.T) {
	req, err := buildRequest(context.Background(), "cristiano")
	if err != nil {
		t.Fatalf("buildRequest: %v", err)
	}

	if req.Method != "POST" {
		t.Errorf("method: want POST, got %s", req.Method)
	}
	if req.URL.String() != graphqlURL {
		t.Errorf("url: want %s, got %s", graphqlURL, req.URL.String())
	}
	// In Go the Host header lives in the req.Host field, not the header map.
	if req.Host != "www.instagram.com" {
		t.Errorf("host: want www.instagram.com, got %s", req.Host)
	}
	if got := req.Header.Get("X-Fb-Friendly-Name"); got != friendlyName {
		t.Errorf("friendly name header: want %s, got %s", friendlyName, got)
	}
	if got := req.Header.Get("Content-Type"); got != "application/x-www-form-urlencoded" {
		t.Errorf("content-type: got %s", got)
	}

	body, err := io.ReadAll(req.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	s := string(body)

	for _, want := range []string{
		"cristiano",
		"doc_id=" + docID,
		"fb_api_req_friendly_name=" + friendlyName,
		`"field_name":"USERNAME"`,
		`"contactpoint_type":"EMAIL"`,
		"server_timestamps=true",
	} {
		if !strings.Contains(s, want) {
			t.Errorf("payload missing %q\nbody: %s", want, s)
		}
	}

	// The lsd value in the body must match the x-fb-lsd header.
	lsdHeader := req.Header.Get("x-fb-lsd")
	if lsdHeader == "" {
		t.Fatal("x-fb-lsd header is empty")
	}
	if !strings.Contains(s, "lsd="+lsdHeader) {
		t.Errorf("body lsd does not match x-fb-lsd header %q\nbody: %s", lsdHeader, s)
	}
}

// Random values must differ between requests, otherwise the fingerprint is static.
func TestRandomValuesDiffer(t *testing.T) {
	r1, _ := buildRequest(context.Background(), "x")
	r2, _ := buildRequest(context.Background(), "x")

	if r1.Header.Get("x-fb-lsd") == r2.Header.Get("x-fb-lsd") {
		t.Error("lsd is identical across requests")
	}
	if r1.Header.Get("Cookie") == r2.Header.Get("Cookie") {
		t.Error("ig_did cookie is identical across requests")
	}
	if r1.Header.Get("x-csrftoken") == r2.Header.Get("x-csrftoken") {
		t.Error("csrf token is identical across requests")
	}
}

func TestRandomHelpersProduceCorrectShape(t *testing.T) {
	if got := len(randomString(20)); got != 20 {
		t.Errorf("randomString(20) length = %d", got)
	}
	// The device id is UUID-shaped and uppercase hex, which is what the real
	// client sends.
	id := randomDeviceID()
	if len(id) != 36 {
		t.Errorf("randomDeviceID length = %d, want 36", len(id))
	}
	for i, r := range id {
		switch i {
		case 8, 13, 18, 23:
			if r != '-' {
				t.Errorf("position %d = %q, want a dash", i, r)
			}
		default:
			if !strings.ContainsRune("0123456789ABCDEF", r) {
				t.Errorf("randomDeviceID produced non-uppercase-hex rune %q", r)
			}
		}
	}
	if randomDeviceID() == randomDeviceID() {
		t.Error("randomDeviceID repeated itself")
	}
	if randomString(0) != "" {
		t.Error("zero-length randomString should return an empty string")
	}
}
