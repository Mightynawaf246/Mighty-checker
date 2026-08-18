package main

import (
	"context"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"strings"
)

// graphqlURL is the target endpoint. A var rather than a const so tests can
// point it at a local server instead of contacting Instagram for real.
var graphqlURL = "https://www.instagram.com/api/graphql"

const (
	friendlyName = "useCAARegistrationFieldValidationQuery"
	docID        = "25391252800555418"

	// Cap on the response body we read, guarding against an unexpectedly huge reply.
	maxBodyBytes = 1 << 20
)

// Check outcomes.
const (
	statusAvailable = "available"
	statusTaken     = "taken"
	statusUnknown   = "unknown"
	statusInvalid   = "invalid"
	statusError     = "error"
)

const (
	alphanum = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	hexchars = "0123456789abcdef"
)

// randomString generates a random alphanumeric string.
// The top-level math/rand/v2 functions are safe for concurrent use.
func randomString(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = alphanum[rand.IntN(len(alphanum))]
	}
	return string(b)
}

// fastHex generates a random lowercase hex string.
func fastHex(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = hexchars[rand.IntN(len(hexchars))]
	}
	return string(b)
}

// validUsername checks the name against Instagram's rules: letters, digits,
// dot and underscore, 1 to 30 characters. We validate before sending because
// characters like & or = would corrupt the form-urlencoded request body.
func validUsername(s string) bool {
	if len(s) == 0 || len(s) > 30 {
		return false
	}
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z',
			r >= 'A' && r <= 'Z',
			r >= '0' && r <= '9',
			r == '.', r == '_':
		default:
			return false
		}
	}
	return true
}

// buildRequest builds the check request with the same headers and payload as
// the original implementation.
func buildRequest(ctx context.Context, target string) (*http.Request, error) {
	igDID := strings.ToUpper(fmt.Sprintf("%s-%s-%s-%s-%s",
		fastHex(8), fastHex(4), fastHex(4), fastHex(4), fastHex(12)))
	csrf := "CSRFT-" + randomString(20)
	lsd := randomString(11)
	email := randomString(8) + "@gmail.com"

	body := fmt.Sprintf(
		`lsd=%s&fb_api_req_friendly_name=%s&server_timestamps=true`+
			`&variables={"input":{"contactpoint":{"sensitive_string_value":"%s"},`+
			`"contactpoint_type":"EMAIL","field_name":"USERNAME",`+
			`"username":{"sensitive_string_value":"%s"}},"scale":1}&doc_id=%s`,
		lsd, friendlyName, email, target, docID)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, graphqlURL, strings.NewReader(body))
	if err != nil {
		return nil, err
	}

	req.Header.Set("User-Agent", "Mozilla/5.0")
	req.Header.Set("X-Fb-Friendly-Name", friendlyName)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("x-csrftoken", csrf)
	req.Header.Set("x-fb-lsd", lsd)
	req.Header.Set("Cookie", "ig_did="+igDID)
	// In Go the Host header is set through req.Host, not through req.Header.
	req.Host = "www.instagram.com"

	return req, nil
}

// Explicit markers meaning the name is taken. Only specific markers are kept;
// loose phrases like "not available" were removed because they misclassified
// block and gateway pages as taken names.
var takenMarkers = []string{
	"username_is_taken",
	"username_is_not_available",
	"this username isn't available",
	"username isn't available",
}

// interpret classifies a check from the HTTP status code and response body.
//
// The endpoint's actual contract, as used by current checkers:
//
//	status SUCCESS          -> the username is AVAILABLE
//	status VALIDATION_ERROR -> the username is TAKEN
//
// Governing rule: never assume "available". Availability is declared only on an
// explicit positive signal; anything ambiguous or non-200 is reported as unknown
// rather than guessed. That keeps available.txt free of false positives.
//
// It also returns retryable: true when the response indicates temporary
// throttling (429/5xx), so the caller retries on a different proxy.
func interpret(httpCode int, body string) (status string, retryable bool) {
	// Rate limited or a transient server error: retry on another proxy.
	switch httpCode {
	case 429, 500, 502, 503, 504:
		return statusUnknown, true
	}
	// Any other non-200 code is untrusted, and not worth retrying.
	if httpCode != 200 {
		return statusUnknown, false
	}

	low := strings.ToLower(body)

	// TAKEN is evaluated before AVAILABLE throughout. If a response ever carries
	// both signals, the safe reading is "taken" — a false "available" is the
	// costliest mistake this tool can make.
	if strings.Contains(low, "validation_error") {
		return statusTaken, false
	}
	for _, m := range takenMarkers {
		if strings.Contains(low, m) {
			return statusTaken, false
		}
	}
	if containsAny(low, `"is_valid":false`, `"is_valid": false`) {
		return statusTaken, false
	}

	// Explicit positive signals. Quoted matching keeps a bare word like "success"
	// appearing in unrelated prose from being read as availability.
	if strings.Contains(low, `"success"`) {
		return statusAvailable, false
	}
	if !strings.Contains(low, `"errors"`) &&
		containsAny(low, `"is_valid":true`, `"is_valid": true`) {
		return statusAvailable, false
	}

	// Everything else is unknown. We do not guess.
	return statusUnknown, false
}

func containsAny(s string, subs ...string) bool {
	for _, sub := range subs {
		if strings.Contains(s, sub) {
			return true
		}
	}
	return false
}

// response is one raw reply from the endpoint, before classification.
type response struct {
	code     int
	body     string
	location string

	// retryAfter is the Retry-After header, when the endpoint sent one. It is
	// the only reliable hint about how long a throttle will last, so waiting
	// exactly that long beats guessing.
	retryAfter string
}

// checkOnce performs a single check attempt through the given client.
func checkOnce(ctx context.Context, client *http.Client, target string) (response, error) {
	req, err := buildRequest(ctx, target)
	if err != nil {
		return response{}, err
	}

	resp, err := client.Do(req)
	if err != nil {
		return response{}, err
	}
	// The body must be drained and closed, otherwise the connection is not reused.
	defer func() {
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}()

	r := response{
		code:       resp.StatusCode,
		location:   resp.Header.Get("Location"),
		retryAfter: resp.Header.Get("Retry-After"),
	}

	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxBodyBytes))
	if err != nil {
		return r, err
	}
	r.body = string(raw)
	return r, nil
}
