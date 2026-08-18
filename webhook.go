package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// sendWebhook posts a simple text notification as JSON. The "content" field
// matches Discord webhooks and works with any service accepting that shape.
func sendWebhook(ctx context.Context, url, content string) error {
	payload, err := json.Marshal(map[string]string{"content": content})
	if err != nil {
		return err
	}

	reqCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		return fmt.Errorf("webhook responded %d", resp.StatusCode)
	}
	return nil
}

// notifyAvailable fires a webhook when an available username is found and
// prints the send progress through the console.
//
// It deliberately uses context.Background rather than the run context, so the
// notification still goes out if the user hits Ctrl-C while remaining results
// are being flushed.
func notifyAvailable(con *console, url, username string) {
	con.log(cGray("Sending webhook notification..."))
	content := fmt.Sprintf(":white_check_mark: Mighty — available username found: @%s", username)
	if err := sendWebhook(context.Background(), url, content); err != nil {
		con.log(cRed("Webhook failed: " + err.Error()))
		return
	}
	con.log(cGreen("Webhook sent successfully!"))
}
