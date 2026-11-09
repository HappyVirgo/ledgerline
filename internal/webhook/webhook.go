// Package webhook implements a transactional outbox, a retrying dispatcher and
// HMAC-SHA256 payload signing with timestamps.
package webhook

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/HappyVirgo/ledgerline/internal/store"
)

// SignatureHeader carries "t=<unix seconds>,v1=<hex hmac>".
const SignatureHeader = "Ledgerline-Signature"

// Verification errors.
var (
	ErrBadHeader    = errors.New("malformed signature header")
	ErrBadSignature = errors.New("signature mismatch")
	ErrStale        = errors.New("timestamp outside tolerance")
)

// Event is the JSON envelope delivered to endpoints.
type Event struct {
	ID      string          `json:"id"`
	Type    string          `json:"type"`
	Created int64           `json:"created"`
	Data    json.RawMessage `json:"data"`
}

// Enqueue writes an event to the outbox inside tx, so it commits (or rolls back)
// atomically with the state change it describes.
func Enqueue(ctx context.Context, tx *sql.Tx, tenantID, eventType string, data any) error {
	raw, err := json.Marshal(data)
	if err != nil {
		return err
	}
	now := time.Now().Unix()
	payload, err := json.Marshal(Event{ID: store.NewID("evt_"), Type: eventType, Created: now, Data: raw})
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx,
		`INSERT INTO outbox (tenant_id, event_type, payload, next_attempt_at, created_at) VALUES (?, ?, ?, ?, ?)`,
		tenantID, eventType, payload, now, now)
	return err
}

// SetEndpoint registers (or replaces) the tenant's webhook URL and returns a new signing secret.
func SetEndpoint(ctx context.Context, q store.DBTX, tenantID, url string) (string, error) {
	b := make([]byte, 24)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	secret := "whsec_" + hex.EncodeToString(b)
	_, err := q.ExecContext(ctx,
		`INSERT INTO webhook_endpoints (tenant_id, url, secret) VALUES (?, ?, ?)
		 ON CONFLICT (tenant_id) DO UPDATE SET url = excluded.url, secret = excluded.secret`,
		tenantID, url, secret)
	return secret, err
}

// Sign returns the signature header value for payload at time ts.
// The MAC covers "<ts>.<payload>" so a captured payload cannot be replayed with a new timestamp.
func Sign(secret string, ts time.Time, payload []byte) string {
	t := strconv.FormatInt(ts.Unix(), 10)
	return "t=" + t + ",v1=" + mac(secret, t, payload)
}

func mac(secret, t string, payload []byte) string {
	h := hmac.New(sha256.New, []byte(secret))
	h.Write([]byte(t))
	h.Write([]byte("."))
	h.Write(payload)
	return hex.EncodeToString(h.Sum(nil))
}

// Verify checks a signature header against payload. It rejects timestamps further
// than tolerance from now and compares MACs in constant time. Receivers should call this
// before trusting a delivery.
func Verify(secret, header string, payload []byte, tolerance time.Duration, now time.Time) error {
	var t string
	var sigs []string
	for _, part := range strings.Split(header, ",") {
		k, v, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok {
			return ErrBadHeader
		}
		switch k {
		case "t":
			t = v
		case "v1":
			sigs = append(sigs, v)
		}
	}
	ts, err := strconv.ParseInt(t, 10, 64)
	if err != nil || len(sigs) == 0 {
		return ErrBadHeader
	}
	if d := now.Sub(time.Unix(ts, 0)); d > tolerance || d < -tolerance {
		return ErrStale
	}
	want := []byte(mac(secret, t, payload))
	for _, s := range sigs {
		if hmac.Equal(want, []byte(s)) {
			return nil
		}
	}
	return ErrBadSignature
}

// HTTPSender returns a Send function that POSTs JSON with the signature header and
// treats any non-2xx response as a failure.
func HTTPSender(client *http.Client) func(ctx context.Context, url string, payload []byte, signature string) error {
	return func(ctx context.Context, url string, payload []byte, signature string) error {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set(SignatureHeader, signature)
		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<16))
		if resp.StatusCode < 200 || resp.StatusCode > 299 {
			return fmt.Errorf("endpoint returned %d", resp.StatusCode)
		}
		return nil
	}
}

// Dispatcher delivers due outbox events with exponential backoff.
// Delivery is at-least-once: receivers should dedupe on Event.ID.
type Dispatcher struct {
	DB          *sql.DB
	Send        func(ctx context.Context, url string, payload []byte, signature string) error
	Now         func() time.Time
	BaseBackoff time.Duration // delay after the first failure; doubles each attempt
	MaxBackoff  time.Duration
	MaxAttempts int
	Lease       time.Duration // how long a claimed event is hidden from other dispatchers
	BatchSize   int
}

// Backoff returns the delay before retry number `attempts` (1-based), capped at MaxBackoff.
func (d *Dispatcher) Backoff(attempts int) time.Duration {
	delay := d.BaseBackoff
	for i := 1; i < attempts && delay < d.MaxBackoff; i++ {
		delay *= 2
	}
	return min(delay, d.MaxBackoff)
}

type due struct {
	id       int64
	attempts int
	payload  []byte
	url      string
	secret   string
}

// RunOnce claims and attempts up to BatchSize due events and returns how many were delivered.
// The database is not held while HTTP requests are in flight.
func (d *Dispatcher) RunOnce(ctx context.Context) (int, error) {
	now := d.Now()
	rows, err := d.DB.QueryContext(ctx, `
		SELECT o.id, o.attempts, o.payload, e.url, e.secret
		FROM outbox o JOIN webhook_endpoints e ON e.tenant_id = o.tenant_id
		WHERE o.status = 'pending' AND o.next_attempt_at <= ?
		ORDER BY o.next_attempt_at, o.id LIMIT ?`, now.Unix(), d.BatchSize)
	if err != nil {
		return 0, err
	}
	var batch []due
	for rows.Next() {
		var e due
		if err := rows.Scan(&e.id, &e.attempts, &e.payload, &e.url, &e.secret); err != nil {
			rows.Close()
			return 0, err
		}
		batch = append(batch, e)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}

	delivered := 0
	for _, e := range batch {
		// Claim with a lease so a concurrent dispatcher skips this event.
		res, err := d.DB.ExecContext(ctx,
			`UPDATE outbox SET next_attempt_at = ? WHERE id = ? AND status = 'pending' AND next_attempt_at <= ?`,
			now.Add(d.Lease).Unix(), e.id, now.Unix())
		if err != nil {
			return delivered, err
		}
		if n, _ := res.RowsAffected(); n == 0 {
			continue
		}
		sendErr := d.Send(ctx, e.url, e.payload, Sign(e.secret, d.Now(), e.payload))
		if err := d.record(ctx, e, sendErr); err != nil {
			return delivered, err
		}
		if sendErr == nil {
			delivered++
		}
	}
	return delivered, nil
}

func (d *Dispatcher) record(ctx context.Context, e due, sendErr error) error {
	attempts := e.attempts + 1
	if sendErr == nil {
		_, err := d.DB.ExecContext(ctx,
			`UPDATE outbox SET status = 'delivered', attempts = ?, last_error = '' WHERE id = ?`, attempts, e.id)
		return err
	}
	status := "pending"
	if attempts >= d.MaxAttempts {
		status = "dead"
	}
	next := d.Now().Add(d.Backoff(attempts)).Unix()
	_, err := d.DB.ExecContext(ctx,
		`UPDATE outbox SET status = ?, attempts = ?, next_attempt_at = ?, last_error = ? WHERE id = ?`,
		status, attempts, next, sendErr.Error(), e.id)
	return err
}

// Run calls RunOnce every interval until ctx is cancelled.
func (d *Dispatcher) Run(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		if _, err := d.RunOnce(ctx); err != nil && ctx.Err() == nil {
			slog.Error("webhook dispatcher", "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}
