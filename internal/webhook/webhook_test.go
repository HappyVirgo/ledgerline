package webhook

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/HappyVirgo/ledgerline/internal/store"
)

func TestSignVerify(t *testing.T) {
	secret := "whsec_test"
	now := time.Unix(1_700_000_000, 0)
	payload := []byte(`{"id":"evt_1"}`)
	good := Sign(secret, now, payload)
	_, oldSig, _ := strings.Cut(Sign("whsec_old", now, payload), ",")

	tests := []struct {
		name    string
		secret  string
		header  string
		payload []byte
		now     time.Time
		want    error
	}{
		{"valid", secret, good, payload, now, nil},
		{"valid within tolerance", secret, good, payload, now.Add(4 * time.Minute), nil},
		{"extra signature during rotation", secret, good + "," + oldSig, payload, now, nil},
		{"tampered payload", secret, good, []byte(`{"id":"evt_2"}`), now, ErrBadSignature},
		{"wrong secret", "whsec_other", good, payload, now, ErrBadSignature},
		{"stale", secret, good, payload, now.Add(6 * time.Minute), ErrStale},
		{"future", secret, good, payload, now.Add(-6 * time.Minute), ErrStale},
		{"no signature", secret, "t=1700000000", payload, now, ErrBadHeader},
		{"garbage", secret, "garbage", payload, now, ErrBadHeader},
		{"bad timestamp", secret, "t=abc,v1=00", payload, now, ErrBadHeader},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := Verify(tt.secret, tt.header, tt.payload, 5*time.Minute, tt.now)
			if !errors.Is(err, tt.want) {
				t.Fatalf("Verify() = %v; want %v", err, tt.want)
			}
		})
	}
}

func TestBackoff(t *testing.T) {
	d := &Dispatcher{BaseBackoff: time.Second, MaxBackoff: 10 * time.Second}
	want := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 10 * time.Second, 10 * time.Second}
	for i, w := range want {
		if got := d.Backoff(i + 1); got != w {
			t.Errorf("Backoff(%d) = %v; want %v", i+1, got, w)
		}
	}
}

func setup(t *testing.T) *sql.DB {
	t.Helper()
	db, err := store.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	if _, err := db.Exec(`INSERT INTO tenants (id, name, created_at) VALUES ('t1', 't1', 0)`); err != nil {
		t.Fatal(err)
	}
	return db
}

func enqueue(t *testing.T, db *sql.DB, fail bool) {
	t.Helper()
	store.InTx(context.Background(), db, func(tx *sql.Tx) error {
		if err := Enqueue(context.Background(), tx, "t1", "test.event", map[string]int{"n": 1}); err != nil {
			t.Fatal(err)
		}
		if fail {
			return errors.New("state change failed")
		}
		return nil
	})
}

func TestEnqueueIsTransactional(t *testing.T) {
	db := setup(t)
	enqueue(t, db, true)
	enqueue(t, db, false)
	var n int
	db.QueryRow(`SELECT COUNT(*) FROM outbox`).Scan(&n)
	if n != 1 {
		t.Fatalf("outbox rows = %d; want 1 (rolled-back tx must not emit events)", n)
	}
}

func TestDispatcherRetriesThenDelivers(t *testing.T) {
	db := setup(t)
	var mu sync.Mutex // the handler runs on the server's goroutine
	var received [][]byte
	var sigs []string
	failures := 2
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		defer mu.Unlock()
		if failures > 0 {
			failures--
			w.WriteHeader(500)
			return
		}
		received = append(received, body)
		sigs = append(sigs, r.Header.Get(SignatureHeader))
	}))
	defer srv.Close()
	secret, err := SetEndpoint(context.Background(), db, "t1", srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	enqueue(t, db, false)

	now := time.Now().Add(time.Second) // events are enqueued with the real clock
	d := &Dispatcher{DB: db, Send: HTTPSender(srv.Client()), Now: func() time.Time { return now },
		BaseBackoff: time.Second, MaxBackoff: time.Minute, MaxAttempts: 5, Lease: 30 * time.Second, BatchSize: 10}
	ctx := context.Background()

	for i, step := range []time.Duration{0, time.Second, 2 * time.Second} {
		now = now.Add(step)
		n, err := d.RunOnce(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if wantDelivered := i == 2; (n == 1) != wantDelivered {
			t.Fatalf("attempt %d delivered=%d", i+1, n)
		}
		// Not yet due: an immediate re-run must not resend.
		if n, _ := d.RunOnce(ctx); n != 0 {
			t.Fatal("event re-sent before backoff elapsed")
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if len(received) != 1 {
		t.Fatalf("received %d deliveries; want 1", len(received))
	}
	if err := Verify(secret, sigs[0], received[0], 5*time.Minute, now); err != nil {
		t.Fatalf("delivered payload failed verification: %v", err)
	}
	var status string
	var attempts int
	db.QueryRow(`SELECT status, attempts FROM outbox`).Scan(&status, &attempts)
	if status != "delivered" || attempts != 3 {
		t.Fatalf("status=%s attempts=%d; want delivered/3", status, attempts)
	}
}

func TestDispatcherDeadLetters(t *testing.T) {
	db := setup(t)
	if _, err := SetEndpoint(context.Background(), db, "t1", "http://unused.invalid"); err != nil {
		t.Fatal(err)
	}
	enqueue(t, db, false)
	now := time.Now().Add(time.Second) // events are enqueued with the real clock
	d := &Dispatcher{DB: db, Now: func() time.Time { return now },
		Send:        func(context.Context, string, []byte, string) error { return errors.New("connection refused") },
		BaseBackoff: time.Second, MaxBackoff: time.Second, MaxAttempts: 3, Lease: time.Second, BatchSize: 10}
	for i := 0; i < 5; i++ {
		if _, err := d.RunOnce(context.Background()); err != nil {
			t.Fatal(err)
		}
		now = now.Add(2 * time.Second)
	}
	var status, lastErr string
	var attempts int
	db.QueryRow(`SELECT status, attempts, last_error FROM outbox`).Scan(&status, &attempts, &lastErr)
	if status != "dead" || attempts != 3 || lastErr != "connection refused" {
		t.Fatalf("got %s/%d/%q; want dead/3/connection refused", status, attempts, lastErr)
	}
}
