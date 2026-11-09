package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/HappyVirgo/ledgerline/internal/ledger"
	"github.com/HappyVirgo/ledgerline/internal/payments"
	"github.com/HappyVirgo/ledgerline/internal/processor"
	"github.com/HappyVirgo/ledgerline/internal/store"
	"github.com/HappyVirgo/ledgerline/internal/tenant"
)

type env struct {
	h    http.Handler
	db   *sql.DB
	key  string
	tid  string
	key2 string
}

func newEnv(t testing.TB, rate float64, burst int) *env {
	t.Helper()
	db, err := store.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	ctx := context.Background()
	tid, key, err := tenant.Create(ctx, db, "Test Tenant One")
	if err != nil {
		t.Fatal(err)
	}
	_, key2, err := tenant.Create(ctx, db, "Test Tenant Two")
	if err != nil {
		t.Fatal(err)
	}
	s := &Server{DB: db, Payments: &payments.Service{DB: db, Proc: processor.NewMock()},
		Limiter: tenant.NewRateLimiter(rate, burst)}
	return &env{h: s.Handler(), db: db, key: key, tid: tid, key2: key2}
}

func (e *env) do(key, method, path, idem, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	if idem != "" {
		req.Header.Set(IdempotencyHeader, idem)
	}
	rec := httptest.NewRecorder()
	e.h.ServeHTTP(rec, req)
	return rec
}

func decodeIntent(t testing.TB, rec *httptest.ResponseRecorder) payments.Intent {
	t.Helper()
	var in payments.Intent
	if err := json.Unmarshal(rec.Body.Bytes(), &in); err != nil {
		t.Fatalf("decode %q: %v", rec.Body.String(), err)
	}
	return in
}

const approved = `{"amount":2500,"currency":"USD","card_number":"4242424242424242"}`

func TestAuthAndRouting(t *testing.T) {
	e := newEnv(t, 1000, 1000)
	tests := []struct {
		name, key, method, path, body string
		want                          int
	}{
		{"no key", "", "POST", "/v1/payment_intents", approved, 401},
		{"bad key", "ll_test_nope", "POST", "/v1/payment_intents", approved, 401},
		{"create", e.key, "POST", "/v1/payment_intents", approved, 201},
		{"declined", e.key, "POST", "/v1/payment_intents", `{"amount":100,"currency":"USD","card_number":"4000000000000002"}`, 402},
		{"unknown field", e.key, "POST", "/v1/payment_intents", `{"amount":1,"currency":"USD","card_number":"4242424242424242","x":1}`, 400},
		{"float amount", e.key, "POST", "/v1/payment_intents", `{"amount":10.5,"currency":"USD","card_number":"4242424242424242"}`, 400},
		{"bad currency", e.key, "POST", "/v1/payment_intents", `{"amount":1,"currency":"ABC","card_number":"4242424242424242"}`, 400},
		{"missing intent", e.key, "GET", "/v1/payment_intents/pi_missing", "", 404},
		{"bad webhook url", e.key, "PUT", "/v1/webhook_endpoint", `{"url":"ftp://x"}`, 400},
		{"webhook", e.key, "PUT", "/v1/webhook_endpoint", `{"url":"https://example.test/hook"}`, 200},
		{"accounts", e.key, "GET", "/v1/accounts", "", 200},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if rec := e.do(tt.key, tt.method, tt.path, "", tt.body); rec.Code != tt.want {
				t.Fatalf("status = %d; want %d; body %s", rec.Code, tt.want, rec.Body)
			}
		})
	}
}

func TestPaymentFlowOverHTTP(t *testing.T) {
	e := newEnv(t, 1000, 1000)
	in := decodeIntent(t, e.do(e.key, "POST", "/v1/payment_intents", "", approved))
	base := "/v1/payment_intents/" + in.ID

	if rec := e.do(e.key2, "GET", base, "", ""); rec.Code != 404 {
		t.Fatalf("other tenant can see intent: %d", rec.Code)
	}
	if rec := e.do(e.key, "POST", base+"/capture", "", ""); rec.Code != 200 {
		t.Fatalf("capture: %d %s", rec.Code, rec.Body)
	}
	if rec := e.do(e.key, "POST", base+"/refunds", "", `{"amount":1000}`); rec.Code != 200 {
		t.Fatalf("refund: %d %s", rec.Code, rec.Body)
	}
	if rec := e.do(e.key, "POST", base+"/refunds", "", `{"amount":2000}`); rec.Code != 400 {
		t.Fatalf("over-refund: %d %s", rec.Code, rec.Body)
	}
	got := decodeIntent(t, e.do(e.key, "GET", base, "", ""))
	if got.Status != payments.StatusPartiallyRefunded || got.AmountRefunded != 1000 {
		t.Fatalf("intent = %+v", got)
	}
}

func TestIdempotency(t *testing.T) {
	e := newEnv(t, 1000, 1000)
	first := e.do(e.key, "POST", "/v1/payment_intents", "k1", approved)
	replay := e.do(e.key, "POST", "/v1/payment_intents", "k1", approved)
	if first.Code != 201 || replay.Code != 201 || first.Body.String() != replay.Body.String() {
		t.Fatalf("replay mismatch: %d %s vs %d %s", first.Code, first.Body, replay.Code, replay.Body)
	}
	if replay.Header().Get("Idempotent-Replayed") != "true" {
		t.Fatal("replay not marked")
	}
	if rec := e.do(e.key, "POST", "/v1/payment_intents", "k1", strings.Replace(approved, "2500", "2600", 1)); rec.Code != 409 {
		t.Fatalf("different body same key: %d; want 409", rec.Code)
	}
	// Keys are scoped per tenant: tenant two may reuse "k1".
	if rec := e.do(e.key2, "POST", "/v1/payment_intents", "k1", approved); rec.Code != 201 ||
		rec.Header().Get("Idempotent-Replayed") != "" {
		t.Fatalf("tenant two reusing key: %d", rec.Code)
	}
	var n int
	e.db.QueryRow(`SELECT COUNT(*) FROM payment_intents WHERE tenant_id = ?`, e.tid).Scan(&n)
	if n != 1 {
		t.Fatalf("tenant one intents = %d; want 1", n)
	}
}

// TestConcurrentIdempotentRequests sends the same keyed request from many goroutines:
// exactly one intent may be created and every caller must see the same response.
func TestConcurrentIdempotentRequests(t *testing.T) {
	e := newEnv(t, 10000, 10000)
	in := decodeIntent(t, e.do(e.key, "POST", "/v1/payment_intents", "", approved))
	e.do(e.key, "POST", "/v1/payment_intents/"+in.ID+"/capture", "", "")
	const n = 32
	bodies := make([]string, n)
	codes := make([]int, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c := e.do(e.key, "POST", "/v1/payment_intents", "create-1", approved)
			r := e.do(e.key, "POST", "/v1/payment_intents/"+in.ID+"/refunds", "refund-1", `{"amount":100}`)
			bodies[i], codes[i] = c.Body.String()+r.Body.String(), c.Code*1000+r.Code
		}(i)
	}
	wg.Wait()
	for i := 1; i < n; i++ {
		if bodies[i] != bodies[0] || codes[i] != codes[0] {
			t.Fatalf("response %d differs:\n%s\nvs\n%s", i, bodies[i], bodies[0])
		}
	}
	var intents int
	e.db.QueryRow(`SELECT COUNT(*) FROM payment_intents WHERE tenant_id = ?`, e.tid).Scan(&intents)
	if intents != 2 {
		t.Fatalf("intents = %d; want 2", intents)
	}
	if got := decodeIntent(t, e.do(e.key, "GET", "/v1/payment_intents/"+in.ID, "", "")); got.AmountRefunded != 100 {
		t.Fatalf("amount_refunded = %d; want 100 (refund applied once)", got.AmountRefunded)
	}
	if err := ledger.Verify(context.Background(), e.db, e.tid); err != nil {
		t.Fatal(err)
	}
}

func TestRateLimit(t *testing.T) {
	e := newEnv(t, 0.001, 3)
	for i := 0; i < 3; i++ {
		if rec := e.do(e.key, "GET", "/v1/accounts", "", ""); rec.Code != 200 {
			t.Fatalf("request %d: %d", i, rec.Code)
		}
	}
	rec := e.do(e.key, "GET", "/v1/accounts", "", "")
	if rec.Code != 429 || rec.Header().Get("Retry-After") == "" {
		t.Fatalf("over limit: %d retry-after=%q", rec.Code, rec.Header().Get("Retry-After"))
	}
	if rec := e.do(e.key2, "GET", "/v1/accounts", "", ""); rec.Code != 200 {
		t.Fatalf("other tenant throttled: %d", rec.Code)
	}
}

func TestReconciliationEndpoint(t *testing.T) {
	e := newEnv(t, 1000, 1000)
	var refs []string
	for i := 0; i < 3; i++ {
		in := decodeIntent(t, e.do(e.key, "POST", "/v1/payment_intents", "", approved))
		c := decodeIntent(t, e.do(e.key, "POST", "/v1/payment_intents/"+in.ID+"/capture", "", ""))
		refs = append(refs, c.CaptureRef)
	}
	csv := fmt.Sprintf("processor_ref,amount,currency\n%s,2500,USD\n%s,2400,USD\nmock_cap_999999,10,USD\n", refs[0], refs[1])
	rec := e.do(e.key, "POST", "/v1/reconciliations", "", csv)
	if rec.Code != 200 {
		t.Fatalf("recon: %d %s", rec.Code, rec.Body)
	}
	var rep struct {
		Matched    int               `json:"matched"`
		Missing    []json.RawMessage `json:"missing_from_settlement"`
		Unknown    []json.RawMessage `json:"unknown_in_settlement"`
		Mismatched []json.RawMessage `json:"mismatched"`
	}
	json.Unmarshal(rec.Body.Bytes(), &rep)
	if rep.Matched != 1 || len(rep.Missing) != 1 || len(rep.Unknown) != 1 || len(rep.Mismatched) != 1 {
		t.Fatalf("report = %s", rec.Body)
	}
	if rec := e.do(e.key, "POST", "/v1/reconciliations", "", "nope"); rec.Code != 400 {
		t.Fatalf("bad csv: %d", rec.Code)
	}
}

// BenchmarkHTTPCreateAndCapture measures the full in-process HTTP path for one payment:
// authenticated, idempotent create followed by capture.
func BenchmarkHTTPCreateAndCapture(b *testing.B) {
	e := newEnv(b, 1e9, 1e9)
	i := 0
	b.ReportAllocs()
	for b.Loop() {
		i++
		rec := e.do(e.key, "POST", "/v1/payment_intents", fmt.Sprintf("bench-%d", i), approved)
		if rec.Code != 201 {
			b.Fatalf("create: %d", rec.Code)
		}
		in := decodeIntent(b, rec)
		if rec := e.do(e.key, "POST", "/v1/payment_intents/"+in.ID+"/capture", "", ""); rec.Code != 200 {
			b.Fatalf("capture: %d", rec.Code)
		}
	}
}
