package payments

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"testing"

	"github.com/HappyVirgo/ledgerline/internal/ledger"
	"github.com/HappyVirgo/ledgerline/internal/processor"
	"github.com/HappyVirgo/ledgerline/internal/store"
)

func newService(t *testing.T, tenants ...string) *Service {
	t.Helper()
	db, err := store.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	for _, id := range tenants {
		if _, err := db.Exec(`INSERT INTO tenants (id, name, created_at) VALUES (?, ?, 0)`, id, id); err != nil {
			t.Fatal(err)
		}
	}
	return &Service{DB: db, Proc: processor.NewMock()}
}

func balances(t *testing.T, db *sql.DB, tenantID string) map[string]int64 {
	t.Helper()
	accts, err := ledger.Accounts(context.Background(), db, tenantID)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]int64{}
	for _, a := range accts {
		out[a.Code] = a.Balance
	}
	if err := ledger.Verify(context.Background(), db, tenantID); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestCreateOutcomes(t *testing.T) {
	s := newService(t, "t1")
	tests := []struct {
		card, status, failure string
	}{
		{processor.CardApproved, StatusAuthorized, ""},
		{processor.CardDeclined, StatusDeclined, "card_declined"},
		{processor.CardInsufficientFunds, StatusDeclined, "insufficient_funds"},
		{processor.CardTimeout, StatusFailed, "processor_timeout"},
	}
	for _, tt := range tests {
		in, err := s.Create(context.Background(), "t1", CreateParams{Amount: 1000, Currency: "usd", Card: tt.card})
		if err != nil {
			t.Fatalf("card %s: %v", tt.card, err)
		}
		if in.Status != tt.status || in.FailureCode != tt.failure || in.CardLast4 != tt.card[12:] {
			t.Errorf("card %s: got %s/%s/%s", tt.card, in.Status, in.FailureCode, in.CardLast4)
		}
	}
	var events int
	s.DB.QueryRow(`SELECT COUNT(*) FROM outbox`).Scan(&events)
	if events != len(tests) {
		t.Errorf("outbox events = %d; want %d", events, len(tests))
	}
}

func TestCreateValidation(t *testing.T) {
	s := newService(t, "t1")
	for _, p := range []CreateParams{
		{Amount: 0, Currency: "USD", Card: processor.CardApproved},
		{Amount: -1, Currency: "USD", Card: processor.CardApproved},
		{Amount: 100, Currency: "XXX", Card: processor.CardApproved},
	} {
		if _, err := s.Create(context.Background(), "t1", p); err == nil {
			t.Errorf("Create(%+v) succeeded; want error", p)
		}
	}
}

func TestCaptureAndRefundLifecycle(t *testing.T) {
	s := newService(t, "t1")
	ctx := context.Background()
	in, _ := s.Create(ctx, "t1", CreateParams{Amount: 1000, Currency: "USD", Card: processor.CardApproved})

	if _, err := s.Refund(ctx, "t1", in.ID, 100); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("refund before capture err = %v; want ErrInvalidState", err)
	}
	if _, err := s.Capture(ctx, "t1", in.ID, 1001); !errors.Is(err, ErrInvalidAmount) {
		t.Fatalf("over-capture err = %v; want ErrInvalidAmount", err)
	}
	c, err := s.Capture(ctx, "t1", in.ID, 800) // partial capture
	if err != nil || c.Status != StatusCaptured || c.AmountCaptured != 800 || c.CaptureRef == "" {
		t.Fatalf("capture = %+v, %v", c, err)
	}
	if _, err := s.Capture(ctx, "t1", in.ID, 0); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("double capture err = %v; want ErrInvalidState", err)
	}
	r1, err := s.Refund(ctx, "t1", in.ID, 300)
	if err != nil || r1.Status != StatusPartiallyRefunded || r1.AmountRefunded != 300 {
		t.Fatalf("partial refund = %+v, %v", r1, err)
	}
	if _, err := s.Refund(ctx, "t1", in.ID, 501); !errors.Is(err, ErrInvalidAmount) {
		t.Fatalf("over-refund err = %v; want ErrInvalidAmount", err)
	}
	r2, err := s.Refund(ctx, "t1", in.ID, 0) // remainder
	if err != nil || r2.Status != StatusRefunded || r2.AmountRefunded != 800 {
		t.Fatalf("full refund = %+v, %v", r2, err)
	}
	got := balances(t, s.DB, "t1")
	want := map[string]int64{AcctClearing: 0, AcctRevenue: 800, AcctRefunds: 800}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s = %d; want %d", k, got[k], v)
		}
	}
}

func TestTenantIsolation(t *testing.T) {
	s := newService(t, "t1", "t2")
	ctx := context.Background()
	in, _ := s.Create(ctx, "t1", CreateParams{Amount: 500, Currency: "EUR", Card: processor.CardApproved})
	if _, err := s.Get(ctx, "t2", in.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cross-tenant get err = %v; want ErrNotFound", err)
	}
	if _, err := s.Capture(ctx, "t2", in.ID, 0); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cross-tenant capture err = %v; want ErrNotFound", err)
	}
}

func TestCaptureTimeoutRevertsToAuthorized(t *testing.T) {
	s := newService(t, "t1")
	ctx := context.Background()
	in, _ := s.Create(ctx, "t1", CreateParams{Amount: 500, Currency: "USD", Card: processor.CardCaptureTimeout})
	if _, err := s.Capture(ctx, "t1", in.ID, 0); !errors.Is(err, processor.ErrTimeout) {
		t.Fatalf("err = %v; want ErrTimeout", err)
	}
	got, _ := s.Get(ctx, "t1", in.ID)
	if got.Status != StatusAuthorized || got.AmountCaptured != 0 {
		t.Fatalf("after timeout: %+v", got)
	}
	if b := balances(t, s.DB, "t1"); b[AcctClearing] != 0 {
		t.Fatalf("ledger posted on failed capture: %v", b)
	}
}

// TestConcurrentRefundsNeverExceedCapture fires more refunds than the capture can cover.
func TestConcurrentRefundsNeverExceedCapture(t *testing.T) {
	s := newService(t, "t1")
	ctx := context.Background()
	in, _ := s.Create(ctx, "t1", CreateParams{Amount: 1000, Currency: "USD", Card: processor.CardApproved})
	if _, err := s.Capture(ctx, "t1", in.ID, 0); err != nil {
		t.Fatal(err)
	}
	const attempts = 20
	var wg sync.WaitGroup
	var mu sync.Mutex
	ok := 0
	for i := 0; i < attempts; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := s.Refund(ctx, "t1", in.ID, 300)
			if err == nil {
				mu.Lock()
				ok++
				mu.Unlock()
			} else if !errors.Is(err, ErrInvalidAmount) {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if ok != 3 {
		t.Fatalf("successful refunds = %d; want 3 (3*300 <= 1000 < 4*300)", ok)
	}
	got, _ := s.Get(ctx, "t1", in.ID)
	if got.AmountRefunded != 900 {
		t.Fatalf("amount_refunded = %d; want 900", got.AmountRefunded)
	}
	if b := balances(t, s.DB, "t1"); b[AcctClearing] != 100 || b[AcctRefunds] != 900 {
		t.Fatalf("balances = %v", b)
	}
}

func TestConcurrentCaptureOnlyOnce(t *testing.T) {
	s := newService(t, "t1")
	ctx := context.Background()
	in, _ := s.Create(ctx, "t1", CreateParams{Amount: 1000, Currency: "USD", Card: processor.CardApproved})
	var wg sync.WaitGroup
	var mu sync.Mutex
	ok := 0
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := s.Capture(ctx, "t1", in.ID, 0); err == nil {
				mu.Lock()
				ok++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if ok != 1 {
		t.Fatalf("successful captures = %d; want 1", ok)
	}
	if b := balances(t, s.DB, "t1"); b[AcctRevenue] != 1000 {
		t.Fatalf("revenue = %d; want 1000", b[AcctRevenue])
	}
}
