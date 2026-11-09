package ledger

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"testing"

	"github.com/HappyVirgo/ledgerline/internal/money"
	"github.com/HappyVirgo/ledgerline/internal/store"
)

func newDB(t testing.TB, tenants ...string) *sql.DB {
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
	return db
}

func mustAccount(t testing.TB, db *sql.DB, tenantID, code string, typ AccountType, cur money.Currency) int64 {
	t.Helper()
	id, err := EnsureAccount(context.Background(), db, tenantID, code, typ, cur)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func TestEntryValidate(t *testing.T) {
	tests := []struct {
		name  string
		lines []Line
		want  error
	}{
		{"balanced", []Line{{1, Debit, 100}, {2, Credit, 100}}, nil},
		{"split credit", []Line{{1, Debit, 100}, {2, Credit, 60}, {3, Credit, 40}}, nil},
		{"unbalanced", []Line{{1, Debit, 100}, {2, Credit, 99}}, ErrUnbalanced},
		{"one line", []Line{{1, Debit, 100}}, ErrTooFewLines},
		{"zero amount", []Line{{1, Debit, 0}, {2, Credit, 0}}, ErrInvalidAmount},
		{"negative amount", []Line{{1, Debit, -5}, {2, Credit, -5}}, ErrInvalidAmount},
		{"bad side", []Line{{1, "X", 5}, {2, Credit, 5}}, ErrInvalidSide},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := Entry{TenantID: "t", Currency: "USD", Lines: tt.lines}.Validate()
			if !errors.Is(err, tt.want) {
				t.Fatalf("Validate() = %v; want %v", err, tt.want)
			}
		})
	}
}

func post(t testing.TB, db *sql.DB, e Entry) error {
	t.Helper()
	return store.InTx(context.Background(), db, func(tx *sql.Tx) error {
		_, err := Post(context.Background(), tx, e)
		return err
	})
}

func TestPostUpdatesBalancesInNormalOrientation(t *testing.T) {
	db := newDB(t, "t1")
	cash := mustAccount(t, db, "t1", "cash", Asset, "USD")
	rev := mustAccount(t, db, "t1", "revenue", Revenue, "USD")
	refunds := mustAccount(t, db, "t1", "refunds", Expense, "USD")

	if err := post(t, db, Entry{TenantID: "t1", Currency: "USD", Memo: "sale",
		Lines: []Line{{cash, Debit, 1000}, {rev, Credit, 1000}}}); err != nil {
		t.Fatal(err)
	}
	if err := post(t, db, Entry{TenantID: "t1", Currency: "USD", Memo: "refund",
		Lines: []Line{{refunds, Debit, 250}, {cash, Credit, 250}}}); err != nil {
		t.Fatal(err)
	}
	accts, err := Accounts(context.Background(), db, "t1")
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]int64{"cash": 750, "revenue": 1000, "refunds": 250}
	for _, a := range accts {
		if a.Balance != want[a.Code] {
			t.Errorf("%s balance = %d; want %d", a.Code, a.Balance, want[a.Code])
		}
	}
	if err := Verify(context.Background(), db, "t1"); err != nil {
		t.Fatal(err)
	}
}

func TestPostRejectsCrossTenantAndCurrency(t *testing.T) {
	db := newDB(t, "t1", "t2")
	mine := mustAccount(t, db, "t1", "cash", Asset, "USD")
	theirs := mustAccount(t, db, "t2", "cash", Asset, "USD")
	eur := mustAccount(t, db, "t1", "rev", Revenue, "EUR")
	usdRev := mustAccount(t, db, "t1", "rev", Revenue, "USD")

	err := post(t, db, Entry{TenantID: "t1", Currency: "USD", Lines: []Line{{mine, Debit, 5}, {theirs, Credit, 5}}})
	if !errors.Is(err, ErrAccountNotFound) {
		t.Fatalf("cross-tenant post err = %v; want ErrAccountNotFound", err)
	}
	err = post(t, db, Entry{TenantID: "t1", Currency: "USD", Lines: []Line{{mine, Debit, 5}, {eur, Credit, 5}}})
	if !errors.Is(err, ErrCurrency) {
		t.Fatalf("currency mismatch err = %v; want ErrCurrency", err)
	}
	// Failed posts must leave no trace.
	if err := post(t, db, Entry{TenantID: "t1", Currency: "USD", Lines: []Line{{mine, Debit, 5}, {usdRev, Credit, 5}}}); err != nil {
		t.Fatal(err)
	}
	var lines int
	db.QueryRow(`SELECT COUNT(*) FROM journal_lines`).Scan(&lines)
	if lines != 2 {
		t.Fatalf("journal_lines = %d; want 2 (rolled-back posts must not persist)", lines)
	}
	for _, tid := range []string{"t1", "t2"} {
		if err := Verify(context.Background(), db, tid); err != nil {
			t.Fatal(err)
		}
	}
}

func TestVerifyDetectsDrift(t *testing.T) {
	db := newDB(t, "t1")
	a := mustAccount(t, db, "t1", "cash", Asset, "USD")
	b := mustAccount(t, db, "t1", "rev", Revenue, "USD")
	if err := post(t, db, Entry{TenantID: "t1", Currency: "USD", Lines: []Line{{a, Debit, 10}, {b, Credit, 10}}}); err != nil {
		t.Fatal(err)
	}
	db.Exec(`UPDATE accounts SET balance = balance + 1 WHERE id = ?`, a)
	if err := Verify(context.Background(), db, "t1"); err == nil {
		t.Fatal("Verify must detect a cached balance that disagrees with journal lines")
	}
}

// TestConcurrentPostingKeepsInvariants posts transfers from many goroutines and checks
// that cached balances equal derived balances and the books still balance.
func TestConcurrentPostingKeepsInvariants(t *testing.T) {
	db := newDB(t, "t1")
	ids := []int64{
		mustAccount(t, db, "t1", "a", Asset, "USD"),
		mustAccount(t, db, "t1", "b", Asset, "USD"),
		mustAccount(t, db, "t1", "c", Liability, "USD"),
	}
	const workers, perWorker = 16, 25
	var wg sync.WaitGroup
	errs := make(chan error, workers*perWorker)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				from, to := ids[(w+i)%3], ids[(w+i+1)%3]
				errs <- post(t, db, Entry{TenantID: "t1", Currency: "USD", Memo: "x",
					Lines: []Line{{to, Debit, int64(i + 1)}, {from, Credit, int64(i + 1)}}})
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := Verify(context.Background(), db, "t1"); err != nil {
		t.Fatal(err)
	}
	var entries int
	db.QueryRow(`SELECT COUNT(*) FROM journal_entries`).Scan(&entries)
	if entries != workers*perWorker {
		t.Fatalf("entries = %d; want %d", entries, workers*perWorker)
	}
}

func BenchmarkPost(b *testing.B) {
	db := newDB(b, "t1")
	cash := mustAccount(b, db, "t1", "cash", Asset, "USD")
	rev := mustAccount(b, db, "t1", "rev", Revenue, "USD")
	e := Entry{TenantID: "t1", Currency: "USD", Memo: "bench", Lines: []Line{{cash, Debit, 100}, {rev, Credit, 100}}}
	b.ReportAllocs()
	for b.Loop() {
		if err := post(b, db, e); err != nil {
			b.Fatal(err)
		}
	}
}
