// Package ledger implements a tenant-scoped double-entry ledger.
//
// Every journal entry must balance (sum of debits == sum of credits) in a single
// currency. Account balances are derived from journal lines; a cached balance column
// is updated in the same transaction as the lines so it can never drift, and Verify
// recomputes balances from scratch to prove it.
package ledger

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/HappyVirgo/ledgerline/internal/money"
	"github.com/HappyVirgo/ledgerline/internal/store"
)

// AccountType classifies an account; it determines the account's normal balance side.
type AccountType string

// The four supported account types.
const (
	Asset     AccountType = "asset"
	Liability AccountType = "liability"
	Revenue   AccountType = "revenue"
	Expense   AccountType = "expense"
)

// Side is the debit or credit side of a journal line.
type Side string

// Debit and Credit sides.
const (
	Debit  Side = "D"
	Credit Side = "C"
)

// Validation errors returned by Post.
var (
	ErrUnbalanced      = errors.New("journal entry does not balance")
	ErrTooFewLines     = errors.New("journal entry needs at least two lines")
	ErrInvalidAmount   = errors.New("line amount must be positive")
	ErrInvalidSide     = errors.New("line side must be D or C")
	ErrAccountNotFound = errors.New("account not found for tenant")
	ErrCurrency        = errors.New("account currency does not match entry currency")
)

// Line is one debit or credit against an account, in minor units.
type Line struct {
	AccountID int64
	Side      Side
	Amount    int64
}

// Entry is a balanced set of lines posted atomically.
type Entry struct {
	TenantID string
	Currency money.Currency
	Memo     string
	Lines    []Line
}

// Account is a ledger account with its cached balance in its normal orientation
// (positive means the account holds value on its normal side).
type Account struct {
	ID       int64          `json:"id"`
	Code     string         `json:"code"`
	Type     AccountType    `json:"type"`
	Currency money.Currency `json:"currency"`
	Balance  int64          `json:"balance"`
}

// debitNormal reports whether debits increase the account's balance.
func (t AccountType) debitNormal() bool { return t == Asset || t == Expense }

// signedDelta converts a line into the change to an account's normal balance.
func signedDelta(t AccountType, s Side, amount int64) int64 {
	if (s == Debit) == t.debitNormal() {
		return amount
	}
	return -amount
}

// Validate checks the entry's structure without touching the database.
func (e Entry) Validate() error {
	if len(e.Lines) < 2 {
		return ErrTooFewLines
	}
	var debits, credits int64
	for _, l := range e.Lines {
		if l.Amount <= 0 {
			return ErrInvalidAmount
		}
		var err error
		switch l.Side {
		case Debit:
			debits, err = money.Add(debits, l.Amount)
		case Credit:
			credits, err = money.Add(credits, l.Amount)
		default:
			return ErrInvalidSide
		}
		if err != nil {
			return err
		}
	}
	if debits != credits {
		return fmt.Errorf("%w: debits %d != credits %d", ErrUnbalanced, debits, credits)
	}
	return nil
}

// EnsureAccount returns the id of the tenant's account (code, currency), creating it if absent.
func EnsureAccount(ctx context.Context, q store.DBTX, tenantID, code string, typ AccountType, cur money.Currency) (int64, error) {
	_, err := q.ExecContext(ctx,
		`INSERT INTO accounts (tenant_id, code, type, currency) VALUES (?, ?, ?, ?)
		 ON CONFLICT (tenant_id, code, currency) DO NOTHING`, tenantID, code, string(typ), string(cur))
	if err != nil {
		return 0, err
	}
	var id int64
	err = q.QueryRowContext(ctx,
		`SELECT id FROM accounts WHERE tenant_id = ? AND code = ? AND currency = ?`,
		tenantID, code, string(cur)).Scan(&id)
	return id, err
}

// Post validates and writes the entry inside tx, updating cached balances. The caller
// owns the transaction so ledger writes commit atomically with related state changes.
func Post(ctx context.Context, tx *sql.Tx, e Entry) (int64, error) {
	if err := e.Validate(); err != nil {
		return 0, err
	}
	res, err := tx.ExecContext(ctx,
		`INSERT INTO journal_entries (tenant_id, currency, memo, created_at) VALUES (?, ?, ?, ?)`,
		e.TenantID, string(e.Currency), e.Memo, time.Now().Unix())
	if err != nil {
		return 0, err
	}
	entryID, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}
	for _, l := range e.Lines {
		var typ AccountType
		var cur money.Currency
		err := tx.QueryRowContext(ctx,
			`SELECT type, currency FROM accounts WHERE id = ? AND tenant_id = ?`,
			l.AccountID, e.TenantID).Scan(&typ, &cur)
		if errors.Is(err, sql.ErrNoRows) {
			return 0, fmt.Errorf("%w: %d", ErrAccountNotFound, l.AccountID)
		} else if err != nil {
			return 0, err
		}
		if cur != e.Currency {
			return 0, fmt.Errorf("%w: account %d is %s", ErrCurrency, l.AccountID, cur)
		}
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO journal_lines (entry_id, account_id, side, amount) VALUES (?, ?, ?, ?)`,
			entryID, l.AccountID, string(l.Side), l.Amount); err != nil {
			return 0, err
		}
		if _, err := tx.ExecContext(ctx,
			`UPDATE accounts SET balance = balance + ? WHERE id = ?`,
			signedDelta(typ, l.Side, l.Amount), l.AccountID); err != nil {
			return 0, err
		}
	}
	return entryID, nil
}

// Accounts lists the tenant's accounts with cached balances.
func Accounts(ctx context.Context, q store.DBTX, tenantID string) ([]Account, error) {
	rows, err := q.QueryContext(ctx,
		`SELECT id, code, type, currency, balance FROM accounts WHERE tenant_id = ? ORDER BY code, currency`, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Account
	for rows.Next() {
		var a Account
		if err := rows.Scan(&a.ID, &a.Code, &a.Type, &a.Currency, &a.Balance); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// Verify recomputes every account balance for the tenant from journal lines and
// checks that it equals the cached balance, and that total debits equal total credits.
func Verify(ctx context.Context, q store.DBTX, tenantID string) error {
	rows, err := q.QueryContext(ctx, `
		SELECT a.id, a.type, a.balance,
		       COALESCE(SUM(CASE WHEN l.side = 'D' THEN l.amount ELSE 0 END), 0),
		       COALESCE(SUM(CASE WHEN l.side = 'C' THEN l.amount ELSE 0 END), 0)
		FROM accounts a LEFT JOIN journal_lines l ON l.account_id = a.id
		WHERE a.tenant_id = ? GROUP BY a.id`, tenantID)
	if err != nil {
		return err
	}
	defer rows.Close()
	var totalD, totalC int64
	for rows.Next() {
		var id, cached, d, c int64
		var typ AccountType
		if err := rows.Scan(&id, &typ, &cached, &d, &c); err != nil {
			return err
		}
		derived := d - c
		if !typ.debitNormal() {
			derived = c - d
		}
		if derived != cached {
			return fmt.Errorf("account %d: cached balance %d != derived %d", id, cached, derived)
		}
		totalD += d
		totalC += c
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if totalD != totalC {
		return fmt.Errorf("%w: tenant %s debits %d != credits %d", ErrUnbalanced, tenantID, totalD, totalC)
	}
	return nil
}
