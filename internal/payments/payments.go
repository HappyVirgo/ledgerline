// Package payments implements payment intents (authorize, capture, refund) on top of
// the ledger, the card processor and the webhook outbox.
package payments

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/HappyVirgo/ledgerline/internal/ledger"
	"github.com/HappyVirgo/ledgerline/internal/money"
	"github.com/HappyVirgo/ledgerline/internal/processor"
	"github.com/HappyVirgo/ledgerline/internal/store"
	"github.com/HappyVirgo/ledgerline/internal/webhook"
)

// Intent statuses.
const (
	StatusAuthorized        = "authorized"
	StatusDeclined          = "declined"
	StatusFailed            = "failed"
	StatusCapturing         = "capturing"
	StatusCaptured          = "captured"
	StatusPartiallyRefunded = "partially_refunded"
	StatusRefunded          = "refunded"
)

// Ledger account codes used for every tenant and currency.
const (
	AcctClearing = "processor_clearing" // asset: money owed to the tenant by the processor
	AcctRevenue  = "sales_revenue"      // revenue
	AcctRefunds  = "refunds"            // expense (contra-revenue)
)

// Service errors; the HTTP layer maps them to status codes.
var (
	ErrNotFound      = errors.New("payment intent not found")
	ErrInvalidState  = errors.New("payment intent is not in a valid state for this operation")
	ErrInvalidAmount = errors.New("invalid amount")
)

// Intent is the public representation of a payment intent.
type Intent struct {
	ID             string         `json:"id"`
	Amount         int64          `json:"amount"`
	Currency       money.Currency `json:"currency"`
	Status         string         `json:"status"`
	CardLast4      string         `json:"card_last4"`
	AmountCaptured int64          `json:"amount_captured"`
	AmountRefunded int64          `json:"amount_refunded"`
	CaptureRef     string         `json:"capture_ref,omitempty"`
	FailureCode    string         `json:"failure_code,omitempty"`
	Created        int64          `json:"created"`
}

// Service coordinates payment state. Processor calls happen outside database
// transactions; state is claimed before and finalised after each call.
type Service struct {
	DB   *sql.DB
	Proc processor.Processor
}

// CreateParams are the inputs for a new payment intent.
type CreateParams struct {
	Amount   int64  `json:"amount"`
	Currency string `json:"currency"`
	Card     string `json:"card_number"`
}

// Create authorizes the card and stores the intent. Declines and processor timeouts are
// not Go errors: they produce an intent with status declined/failed and a failure code.
func (s *Service) Create(ctx context.Context, tenantID string, p CreateParams) (Intent, error) {
	cur, err := money.ParseCurrency(p.Currency)
	if err != nil {
		return Intent{}, err
	}
	if p.Amount <= 0 {
		return Intent{}, fmt.Errorf("%w: must be positive", ErrInvalidAmount)
	}
	in := Intent{ID: store.NewID("pi_"), Amount: p.Amount, Currency: cur, Status: StatusAuthorized,
		CardLast4: last4(p.Card), Created: time.Now().Unix()}

	authRef, err := s.Proc.Authorize(ctx, p.Card, p.Amount, cur)
	var decline *processor.DeclineError
	switch {
	case errors.As(err, &decline):
		in.Status, in.FailureCode = StatusDeclined, decline.Code
	case errors.Is(err, processor.ErrTimeout):
		in.Status, in.FailureCode = StatusFailed, "processor_timeout"
	case err != nil:
		return Intent{}, err
	}
	err = store.InTx(ctx, s.DB, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO payment_intents (id, tenant_id, amount, currency, status, card_last4, auth_ref, failure_code, created_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			in.ID, tenantID, in.Amount, string(cur), in.Status, in.CardLast4, authRef, in.FailureCode, in.Created); err != nil {
			return err
		}
		return webhook.Enqueue(ctx, tx, tenantID, "payment_intent."+in.Status, in)
	})
	return in, err
}

// Get returns the tenant's intent by id.
func (s *Service) Get(ctx context.Context, tenantID, id string) (Intent, error) {
	in, _, err := get(ctx, s.DB, tenantID, id)
	return in, err
}

func get(ctx context.Context, q store.DBTX, tenantID, id string) (Intent, string, error) {
	var in Intent
	var authRef string
	err := q.QueryRowContext(ctx, `
		SELECT id, amount, currency, status, card_last4, auth_ref, capture_ref, amount_captured, amount_refunded, failure_code, created_at
		FROM payment_intents WHERE id = ? AND tenant_id = ?`, id, tenantID).
		Scan(&in.ID, &in.Amount, &in.Currency, &in.Status, &in.CardLast4, &authRef, &in.CaptureRef,
			&in.AmountCaptured, &in.AmountRefunded, &in.FailureCode, &in.Created)
	if errors.Is(err, sql.ErrNoRows) {
		return Intent{}, "", ErrNotFound
	}
	return in, authRef, err
}

// Capture settles an authorized intent. amount 0 means the full authorized amount.
func (s *Service) Capture(ctx context.Context, tenantID, id string, amount int64) (Intent, error) {
	var authRef string
	// Claim: authorized -> capturing, so concurrent captures cannot both reach the processor.
	err := store.InTx(ctx, s.DB, func(tx *sql.Tx) error {
		in, ref, err := get(ctx, tx, tenantID, id)
		if err != nil {
			return err
		}
		if in.Status != StatusAuthorized {
			return fmt.Errorf("%w: status is %s", ErrInvalidState, in.Status)
		}
		if amount == 0 {
			amount = in.Amount
		}
		if amount < 0 || amount > in.Amount {
			return fmt.Errorf("%w: capture must be between 1 and %d", ErrInvalidAmount, in.Amount)
		}
		authRef = ref
		_, err = tx.ExecContext(ctx, `UPDATE payment_intents SET status = ? WHERE id = ?`, StatusCapturing, id)
		return err
	})
	if err != nil {
		return Intent{}, err
	}

	capRef, procErr := s.Proc.Capture(ctx, authRef, amount)
	var out Intent
	err = store.InTx(ctx, s.DB, func(tx *sql.Tx) error {
		if procErr != nil {
			_, err := tx.ExecContext(ctx, `UPDATE payment_intents SET status = ? WHERE id = ?`, StatusAuthorized, id)
			return err
		}
		if _, err := tx.ExecContext(ctx,
			`UPDATE payment_intents SET status = ?, amount_captured = ?, capture_ref = ? WHERE id = ?`,
			StatusCaptured, amount, capRef, id); err != nil {
			return err
		}
		in, _, err := get(ctx, tx, tenantID, id)
		if err != nil {
			return err
		}
		if err := post(ctx, tx, tenantID, in.Currency, "capture "+id, AcctClearing, ledger.Asset, AcctRevenue, ledger.Revenue, amount); err != nil {
			return err
		}
		out = in
		return webhook.Enqueue(ctx, tx, tenantID, "payment_intent.captured", in)
	})
	if err != nil {
		return Intent{}, err
	}
	if procErr != nil {
		return Intent{}, procErr
	}
	return out, nil
}

// Refund returns amount (0 = everything not yet refunded) of a captured intent.
// The amount is reserved atomically before calling the processor, so concurrent
// refunds can never exceed the captured amount.
func (s *Service) Refund(ctx context.Context, tenantID, id string, amount int64) (Intent, error) {
	var capRef string
	err := store.InTx(ctx, s.DB, func(tx *sql.Tx) error {
		in, _, err := get(ctx, tx, tenantID, id)
		if err != nil {
			return err
		}
		switch in.Status {
		case StatusCaptured, StatusPartiallyRefunded, StatusRefunded:
		default:
			return fmt.Errorf("%w: status is %s", ErrInvalidState, in.Status)
		}
		remaining := in.AmountCaptured - in.AmountRefunded
		if amount == 0 {
			amount = remaining
		}
		if amount <= 0 || amount > remaining {
			return fmt.Errorf("%w: refundable amount is %d", ErrInvalidAmount, remaining)
		}
		capRef = in.CaptureRef
		_, err = tx.ExecContext(ctx,
			`UPDATE payment_intents SET amount_refunded = amount_refunded + ? WHERE id = ?`, amount, id)
		return err
	})
	if err != nil {
		return Intent{}, err
	}

	refundRef, procErr := s.Proc.Refund(ctx, capRef, amount)
	var out Intent
	err = store.InTx(ctx, s.DB, func(tx *sql.Tx) error {
		if procErr != nil { // release the reservation
			_, err := tx.ExecContext(ctx,
				`UPDATE payment_intents SET amount_refunded = amount_refunded - ? WHERE id = ?`, amount, id)
			return err
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE payment_intents SET status = CASE WHEN amount_refunded = amount_captured THEN ? ELSE ? END
			WHERE id = ?`, StatusRefunded, StatusPartiallyRefunded, id); err != nil {
			return err
		}
		in, _, err := get(ctx, tx, tenantID, id)
		if err != nil {
			return err
		}
		if err := post(ctx, tx, tenantID, in.Currency, "refund "+id+" "+refundRef, AcctRefunds, ledger.Expense, AcctClearing, ledger.Asset, amount); err != nil {
			return err
		}
		out = in
		return webhook.Enqueue(ctx, tx, tenantID, "payment_intent.refunded", map[string]any{
			"payment_intent": in, "refund_ref": refundRef, "amount": amount})
	})
	if err != nil {
		return Intent{}, err
	}
	if procErr != nil {
		return Intent{}, procErr
	}
	return out, nil
}

// Captured lists the tenant's intents that have been captured, for reconciliation.
func (s *Service) Captured(ctx context.Context, tenantID string) ([]Intent, error) {
	rows, err := s.DB.QueryContext(ctx, `
		SELECT id, amount_captured, currency, capture_ref FROM payment_intents
		WHERE tenant_id = ? AND capture_ref != '' ORDER BY created_at, id`, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Intent
	for rows.Next() {
		var in Intent
		if err := rows.Scan(&in.ID, &in.AmountCaptured, &in.Currency, &in.CaptureRef); err != nil {
			return nil, err
		}
		out = append(out, in)
	}
	return out, rows.Err()
}

// post writes a two-line entry debiting one account and crediting another.
func post(ctx context.Context, tx *sql.Tx, tenantID string, cur money.Currency, memo string,
	debitCode string, debitType ledger.AccountType, creditCode string, creditType ledger.AccountType, amount int64) error {
	dr, err := ledger.EnsureAccount(ctx, tx, tenantID, debitCode, debitType, cur)
	if err != nil {
		return err
	}
	cr, err := ledger.EnsureAccount(ctx, tx, tenantID, creditCode, creditType, cur)
	if err != nil {
		return err
	}
	_, err = ledger.Post(ctx, tx, ledger.Entry{TenantID: tenantID, Currency: cur, Memo: memo, Lines: []ledger.Line{
		{AccountID: dr, Side: ledger.Debit, Amount: amount},
		{AccountID: cr, Side: ledger.Credit, Amount: amount},
	}})
	return err
}

// last4 keeps only the last four digits; the full card number is never stored.
func last4(card string) string {
	if len(card) < 4 {
		return card
	}
	return card[len(card)-4:]
}
