// Package processor defines the card processor interface and a deterministic mock.
package processor

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/HappyVirgo/ledgerline/internal/money"
)

// ErrTimeout means the processor did not answer; the outcome is unknown to the caller.
var ErrTimeout = errors.New("processor timeout")

// DeclineError is a definitive refusal from the processor.
type DeclineError struct{ Code string }

func (e *DeclineError) Error() string { return "card declined: " + e.Code }

// Processor is the boundary to an external card network.
type Processor interface {
	// Authorize places a hold for amount on the card and returns an authorization reference.
	Authorize(ctx context.Context, card string, amount int64, cur money.Currency) (authRef string, err error)
	// Capture settles up to the authorized amount and returns a capture reference.
	Capture(ctx context.Context, authRef string, amount int64) (captureRef string, err error)
	// Refund returns amount from a capture and returns a refund reference.
	Refund(ctx context.Context, captureRef string, amount int64) (refundRef string, err error)
}

// Test card numbers understood by Mock. Any other well-formed number is approved.
const (
	CardApproved          = "4242424242424242"
	CardDeclined          = "4000000000000002"
	CardInsufficientFunds = "4000000000009995"
	CardTimeout           = "4000000000000119"
	// CardCaptureTimeout authorizes but times out when captured.
	CardCaptureTimeout = "4000000000000259"
)

// Mock is an in-memory Processor whose outcomes depend only on the card number.
// References are "mock_<kind>_<instance>_<seq>", unique across process restarts.
type Mock struct {
	instance string
	seq      atomic.Int64
}

// NewMock returns a ready-to-use mock processor.
func NewMock() *Mock {
	b := make([]byte, 3)
	rand.Read(b)
	return &Mock{instance: hex.EncodeToString(b)}
}

func (m *Mock) ref(kind string) string {
	return fmt.Sprintf("mock_%s_%s_%06d", kind, m.instance, m.seq.Add(1))
}

// Authorize implements Processor.
func (m *Mock) Authorize(ctx context.Context, card string, amount int64, _ money.Currency) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if len(card) < 12 || len(card) > 19 {
		return "", &DeclineError{Code: "invalid_number"}
	}
	switch card {
	case CardDeclined:
		return "", &DeclineError{Code: "card_declined"}
	case CardInsufficientFunds:
		return "", &DeclineError{Code: "insufficient_funds"}
	case CardTimeout:
		return "", ErrTimeout
	case CardCaptureTimeout:
		return captureTimeoutPrefix + m.ref("auth"), nil
	}
	return m.ref("auth"), nil
}

// Capture implements Processor.
func (m *Mock) Capture(ctx context.Context, authRef string, amount int64) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if strings.HasPrefix(authRef, captureTimeoutPrefix) {
		return "", ErrTimeout
	}
	return m.ref("cap"), nil
}

// Refund implements Processor.
func (m *Mock) Refund(ctx context.Context, captureRef string, amount int64) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	return m.ref("ref"), nil
}

// captureTimeoutPrefix marks authorizations made with CardCaptureTimeout.
const captureTimeoutPrefix = "ct_"
