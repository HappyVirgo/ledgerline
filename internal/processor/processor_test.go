package processor

import (
	"context"
	"errors"
	"testing"
)

func TestMockAuthorize(t *testing.T) {
	tests := []struct {
		card        string
		wantDecline string
		wantTimeout bool
	}{
		{CardApproved, "", false},
		{"5555555555554444", "", false},
		{CardDeclined, "card_declined", false},
		{CardInsufficientFunds, "insufficient_funds", false},
		{CardTimeout, "", true},
		{"123", "invalid_number", false},
	}
	m := NewMock()
	for _, tt := range tests {
		ref, err := m.Authorize(context.Background(), tt.card, 100, "USD")
		var d *DeclineError
		switch {
		case tt.wantDecline != "":
			if !errors.As(err, &d) || d.Code != tt.wantDecline {
				t.Errorf("card %s: got %v, want decline %s", tt.card, err, tt.wantDecline)
			}
		case tt.wantTimeout:
			if !errors.Is(err, ErrTimeout) {
				t.Errorf("card %s: got %v, want timeout", tt.card, err)
			}
		default:
			if err != nil || ref == "" {
				t.Errorf("card %s: got ref=%q err=%v, want approval", tt.card, ref, err)
			}
		}
	}
}

func TestMockCaptureTimeoutCard(t *testing.T) {
	m := NewMock()
	ctx := context.Background()
	ref, err := m.Authorize(ctx, CardCaptureTimeout, 100, "USD")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.Capture(ctx, ref, 100); !errors.Is(err, ErrTimeout) {
		t.Fatalf("capture: got %v, want timeout", err)
	}
	ok, _ := m.Authorize(ctx, CardApproved, 100, "USD")
	if _, err := m.Capture(ctx, ok, 100); err != nil {
		t.Fatalf("capture approved card: %v", err)
	}
}
