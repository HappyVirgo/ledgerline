package money

import (
	"errors"
	"math"
	"testing"
)

func TestParseCurrency(t *testing.T) {
	tests := []struct {
		in      string
		want    Currency
		wantErr bool
	}{
		{"USD", "USD", false},
		{" eur ", "EUR", false},
		{"jpy", "JPY", false},
		{"XYZ", "", true},
		{"", "", true},
	}
	for _, tt := range tests {
		got, err := ParseCurrency(tt.in)
		if (err != nil) != tt.wantErr || got != tt.want {
			t.Errorf("ParseCurrency(%q) = %q, %v; want %q, err=%v", tt.in, got, err, tt.want, tt.wantErr)
		}
		if tt.wantErr && !errors.Is(err, ErrUnsupportedCurrency) {
			t.Errorf("ParseCurrency(%q) error %v is not ErrUnsupportedCurrency", tt.in, err)
		}
	}
}

func TestAdd(t *testing.T) {
	tests := []struct {
		a, b    int64
		want    int64
		wantErr bool
	}{
		{1, 2, 3, false},
		{-5, 3, -2, false},
		{math.MaxInt64, 1, 0, true},
		{math.MinInt64, -1, 0, true},
		{math.MaxInt64, -1, math.MaxInt64 - 1, false},
	}
	for _, tt := range tests {
		got, err := Add(tt.a, tt.b)
		if (err != nil) != tt.wantErr || got != tt.want {
			t.Errorf("Add(%d, %d) = %d, %v; want %d, err=%v", tt.a, tt.b, got, err, tt.want, tt.wantErr)
		}
	}
}

func TestFormat(t *testing.T) {
	tests := []struct {
		amount int64
		cur    Currency
		want   string
	}{
		{1050, "USD", "10.50 USD"},
		{5, "USD", "0.05 USD"},
		{-1050, "EUR", "-10.50 EUR"},
		{1200, "JPY", "1200 JPY"},
		{1234, "KWD", "1.234 KWD"},
		{math.MinInt64, "JPY", "-9223372036854775808 JPY"},
	}
	for _, tt := range tests {
		if got := Format(tt.amount, tt.cur); got != tt.want {
			t.Errorf("Format(%d, %s) = %q; want %q", tt.amount, tt.cur, got, tt.want)
		}
	}
}
