// Package money provides integer minor-unit amounts tagged with ISO 4217 currencies.
// Floats are never used: every amount is an int64 count of the currency's minor unit.
package money

import (
	"errors"
	"fmt"
	"math"
	"strings"
)

// Currency is an upper-case ISO 4217 alphabetic code, e.g. "USD".
type Currency string

// exponents lists the supported currencies and their minor-unit exponent.
// It is intentionally a small subset of ISO 4217.
var exponents = map[Currency]int{
	"USD": 2, "EUR": 2, "GBP": 2, "CAD": 2, "AUD": 2, "CHF": 2,
	"JPY": 0, "KRW": 0, "VND": 0,
	"BHD": 3, "KWD": 3,
}

// ErrUnsupportedCurrency is returned for codes outside the supported subset.
var ErrUnsupportedCurrency = errors.New("unsupported currency")

// ErrOverflow is returned when an arithmetic operation would overflow int64.
var ErrOverflow = errors.New("amount overflow")

// ParseCurrency normalises and validates an ISO 4217 code.
func ParseCurrency(s string) (Currency, error) {
	c := Currency(strings.ToUpper(strings.TrimSpace(s)))
	if _, ok := exponents[c]; !ok {
		return "", fmt.Errorf("%w: %q", ErrUnsupportedCurrency, s)
	}
	return c, nil
}

// Exponent returns the number of minor-unit digits for c (2 for USD, 0 for JPY).
func (c Currency) Exponent() int { return exponents[c] }

// Add returns a+b, or ErrOverflow if the result does not fit in int64.
func Add(a, b int64) (int64, error) {
	if (b > 0 && a > math.MaxInt64-b) || (b < 0 && a < math.MinInt64-b) {
		return 0, ErrOverflow
	}
	return a + b, nil
}

// Format renders an amount in major units for display only, e.g. 1050 USD -> "10.50 USD".
func Format(amount int64, c Currency) string {
	exp := c.Exponent()
	sign := ""
	u := uint64(amount)
	if amount < 0 {
		sign = "-"
		u = uint64(-(amount + 1)) + 1
	}
	if exp == 0 {
		return fmt.Sprintf("%s%d %s", sign, u, c)
	}
	div := uint64(math.Pow10(exp))
	return fmt.Sprintf("%s%d.%0*d %s", sign, u/div, exp, u%div, c)
}
