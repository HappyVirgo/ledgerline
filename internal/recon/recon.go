// Package recon reconciles a processor settlement CSV against captured payments.
package recon

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"

	"github.com/HappyVirgo/ledgerline/internal/money"
)

// Row is one settlement line reported by the processor.
type Row struct {
	Ref      string         `json:"processor_ref"`
	Amount   int64          `json:"amount"`
	Currency money.Currency `json:"currency"`
}

// Captured is one capture recorded by ledgerline.
type Captured struct {
	PaymentID string         `json:"payment_id"`
	Ref       string         `json:"processor_ref"`
	Amount    int64          `json:"amount"`
	Currency  money.Currency `json:"currency"`
}

// Mismatch is a capture the processor settled for a different amount or currency.
type Mismatch struct {
	PaymentID        string         `json:"payment_id"`
	Ref              string         `json:"processor_ref"`
	ExpectedAmount   int64          `json:"expected_amount"`
	ExpectedCurrency money.Currency `json:"expected_currency"`
	SettledAmount    int64          `json:"settled_amount"`
	SettledCurrency  money.Currency `json:"settled_currency"`
}

// Report summarises a reconciliation run.
type Report struct {
	Matched int `json:"matched"`
	// MissingFromSettlement are captures the processor did not report.
	MissingFromSettlement []Captured `json:"missing_from_settlement"`
	// UnknownInSettlement are settlement rows with no matching capture (or duplicates).
	UnknownInSettlement []Row      `json:"unknown_in_settlement"`
	Mismatched          []Mismatch `json:"mismatched"`
}

// ErrBadCSV wraps settlement parse failures.
var ErrBadCSV = errors.New("invalid settlement csv")

// ParseCSV reads rows with header "processor_ref,amount,currency"; amount is in minor units.
func ParseCSV(r io.Reader) ([]Row, error) {
	cr := csv.NewReader(r)
	cr.FieldsPerRecord = 3
	header, err := cr.Read()
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrBadCSV, err)
	}
	if strings.Join(header, ",") != "processor_ref,amount,currency" {
		return nil, fmt.Errorf("%w: unexpected header %q", ErrBadCSV, header)
	}
	var rows []Row
	for line := 2; ; line++ {
		rec, err := cr.Read()
		if errors.Is(err, io.EOF) {
			return rows, nil
		}
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrBadCSV, err)
		}
		amt, err := strconv.ParseInt(strings.TrimSpace(rec[1]), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("%w: line %d: amount %q is not an integer", ErrBadCSV, line, rec[1])
		}
		cur, err := money.ParseCurrency(rec[2])
		if err != nil {
			return nil, fmt.Errorf("%w: line %d: %v", ErrBadCSV, line, err)
		}
		rows = append(rows, Row{Ref: strings.TrimSpace(rec[0]), Amount: amt, Currency: cur})
	}
}

// Reconcile matches settlement rows to captures by processor reference.
// Output slices are sorted by reference for stable reports.
func Reconcile(settled []Row, captured []Captured) Report {
	byRef := make(map[string]Captured, len(captured))
	for _, c := range captured {
		byRef[c.Ref] = c
	}
	seen := make(map[string]bool, len(settled))
	rep := Report{MissingFromSettlement: []Captured{}, UnknownInSettlement: []Row{}, Mismatched: []Mismatch{}}
	for _, row := range settled {
		c, ok := byRef[row.Ref]
		if !ok || seen[row.Ref] {
			rep.UnknownInSettlement = append(rep.UnknownInSettlement, row)
			continue
		}
		seen[row.Ref] = true
		if c.Amount != row.Amount || c.Currency != row.Currency {
			rep.Mismatched = append(rep.Mismatched, Mismatch{PaymentID: c.PaymentID, Ref: c.Ref,
				ExpectedAmount: c.Amount, ExpectedCurrency: c.Currency, SettledAmount: row.Amount, SettledCurrency: row.Currency})
			continue
		}
		rep.Matched++
	}
	for _, c := range captured {
		if !seen[c.Ref] {
			rep.MissingFromSettlement = append(rep.MissingFromSettlement, c)
		}
	}
	sort.Slice(rep.MissingFromSettlement, func(i, j int) bool {
		return rep.MissingFromSettlement[i].Ref < rep.MissingFromSettlement[j].Ref
	})
	sort.SliceStable(rep.UnknownInSettlement, func(i, j int) bool {
		return rep.UnknownInSettlement[i].Ref < rep.UnknownInSettlement[j].Ref
	})
	sort.Slice(rep.Mismatched, func(i, j int) bool { return rep.Mismatched[i].Ref < rep.Mismatched[j].Ref })
	return rep
}
