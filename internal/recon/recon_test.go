package recon

import (
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestParseCSV(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		want    []Row
		wantErr bool
	}{
		{"ok", "processor_ref,amount,currency\ncap_1,1000,usd\ncap_2, 5 ,JPY\n",
			[]Row{{"cap_1", 1000, "USD"}, {"cap_2", 5, "JPY"}}, false},
		{"header only", "processor_ref,amount,currency\n", nil, false},
		{"bad header", "ref,amount,currency\n", nil, true},
		{"float amount", "processor_ref,amount,currency\ncap_1,10.50,USD\n", nil, true},
		{"bad currency", "processor_ref,amount,currency\ncap_1,10,ZZZ\n", nil, true},
		{"wrong field count", "processor_ref,amount,currency\ncap_1,10\n", nil, true},
		{"empty", "", nil, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseCSV(strings.NewReader(tt.in))
			if tt.wantErr {
				if !errors.Is(err, ErrBadCSV) {
					t.Fatalf("err = %v; want ErrBadCSV", err)
				}
				return
			}
			if err != nil || !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("got %v, %v; want %v", got, err, tt.want)
			}
		})
	}
}

func TestReconcile(t *testing.T) {
	captured := []Captured{
		{"pi_a", "cap_a", 1000, "USD"},
		{"pi_b", "cap_b", 500, "USD"},
		{"pi_c", "cap_c", 700, "EUR"},
		{"pi_d", "cap_d", 300, "USD"},
	}
	settled := []Row{
		{"cap_a", 1000, "USD"}, // matched
		{"cap_b", 450, "USD"},  // amount mismatch
		{"cap_c", 700, "USD"},  // currency mismatch
		{"cap_x", 99, "USD"},   // unknown
		{"cap_a", 1000, "USD"}, // duplicate
	}
	rep := Reconcile(settled, captured)
	if rep.Matched != 1 {
		t.Errorf("matched = %d; want 1", rep.Matched)
	}
	if len(rep.Mismatched) != 2 || rep.Mismatched[0].Ref != "cap_b" || rep.Mismatched[0].SettledAmount != 450 ||
		rep.Mismatched[1].SettledCurrency != "USD" {
		t.Errorf("mismatched = %+v", rep.Mismatched)
	}
	if len(rep.MissingFromSettlement) != 1 || rep.MissingFromSettlement[0].PaymentID != "pi_d" {
		t.Errorf("missing = %+v; want pi_d", rep.MissingFromSettlement)
	}
	if len(rep.UnknownInSettlement) != 2 || rep.UnknownInSettlement[0].Ref != "cap_a" || rep.UnknownInSettlement[1].Ref != "cap_x" {
		t.Errorf("unknown = %+v; want duplicate cap_a and cap_x", rep.UnknownInSettlement)
	}
}

func TestReconcileEmpty(t *testing.T) {
	rep := Reconcile(nil, nil)
	if rep.Matched != 0 || len(rep.MissingFromSettlement)+len(rep.UnknownInSettlement)+len(rep.Mismatched) != 0 {
		t.Fatalf("empty reconcile = %+v", rep)
	}
}
