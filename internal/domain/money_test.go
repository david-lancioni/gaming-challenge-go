package domain

import (
	"encoding/json"
	"errors"
	"math"
	"testing"
)

func mustMoney(t testing.TB, amount, currency string) Money {
	t.Helper()
	m, err := NewMoney(amount, currency)
	if err != nil {
		t.Fatalf("NewMoney(%q, %q): %v", amount, currency, err)
	}
	return m
}

func TestNewMoneyAccepts(t *testing.T) {
	tests := []struct {
		in    string
		minor int64
	}{
		{"0.00", 0},
		{"0.01", 1},
		{"0.99", 99},
		{"1.00", 100},
		{"25.00", 2500},
		{"1000.50", 100050},
		{"92233720368547758.07", math.MaxInt64},
	}
	for _, tc := range tests {
		m, err := NewMoney(tc.in, "BRL")
		if err != nil {
			t.Errorf("NewMoney(%q): unexpected error %v", tc.in, err)
			continue
		}
		if m.Minor() != tc.minor {
			t.Errorf("NewMoney(%q).Minor() = %d, want %d", tc.in, m.Minor(), tc.minor)
		}
		if m.Amount() != tc.in {
			t.Errorf("round trip of %q gave %q", tc.in, m.Amount())
		}
	}
}

func TestNewMoneyRejects(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want error
	}{
		{"empty", "", ErrInvalidMoney},
		{"NaN", "NaN", ErrInvalidMoney},
		{"Infinity", "Infinity", ErrInvalidMoney},
		{"negative infinity", "-Infinity", ErrInvalidMoney},
		{"scientific", "1e2", ErrInvalidMoney},
		{"scientific with dot", "1.0e2", ErrInvalidMoney},
		{"no decimals", "25", ErrInvalidMoney},
		{"one decimal", "25.5", ErrInvalidMoney},
		{"three decimals", "25.001", ErrScaleExceeded},
		{"three decimals zero", "25.000", ErrScaleExceeded},
		{"no fraction digits", "25.", ErrInvalidMoney},
		{"no integer digits", ".50", ErrInvalidMoney},
		{"plus sign", "+25.00", ErrInvalidMoney},
		{"negative", "-25.00", ErrNegativeAmount},
		{"negative zero", "-0.00", ErrNegativeAmount},
		{"spaces", " 25.00", ErrInvalidMoney},
		{"trailing space", "25.00 ", ErrInvalidMoney},
		{"comma", "25,00", ErrInvalidMoney},
		{"thousand separator", "1,000.00", ErrInvalidMoney},
		{"leading zeros", "025.00", ErrInvalidMoney},
		{"hex", "0x10.00", ErrInvalidMoney},
		{"unicode digit", "２５.00", ErrInvalidMoney},
		{"overflow units", "92233720368547759.00", ErrMoneyOverflow},
		{"overflow cents", "92233720368547758.08", ErrMoneyOverflow},
		{"huge", "99999999999999999999999999.00", ErrMoneyOverflow},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewMoney(tc.in, "BRL")
			if !errors.Is(err, tc.want) {
				t.Fatalf("NewMoney(%q) error = %v, want %v", tc.in, err, tc.want)
			}
		})
	}
}

func TestNewMoneyCurrency(t *testing.T) {
	for _, c := range []string{"", "brl", "BR", "BRLL", "XXX", "JPY", "BHD", "123"} {
		if _, err := NewMoney("1.00", c); !errors.Is(err, ErrUnsupportedCurrency) {
			t.Errorf("currency %q: error = %v, want ErrUnsupportedCurrency", c, err)
		}
	}
	if _, err := NewMoney("1.00", "USD"); err != nil {
		t.Errorf("USD should be supported: %v", err)
	}
}

func TestMoneyZeroValueRejected(t *testing.T) {
	var z Money
	if z.IsValid() {
		t.Fatal("zero value must not be valid")
	}
	ok := mustMoney(t, "1.00", "BRL")
	if _, err := z.Add(ok); !errors.Is(err, ErrInvalidMoney) {
		t.Errorf("Add on uninitialized: %v", err)
	}
	if _, err := ok.Sub(z); !errors.Is(err, ErrInvalidMoney) {
		t.Errorf("Sub with uninitialized: %v", err)
	}
	if _, err := z.Neg(); !errors.Is(err, ErrInvalidMoney) {
		t.Errorf("Neg on uninitialized: %v", err)
	}
	if _, err := z.Cmp(ok); !errors.Is(err, ErrInvalidMoney) {
		t.Errorf("Cmp on uninitialized: %v", err)
	}
	if _, err := json.Marshal(z); err == nil {
		t.Error("marshalling an uninitialized Money must fail")
	}
}

func TestMoneyArithmetic(t *testing.T) {
	a, b := mustMoney(t, "10.50", "BRL"), mustMoney(t, "0.75", "BRL")

	sum, err := a.Add(b)
	if err != nil || sum.Amount() != "11.25" {
		t.Fatalf("Add = %v, %v", sum, err)
	}
	diff, err := b.Sub(a)
	if err != nil || diff.Amount() != "-9.75" || !diff.IsNegative() {
		t.Fatalf("Sub = %v, %v", diff, err)
	}
	neg, err := a.Neg()
	if err != nil || neg.Amount() != "-10.50" {
		t.Fatalf("Neg = %v, %v", neg, err)
	}
	if c, _ := a.Cmp(b); c != 1 {
		t.Errorf("Cmp(a,b) = %d", c)
	}
	if c, _ := b.Cmp(a); c != -1 {
		t.Errorf("Cmp(b,a) = %d", c)
	}
	if c, _ := a.Cmp(a); c != 0 {
		t.Errorf("Cmp(a,a) = %d", c)
	}
	s, _ := mustMoney(t, "0.10", "BRL").Add(mustMoney(t, "0.20", "BRL"))
	if s.Amount() != "0.30" {
		t.Errorf("0.10+0.20 = %s", s.Amount())
	}
	if z, _ := Zero("BRL"); !z.IsZero() || z.IsPositive() || z.IsNegative() {
		t.Error("Zero predicates")
	}
}

func TestMoneyCurrencyMismatch(t *testing.T) {
	brl, usd := mustMoney(t, "1.00", "BRL"), mustMoney(t, "1.00", "USD")
	if _, err := brl.Add(usd); !errors.Is(err, ErrCurrencyMismatch) {
		t.Errorf("Add: %v", err)
	}
	if _, err := brl.Sub(usd); !errors.Is(err, ErrCurrencyMismatch) {
		t.Errorf("Sub: %v", err)
	}
	if _, err := brl.Cmp(usd); !errors.Is(err, ErrCurrencyMismatch) {
		t.Errorf("Cmp: %v", err)
	}
	if brl.Equal(usd) {
		t.Error("same amount in different currencies must not be equal")
	}
}

func TestMoneyOverflow(t *testing.T) {
	max, _ := MoneyFromMinor(math.MaxInt64, "BRL")
	min, _ := MoneyFromMinor(math.MinInt64, "BRL")
	one := mustMoney(t, "0.01", "BRL")
	negOne, _ := one.Neg()

	if _, err := max.Add(one); !errors.Is(err, ErrMoneyOverflow) {
		t.Errorf("max+1: %v", err)
	}
	if _, err := min.Add(negOne); !errors.Is(err, ErrMoneyOverflow) {
		t.Errorf("min-1 via Add: %v", err)
	}
	if _, err := min.Sub(one); !errors.Is(err, ErrMoneyOverflow) {
		t.Errorf("min-1: %v", err)
	}
	if _, err := max.Sub(negOne); !errors.Is(err, ErrMoneyOverflow) {
		t.Errorf("max-(-1): %v", err)
	}
	if _, err := min.Neg(); !errors.Is(err, ErrMoneyOverflow) {
		t.Errorf("-min: %v", err)
	}
	if got, err := max.Add(negOne); err != nil || got.Minor() != math.MaxInt64-1 {
		t.Errorf("max-1 via Add = %v, %v", got, err)
	}
	if min.Amount() != "-92233720368547758.08" {
		t.Errorf("min amount rendering = %s", min.Amount())
	}
}

func TestMoneyJSON(t *testing.T) {
	m := mustMoney(t, "25.00", "BRL")
	b, err := json.Marshal(m)
	if err != nil || string(b) != `{"amount":"25.00","currency":"BRL"}` {
		t.Fatalf("Marshal = %s, %v", b, err)
	}
	var back Money
	if err := json.Unmarshal(b, &back); err != nil || !back.Equal(m) {
		t.Fatalf("Unmarshal = %v, %v", back, err)
	}

	bad := []string{
		`{"amount":25.00,"currency":"BRL"}`,
		`{"amount":"25.00"}`,
		`{"amount":"25.00","currency":"BRL","x":1}`,
		`{"amount":"-1.00","currency":"BRL"}`,
		`{"amount":"1e2","currency":"BRL"}`,
		`"25.00"`,
		`[]`,
	}
	for _, in := range bad {
		var m Money
		if err := json.Unmarshal([]byte(in), &m); err == nil {
			t.Errorf("Unmarshal(%s) should fail, got %v", in, m)
		}
	}
	var n Money
	if err := json.Unmarshal([]byte(`null`), &n); err != nil || n.IsValid() {
		t.Errorf("null must leave the value uninitialized: %v %v", n, err)
	}
}

func FuzzNewMoney(f *testing.F) {
	for _, s := range []string{"0.00", "25.00", "-1.00", "1e5", "NaN", "92233720368547758.07", "٣.٠٠"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		m, err := NewMoney(s, "BRL")
		if err != nil {
			return
		}
		if m.Amount() != s || m.IsNegative() {
			t.Fatalf("accepted %q but rendered %q", s, m.Amount())
		}
	})
}
