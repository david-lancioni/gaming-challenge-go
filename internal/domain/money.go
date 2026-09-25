package domain

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
)

// Money é um value object imutável: valor exato em unidades mínimas (centavos) de
// uma única moeda. Representação int64 com escala fixa de 2 casas; o maior módulo
// é 92233720368547758.07. Toda construção e operação verifica overflow, e nenhum
// valor passa por float32/float64.
//
// O zero value de Money é considerado "não inicializado" e rejeitado por todas as
// operações; valores válidos vêm de NewMoney, MoneyFromMinor ou Zero.
type Money struct {
	minor    int64
	currency Currency
}

const Scale = 2

const minorPerUnit int64 = 100

type Currency string

var supportedCurrencies = map[Currency]struct{}{
	"AED": {}, "ARS": {}, "AUD": {}, "BRL": {}, "CAD": {}, "CHF": {}, "CNY": {},
	"COP": {}, "CZK": {}, "DKK": {}, "EGP": {}, "EUR": {}, "GBP": {}, "HKD": {},
	"HUF": {}, "ILS": {}, "INR": {}, "KES": {}, "MXN": {}, "MYR": {}, "NGN": {},
	"NOK": {}, "NZD": {}, "PEN": {}, "PHP": {}, "PLN": {}, "RON": {}, "SAR": {},
	"SEK": {}, "SGD": {}, "THB": {}, "TRY": {}, "USD": {}, "UYU": {}, "ZAR": {},
}

func ParseCurrency(code string) (Currency, error) {
	c := Currency(code)
	if _, ok := supportedCurrencies[c]; !ok {
		return "", fmt.Errorf("%w: %q", ErrUnsupportedCurrency, code)
	}
	return c, nil
}

func (c Currency) String() string { return string(c) }

// NewMoney interpreta um valor externo não negativo, como "25.00". Gramática aceita:
// `0|[1-9][0-9]*` "." `[0-9]{2}`. Todo o resto é rejeitado sem arredondar nem
// normalizar (vazio, NaN, Infinity, notação científica, sinal, escala diferente de
// 2, zeros à esquerda), então o hash de idempotência nunca depende de normalização.
func NewMoney(amount, currency string) (Money, error) {
	cur, err := ParseCurrency(currency)
	if err != nil {
		return Money{}, err
	}
	minor, err := parseMinor(amount)
	if err != nil {
		return Money{}, err
	}
	return Money{minor: minor, currency: cur}, nil
}

func MoneyFromMinor(minor int64, currency Currency) (Money, error) {
	if _, ok := supportedCurrencies[currency]; !ok {
		return Money{}, fmt.Errorf("%w: %q", ErrUnsupportedCurrency, string(currency))
	}
	return Money{minor: minor, currency: currency}, nil
}

func Zero(currency Currency) (Money, error) { return MoneyFromMinor(0, currency) }

func parseMinor(s string) (int64, error) {
	if s == "" {
		return 0, fmt.Errorf("%w: empty amount", ErrInvalidMoney)
	}
	body := s
	negative := false
	if body[0] == '-' {
		negative = true
		body = body[1:]
	}
	dot := strings.IndexByte(body, '.')
	if dot < 0 {
		return 0, fmt.Errorf("%w: amount must have exactly %d decimal places: %q", ErrInvalidMoney, Scale, s)
	}
	intPart, frac := body[:dot], body[dot+1:]
	if intPart == "" || !allDigits(intPart) || !allDigits(frac) || frac == "" {
		return 0, fmt.Errorf("%w: malformed decimal %q", ErrInvalidMoney, s)
	}
	if len(intPart) > 1 && intPart[0] == '0' {
		return 0, fmt.Errorf("%w: leading zeros are not allowed: %q", ErrInvalidMoney, s)
	}
	if len(frac) > Scale {
		return 0, fmt.Errorf("%w: at most %d decimal places are allowed: %q", ErrScaleExceeded, Scale, s)
	}
	if len(frac) < Scale {
		return 0, fmt.Errorf("%w: amount must have exactly %d decimal places: %q", ErrInvalidMoney, Scale, s)
	}
	if negative {
		return 0, fmt.Errorf("%w: %q", ErrNegativeAmount, s)
	}
	units, err := strconv.ParseInt(intPart, 10, 64)
	if err != nil || units > math.MaxInt64/minorPerUnit {
		return 0, fmt.Errorf("%w: %q", ErrMoneyOverflow, s)
	}
	cents, _ := strconv.ParseInt(frac, 10, 64)
	minor := units*minorPerUnit + cents
	if minor < 0 {
		return 0, fmt.Errorf("%w: %q", ErrMoneyOverflow, s)
	}
	return minor, nil
}

func allDigits(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

func (m Money) IsValid() bool { return m.currency != "" }

func (m Money) Minor() int64 { return m.minor }

func (m Money) Currency() Currency { return m.currency }

func (m Money) Amount() string {
	var sb strings.Builder
	u := uint64(m.minor)
	if m.minor < 0 {
		sb.WriteByte('-')
		u = -u
	}
	units, cents := u/uint64(minorPerUnit), u%uint64(minorPerUnit)
	sb.WriteString(strconv.FormatUint(units, 10))
	sb.WriteByte('.')
	if cents < 10 {
		sb.WriteByte('0')
	}
	sb.WriteString(strconv.FormatUint(cents, 10))
	return sb.String()
}

func (m Money) String() string {
	if !m.IsValid() {
		return "<invalid money>"
	}
	return m.Amount() + " " + string(m.currency)
}

func (m Money) IsZero() bool     { return m.IsValid() && m.minor == 0 }
func (m Money) IsPositive() bool { return m.IsValid() && m.minor > 0 }
func (m Money) IsNegative() bool { return m.IsValid() && m.minor < 0 }

func (m Money) compatible(o Money) error {
	if !m.IsValid() || !o.IsValid() {
		return fmt.Errorf("%w: uninitialized value", ErrInvalidMoney)
	}
	if m.currency != o.currency {
		return fmt.Errorf("%w: %s vs %s", ErrCurrencyMismatch, m.currency, o.currency)
	}
	return nil
}

func (m Money) Add(o Money) (Money, error) {
	if err := m.compatible(o); err != nil {
		return Money{}, err
	}
	if (o.minor > 0 && m.minor > math.MaxInt64-o.minor) || (o.minor < 0 && m.minor < math.MinInt64-o.minor) {
		return Money{}, ErrMoneyOverflow
	}
	return Money{minor: m.minor + o.minor, currency: m.currency}, nil
}

func (m Money) Sub(o Money) (Money, error) {
	if err := m.compatible(o); err != nil {
		return Money{}, err
	}
	if (o.minor < 0 && m.minor > math.MaxInt64+o.minor) || (o.minor > 0 && m.minor < math.MinInt64+o.minor) {
		return Money{}, ErrMoneyOverflow
	}
	return Money{minor: m.minor - o.minor, currency: m.currency}, nil
}

func (m Money) Neg() (Money, error) {
	if !m.IsValid() {
		return Money{}, fmt.Errorf("%w: uninitialized value", ErrInvalidMoney)
	}
	if m.minor == math.MinInt64 {
		return Money{}, ErrMoneyOverflow
	}
	return Money{minor: -m.minor, currency: m.currency}, nil
}

func (m Money) Cmp(o Money) (int, error) {
	if err := m.compatible(o); err != nil {
		return 0, err
	}
	switch {
	case m.minor < o.minor:
		return -1, nil
	case m.minor > o.minor:
		return 1, nil
	}
	return 0, nil
}

func (m Money) Equal(o Money) bool { return m == o }

type moneyJSON struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}

func (m Money) MarshalJSON() ([]byte, error) {
	if !m.IsValid() {
		return nil, fmt.Errorf("%w: uninitialized value", ErrInvalidMoney)
	}
	return json.Marshal(moneyJSON{Amount: m.Amount(), Currency: string(m.currency)})
}

func (m *Money) UnmarshalJSON(data []byte) error {
	if bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		return nil
	}
	var raw moneyJSON
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&raw); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidMoney, err)
	}
	parsed, err := NewMoney(raw.Amount, raw.Currency)
	if err != nil {
		return err
	}
	*m = parsed
	return nil
}

func IsMoneyError(err error) bool {
	for _, target := range []error{ErrInvalidMoney, ErrNegativeAmount, ErrScaleExceeded, ErrMoneyOverflow, ErrCurrencyMismatch, ErrUnsupportedCurrency} {
		if errors.Is(err, target) {
			return true
		}
	}
	return false
}
