package httpapi_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/dlancioni/backend-challenge-go/internal/application"
	"github.com/dlancioni/backend-challenge-go/internal/infra/auth"
	"github.com/dlancioni/backend-challenge-go/internal/infra/httpapi"
	"github.com/dlancioni/backend-challenge-go/internal/testutil/memory"
)

const (
	roleProvider = "wagering-provider"
	roleInternal = "wagering-internal"
)

type fakeAuth struct{}

func (fakeAuth) Authenticate(_ context.Context, header string) (*auth.Principal, error) {
	switch strings.TrimPrefix(header, "Bearer ") {
	case "provider-a":
		return auth.NewPrincipal("provider-a", "provider-a", roleProvider, roleInternal, roleProvider), nil
	case "provider-b":
		return auth.NewPrincipal("provider-b", "provider-b", roleProvider, roleInternal, roleProvider), nil
	case "internal":
		return auth.NewPrincipal("wallet-service", "", roleProvider, roleInternal, roleInternal), nil
	case "no-role":
		return auth.NewPrincipal("nobody", "", roleProvider, roleInternal), nil
	}
	return nil, auth.ErrUnauthenticated
}

type checker struct {
	name string
	err  error
}

func (c checker) Name() string                { return c.name }
func (c checker) Check(context.Context) error { return c.err }

type env struct {
	t     *testing.T
	store *memory.Store
	h     http.Handler
}

func newEnv(t *testing.T, checkers ...httpapi.HealthChecker) *env {
	t.Helper()
	store := memory.NewStore()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	policy := application.PendingPolicy{TTL: time.Minute, MaxAttempts: 3, BaseBackoff: time.Second, MaxBackoff: time.Second}
	proc := application.NewProcessor(time.Now, policy, nil)
	srv := httpapi.NewServer(httpapi.Deps{
		Wallets:  application.NewWalletService(store, store.WalletReader(), time.Now, nil, log),
		Wagering: application.NewWageringService(store, store.TransactionReader(), proc, nil, log),
		Auth:     fakeAuth{}, Checkers: checkers, Log: log,
	})
	return &env{t: t, store: store, h: srv.Handler()}
}

type resp struct {
	code   int
	header http.Header
	body   string
}

func (e *env) do(method, path, token string, headers map[string]string, body any) resp {
	e.t.Helper()
	var rdr io.Reader
	switch b := body.(type) {
	case nil:
	case string:
		rdr = strings.NewReader(b)
	default:
		raw, _ := json.Marshal(b)
		rdr = strings.NewReader(string(raw))
	}
	req := httptest.NewRequest(method, path, rdr)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	e.h.ServeHTTP(rec, req)
	return resp{code: rec.Code, header: rec.Header(), body: rec.Body.String()}
}

func (r resp) field(t *testing.T, path ...string) string {
	t.Helper()
	var cur any
	if err := json.Unmarshal([]byte(r.body), &cur); err != nil {
		t.Fatalf("not JSON: %s", r.body)
	}
	for _, p := range path {
		m, _ := cur.(map[string]any)
		cur = m[p]
	}
	s, _ := cur.(string)
	return s
}

func (e *env) wallet(amount string) (id, player string) {
	e.t.Helper()
	player = uuid.NewString()
	r := e.do(http.MethodPost, "/wallets", "internal", nil, map[string]any{
		"playerId": player, "initialBalance": map[string]string{"amount": amount, "currency": "BRL"}})
	if r.code != http.StatusCreated {
		e.t.Fatalf("open wallet: %d %s", r.code, r.body)
	}
	return r.field(e.t, "id"), player
}

func payload(wallet, player, ext, kind, amount string) map[string]any {
	return map[string]any{"providerId": "provider-a", "externalTransactionId": ext, "playerId": player, "walletId": wallet,
		"roundId": "r1", "gameId": "g1", "kind": kind, "money": map[string]string{"amount": amount, "currency": "BRL"}}
}

func idem(ext string) map[string]string {
	return map[string]string{"Idempotency-Key": "provider-a:" + ext}
}

func TestAuthenticationAndAuthorizationOfEveryBusinessEndpoint(t *testing.T) {
	e := newEnv(t)
	w, player := e.wallet("100.00")
	tx := payload(w, player, "t1", "BET", "1.00")
	first := e.do(http.MethodPost, "/wagering/transactions", "provider-a", idem("t1"), tx)
	if first.code != http.StatusOK {
		t.Fatal(first.body)
	}
	txID := first.field(t, "transactionId")

	type call struct {
		method, path string
		body         any
		headers      map[string]string
	}
	internal := []call{
		{http.MethodPost, "/wallets", map[string]any{"playerId": uuid.NewString(), "initialBalance": map[string]string{"amount": "1.00", "currency": "BRL"}}, nil},
		{http.MethodGet, "/wallets/" + w, nil, nil},
		{http.MethodGet, "/wallets/" + w + "/ledger", nil, nil},
		{http.MethodPost, "/wallets/" + w + "/reconciliation", nil, nil},
	}
	for _, c := range internal {
		for token, want := range map[string]int{"": 401, "garbage": 401, "provider-a": 403, "no-role": 403} {
			if got := e.do(c.method, c.path, token, c.headers, c.body); got.code != want {
				t.Errorf("%s %s as %q = %d, want %d", c.method, c.path, token, got.code, want)
			}
		}
	}
	provider := []call{{http.MethodPost, "/wagering/transactions", tx, idem("t1")}}
	for _, c := range provider {
		for token, want := range map[string]int{"": 401, "garbage": 401, "internal": 403, "no-role": 403} {
			if got := e.do(c.method, c.path, token, c.headers, c.body); got.code != want {
				t.Errorf("%s %s as %q = %d, want %d", c.method, c.path, token, got.code, want)
			}
		}
	}
	reads := []string{"/wagering/transactions/" + txID, "/providers/provider-a/wagering/transactions/t1"}
	for _, path := range reads {
		for token, want := range map[string]int{"": 401, "garbage": 401, "no-role": 403, "provider-a": 200, "internal": 200} {
			if got := e.do(http.MethodGet, path, token, nil, nil); got.code != want {
				t.Errorf("GET %s as %q = %d, want %d", path, token, got.code, want)
			}
		}
	}
	if r := e.do(http.MethodGet, "/wagering/transactions/"+txID, "provider-b", nil, nil); r.code != 404 || strings.Contains(r.body, "provider-a") {
		t.Errorf("another provider's transaction by id = %d %s", r.code, r.body)
	}
	if r := e.do(http.MethodGet, "/providers/provider-a/wagering/transactions/t1", "provider-b", nil, nil); r.code != 403 {
		t.Errorf("another provider's path = %d", r.code)
	}
	forged := payload(w, player, "t2", "BET", "1.00")
	if r := e.do(http.MethodPost, "/wagering/transactions", "provider-b", idem("t2"), forged); r.code != 403 || r.field(t, "error", "code") != "PROVIDER_MISMATCH" {
		t.Errorf("forged providerId = %d %s", r.code, r.body)
	}
	if len(e.store.Ledger()) != 2 {
		t.Errorf("unauthorized calls changed the ledger: %d entries", len(e.store.Ledger()))
	}
}

func TestSubmissionContract(t *testing.T) {
	e := newEnv(t)
	w, player := e.wallet("100.00")

	r := e.do(http.MethodPost, "/wagering/transactions", "provider-a", idem("bet"), payload(w, player, "bet", "BET", "25.00"))
	if r.code != 200 || r.field(t, "status") != "PROCESSED" || r.field(t, "balance", "amount") != "75.00" || !strings.Contains(r.body, `"idempotentReplay":false`) {
		t.Fatalf("bet = %d %s", r.code, r.body)
	}
	if r.header.Get("X-Correlation-Id") == "" {
		t.Error("every response carries a correlation id")
	}
	replay := e.do(http.MethodPost, "/wagering/transactions", "provider-a", idem("bet"), payload(w, player, "bet", "BET", "25.00"))
	if replay.code != 200 || !strings.Contains(replay.body, `"idempotentReplay":true`) {
		t.Fatalf("replay = %d %s", replay.code, replay.body)
	}
	c1 := e.do(http.MethodPost, "/wagering/transactions", "provider-a", idem("bet"), payload(w, player, "bet", "BET", "26.00"))
	c2 := e.do(http.MethodPost, "/wagering/transactions", "provider-a", map[string]string{"Idempotency-Key": "other"}, payload(w, player, "bet", "BET", "25.00"))
	if c1.code != 409 || c1.field(t, "error", "code") != "IDEMPOTENCY_KEY_CONFLICT" || c2.code != 409 || c2.field(t, "error", "code") != "EXTERNAL_TRANSACTION_CONFLICT" {
		t.Fatalf("conflicts: %d %s / %d %s", c1.code, c1.body, c2.code, c2.body)
	}
	rej := e.do(http.MethodPost, "/wagering/transactions", "provider-a", idem("big"), payload(w, player, "big", "BET", "999.00"))
	if rej.code != 422 || rej.field(t, "status") != "REJECTED" || rej.field(t, "failureCode") != "INSUFFICIENT_FUNDS" {
		t.Fatalf("rejection = %d %s", rej.code, rej.body)
	}
	p := payload(w, player, "rf", "REFUND", "5.00")
	p["referenceExternalTransactionId"] = "not-yet"
	pend := e.do(http.MethodPost, "/wagering/transactions", "provider-a", idem("rf"), p)
	if pend.code != 202 || pend.field(t, "status") != "PENDING_REFERENCE" || !strings.HasPrefix(pend.header.Get("Location"), "/wagering/transactions/") {
		t.Fatalf("pending = %d %s", pend.code, pend.body)
	}
	get := e.do(http.MethodGet, pend.header.Get("Location"), "provider-a", nil, nil)
	if get.code != 200 || get.field(t, "status") != "PENDING_REFERENCE" || get.field(t, "expiresAt") == "" {
		t.Fatalf("get pending = %d %s", get.code, get.body)
	}
	bad := map[string]any{
		"opening": payload(w, player, "o", "OPENING", "1.00"),
		"scale":   payload(w, player, "s", "BET", "1.001"),
		"neg":     payload(w, player, "n", "BET", "-1.00"),
		"noround": func() map[string]any { m := payload(w, player, "r", "BET", "1.00"); delete(m, "roundId"); return m }(),
		"float":   `{"providerId":"provider-a","externalTransactionId":"f","playerId":"` + player + `","walletId":"` + w + `","roundId":"r","gameId":"g","kind":"BET","money":{"amount":1.5,"currency":"BRL"}}`,
		"unknown": `{"providerId":"provider-a","zzz":1}`,
		"nojson":  `nope`,
	}
	txnsBefore := len(e.store.Events())
	for name, body := range bad {
		if r := e.do(http.MethodPost, "/wagering/transactions", "provider-a", idem("v-"+name), body); r.code != 400 {
			t.Errorf("%s = %d %s", name, r.code, r.body)
		}
	}
	if len(e.store.Events()) != txnsBefore {
		t.Error("invalid requests produced events")
	}
	if r := e.do(http.MethodPost, "/wagering/transactions", "provider-a", nil, payload(w, player, "k", "BET", "1.00")); r.code != 400 {
		t.Errorf("missing Idempotency-Key = %d", r.code)
	}
	if r := e.do(http.MethodPost, "/wagering/transactions", "provider-a", map[string]string{"Idempotency-Key": "k", "Content-Type": "text/plain"}, payload(w, player, "k", "BET", "1.00")); r.code != 415 {
		t.Errorf("content type = %d", r.code)
	}
	if r := e.do(http.MethodPost, "/wagering/transactions", "provider-a", idem("big"), `{"providerId":"`+strings.Repeat("a", 70000)+`"}`); r.code != 413 {
		t.Errorf("oversized = %d", r.code)
	}
	ghost := payload(uuid.NewString(), player, "ghost", "BET", "1.00")
	if r := e.do(http.MethodPost, "/wagering/transactions", "provider-a", idem("ghost"), ghost); r.code != 404 || r.field(t, "error", "code") != "WALLET_NOT_FOUND" {
		t.Errorf("unknown wallet = %d %s", r.code, r.body)
	}
}

func TestWalletEndpoints(t *testing.T) {
	e := newEnv(t)
	w, player := e.wallet("10.00")
	if r := e.do(http.MethodPost, "/wallets", "internal", nil, map[string]any{"playerId": player,
		"initialBalance": map[string]string{"amount": "1.00", "currency": "BRL"}}); r.code != 409 || r.field(t, "error", "code") != "WALLET_ALREADY_EXISTS" {
		t.Errorf("duplicate wallet = %d %s", r.code, r.body)
	}
	if r := e.do(http.MethodGet, "/wallets/"+w, "internal", nil, nil); r.code != 200 || r.field(t, "balance", "amount") != "10.00" {
		t.Errorf("get = %d %s", r.code, r.body)
	}
	for _, id := range []string{uuid.NewString(), "x"} {
		if r := e.do(http.MethodGet, "/wallets/"+id, "internal", nil, nil); r.code != 404 {
			t.Errorf("unknown wallet %q = %d", id, r.code)
		}
	}
	for i := 0; i < 3; i++ {
		e.do(http.MethodPost, "/wagering/transactions", "provider-a", idem("b"+string(rune('0'+i))), payload(w, player, "b"+string(rune('0'+i)), "BET", "1.00"))
	}
	page := e.do(http.MethodGet, "/wallets/"+w+"/ledger?limit=2", "internal", nil, nil)
	var body struct {
		Entries    []map[string]any `json:"entries"`
		NextCursor *string          `json:"nextCursor"`
	}
	if err := json.Unmarshal([]byte(page.body), &body); err != nil || len(body.Entries) != 2 || body.NextCursor == nil {
		t.Fatalf("page = %s", page.body)
	}
	next := e.do(http.MethodGet, "/wallets/"+w+"/ledger?limit=2&cursor="+*body.NextCursor, "internal", nil, nil)
	if err := json.Unmarshal([]byte(next.body), &body); err != nil || len(body.Entries) != 2 || body.NextCursor != nil {
		t.Fatalf("last page = %s", next.body)
	}
	for _, q := range []string{"?limit=0", "?limit=abc", "?limit=999", "?cursor=zzz"} {
		if r := e.do(http.MethodGet, "/wallets/"+w+"/ledger"+q, "internal", nil, nil); r.code != 400 {
			t.Errorf("ledger%s = %d", q, r.code)
		}
	}
	rec := e.do(http.MethodPost, "/wallets/"+w+"/reconciliation", "internal", nil, nil)
	if rec.code != 200 || !strings.Contains(rec.body, `"consistent":true`) || rec.field(t, "difference", "amount") != "0.00" || !strings.Contains(rec.body, `"checkedEntries":4`) {
		t.Fatalf("reconciliation = %s", rec.body)
	}
}

func TestHealthEndpointsArePublic(t *testing.T) {
	up := newEnv(t, checker{"postgres", nil}, checker{"sqs", nil})
	if r := up.do(http.MethodGet, "/health/live", "", nil, nil); r.code != 200 {
		t.Errorf("live = %d", r.code)
	}
	if r := up.do(http.MethodGet, "/health/ready", "", nil, nil); r.code != 200 || r.field(t, "checks", "sqs") != "UP" {
		t.Errorf("ready = %d %s", r.code, r.body)
	}
	down := newEnv(t, checker{"postgres", nil}, checker{"sqs", errors.New("connection refused: secret-host:4566")})
	r := down.do(http.MethodGet, "/health/ready", "", nil, nil)
	if r.code != 503 || r.field(t, "checks", "sqs") != "DOWN" || r.field(t, "checks", "postgres") != "UP" {
		t.Errorf("degraded ready = %d %s", r.code, r.body)
	}
	if strings.Contains(r.body, "secret-host") {
		t.Error("readiness must not leak dependency error details")
	}
	if r := down.do(http.MethodGet, "/health/live", "", nil, nil); r.code != 200 {
		t.Errorf("live = %d", r.code)
	}
	if r := up.do(http.MethodGet, "/unknown", "", nil, nil); r.code != 404 || !strings.Contains(r.body, "NOT_FOUND") {
		t.Errorf("unknown route = %d %s", r.code, r.body)
	}
}
