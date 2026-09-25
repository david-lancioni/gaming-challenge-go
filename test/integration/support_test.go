//go:build integration

package integration

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dlancioni/backend-challenge-go/internal/config"
	"github.com/dlancioni/backend-challenge-go/internal/infra/messaging"
	"github.com/dlancioni/backend-challenge-go/internal/infra/postgres"
	"github.com/dlancioni/backend-challenge-go/migrations"
)

var (
	repoRoot    string
	binaryPath  string
	adminDBURL  = envOr("TEST_ADMIN_DATABASE_URL", "postgres://wagering:wagering@localhost:5432/postgres?sslmode=disable")
	awsEndpoint = envOr("TEST_AWS_ENDPOINT_URL", "http://localhost:4566")
	keycloakURL = envOr("TEST_KEYCLOAK_URL", "http://localhost:8080")
	issuer      = envOr("TEST_OIDC_ISSUER", "http://localhost:8080/realms/wagering")
	realmURL    = keycloakURL + "/realms/wagering"
)

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func TestMain(m *testing.M) {
	_, file, _, _ := runtime.Caller(0)
	repoRoot = filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))

	if err := checkDependencies(); err != nil {
		fmt.Fprintf(os.Stderr, "integration dependencies are not available: %v\n\n"+
			"Start them with:  docker compose up -d postgres localstack keycloak\n", err)
		os.Exit(1)
	}
	dir, err := os.MkdirTemp("", "wagering-it-")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	binaryPath = filepath.Join(dir, "wagering")
	if runtime.GOOS == "windows" {
		binaryPath += ".exe"
	}
	build := exec.Command("go", "build", "-o", binaryPath, "./cmd/wagering")
	build.Dir = repoRoot
	if out, err := build.CombinedOutput(); err != nil {
		fmt.Fprintf(os.Stderr, "building the service binary failed: %v\n%s\n", err, out)
		os.Exit(1)
	}
	code := m.Run()
	_ = os.RemoveAll(dir)
	os.Exit(code)
}

func checkDependencies() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, adminDBURL)
	if err != nil {
		return fmt.Errorf("postgres: %w", err)
	}
	_ = conn.Close(ctx)
	for name, u := range map[string]string{
		"keycloak":   realmURL + "/.well-known/openid-configuration",
		"localstack": awsEndpoint + "/_localstack/health",
	} {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("%s: status %d", name, resp.StatusCode)
		}
	}
	return nil
}

type stack struct {
	t      testing.TB
	dbName string
	dbURL  string
	pool   *pgxpool.Pool
	queues config.QueuesConfig
	urls   messaging.QueueURLs
	sqs    *sqs.Client
}

func newStack(t testing.TB) *stack {
	t.Helper()
	ctx := context.Background()
	id := strings.ReplaceAll(uuid.NewString()[:13], "-", "")
	s := &stack{t: t, dbName: "it_" + id}

	admin, err := pgx.Connect(ctx, adminDBURL)
	if err != nil {
		t.Fatalf("connect admin database: %v", err)
	}
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+s.dbName); err != nil {
		t.Fatalf("create database: %v", err)
	}
	_ = admin.Close(ctx)

	s.dbURL = replaceDB(adminDBURL, s.dbName)

	mg, err := postgres.NewMigrator(ctx, s.dbURL, migrations.FS)
	if err != nil {
		t.Fatalf("migrator: %v", err)
	}
	if _, err := mg.Up(ctx); err != nil {
		t.Fatalf("migrate up: %v", err)
	}
	_ = mg.Close(ctx)

	s.pool, err = pgxpool.New(ctx, s.dbURL)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}

	s.queues = config.QueuesConfig{
		Input:                  "it-" + id + "-wager-transactions.fifo",
		InputDLQ:               "it-" + id + "-wager-transactions-dlq.fifo",
		Events:                 "it-" + id + "-wager-events.fifo",
		VisibilityTimeout:      5 * time.Second,
		MaxReceiveCount:        3,
		ProducerPrincipal:      "arn:aws:iam::000000000000:user/wager-producer",
		ServicePrincipal:       "arn:aws:iam::000000000000:user/wagering-service",
		EventConsumerPrincipal: "arn:aws:iam::000000000000:user/event-consumer",
	}
	s.sqs, err = messaging.NewClient(ctx, config.AWSConfig{Region: "us-east-1", Endpoint: awsEndpoint})
	if err != nil {
		t.Fatalf("sqs client: %v", err)
	}
	s.urls, err = messaging.EnsureQueues(ctx, s.sqs, awsEndpoint, s.queues)
	if err != nil {
		t.Fatalf("ensure queues: %v", err)
	}

	t.Cleanup(func() {
		s.pool.Close()
		admin, err := pgx.Connect(context.Background(), adminDBURL)
		if err != nil {
			return
		}
		defer admin.Close(context.Background())
		_, _ = admin.Exec(context.Background(), "DROP DATABASE IF EXISTS "+s.dbName+" WITH (FORCE)")
	})
	return s
}

func replaceDB(url, db string) string {
	i := strings.LastIndex(url, "/")
	j := strings.Index(url[i:], "?")
	if j < 0 {
		return url[:i+1] + db
	}
	return url[:i+1] + db + url[i+j:]
}

func (s *stack) envFor(overrides map[string]string) map[string]string {
	env := map[string]string{
		"DATABASE_URL":             s.dbURL,
		"AWS_REGION":               "us-east-1",
		"AWS_ACCESS_KEY_ID":        "test",
		"AWS_SECRET_ACCESS_KEY":    "test",
		"AWS_ENDPOINT_URL":         awsEndpoint,
		"SQS_INPUT_QUEUE":          s.queues.Input,
		"SQS_INPUT_DLQ":            s.queues.InputDLQ,
		"SQS_EVENTS_QUEUE":         s.queues.Events,
		"SQS_VISIBILITY_TIMEOUT":   s.queues.VisibilityTimeout.String(),
		"SQS_MAX_RECEIVE_COUNT":    fmt.Sprint(s.queues.MaxReceiveCount),
		"OIDC_ISSUER":              issuer,
		"OIDC_JWKS_URL":            realmURL + "/protocol/openid-connect/certs",
		"HTTP_ADDR":                "127.0.0.1:0",
		"METRICS_ADDR":             "127.0.0.1:0",
		"LOG_LEVEL":                "info",
		"SHUTDOWN_TIMEOUT":         "10s",
		"CONSUMER_WAIT_TIME":       "1s",
		"CONSUMER_PROCESS_TIMEOUT": "3s",
		"CONSUMER_RETRY_BASE":      "1s",
		"CONSUMER_RETRY_MAX":       "2s",
		"OUTBOX_POLL_INTERVAL":     "100ms",
		"OUTBOX_LEASE":             "3s",
		"OUTBOX_BACKOFF_BASE":      "200ms",
		"OUTBOX_BACKOFF_MAX":       "1s",
		"PENDING_POLL_INTERVAL":    "200ms",
		"PENDING_BACKOFF_BASE":     "300ms",
		"PENDING_BACKOFF_MAX":      "1s",
		"PENDING_LEASE":            "2s",
		"PENDING_TTL":              "30s",
		"PENDING_MAX_ATTEMPTS":     "50",
		"DB_LOCK_TIMEOUT":          "10s",
	}
	for k, v := range overrides {
		env[k] = v
	}
	return env
}

var envMu sync.Mutex

func loadConfig(t testing.TB, env map[string]string) config.Config {
	t.Helper()
	envMu.Lock()
	defer envMu.Unlock()
	prev := map[string]*string{}
	for k, v := range env {
		if old, ok := os.LookupEnv(k); ok {
			o := old
			prev[k] = &o
		} else {
			prev[k] = nil
		}
		os.Setenv(k, v)
	}
	defer func() {
		for k, old := range prev {
			if old == nil {
				os.Unsetenv(k)
			} else {
				os.Setenv(k, *old)
			}
		}
	}()
	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	return cfg
}

func (s *stack) queryInt(sql string, args ...any) int64 {
	s.t.Helper()
	var n int64
	if err := s.pool.QueryRow(context.Background(), sql, args...).Scan(&n); err != nil {
		s.t.Fatalf("query %q: %v", sql, err)
	}
	return n
}

func (s *stack) queryString(sql string, args ...any) string {
	s.t.Helper()
	var v string
	if err := s.pool.QueryRow(context.Background(), sql, args...).Scan(&v); err != nil {
		s.t.Fatalf("query %q: %v", sql, err)
	}
	return v
}

func (s *stack) exec(sql string, args ...any) error {
	_, err := s.pool.Exec(context.Background(), sql, args...)
	return err
}

func (s *stack) ledgerSum(walletID string) int64 {
	return s.queryInt(`SELECT COALESCE(SUM(CASE direction WHEN 'CREDIT' THEN amount_minor ELSE -amount_minor END),0)
		FROM wallet_ledger_entries WHERE wallet_id = $1`, walletID)
}

func (s *stack) balanceMinor(walletID string) int64 {
	return s.queryInt(`SELECT balance_minor FROM wallets WHERE id = $1`, walletID)
}

func (s *stack) assertConsistent(walletID string) {
	s.t.Helper()
	if b, l := s.balanceMinor(walletID), s.ledgerSum(walletID); b != l {
		s.t.Fatalf("wallet %s: stored balance %d != ledger sum %d", walletID, b, l)
	}
}

type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

type instance struct {
	name    string
	base    string
	metrics string
	cmd     *exec.Cmd
	out     *lockedBuffer
	exited  chan struct{}
}

func freeAddr(t testing.TB) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

func (s *stack) startProcess(t testing.TB, name string, overrides map[string]string) *instance {
	t.Helper()
	inst := s.spawn(t, name, overrides)
	inst.waitReady(t, 60*time.Second)
	return inst
}

func (s *stack) spawn(t testing.TB, name string, overrides map[string]string) *instance {
	t.Helper()
	httpAddr, metricsAddr := freeAddr(t), freeAddr(t)
	env := s.envFor(map[string]string{"HTTP_ADDR": httpAddr, "METRICS_ADDR": metricsAddr, "INSTANCE_ID": name})
	for k, v := range overrides {
		env[k] = v
	}
	cmd := exec.Command(binaryPath, "serve")
	cmd.Dir = repoRoot
	cmd.Env = os.Environ()
	for k, v := range env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	out := &lockedBuffer{}
	cmd.Stdout, cmd.Stderr = out, out
	if err := cmd.Start(); err != nil {
		t.Fatalf("start %s: %v", name, err)
	}
	inst := &instance{name: name, base: "http://" + httpAddr, metrics: "http://" + metricsAddr, cmd: cmd, out: out, exited: make(chan struct{})}
	go func() { _ = cmd.Wait(); close(inst.exited) }()
	t.Cleanup(func() {
		inst.kill()
		if t.Failed() {
			t.Logf("---- warnings/errors of %s ----\n%s", name, tail(problems(inst.out.String()), 6000))
		}
	})
	return inst
}

func problems(logs string) string {
	var keep []string
	for _, line := range strings.Split(logs, "\n") {
		if strings.Contains(line, `"level":"WARN"`) || strings.Contains(line, `"level":"ERROR"`) || !strings.HasPrefix(line, "{") {
			keep = append(keep, line)
		}
	}
	return strings.Join(keep, "\n")
}

func tail(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[len(s)-n:]
}

func (i *instance) waitReady(t testing.TB, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		select {
		case <-i.exited:
			t.Fatalf("%s exited before becoming ready:\n%s", i.name, tail(i.out.String(), 4000))
		default:
		}
		resp, err := http.Get(i.base + "/health/ready")
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("%s did not become ready:\n%s", i.name, tail(i.out.String(), 4000))
}

func (i *instance) kill() {
	if i.cmd.Process != nil {
		_ = i.cmd.Process.Kill()
	}
	<-i.exited
}

func (i *instance) terminate(t testing.TB, timeout time.Duration) {
	t.Helper()
	if runtime.GOOS == "windows" {
		i.kill()
		return
	}
	_ = i.cmd.Process.Signal(sigterm)
	select {
	case <-i.exited:
	case <-time.After(timeout):
		i.kill()
		t.Fatalf("%s did not stop within %s", i.name, timeout)
	}
}

func (i *instance) waitExit(t testing.TB, timeout time.Duration) {
	t.Helper()
	select {
	case <-i.exited:
	case <-time.After(timeout):
		t.Fatalf("%s did not exit within %s:\n%s", i.name, timeout, tail(i.out.String(), 3000))
	}
}

var tokenCache sync.Map

type cachedToken struct {
	value   string
	expires time.Time
}

func token(t testing.TB, clientID string) string {
	t.Helper()
	if clientID != "expiring-provider" {
		if v, ok := tokenCache.Load(clientID); ok && time.Now().Before(v.(cachedToken).expires) {
			return v.(cachedToken).value
		}
	}
	form := "grant_type=client_credentials&client_id=" + clientID + "&client_secret=" + clientID + "-secret"
	resp, err := http.Post(realmURL+"/protocol/openid-connect/token", "application/x-www-form-urlencoded", strings.NewReader(form))
	if err != nil {
		t.Fatalf("token request: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	var tr struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.Unmarshal(body, &tr); err != nil || tr.AccessToken == "" {
		t.Fatalf("token response (%d): %s", resp.StatusCode, body)
	}
	tokenCache.Store(clientID, cachedToken{value: tr.AccessToken, expires: tokenExpiry(tr.AccessToken).Add(-20 * time.Second)})
	return tr.AccessToken
}

type response struct {
	status int
	header http.Header
	body   []byte
}

func (r response) json(t testing.TB) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(r.body, &m); err != nil {
		t.Fatalf("response is not a JSON object (status %d): %s", r.status, r.body)
	}
	return m
}

func (r response) str(t testing.TB, path ...string) string {
	t.Helper()
	var cur any = r.json(t)
	for _, p := range path {
		m, ok := cur.(map[string]any)
		if !ok {
			t.Fatalf("path %v: not an object in %s", path, r.body)
		}
		cur = m[p]
	}
	s, _ := cur.(string)
	return s
}

var httpClient = &http.Client{Timeout: 60 * time.Second, Transport: &http.Transport{MaxIdleConnsPerHost: 64}}

func call(t testing.TB, method, url, bearer string, headers map[string]string, body any) response {
	t.Helper()
	var rdr io.Reader
	if body != nil {
		switch b := body.(type) {
		case string:
			rdr = strings.NewReader(b)
		case []byte:
			rdr = bytes.NewReader(b)
		default:
			raw, err := json.Marshal(body)
			if err != nil {
				t.Fatal(err)
			}
			rdr = bytes.NewReader(raw)
		}
	}
	req, err := http.NewRequest(method, url, rdr)
	if err != nil {
		t.Fatal(err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	return response{status: resp.StatusCode, header: resp.Header, body: raw}
}

type walletRef struct {
	id     string
	player string
}

func openWallet(t testing.TB, base, amount string) walletRef {
	t.Helper()
	player := uuid.NewString()
	r := call(t, http.MethodPost, base+"/wallets", token(t, "wallet-service"), nil, map[string]any{
		"playerId":       player,
		"initialBalance": map[string]string{"amount": amount, "currency": "BRL"},
	})
	if r.status != http.StatusCreated {
		t.Fatalf("open wallet: %d %s", r.status, r.body)
	}
	return walletRef{id: r.str(t, "id"), player: player}
}

type op struct {
	provider  string
	ext       string
	key       string
	wallet    walletRef
	round     string
	game      string
	kind      string
	amount    string
	reference string
}

func (o op) payload() map[string]any {
	p := map[string]any{
		"providerId": o.provider, "externalTransactionId": o.ext, "playerId": o.wallet.player,
		"walletId": o.wallet.id, "roundId": orDefault(o.round, "round-1"), "gameId": orDefault(o.game, "game-1"),
		"kind": o.kind, "money": map[string]string{"amount": o.amount, "currency": "BRL"},
	}
	if o.reference != "" {
		p["referenceExternalTransactionId"] = o.reference
	}
	return p
}

func orDefault(v, def string) string {
	if v == "" {
		return def
	}
	return v
}

func newOp(w walletRef, kind, amount string) op {
	ext := "tx-" + uuid.NewString()
	return op{provider: "provider-a", ext: ext, key: "provider-a:" + ext, wallet: w, kind: kind, amount: amount}
}

func submit(t testing.TB, base string, o op) response {
	t.Helper()
	return submitAs(t, base, token(t, orDefault(o.provider, "provider-a")), o)
}

func submitRetry(t testing.TB, base string, o op) response {
	t.Helper()
	var r response
	for attempt := 0; attempt < 10; attempt++ {
		r = submit(t, base, o)
		if r.status != http.StatusServiceUnavailable {
			return r
		}
		time.Sleep(time.Duration(100*(attempt+1)) * time.Millisecond)
	}
	return r
}

func submitAs(t testing.TB, base, bearer string, o op) response {
	t.Helper()
	key := o.key
	if key == "" {
		key = o.provider + ":" + o.ext
	}
	return call(t, http.MethodPost, base+"/wagering/transactions", bearer, map[string]string{"Idempotency-Key": key}, o.payload())
}

func wantStatus(t testing.TB, r response, want int) {
	t.Helper()
	if r.status != want {
		t.Fatalf("status = %d, want %d; body: %s", r.status, want, r.body)
	}
}

func requestMessage(messageID string, o op) string {
	data := o.payload()
	data["idempotencyKey"] = orDefault(o.key, o.provider+":"+o.ext)
	raw, _ := json.Marshal(map[string]any{
		"messageId": messageID, "type": "WagerTransactionRequested",
		"occurredAt": time.Now().UTC().Format("2006-01-02T15:04:05.000Z"), "data": data,
	})
	return string(raw)
}

func (s *stack) sendRaw(t testing.TB, body, group, dedup string) {
	t.Helper()
	_, err := s.sqs.SendMessage(context.Background(), &sqs.SendMessageInput{
		QueueUrl: aws.String(s.urls.Input), MessageBody: aws.String(body),
		MessageGroupId: aws.String(group), MessageDeduplicationId: aws.String(dedup)})
	if err != nil {
		t.Fatalf("send message: %v", err)
	}
}

func (s *stack) sendRequest(t testing.TB, messageID string, o op) {
	t.Helper()
	s.sendRaw(t, requestMessage(messageID, o), o.wallet.id, messageID+"-"+uuid.NewString())
}

func (s *stack) drain(t testing.TB, queueURL string, timeout time.Duration, stop func([]types.Message) bool) []types.Message {
	t.Helper()
	var all []types.Message
	seen := map[string]bool{}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		out, err := s.sqs.ReceiveMessage(context.Background(), &sqs.ReceiveMessageInput{
			QueueUrl: aws.String(queueURL), MaxNumberOfMessages: 10, WaitTimeSeconds: 0, VisibilityTimeout: 5,
			MessageAttributeNames:       []string{"All"},
			MessageSystemAttributeNames: []types.MessageSystemAttributeName{types.MessageSystemAttributeNameAll},
		})
		if err != nil {
			t.Fatalf("receive: %v", err)
		}
		for _, m := range out.Messages {
			if !seen[aws.ToString(m.MessageId)] {
				seen[aws.ToString(m.MessageId)] = true
				all = append(all, m)
			}
			if _, err := s.sqs.DeleteMessage(context.Background(), &sqs.DeleteMessageInput{QueueUrl: aws.String(queueURL), ReceiptHandle: m.ReceiptHandle}); err != nil {
				_, _ = s.sqs.ChangeMessageVisibility(context.Background(), &sqs.ChangeMessageVisibilityInput{
					QueueUrl: aws.String(queueURL), ReceiptHandle: m.ReceiptHandle, VisibilityTimeout: 0})
			}
		}
		if stop != nil && stop(all) {
			return all
		}
		if len(out.Messages) == 0 {
			time.Sleep(200 * time.Millisecond)
		}
	}
	return all
}

type messageT = types.Message

type event struct {
	EventID       string         `json:"eventId"`
	EventType     string         `json:"eventType"`
	AggregateID   string         `json:"aggregateId"`
	CorrelationID string         `json:"correlationId"`
	CausationID   string         `json:"causationId"`
	OccurredAt    string         `json:"occurredAt"`
	Version       int            `json:"version"`
	Data          map[string]any `json:"data"`
}

func parseEvents(t testing.TB, msgs []types.Message) []event {
	t.Helper()
	var out []event
	for _, m := range msgs {
		var e event
		if err := json.Unmarshal([]byte(aws.ToString(m.Body)), &e); err != nil {
			t.Fatalf("event body: %v: %s", err, aws.ToString(m.Body))
		}
		out = append(out, e)
	}
	return out
}

func eventsOf(evs []event, walletID string) []event {
	var out []event
	for _, e := range evs {
		if e.Data["walletId"] == walletID {
			out = append(out, e)
		}
	}
	return out
}

func countType(evs []event, typ string) int {
	n := 0
	for _, e := range evs {
		if e.EventType == typ {
			n++
		}
	}
	return n
}

func eventually(t testing.TB, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(150 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for: %s", what)
}

func recreateQueues(s *stack) (string, error) {
	u, err := messaging.EnsureQueues(context.Background(), s.sqs, awsEndpoint, s.queues)
	return u.Events, err
}

func tokenExpiry(jwt string) time.Time {
	parts := strings.Split(jwt, ".")
	if len(parts) != 3 {
		return time.Now()
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return time.Now()
	}
	var c struct {
		Exp int64 `json:"exp"`
	}
	_ = json.Unmarshal(raw, &c)
	return time.Unix(c.Exp, 0)
}
