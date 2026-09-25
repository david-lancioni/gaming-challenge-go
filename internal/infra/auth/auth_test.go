package auth

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-jose/go-jose/v4"
	"github.com/go-jose/go-jose/v4/jwt"

	"github.com/dlancioni/backend-challenge-go/internal/config"
)

const (
	issuer   = "https://idp.example/realms/wagering"
	audience = "wagering-api"
)

type idp struct {
	key    *rsa.PrivateKey
	server *httptest.Server
	auth   *Authenticator
}

func newIdP(t *testing.T) *idp {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	set := jose.JSONWebKeySet{Keys: []jose.JSONWebKey{{Key: &key.PublicKey, KeyID: "k1", Algorithm: "RS256", Use: "sig"}}}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(set)
	}))
	t.Cleanup(srv.Close)
	a := New(context.Background(), config.AuthConfig{
		Issuer: issuer, JWKSURL: srv.URL, Audience: audience,
		ProviderRole: "wagering-provider", InternalRole: "wagering-internal", ProviderClaim: "provider_id",
	})
	return &idp{key: key, server: srv, auth: a}
}

type claims map[string]any

func (i *idp) sign(t *testing.T, key *rsa.PrivateKey, c claims) string {
	t.Helper()
	signer, err := jose.NewSigner(jose.SigningKey{Algorithm: jose.RS256, Key: key},
		(&jose.SignerOptions{}).WithType("JWT").WithHeader("kid", "k1"))
	if err != nil {
		t.Fatal(err)
	}
	raw, err := jwt.Signed(signer).Claims(map[string]any(c)).Serialize()
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func base(extra claims) claims {
	c := claims{
		"iss": issuer, "aud": []string{audience}, "sub": "service-account-provider-a",
		"exp": time.Now().Add(5 * time.Minute).Unix(), "iat": time.Now().Unix(),
		"realm_access": map[string]any{"roles": []string{"wagering-provider"}}, "provider_id": "provider-a",
	}
	for k, v := range extra {
		if v == nil {
			delete(c, k)
		} else {
			c[k] = v
		}
	}
	return c
}

func TestAuthenticateAcceptsValidTokens(t *testing.T) {
	i := newIdP(t)
	p, err := i.auth.Authenticate(context.Background(), "Bearer "+i.sign(t, i.key, base(nil)))
	if err != nil {
		t.Fatal(err)
	}
	if !p.IsProvider() || p.IsInternal() || p.ProviderID != "provider-a" || p.Subject == "" {
		t.Fatalf("principal = %+v", p)
	}
	internal := base(claims{"provider_id": nil, "realm_access": map[string]any{"roles": []string{"wagering-internal"}}})
	p, err = i.auth.Authenticate(context.Background(), "bearer "+i.sign(t, i.key, internal))
	if err != nil || !p.IsInternal() || p.IsProvider() {
		t.Fatalf("internal principal = %+v, %v", p, err)
	}
}

func TestAuthenticateRejectsInvalidCredentials(t *testing.T) {
	i := newIdP(t)
	other, _ := rsa.GenerateKey(rand.Reader, 2048)
	cases := map[string]string{
		"missing":        "",
		"wrong scheme":   "Basic " + i.sign(t, i.key, base(nil)),
		"empty token":    "Bearer ",
		"garbage":        "Bearer not.a.jwt",
		"expired":        "Bearer " + i.sign(t, i.key, base(claims{"exp": time.Now().Add(-time.Minute).Unix()})),
		"not yet valid":  "Bearer " + i.sign(t, i.key, base(claims{"nbf": time.Now().Add(time.Hour).Unix()})),
		"wrong issuer":   "Bearer " + i.sign(t, i.key, base(claims{"iss": "https://evil.example"})),
		"wrong audience": "Bearer " + i.sign(t, i.key, base(claims{"aud": []string{"account"}})),
		"no audience":    "Bearer " + i.sign(t, i.key, base(claims{"aud": nil})),
		"foreign key":    "Bearer " + i.sign(t, other, base(nil)),
		"no expiration":  "Bearer " + i.sign(t, i.key, base(claims{"exp": nil})),
	}
	for name, header := range cases {
		t.Run(name, func(t *testing.T) {
			if p, err := i.auth.Authenticate(context.Background(), header); err == nil {
				t.Fatalf("accepted: %+v", p)
			}
		})
	}
}

func TestPrincipalRolesAndProviderIdentity(t *testing.T) {
	i := newIdP(t)
	p, err := i.auth.Authenticate(context.Background(), "Bearer "+i.sign(t, i.key, base(claims{"provider_id": nil})))
	if err != nil || p.IsProvider() {
		t.Fatalf("provider without identity: %+v %v", p, err)
	}
	p, err = i.auth.Authenticate(context.Background(), "Bearer "+i.sign(t, i.key, base(claims{"realm_access": map[string]any{"roles": []string{"offline_access"}}})))
	if err != nil || p.IsProvider() || p.IsInternal() {
		t.Fatalf("no role: %+v %v", p, err)
	}
	p, err = i.auth.Authenticate(context.Background(), "Bearer "+i.sign(t, i.key, base(claims{"provider_id": []string{"a", "b"}})))
	if err != nil || p.ProviderID != "" || p.IsProvider() {
		t.Fatalf("malformed claim: %+v %v", p, err)
	}
	ctx := NewContext(context.Background(), p)
	if got, ok := FromContext(ctx); !ok || got != p {
		t.Fatal("principal round trip through the context")
	}
	if _, ok := FromContext(context.Background()); ok {
		t.Fatal("no principal in an empty context")
	}
}
