package auth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/coreos/go-oidc/v3/oidc"

	"github.com/dlancioni/backend-challenge-go/internal/config"
)

var (
	ErrUnauthenticated = errors.New("unauthenticated")
)

type Principal struct {
	Subject    string
	ProviderID string
	roles      map[string]struct{}
	provider   string
	internal   string
}

func (p *Principal) HasRole(role string) bool {
	_, ok := p.roles[role]
	return ok
}

func (p *Principal) IsProvider() bool { return p.ProviderID != "" && p.HasRole(p.provider) }

func (p *Principal) IsInternal() bool { return p.HasRole(p.internal) }

func NewPrincipal(subject, providerID, providerRole, internalRole string, roles ...string) *Principal {
	p := &Principal{Subject: subject, ProviderID: providerID, roles: map[string]struct{}{}, provider: providerRole, internal: internalRole}
	for _, r := range roles {
		p.roles[r] = struct{}{}
	}
	return p
}

type ctxKey struct{}

func NewContext(ctx context.Context, p *Principal) context.Context {
	return context.WithValue(ctx, ctxKey{}, p)
}

func FromContext(ctx context.Context) (*Principal, bool) {
	p, ok := ctx.Value(ctxKey{}).(*Principal)
	return p, ok
}

type Authenticator struct {
	verifier *oidc.IDTokenVerifier
	cfg      config.AuthConfig
}

func New(ctx context.Context, cfg config.AuthConfig) *Authenticator {
	keySet := oidc.NewRemoteKeySet(ctx, cfg.JWKSURL)
	verifier := oidc.NewVerifier(cfg.Issuer, keySet, &oidc.Config{
		ClientID:             cfg.Audience,
		SupportedSigningAlgs: []string{oidc.RS256, oidc.ES256, oidc.PS256},
	})
	return &Authenticator{verifier: verifier, cfg: cfg}
}

func (a *Authenticator) Authenticate(ctx context.Context, authorization string) (*Principal, error) {
	scheme, token, ok := strings.Cut(authorization, " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") || strings.TrimSpace(token) == "" {
		return nil, ErrUnauthenticated
	}
	idToken, err := a.verifier.Verify(ctx, strings.TrimSpace(token))
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrUnauthenticated, err)
	}
	var claims map[string]json.RawMessage
	if err := idToken.Claims(&claims); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrUnauthenticated, err)
	}
	p := &Principal{
		Subject:  idToken.Subject,
		roles:    map[string]struct{}{},
		provider: a.cfg.ProviderRole,
		internal: a.cfg.InternalRole,
	}
	var realm struct {
		Roles []string `json:"roles"`
	}
	if raw, ok := claims["realm_access"]; ok {
		_ = json.Unmarshal(raw, &realm)
	}
	for _, r := range realm.Roles {
		p.roles[r] = struct{}{}
	}
	if raw, ok := claims[a.cfg.ProviderClaim]; ok {
		var id string
		if err := json.Unmarshal(raw, &id); err == nil {
			p.ProviderID = id
		}
	}
	return p, nil
}
