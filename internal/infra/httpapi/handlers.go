package httpapi

import (
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/dlancioni/backend-challenge-go/internal/application"
	"github.com/dlancioni/backend-challenge-go/internal/domain"
	"github.com/dlancioni/backend-challenge-go/internal/infra/auth"
	"github.com/dlancioni/backend-challenge-go/internal/observability"
)

type openWalletRequest struct {
	PlayerID       string       `json:"playerId"`
	InitialBalance domain.Money `json:"initialBalance"`
}

type walletResponse struct {
	ID        uuid.UUID    `json:"id"`
	PlayerID  uuid.UUID    `json:"playerId"`
	Balance   domain.Money `json:"balance"`
	Version   int64        `json:"version"`
	CreatedAt string       `json:"createdAt"`
	UpdatedAt string       `json:"updatedAt"`
}

func toWalletResponse(w *domain.Wallet) walletResponse {
	return walletResponse{
		ID: w.ID(), PlayerID: w.PlayerID(), Balance: w.Balance(), Version: w.Version(),
		CreatedAt: w.CreatedAt().Format(domain.EventTimeLayout), UpdatedAt: w.UpdatedAt().Format(domain.EventTimeLayout),
	}
}

func (s *Server) openWallet(w http.ResponseWriter, r *http.Request, _ *auth.Principal) {
	var req openWalletRequest
	if err := decodeJSON(w, r, &req); err != nil {
		s.writeDecodeError(w, r, err)
		return
	}
	corr := w.Header().Get("X-Correlation-Id")
	wallet, err := s.Wallets.Open(r.Context(), req.PlayerID, req.InitialBalance, corr)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	s.Log.InfoContext(observability.With(r.Context(), slog.String("walletId", wallet.ID().String())), "wallet opened")
	w.Header().Set("Location", "/wallets/"+wallet.ID().String())
	writeJSON(w, http.StatusCreated, toWalletResponse(wallet))
}

func walletID(w http.ResponseWriter, r *http.Request, s *Server) (uuid.UUID, bool) {
	id, err := domain.ParseID("walletId", r.PathValue("walletId"))
	if err != nil {
		writeAPIError(w, r, http.StatusNotFound, "WALLET_NOT_FOUND", "wallet not found", "walletId", false)
		return uuid.Nil, false
	}
	return id, true
}

func (s *Server) getWallet(w http.ResponseWriter, r *http.Request, _ *auth.Principal) {
	id, ok := walletID(w, r, s)
	if !ok {
		return
	}
	wallet, err := s.Wallets.Get(r.Context(), id)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toWalletResponse(wallet))
}

type ledgerEntryResponse struct {
	ID            uuid.UUID        `json:"id"`
	WalletID      uuid.UUID        `json:"walletId"`
	TransactionID uuid.UUID        `json:"transactionId"`
	Direction     domain.Direction `json:"direction"`
	Money         domain.Money     `json:"money"`
	BalanceBefore domain.Money     `json:"balanceBefore"`
	BalanceAfter  domain.Money     `json:"balanceAfter"`
	WalletVersion int64            `json:"walletVersion"`
	CreatedAt     string           `json:"createdAt"`
}

type ledgerPageResponse struct {
	Entries    []ledgerEntryResponse `json:"entries"`
	NextCursor *string               `json:"nextCursor"`
}

func (s *Server) getLedger(w http.ResponseWriter, r *http.Request, _ *auth.Principal) {
	id, ok := walletID(w, r, s)
	if !ok {
		return
	}
	limit := 0
	if v := r.URL.Query().Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			s.writeError(w, r, domain.NewValidationError("limit", "must be an integer"))
			return
		}
		limit = n
		if n == 0 {
			limit = -1
		}
	}
	page, err := s.Wallets.Ledger(r.Context(), id, r.URL.Query().Get("cursor"), limit)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	resp := ledgerPageResponse{Entries: make([]ledgerEntryResponse, 0, len(page.Entries))}
	for _, e := range page.Entries {
		resp.Entries = append(resp.Entries, ledgerEntryResponse{
			ID: e.ID(), WalletID: e.WalletID(), TransactionID: e.TransactionID(), Direction: e.Direction(),
			Money: e.Money(), BalanceBefore: e.BalanceBefore(), BalanceAfter: e.BalanceAfter(),
			WalletVersion: e.WalletVersion(), CreatedAt: e.CreatedAt().Format(domain.EventTimeLayout),
		})
	}
	if page.NextCursor != "" {
		resp.NextCursor = &page.NextCursor
	}
	writeJSON(w, http.StatusOK, resp)
}

type reconciliationResponse struct {
	WalletID          uuid.UUID    `json:"walletId"`
	StoredBalance     domain.Money `json:"storedBalance"`
	CalculatedBalance domain.Money `json:"calculatedBalance"`
	Difference        domain.Money `json:"difference"`
	Consistent        bool         `json:"consistent"`
	CheckedEntries    int64        `json:"checkedEntries"`
}

func (s *Server) reconcile(w http.ResponseWriter, r *http.Request, _ *auth.Principal) {
	id, ok := walletID(w, r, s)
	if !ok {
		return
	}
	res, err := s.Wallets.Reconcile(r.Context(), id)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, reconciliationResponse{
		WalletID: res.WalletID, StoredBalance: res.StoredBalance, CalculatedBalance: res.CalculatedBalance,
		Difference: res.Difference, Consistent: res.Consistent, CheckedEntries: res.CheckedEntries,
	})
}

type submitRequest struct {
	ProviderID                     string       `json:"providerId"`
	ExternalTransactionID          string       `json:"externalTransactionId"`
	PlayerID                       string       `json:"playerId"`
	WalletID                       string       `json:"walletId"`
	RoundID                        string       `json:"roundId"`
	GameID                         string       `json:"gameId"`
	Kind                           string       `json:"kind"`
	Money                          domain.Money `json:"money"`
	ReferenceExternalTransactionID string       `json:"referenceExternalTransactionId"`
}

type outcomeResponse struct {
	TransactionID    uuid.UUID     `json:"transactionId"`
	Status           string        `json:"status"`
	Balance          *domain.Money `json:"balance,omitempty"`
	IdempotentReplay bool          `json:"idempotentReplay"`
	FailureCode      string        `json:"failureCode,omitempty"`
	FailureMessage   string        `json:"failureMessage,omitempty"`
	ExpiresAt        string        `json:"expiresAt,omitempty"`
	NextAttemptAt    string        `json:"nextAttemptAt,omitempty"`
}

func (s *Server) submitTransaction(w http.ResponseWriter, r *http.Request, p *auth.Principal) {
	keys := r.Header.Values("Idempotency-Key")
	if len(keys) != 1 || keys[0] == "" {
		s.writeError(w, r, domain.NewValidationError("Idempotency-Key", "header is required exactly once"))
		return
	}
	var req submitRequest
	if err := decodeJSON(w, r, &req); err != nil {
		s.writeDecodeError(w, r, err)
		return
	}
	// O provedor autorizado vem do token validado (claim provider_id), nunca do
	// corpo: um provedor não consegue operar em nome de outro.
	if req.ProviderID != p.ProviderID {
		s.Log.WarnContext(r.Context(), "provider mismatch", slog.String("bodyProviderId", req.ProviderID))
		writeAPIError(w, r, http.StatusForbidden, "PROVIDER_MISMATCH",
			"providerId does not match the authenticated provider", "providerId", false)
		return
	}
	corr := w.Header().Get("X-Correlation-Id")
	out, err := s.Wagering.Submit(r.Context(), application.SourceHTTP, domain.ExternalParams{
		ProviderID: req.ProviderID, ExternalTransactionID: req.ExternalTransactionID, IdempotencyKey: keys[0],
		PlayerID: req.PlayerID, WalletID: req.WalletID, RoundID: req.RoundID, GameID: req.GameID,
		Kind: domain.TransactionKind(req.Kind), Money: req.Money,
		ReferenceExternalTransactionID: req.ReferenceExternalTransactionID,
		CorrelationID:                  corr, CausationID: keys[0],
	})
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	t := out.Transaction
	ctx := observability.With(r.Context(), slog.String("transactionId", t.ID().String()), slog.String("walletId", t.WalletID().String()))
	s.Log.InfoContext(ctx, "wager transaction handled", slog.String("status", string(t.Status())),
		slog.String("kind", string(t.Kind())), slog.Bool("replay", out.Replay))

	resp := outcomeResponse{TransactionID: t.ID(), Status: string(t.Status()), IdempotentReplay: out.Replay,
		Balance: t.ResultBalance(), FailureCode: string(t.FailureCode()), FailureMessage: t.FailureMessage()}
	switch t.Status() {
	case domain.StatusProcessed:
		writeJSON(w, http.StatusOK, resp)
	case domain.StatusPending, domain.StatusPendingReference:
		if t.ExpiresAt() != nil {
			resp.ExpiresAt = t.ExpiresAt().Format(domain.EventTimeLayout)
		}
		if t.NextAttemptAt() != nil {
			resp.NextAttemptAt = t.NextAttemptAt().Format(domain.EventTimeLayout)
		}
		w.Header().Set("Location", "/wagering/transactions/"+t.ID().String())
		writeJSON(w, http.StatusAccepted, resp)
	case domain.StatusRejected:
		writeJSON(w, http.StatusUnprocessableEntity, resp)
	default:
		writeJSON(w, http.StatusInternalServerError, resp)
	}
}

type transactionView struct {
	TransactionID                  uuid.UUID     `json:"transactionId"`
	Status                         string        `json:"status"`
	Kind                           string        `json:"kind"`
	ProviderID                     string        `json:"providerId"`
	ExternalTransactionID          string        `json:"externalTransactionId"`
	IdempotencyKey                 string        `json:"idempotencyKey"`
	WalletID                       uuid.UUID     `json:"walletId"`
	PlayerID                       uuid.UUID     `json:"playerId"`
	RoundID                        string        `json:"roundId"`
	GameID                         string        `json:"gameId"`
	Money                          domain.Money  `json:"money"`
	ReferenceExternalTransactionID string        `json:"referenceExternalTransactionId,omitempty"`
	ReferenceTransactionID         *uuid.UUID    `json:"referenceTransactionId,omitempty"`
	Balance                        *domain.Money `json:"balance,omitempty"`
	FailureCode                    string        `json:"failureCode,omitempty"`
	FailureMessage                 string        `json:"failureMessage,omitempty"`
	Attempts                       int           `json:"attempts"`
	NextAttemptAt                  string        `json:"nextAttemptAt,omitempty"`
	ExpiresAt                      string        `json:"expiresAt,omitempty"`
	CreatedAt                      string        `json:"createdAt"`
	UpdatedAt                      string        `json:"updatedAt"`
	ProcessedAt                    string        `json:"processedAt,omitempty"`
}

func toTransactionView(t *domain.WagerTransaction) transactionView {
	fmtTime := func(p *time.Time) string {
		if p == nil {
			return ""
		}
		return p.UTC().Format(domain.EventTimeLayout)
	}
	return transactionView{
		TransactionID: t.ID(), Status: string(t.Status()), Kind: string(t.Kind()), ProviderID: t.ProviderID(),
		ExternalTransactionID: t.ExternalTransactionID(), IdempotencyKey: t.IdempotencyKey(), WalletID: t.WalletID(),
		PlayerID: t.PlayerID(), RoundID: t.RoundID(), GameID: t.GameID(), Money: t.Money(),
		ReferenceExternalTransactionID: t.ReferenceExternalID(), ReferenceTransactionID: t.ReferenceTransactionID(),
		Balance: t.ResultBalance(), FailureCode: string(t.FailureCode()), FailureMessage: t.FailureMessage(),
		Attempts: t.Attempts(), NextAttemptAt: fmtTime(t.NextAttemptAt()), ExpiresAt: fmtTime(t.ExpiresAt()),
		CreatedAt: t.CreatedAt().Format(domain.EventTimeLayout), UpdatedAt: t.UpdatedAt().Format(domain.EventTimeLayout),
		ProcessedAt: fmtTime(t.ProcessedAt()),
	}
}

func (s *Server) getTransaction(w http.ResponseWriter, r *http.Request, p *auth.Principal) {
	id, err := domain.ParseID("transactionId", r.PathValue("transactionId"))
	if err != nil {
		writeAPIError(w, r, http.StatusNotFound, "TRANSACTION_NOT_FOUND", "transaction not found", "", false)
		return
	}
	t, err := s.Wagering.GetTransaction(r.Context(), id)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	// Transação de outro provedor responde 404, não 403: um 403 confirmaria que a
	// transação existe, vazando informação entre provedores.
	if !p.IsInternal() && t.ProviderID() != p.ProviderID {
		writeAPIError(w, r, http.StatusNotFound, "TRANSACTION_NOT_FOUND", "transaction not found", "", false)
		return
	}
	writeJSON(w, http.StatusOK, toTransactionView(t))
}

func (s *Server) getProviderTransaction(w http.ResponseWriter, r *http.Request, p *auth.Principal) {
	providerID := r.PathValue("providerId")
	if !p.IsInternal() && providerID != p.ProviderID {
		writeAPIError(w, r, http.StatusForbidden, "FORBIDDEN", "the caller is not allowed to read another provider's transactions", "providerId", false)
		return
	}
	t, err := s.Wagering.GetProviderTransaction(r.Context(), providerID, r.PathValue("externalTransactionId"))
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toTransactionView(t))
}
