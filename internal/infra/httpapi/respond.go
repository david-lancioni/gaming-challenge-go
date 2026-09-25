package httpapi

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net/http"

	"github.com/dlancioni/backend-challenge-go/internal/application"
	"github.com/dlancioni/backend-challenge-go/internal/domain"
	"github.com/dlancioni/backend-challenge-go/internal/observability"
)

func writeJSON(w http.ResponseWriter, status int, body any) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(body); err != nil {
		http.Error(w, `{"error":{"code":"INTERNAL_ERROR","message":"encoding failure"}}`, http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(buf.Bytes())
}

type apiError struct {
	Code          string `json:"code"`
	Message       string `json:"message"`
	Field         string `json:"field,omitempty"`
	Retryable     bool   `json:"retryable"`
	CorrelationID string `json:"correlationId,omitempty"`
}

func writeAPIError(w http.ResponseWriter, r *http.Request, status int, code, message, field string, retryable bool) {
	corr := w.Header().Get("X-Correlation-Id")
	if retryable {
		w.Header().Set("Retry-After", "1")
	}
	writeJSON(w, status, map[string]apiError{"error": {
		Code: code, Message: message, Field: field, Retryable: retryable, CorrelationID: corr,
	}})
}

func (s *Server) writeError(w http.ResponseWriter, r *http.Request, err error) {
	var verr *domain.ValidationError
	switch {
	case errors.As(err, &verr):
		code := "INVALID_REQUEST"
		if domain.IsMoneyError(err) {
			code = "INVALID_MONEY"
		}
		writeAPIError(w, r, http.StatusBadRequest, code, verr.Error(), verr.Field, false)
	case errors.Is(err, application.ErrInvalidCursor):
		writeAPIError(w, r, http.StatusBadRequest, "INVALID_CURSOR", "the cursor is not valid", "cursor", false)
	case errors.Is(err, domain.ErrIdempotencyConflict):
		writeAPIError(w, r, http.StatusConflict, "IDEMPOTENCY_KEY_CONFLICT",
			"the idempotency key was already used with a different payload", "Idempotency-Key", false)
	case errors.Is(err, domain.ErrExternalTransactionConflict):
		writeAPIError(w, r, http.StatusConflict, "EXTERNAL_TRANSACTION_CONFLICT",
			"the external transaction was already registered under another idempotency key", "externalTransactionId", false)
	case errors.Is(err, domain.ErrWalletAlreadyExists):
		writeAPIError(w, r, http.StatusConflict, "WALLET_ALREADY_EXISTS",
			"a wallet already exists for this player and currency", "", false)
	case errors.Is(err, domain.ErrWalletNotFound):
		writeAPIError(w, r, http.StatusNotFound, "WALLET_NOT_FOUND", "wallet not found", "walletId", false)
	case errors.Is(err, domain.ErrTransactionNotFound):
		writeAPIError(w, r, http.StatusNotFound, "TRANSACTION_NOT_FOUND", "transaction not found", "", false)
	case errors.Is(err, application.ErrTransient), errors.Is(err, application.ErrConcurrentModification):
		s.Log.WarnContext(r.Context(), "transient failure", slog.String("error", err.Error()))
		writeAPIError(w, r, http.StatusServiceUnavailable, "SERVICE_UNAVAILABLE",
			"temporarily unavailable, retry with the same Idempotency-Key", "", true)
	default:
		s.Log.ErrorContext(observability.With(r.Context()), "unexpected error", slog.String("error", err.Error()))
		writeAPIError(w, r, http.StatusInternalServerError, "INTERNAL_ERROR", "internal error", "", false)
	}
}

func decodeJSON(w http.ResponseWriter, r *http.Request, dst any) error {
	if ct := r.Header.Get("Content-Type"); ct != "" {
		if mt, _, err := mime.ParseMediaType(ct); err != nil || mt != "application/json" {
			return &unsupportedMediaType{}
		}
	} else {
		return &unsupportedMediaType{}
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			return &bodyTooLarge{}
		}
		if domain.IsMoneyError(err) {
			return domain.NewValidationErrorWrap("money", err)
		}
		return domain.NewValidationError("body", fmt.Sprintf("malformed JSON: %v", err))
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return domain.NewValidationError("body", "unexpected data after the JSON object")
	}
	return nil
}

type unsupportedMediaType struct{}

func (*unsupportedMediaType) Error() string { return "content type must be application/json" }

type bodyTooLarge struct{}

func (*bodyTooLarge) Error() string { return "request body too large" }

func (s *Server) writeDecodeError(w http.ResponseWriter, r *http.Request, err error) {
	var ump *unsupportedMediaType
	var big *bodyTooLarge
	switch {
	case errors.As(err, &ump):
		writeAPIError(w, r, http.StatusUnsupportedMediaType, "UNSUPPORTED_MEDIA_TYPE", err.Error(), "Content-Type", false)
	case errors.As(err, &big):
		writeAPIError(w, r, http.StatusRequestEntityTooLarge, "BODY_TOO_LARGE", err.Error(), "", false)
	default:
		s.writeError(w, r, err)
	}
}
