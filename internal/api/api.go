// Package api exposes the ledgerline HTTP API.
package api

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"

	"github.com/HappyVirgo/ledgerline/internal/ledger"
	"github.com/HappyVirgo/ledgerline/internal/money"
	"github.com/HappyVirgo/ledgerline/internal/payments"
	"github.com/HappyVirgo/ledgerline/internal/processor"
	"github.com/HappyVirgo/ledgerline/internal/recon"
	"github.com/HappyVirgo/ledgerline/internal/tenant"
	"github.com/HappyVirgo/ledgerline/internal/webhook"
)

const maxBody = 1 << 20

// Server wires HTTP routes to the domain services.
type Server struct {
	DB       *sql.DB
	Payments *payments.Service
	Limiter  *tenant.RateLimiter

	keyLocks [256]sync.Mutex // striped locks serialising requests that share an idempotency key
}

// handlerFunc is an authenticated handler that returns a status code and a JSON body.
type handlerFunc func(r *http.Request, tenantID string, body []byte) (int, any)

type errorBody struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

func errBody(code, msg string) errorBody {
	var b errorBody
	b.Error.Code, b.Error.Message = code, msg
	return b
}

func errResp(status int, code, msg string) (int, any) { return status, errBody(code, msg) }

// Handler returns the router.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok")) })
	mux.Handle("POST /v1/payment_intents", s.auth(s.idempotent(s.createIntent)))
	mux.Handle("GET /v1/payment_intents/{id}", s.auth(s.plain(s.getIntent)))
	mux.Handle("POST /v1/payment_intents/{id}/capture", s.auth(s.idempotent(s.capture)))
	mux.Handle("POST /v1/payment_intents/{id}/refunds", s.auth(s.idempotent(s.refund)))
	mux.Handle("GET /v1/accounts", s.auth(s.plain(s.accounts)))
	mux.Handle("PUT /v1/webhook_endpoint", s.auth(s.plain(s.setWebhook)))
	mux.Handle("POST /v1/reconciliations", s.auth(s.plain(s.reconcile)))
	return mux
}

type ctxKey struct{}

// auth resolves the bearer API key to a tenant and applies the tenant's rate limit.
func (s *Server) auth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !ok {
			writeJSON(w, nil, 401, errBody("unauthorized", "missing bearer api key"))
			return
		}
		tenantID, err := tenant.Authenticate(r.Context(), s.DB, key)
		if errors.Is(err, tenant.ErrInvalidKey) {
			writeJSON(w, nil, 401, errBody("unauthorized", "invalid api key"))
			return
		} else if err != nil {
			writeJSON(w, nil, 500, errBody("internal", "authentication failed"))
			return
		}
		if ok, wait := s.Limiter.Allow(tenantID); !ok {
			w.Header().Set("Retry-After", strconv.Itoa(int(wait.Seconds())+1))
			writeJSON(w, nil, 429, errBody("rate_limited", "too many requests"))
			return
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), ctxKey{}, tenantID)))
	})
}

// plain adapts a handlerFunc without idempotency handling.
func (s *Server) plain(h handlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBody))
		if err != nil {
			writeJSON(w, nil, 413, errBody("body_too_large", err.Error()))
			return
		}
		status, resp := h(r, r.Context().Value(ctxKey{}).(string), body)
		writeJSON(w, nil, status, resp)
	})
}

// writeJSON writes raw if non-nil, otherwise the JSON encoding of v.
func writeJSON(w http.ResponseWriter, raw []byte, status int, v any) {
	if raw == nil {
		var err error
		if raw, err = json.Marshal(v); err != nil {
			status, raw = 500, []byte(`{"error":{"code":"internal","message":"encode failed"}}`)
		}
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	w.Write(raw)
}

func decode(body []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	return dec.Decode(v)
}

// domainError maps service errors to HTTP responses.
func domainError(err error) (int, any) {
	var decline *processor.DeclineError
	switch {
	case errors.Is(err, payments.ErrNotFound):
		return errResp(404, "not_found", err.Error())
	case errors.Is(err, payments.ErrInvalidState):
		return errResp(409, "invalid_state", err.Error())
	case errors.Is(err, payments.ErrInvalidAmount), errors.Is(err, money.ErrUnsupportedCurrency):
		return errResp(400, "invalid_request", err.Error())
	case errors.As(err, &decline):
		return errResp(402, decline.Code, err.Error())
	case errors.Is(err, processor.ErrTimeout):
		return errResp(504, "processor_timeout", err.Error())
	}
	return errResp(500, "internal", "internal error")
}

func (s *Server) createIntent(r *http.Request, tenantID string, body []byte) (int, any) {
	var p payments.CreateParams
	if err := decode(body, &p); err != nil {
		return errResp(400, "invalid_json", err.Error())
	}
	in, err := s.Payments.Create(r.Context(), tenantID, p)
	if err != nil {
		return domainError(err)
	}
	if in.Status != payments.StatusAuthorized {
		return 402, in
	}
	return 201, in
}

func (s *Server) getIntent(r *http.Request, tenantID string, _ []byte) (int, any) {
	in, err := s.Payments.Get(r.Context(), tenantID, r.PathValue("id"))
	if err != nil {
		return domainError(err)
	}
	return 200, in
}

type amountParams struct {
	Amount int64 `json:"amount"`
}

func parseAmount(body []byte) (int64, error) {
	var p amountParams
	if len(bytes.TrimSpace(body)) == 0 {
		return 0, nil
	}
	err := decode(body, &p)
	return p.Amount, err
}

func (s *Server) capture(r *http.Request, tenantID string, body []byte) (int, any) {
	amt, err := parseAmount(body)
	if err != nil {
		return errResp(400, "invalid_json", err.Error())
	}
	in, err := s.Payments.Capture(r.Context(), tenantID, r.PathValue("id"), amt)
	if err != nil {
		return domainError(err)
	}
	return 200, in
}

func (s *Server) refund(r *http.Request, tenantID string, body []byte) (int, any) {
	amt, err := parseAmount(body)
	if err != nil {
		return errResp(400, "invalid_json", err.Error())
	}
	in, err := s.Payments.Refund(r.Context(), tenantID, r.PathValue("id"), amt)
	if err != nil {
		return domainError(err)
	}
	return 200, in
}

func (s *Server) accounts(r *http.Request, tenantID string, _ []byte) (int, any) {
	accts, err := ledger.Accounts(r.Context(), s.DB, tenantID)
	if err != nil {
		return domainError(err)
	}
	if accts == nil {
		accts = []ledger.Account{}
	}
	return 200, map[string]any{"accounts": accts}
}

func (s *Server) setWebhook(r *http.Request, tenantID string, body []byte) (int, any) {
	var p struct {
		URL string `json:"url"`
	}
	if err := decode(body, &p); err != nil {
		return errResp(400, "invalid_json", err.Error())
	}
	if u, err := url.Parse(p.URL); err != nil || (u.Scheme != "https" && u.Scheme != "http") || u.Host == "" {
		return errResp(400, "invalid_request", "url must be an absolute http(s) URL")
	}
	secret, err := webhook.SetEndpoint(r.Context(), s.DB, tenantID, p.URL)
	if err != nil {
		return domainError(err)
	}
	return 200, map[string]string{"url": p.URL, "secret": secret}
}

func (s *Server) reconcile(r *http.Request, tenantID string, body []byte) (int, any) {
	rows, err := recon.ParseCSV(bytes.NewReader(body))
	if err != nil {
		return errResp(400, "invalid_csv", err.Error())
	}
	intents, err := s.Payments.Captured(r.Context(), tenantID)
	if err != nil {
		return domainError(err)
	}
	captured := make([]recon.Captured, len(intents))
	for i, in := range intents {
		captured[i] = recon.Captured{PaymentID: in.ID, Ref: in.CaptureRef, Amount: in.AmountCaptured, Currency: in.Currency}
	}
	return 200, recon.Reconcile(rows, captured)
}
