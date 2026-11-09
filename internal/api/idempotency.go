package api

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"hash/fnv"
	"io"
	"net/http"
	"sync"
	"time"
)

// IdempotencyHeader names the client-chosen key that makes a POST safe to retry.
const IdempotencyHeader = "Idempotency-Key"

// idempotent wraps a mutating handler. For a given tenant and key:
//   - the first request runs and its response is stored;
//   - a retry with the same method, path and body replays the stored response;
//   - a request with a different method, path or body gets 409;
//   - a request arriving while the first is still running waits for it (same process)
//     or gets 409 (another process).
//
// 5xx responses are not stored, so the client may retry after a transient failure.
// The in-progress row is committed before the handler runs, so a crash mid-request
// leaves the key locked rather than allowing a duplicate charge.
func (s *Server) idempotent(h handlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tenantID := r.Context().Value(ctxKey{}).(string)
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBody))
		if err != nil {
			writeJSON(w, nil, 413, errBody("body_too_large", err.Error()))
			return
		}
		key := r.Header.Get(IdempotencyHeader)
		if key == "" {
			status, resp := h(r, tenantID, body)
			writeJSON(w, nil, status, resp)
			return
		}
		if len(key) > 255 {
			writeJSON(w, nil, 400, errBody("invalid_request", "idempotency key too long"))
			return
		}
		lock := s.keyLock(tenantID, key)
		lock.Lock()
		defer lock.Unlock()

		sum := sha256.Sum256([]byte(r.Method + " " + r.URL.Path + "\n" + string(body)))
		reqHash := hex.EncodeToString(sum[:])
		ctx := r.Context()

		var storedHash string
		var storedStatus int
		var stored []byte
		err = s.DB.QueryRowContext(ctx,
			`SELECT request_hash, status_code, response FROM idempotency_keys WHERE tenant_id = ? AND key = ?`,
			tenantID, key).Scan(&storedHash, &storedStatus, &stored)
		switch {
		case err == nil && storedHash != reqHash:
			writeJSON(w, nil, 409, errBody("idempotency_key_reused", "idempotency key was used with a different request"))
			return
		case err == nil && storedStatus == 0:
			writeJSON(w, nil, 409, errBody("idempotency_in_progress", "a request with this key is still in progress"))
			return
		case err == nil:
			w.Header().Set("Idempotent-Replayed", "true")
			writeJSON(w, stored, storedStatus, nil)
			return
		case !errors.Is(err, sql.ErrNoRows):
			writeJSON(w, nil, 500, errBody("internal", "idempotency lookup failed"))
			return
		}

		if _, err := s.DB.ExecContext(ctx,
			`INSERT INTO idempotency_keys (tenant_id, key, request_hash, created_at) VALUES (?, ?, ?, ?)`,
			tenantID, key, reqHash, time.Now().Unix()); err != nil {
			writeJSON(w, nil, 409, errBody("idempotency_in_progress", "a request with this key is still in progress"))
			return
		}
		status, resp := h(r, tenantID, body)
		raw, err := json.Marshal(resp)
		if err != nil {
			status, raw = 500, []byte(`{"error":{"code":"internal","message":"encode failed"}}`)
		}
		if status >= 500 {
			_, err = s.DB.ExecContext(ctx, `DELETE FROM idempotency_keys WHERE tenant_id = ? AND key = ?`, tenantID, key)
		} else {
			_, err = s.DB.ExecContext(ctx,
				`UPDATE idempotency_keys SET status_code = ?, response = ? WHERE tenant_id = ? AND key = ?`,
				status, raw, tenantID, key)
		}
		if err != nil {
			status, raw = 500, []byte(`{"error":{"code":"internal","message":"idempotency save failed"}}`)
		}
		writeJSON(w, raw, status, nil)
	})
}

func (s *Server) keyLock(tenantID, key string) *sync.Mutex {
	h := fnv.New32a()
	h.Write([]byte(tenantID))
	h.Write([]byte{0})
	h.Write([]byte(key))
	return &s.keyLocks[h.Sum32()%uint32(len(s.keyLocks))]
}
