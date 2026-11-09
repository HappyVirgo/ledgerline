// Package tenant manages tenants, their hashed API keys and per-tenant rate limits.
package tenant

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/HappyVirgo/ledgerline/internal/store"
)

// ErrInvalidKey is returned when an API key is unknown or malformed.
var ErrInvalidKey = errors.New("invalid api key")

const keyPrefix = "ll_test_"

// hashKey returns the hex SHA-256 of an API key. Keys carry 256 bits of randomness,
// so a fast hash is sufficient (no password-style stretching needed).
func hashKey(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])
}

// Create inserts a tenant and issues its first API key. The plaintext key is
// returned once and only its hash is stored.
func Create(ctx context.Context, db *sql.DB, name string) (tenantID, apiKey string, err error) {
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", "", err
	}
	apiKey = keyPrefix + hex.EncodeToString(raw)
	tenantID = store.NewID("ten_")
	now := time.Now().Unix()
	err = store.InTx(ctx, db, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `INSERT INTO tenants (id, name, created_at) VALUES (?, ?, ?)`,
			tenantID, name, now); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `INSERT INTO api_keys (key_hash, tenant_id, prefix, created_at) VALUES (?, ?, ?, ?)`,
			hashKey(apiKey), tenantID, apiKey[:len(keyPrefix)+6], now)
		return err
	})
	return tenantID, apiKey, err
}

// Authenticate resolves an API key to its tenant id.
func Authenticate(ctx context.Context, q store.DBTX, apiKey string) (string, error) {
	if !strings.HasPrefix(apiKey, keyPrefix) {
		return "", ErrInvalidKey
	}
	var tenantID string
	err := q.QueryRowContext(ctx, `SELECT tenant_id FROM api_keys WHERE key_hash = ?`, hashKey(apiKey)).Scan(&tenantID)
	if errors.Is(err, sql.ErrNoRows) {
		return "", ErrInvalidKey
	}
	return tenantID, err
}

// RateLimiter is an in-memory token bucket per tenant.
type RateLimiter struct {
	rate  float64 // tokens per second
	burst float64
	now   func() time.Time

	mu      sync.Mutex
	buckets map[string]*bucket
}

type bucket struct {
	tokens float64
	last   time.Time
}

// NewRateLimiter allows `rate` requests per second per tenant with bursts up to `burst`.
func NewRateLimiter(rate float64, burst int) *RateLimiter {
	return &RateLimiter{rate: rate, burst: float64(burst), now: time.Now, buckets: map[string]*bucket{}}
}

// Allow consumes one token for tenantID. When the bucket is empty it returns false
// and how long until the next token is available.
func (r *RateLimiter) Allow(tenantID string) (bool, time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.now()
	b, ok := r.buckets[tenantID]
	if !ok {
		b = &bucket{tokens: r.burst, last: now}
		r.buckets[tenantID] = b
	}
	b.tokens = math.Min(r.burst, b.tokens+now.Sub(b.last).Seconds()*r.rate)
	b.last = now
	if b.tokens >= 1 {
		b.tokens--
		return true, 0
	}
	wait := time.Duration((1 - b.tokens) / r.rate * float64(time.Second))
	return false, wait
}
