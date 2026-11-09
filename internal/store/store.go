// Package store opens the SQLite database and owns the schema.
package store

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"

	_ "modernc.org/sqlite" // pure-Go SQLite driver, no cgo.
)

// DBTX is satisfied by both *sql.DB and *sql.Tx so helpers can run inside a transaction.
type DBTX interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

const schema = `
CREATE TABLE IF NOT EXISTS tenants (
	id TEXT PRIMARY KEY,
	name TEXT NOT NULL,
	created_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS api_keys (
	key_hash TEXT PRIMARY KEY,
	tenant_id TEXT NOT NULL REFERENCES tenants(id),
	prefix TEXT NOT NULL,
	created_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS accounts (
	id INTEGER PRIMARY KEY,
	tenant_id TEXT NOT NULL REFERENCES tenants(id),
	code TEXT NOT NULL,
	type TEXT NOT NULL CHECK (type IN ('asset','liability','revenue','expense')),
	currency TEXT NOT NULL,
	balance INTEGER NOT NULL DEFAULT 0,
	UNIQUE (tenant_id, code, currency)
);
CREATE TABLE IF NOT EXISTS journal_entries (
	id INTEGER PRIMARY KEY,
	tenant_id TEXT NOT NULL REFERENCES tenants(id),
	currency TEXT NOT NULL,
	memo TEXT NOT NULL,
	created_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS journal_lines (
	id INTEGER PRIMARY KEY,
	entry_id INTEGER NOT NULL REFERENCES journal_entries(id),
	account_id INTEGER NOT NULL REFERENCES accounts(id),
	side TEXT NOT NULL CHECK (side IN ('D','C')),
	amount INTEGER NOT NULL CHECK (amount > 0)
);
CREATE INDEX IF NOT EXISTS journal_lines_account ON journal_lines(account_id);
CREATE TABLE IF NOT EXISTS payment_intents (
	id TEXT PRIMARY KEY,
	tenant_id TEXT NOT NULL REFERENCES tenants(id),
	amount INTEGER NOT NULL CHECK (amount > 0),
	currency TEXT NOT NULL,
	status TEXT NOT NULL,
	card_last4 TEXT NOT NULL,
	auth_ref TEXT NOT NULL DEFAULT '',
	capture_ref TEXT NOT NULL DEFAULT '',
	amount_captured INTEGER NOT NULL DEFAULT 0,
	amount_refunded INTEGER NOT NULL DEFAULT 0,
	failure_code TEXT NOT NULL DEFAULT '',
	created_at INTEGER NOT NULL,
	CHECK (amount_refunded <= amount_captured AND amount_captured <= amount)
);
CREATE INDEX IF NOT EXISTS payment_intents_tenant ON payment_intents(tenant_id, status);
CREATE TABLE IF NOT EXISTS idempotency_keys (
	tenant_id TEXT NOT NULL,
	key TEXT NOT NULL,
	request_hash TEXT NOT NULL,
	status_code INTEGER NOT NULL DEFAULT 0,
	response BLOB,
	created_at INTEGER NOT NULL,
	PRIMARY KEY (tenant_id, key)
);
CREATE TABLE IF NOT EXISTS webhook_endpoints (
	tenant_id TEXT PRIMARY KEY REFERENCES tenants(id),
	url TEXT NOT NULL,
	secret TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS outbox (
	id INTEGER PRIMARY KEY,
	tenant_id TEXT NOT NULL,
	event_type TEXT NOT NULL,
	payload BLOB NOT NULL,
	status TEXT NOT NULL DEFAULT 'pending',
	attempts INTEGER NOT NULL DEFAULT 0,
	next_attempt_at INTEGER NOT NULL,
	last_error TEXT NOT NULL DEFAULT '',
	created_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS outbox_due ON outbox(status, next_attempt_at);
`

// Open opens (creating if needed) the database at path and applies the schema.
// Use ":memory:" for an ephemeral database. The pool is limited to one connection,
// which serialises writers: SQLite allows a single writer anyway, and it keeps
// in-memory databases shared across callers.
func Open(path string) (*sql.DB, error) {
	dsn := "file:" + path + "?_pragma=foreign_keys(1)&_pragma=busy_timeout(5000)"
	if path != ":memory:" {
		dsn += "&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)"
	}
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("apply schema: %w", err)
	}
	return db, nil
}

// NewID returns prefix + 24 random hex characters, e.g. "pi_3f9c...".
func NewID(prefix string) string {
	b := make([]byte, 12)
	if _, err := rand.Read(b); err != nil {
		panic(err) // crypto/rand never fails on supported platforms
	}
	return prefix + hex.EncodeToString(b)
}

// InTx runs fn in a transaction, committing on success and rolling back on error.
func InTx(ctx context.Context, db *sql.DB, fn func(tx *sql.Tx) error) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}
