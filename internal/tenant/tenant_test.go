package tenant

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/HappyVirgo/ledgerline/internal/store"
)

func TestCreateAndAuthenticate(t *testing.T) {
	db, err := store.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	id, key, err := Create(ctx, db, "Test Tenant")
	if err != nil {
		t.Fatal(err)
	}
	got, err := Authenticate(ctx, db, key)
	if err != nil || got != id {
		t.Fatalf("Authenticate = %q, %v; want %q", got, err, id)
	}
	var stored string
	if err := db.QueryRow(`SELECT key_hash FROM api_keys`).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(stored, key[len(keyPrefix):]) {
		t.Fatal("plaintext api key stored at rest")
	}
	for _, bad := range []string{"", "nope", keyPrefix + "00", key + "x"} {
		if _, err := Authenticate(ctx, db, bad); !errors.Is(err, ErrInvalidKey) {
			t.Errorf("Authenticate(%q) err = %v; want ErrInvalidKey", bad, err)
		}
	}
}

func TestRateLimiter(t *testing.T) {
	now := time.Unix(1000, 0)
	rl := NewRateLimiter(2, 3)
	rl.now = func() time.Time { return now }

	for i := 0; i < 3; i++ {
		if ok, _ := rl.Allow("a"); !ok {
			t.Fatalf("request %d within burst denied", i)
		}
	}
	ok, wait := rl.Allow("a")
	if ok || wait != 500*time.Millisecond {
		t.Fatalf("over burst: ok=%v wait=%v; want denied with 500ms", ok, wait)
	}
	if ok, _ := rl.Allow("b"); !ok {
		t.Fatal("tenant b must have its own bucket")
	}
	now = now.Add(time.Second) // refills 2 tokens
	for i := 0; i < 2; i++ {
		if ok, _ := rl.Allow("a"); !ok {
			t.Fatalf("refilled request %d denied", i)
		}
	}
	if ok, _ := rl.Allow("a"); ok {
		t.Fatal("bucket should be empty again")
	}
}
