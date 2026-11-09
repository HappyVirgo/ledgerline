// Command ledgerline runs the payments API server or administers tenants.
//
//	ledgerline create-tenant -db ledgerline.db -name "Acme Test Co"
//	ledgerline serve -db ledgerline.db -addr :8080
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/HappyVirgo/ledgerline/internal/api"
	"github.com/HappyVirgo/ledgerline/internal/payments"
	"github.com/HappyVirgo/ledgerline/internal/processor"
	"github.com/HappyVirgo/ledgerline/internal/store"
	"github.com/HappyVirgo/ledgerline/internal/tenant"
	"github.com/HappyVirgo/ledgerline/internal/webhook"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: ledgerline <serve|create-tenant> [flags]")
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "serve":
		err = serve(os.Args[2:])
	case "create-tenant":
		err = createTenant(os.Args[2:])
	default:
		err = fmt.Errorf("unknown command %q", os.Args[1])
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func createTenant(args []string) error {
	fs := flag.NewFlagSet("create-tenant", flag.ExitOnError)
	dbPath := fs.String("db", "ledgerline.db", "SQLite database path")
	name := fs.String("name", "", "tenant display name")
	fs.Parse(args)
	if *name == "" {
		return errors.New("-name is required")
	}
	db, err := store.Open(*dbPath)
	if err != nil {
		return err
	}
	defer db.Close()
	id, key, err := tenant.Create(context.Background(), db, *name)
	if err != nil {
		return err
	}
	fmt.Printf("tenant_id=%s\napi_key=%s\n(the api key is shown once; only its SHA-256 hash is stored)\n", id, key)
	return nil
}

func serve(args []string) error {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	dbPath := fs.String("db", "ledgerline.db", "SQLite database path")
	addr := fs.String("addr", ":8080", "listen address")
	rate := fs.Float64("rate", 50, "per-tenant requests per second")
	burst := fs.Int("burst", 100, "per-tenant burst size")
	fs.Parse(args)

	db, err := store.Open(*dbPath)
	if err != nil {
		return err
	}
	defer db.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	disp := &webhook.Dispatcher{
		DB: db, Send: webhook.HTTPSender(&http.Client{Timeout: 10 * time.Second}), Now: time.Now,
		BaseBackoff: 5 * time.Second, MaxBackoff: time.Hour, MaxAttempts: 10, Lease: time.Minute, BatchSize: 50,
	}
	go disp.Run(ctx, time.Second)

	srv := &api.Server{DB: db, Payments: &payments.Service{DB: db, Proc: processor.NewMock()},
		Limiter: tenant.NewRateLimiter(*rate, *burst)}
	httpSrv := &http.Server{Addr: *addr, Handler: srv.Handler(), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		httpSrv.Shutdown(shutdownCtx)
	}()
	slog.Info("ledgerline listening", "addr", *addr, "db", *dbPath)
	if err := httpSrv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
