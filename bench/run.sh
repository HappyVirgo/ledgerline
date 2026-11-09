#!/usr/bin/env bash
# Reproduces bench/RESULTS.md: the load driver followed by the Go micro-benchmarks.
set -euo pipefail
cd "$(dirname "$0")/.."
go run ./cmd/loadgen -n "${N:-5000}" -workers "${WORKERS:-8}" -tenants 4 -db file -out bench/RESULTS.md
{
  echo
  echo '## Go benchmarks'
  echo
  echo 'Command: `go test -run ^$ -bench . -benchmem -count 3 ./internal/ledger ./internal/api`'
  echo
  echo '```'
  go test -run '^$' -bench . -benchmem -count 3 ./internal/ledger ./internal/api
  echo '```'
} | tee -a bench/RESULTS.md
