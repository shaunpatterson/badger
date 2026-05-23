#!/bin/bash
# Guard: run short race tests, excluding the known-broken protoc test.
# Excludes pb package (requires protoc which isn't installed locally).
# Exit 0 = pass, non-zero = fail.
set -eo pipefail
PKGS=$(go list ./... | grep -v '/pb$' | grep -v '/badger/cmd$')
go test -short -race -count=1 -timeout=10m -skip='TestProtosRegenerate' $PKGS
