package main

import (
	"context"
	"errors"
	"net/http"
	"testing"
)

func TestRunEntrypoint(t *testing.T) {
	t.Setenv("KUBE_SIMULATION_MODE", "true")
	t.Setenv("DISABLE_UPSTREAM_LLM", "true")
	t.Setenv("PORT", "")
	t.Setenv("KNOWLEDGE_BASE_PATH", "")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. Default standalone mode & port
	var boundAddr string
	err := run(ctx, nil, func(srv *http.Server) error {
		boundAddr = srv.Addr
		return http.ErrServerClosed
	})
	if err != nil || boundAddr != ":8080" {
		t.Fatalf("expected :8080 and nil error, got addr=%q err=%v", boundAddr, err)
	}

	// 2. Custom PORT and --mode=hub
	t.Setenv("PORT", "9090")
	err = run(ctx, []string{"--mode=hub"}, func(srv *http.Server) error {
		boundAddr = srv.Addr
		return nil
	})
	if err != nil || boundAddr != ":9090" {
		t.Fatalf("expected :9090 and nil error, got addr=%q err=%v", boundAddr, err)
	}

	// 3. Invalid flag error
	if err := run(ctx, []string{"--invalid-flag"}, func(srv *http.Server) error { return nil }); err == nil {
		t.Fatalf("expected error on invalid flag")
	}

	// 4. Listen error propagation
	if err := run(ctx, nil, func(srv *http.Server) error { return errors.New("bind failed") }); err == nil {
		t.Fatalf("expected error when listenFn fails")
	}
}
