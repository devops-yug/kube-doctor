package main

import (
	"context"
	"errors"
	"net/http"
	"os"
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

	// 2. Custom PORT, TLS_PORT, TLS_CERT_FILE and --mode=hub
	tmpCert := t.TempDir() + "/tls.crt"
	_ = os.WriteFile(tmpCert, []byte("invalid-cert-for-fast-exit"), 0600)
	t.Setenv("PORT", "9090")
	t.Setenv("TLS_PORT", "19443")
	t.Setenv("TLS_CERT_FILE", tmpCert)
	t.Setenv("TLS_KEY_FILE", tmpCert)
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
