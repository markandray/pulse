package main

import (
    "net/http"
    "net/http/httptest"
    "sync/atomic"
    "testing"
)

func TestGateBlocksUntilReady(t *testing.T) {
    var ready atomic.Bool
    h := gate(&ready, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        w.WriteHeader(http.StatusOK)
    }))

    rec := httptest.NewRecorder()
    h.ServeHTTP(rec, httptest.NewRequest("GET", "/", nil))
    if rec.Code != http.StatusServiceUnavailable {
        t.Fatalf("before ready: status %d, want 503", rec.Code)
    }

    ready.Store(true)
    rec = httptest.NewRecorder()
    h.ServeHTTP(rec, httptest.NewRequest("GET", "/", nil))
    if rec.Code != http.StatusOK {
        t.Fatalf("when ready: status %d, want 200", rec.Code)
    }

    ready.Store(false) // shutdown started
    rec = httptest.NewRecorder()
    h.ServeHTTP(rec, httptest.NewRequest("GET", "/", nil))
    if rec.Code != http.StatusServiceUnavailable {
        t.Fatalf("while draining: status %d, want 503", rec.Code)
    }
}