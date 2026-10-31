package main

import (
    "context"
    "errors"
    "log"
    "net/http"
    "os"
    "os/signal"
    "strings"
    "sync/atomic"
    "syscall"
    "time"

    "github.com/markandray/pulse/internal/eventlog"
    "github.com/markandray/pulse/internal/hub"
    "github.com/markandray/pulse/internal/store"
    "github.com/prometheus/client_golang/prometheus/promhttp"
)

func env(key, def string) string {
    if v := os.Getenv(key); v != "" {
        return v
    }
    return def
}

func mustDuration(key, def string) time.Duration {
    d, err := time.ParseDuration(env(key, def))
    if err != nil {
        log.Fatalf("%s: %v", key, err)
    }
    return d
}

// gate answers 503 until the instance is ready to serve traffic.
func gate(ready *atomic.Bool, next http.Handler) http.Handler {
    return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        if !ready.Load() {
            http.Error(w, "starting up or shutting down", http.StatusServiceUnavailable)
            return
        }
        next.ServeHTTP(w, r)
    })
}

func main() {
    ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
    defer stop()

    h := hub.New()
    // Metrics get their own port, which is not routed through NGINX, so they
    // are not exposed to the internet.
    go func() {
        mm := http.NewServeMux()
        mm.Handle("/metrics", promhttp.Handler())
        if err := http.ListenAndServe(env("METRICS_ADDR", ":9100"), mm); err != nil {
            log.Println("metrics server:", err)
        }
    }()
    var ready atomic.Bool

    // The server starts first so /healthz answers during replay. Everything
    // that serves users stays behind the ready gate.
    mux := http.NewServeMux()
    mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
        w.Write([]byte("ok"))
    })
    mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
        if !ready.Load() {
            http.Error(w, "not ready", http.StatusServiceUnavailable)
            return
        }
        w.Write([]byte("ok"))
    })
    mux.Handle("/ws", gate(&ready, http.HandlerFunc(h.ServeWS)))
    mux.Handle("/", gate(&ready, http.FileServer(http.Dir(env("WEB_DIR", "web")))))

    srv := &http.Server{Addr: env("ADDR", ":8080"), Handler: mux}
    go func() {
        if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
            log.Fatal(err)
        }
    }()
    log.Println("listening on", srv.Addr, "(not ready until startup completes)")

    st, err := store.New(ctx, env("MONGO_URI", "mongodb://localhost:27017"))
    if err != nil {
        log.Fatal("mongo: ", err)
    }
    defer st.Close(context.Background())
    log.Println("connected to mongo")

    interval := mustDuration("PERSIST_INTERVAL", "1s")

    var lg *eventlog.Log
    var offsets map[int32]int64

    if brokers := env("KAFKA_BROKERS", ""); brokers != "" {
        // Log mode: Kafka is the source of truth; Mongo snapshots only shorten the replay.
        lg, err = eventlog.New(strings.Split(brokers, ","), env("KAFKA_TOPIC", "pulse-events"))
        if err != nil {
            log.Fatal("kafka: ", err)
        }
        defer lg.Close()
        log.Println("instance", lg.Instance())
        h.SetEventLog(lg)

        var boards []*hub.Board
        boards, offsets, err = st.LoadCheckpoint(ctx)
        if err != nil {
            log.Fatal("load checkpoint: ", err)
        }
        h.Restore(boards, offsets)
        log.Printf("restored %d boards; resuming log after offsets %v", len(boards), offsets)
    }

    h.EnablePersistence(st, interval)
    go h.Run()

    if lg != nil {
        replayStart := time.Now()
        go func() {
            if err := lg.Consume(ctx, offsets, h.Deliver); err != nil {
                log.Fatal("kafka consume: ", err)
            }
        }()
        go func() {
            if err := lg.ConsumeEphemeral(ctx, h.DeliverEphemeral); err != nil {
                log.Println("ephemeral consume:", err)
            }
        }()
        select {
        case <-lg.Ready():
            hub.RecordReplay(time.Since(replayStart))
            h.MarkLive()
            log.Println("replay complete")
        case <-ctx.Done():
            return
        }
    }

    ready.Store(true)
    log.Println("ready")

    <-ctx.Done()
    log.Println("shutting down")

    // Stop advertising readiness, then give the proxies a moment to notice
    // before we start closing connections.
    ready.Store(false)
    time.Sleep(mustDuration("SHUTDOWN_DELAY", "0s"))

    shutCtx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
    defer cancel()
    if err := srv.Shutdown(shutCtx); err != nil {
        log.Println("http shutdown:", err)
    }
    if lg != nil {
        if err := lg.Flush(shutCtx); err != nil {
            log.Println("kafka flush:", err)
        }
    }
    if err := h.Shutdown(shutCtx); err != nil {
        log.Println("hub shutdown:", err)
    }
    log.Println("bye")
}