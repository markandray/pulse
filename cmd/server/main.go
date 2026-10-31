package main

import (
    "context"
    "errors"
    "log"
    "net/http"
    "os"
    "os/signal"
    "strings"
    "syscall"
    "time"

    "github.com/markandray/pulse/internal/eventlog"
    "github.com/markandray/pulse/internal/hub"
    "github.com/markandray/pulse/internal/store"
)

func env(key, def string) string {
    if v := os.Getenv(key); v != "" {
        return v
    }
    return def
}

func main() {
    ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
    defer stop()

    st, err := store.New(ctx, env("MONGO_URI", "mongodb://localhost:27017"))
    if err != nil {
        log.Fatal("mongo: ", err)
    }
    defer st.Close(context.Background())
    log.Println("connected to mongo")

    interval, err := time.ParseDuration(env("PERSIST_INTERVAL", "1s"))
    if err != nil {
        log.Fatal("PERSIST_INTERVAL: ", err)
    }

    h := hub.New()
    var lg *eventlog.Log
    var offsets map[int32]int64

    if brokers := env("KAFKA_BROKERS", ""); brokers != "" {
        // Log mode: Kafka is the source of truth; Mongo snapshots only shorten the replay.
        lg, err = eventlog.New(strings.Split(brokers, ","), env("KAFKA_TOPIC", "pulse-events"))
        if err != nil {
            log.Fatal("kafka: ", err)
        }
        defer lg.Close()
        h.SetEventLog(lg)
        log.Println("instance", lg.Instance())

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
            log.Println("replay complete")
        case <-ctx.Done():
            return
        }
    }

    mux := http.NewServeMux()
    mux.HandleFunc("/ws", h.ServeWS)
    mux.Handle("/", http.FileServer(http.Dir(env("WEB_DIR", "web"))))

    srv := &http.Server{Addr: env("ADDR", ":8080"), Handler: mux}
    go func() {
        if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
            log.Fatal(err)
        }
    }()
    log.Println("listening on", srv.Addr)

    <-ctx.Done()
    log.Println("shutting down")

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