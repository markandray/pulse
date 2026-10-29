package main

import (
    "context"
    "errors"
    "log"
    "net/http"
    "os"
    "os/signal"
    "syscall"
    "time"

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
    if interval <= 0 {
        log.Fatal("PERSIST_INTERVAL must be positive")
    }

    h := hub.New()
    h.EnablePersistence(st, interval)
    go h.Run()

    mux := http.NewServeMux()
    mux.HandleFunc("/ws", h.ServeWS)
    mux.Handle("/", http.FileServer(http.Dir(env("WEB_DIR", "web"))))

    srv := &http.Server{Addr: env("ADDR", ":8080"), Handler: mux}
    serveErr := make(chan error, 1)
    go func() {
        if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
            serveErr <- err
        }
    }()
    log.Println("listening on", srv.Addr)

    select {
    case <-ctx.Done():
        log.Println("shutting down")
    case err := <-serveErr:
        log.Println("http server:", err)
    }

    shutCtx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
    defer cancel()
    if err := srv.Shutdown(shutCtx); err != nil {
        log.Println("http shutdown:", err)
    }
    if err := h.Shutdown(shutCtx); err != nil {
        log.Println("hub shutdown:", err)
    }
    log.Println("bye")
}
