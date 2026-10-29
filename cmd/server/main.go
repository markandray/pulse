package main

import (
    "context"
    "log"
    "net/http"
    "os"
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
    st, err := store.New(context.Background(), env("MONGO_URI", "mongodb://localhost:27017"))
    if err != nil {
        log.Fatal("mongo: ", err)
    }
    defer st.Close(context.Background())
    log.Println("connected to mongo")

    h := hub.New()
    h.EnablePersistence(st, time.Second)
    go h.Run()

    http.HandleFunc("/ws", h.ServeWS)
    http.Handle("/", http.FileServer(http.Dir(env("WEB_DIR", "web"))))

    addr := env("ADDR", ":8080")
    log.Println("listening on", addr)
    log.Fatal(http.ListenAndServe(addr, nil))
}