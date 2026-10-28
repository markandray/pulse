package main

import (
    "context"
    "log"
    "net/http"

    "github.com/markandray/pulse/internal/hub"
    "github.com/markandray/pulse/internal/store"
)

func main() {
    st, err := store.New(context.Background(), "mongodb://localhost:27017")
    if err != nil {
        log.Fatal("mongo: ", err)
    }
    defer st.Close(context.Background())
    log.Println("connected to mongo")

    h := hub.New()
    go h.Run()

    http.HandleFunc("/ws", h.ServeWS)
    http.Handle("/", http.FileServer(http.Dir("web")))

    log.Println("listening on :8080")
    log.Fatal(http.ListenAndServe(":8080", nil))
}