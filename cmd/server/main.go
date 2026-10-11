package main

import (
    "log"
    "net/http"

    "github.com/markandray/pulse/internal/hub"
)

func main() {
    h := hub.New()
    go h.Run()

    http.HandleFunc("/ws", h.ServeWS)
    http.Handle("/", http.FileServer(http.Dir("web")))

    log.Println("listening on :8080")
    log.Fatal(http.ListenAndServe(":8080", nil))
}