package hub

import (
    "log"
    "net/http"

    "github.com/gorilla/websocket"
)

type Client struct {
    hub  *Hub
    conn *websocket.Conn
    room string
    send chan []byte
}

func (c *Client) readPump() {
    defer func() {
        c.hub.leave <- c
        c.conn.Close()
    }()
    for {
        _, data, err := c.conn.ReadMessage()
        if err != nil {
            return
        }
        c.hub.broadcast <- Message{Room: c.room, Data: data, From: c}
    }
}

func (c *Client) writePump() {
    defer c.conn.Close()
    for data := range c.send {
        if err := c.conn.WriteMessage(websocket.TextMessage, data); err != nil {
            return
        }
    }
}

var upgrader = websocket.Upgrader{
    CheckOrigin: func(r *http.Request) bool { return true }, // dev only
}

func (h *Hub) ServeWS(w http.ResponseWriter, r *http.Request) {
    room := r.URL.Query().Get("room")
    if room == "" {
        http.Error(w, "room required", http.StatusBadRequest)
        return
    }

    conn, err := upgrader.Upgrade(w, r, nil)
    if err != nil {
        log.Println("upgrade:", err)
        return
    }

    c := &Client{
        hub:  h,
        conn: conn,
        room: room,
        send: make(chan []byte, 64),
    }
    h.join <- c

    go c.writePump()
    c.readPump() // blocks until the connection closes
}