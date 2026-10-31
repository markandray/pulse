package hub

import (
    "context"
    "encoding/json"
    "log"
    "net/http"
    "time"

    "github.com/gorilla/websocket"
)

const (
    writeWait      = 10 * time.Second
    pongWait       = 30 * time.Second
    pingPeriod     = pongWait * 9 / 10
    maxMessageSize = 8 * 1024
)

type Client struct {
    hub     *Hub
    conn    *websocket.Conn
    room    string
    send    chan []byte
    preload *Board
}

func (c *Client) readPump() {
    defer func() {
        select {
        case c.hub.leave <- c:
        case <-c.hub.done:
        }
        c.conn.Close()
    }()
    c.conn.SetReadLimit(maxMessageSize)
    c.conn.SetReadDeadline(time.Now().Add(pongWait))
    c.conn.SetPongHandler(func(string) error {
        c.conn.SetReadDeadline(time.Now().Add(pongWait))
        return nil
    })
    for {
        _, data, err := c.conn.ReadMessage()
        if err != nil {
            return
        }
        var ev Event
        if err := json.Unmarshal(data, &ev); err != nil {
            continue
        }
        switch ev.Type {
        case "add", "move", "delete":
            if !ev.Valid() {
                continue
            }
            c.hub.submit(c.room, &ev, c)
        case "seg":
            // Ephemeral: relayed to this instance's other clients immediately,
            // and to the other instances through the short-lived topic.
            // Never logged, replayed or persisted.
            select {
            case c.hub.broadcast <- Message{Room: c.room, Data: data, From: c}:
            case <-c.hub.done:
                return
            }
            if c.hub.events != nil {
                c.hub.events.PublishEphemeral(c.room, data)
            }
        default:
            // unknown type: ignore
        }
    }
}

func (c *Client) writePump() {
    ticker := time.NewTicker(pingPeriod)
    defer func() {
        ticker.Stop()
        c.conn.Close()
    }()
    for {
        select {
        case data, ok := <-c.send:
            c.conn.SetWriteDeadline(time.Now().Add(writeWait))
            if !ok { // hub closed the channel
                c.conn.WriteMessage(websocket.CloseMessage, []byte{})
                return
            }
            if err := c.conn.WriteMessage(websocket.TextMessage, data); err != nil {
                return
            }
        case <-ticker.C:
            c.conn.SetWriteDeadline(time.Now().Add(writeWait))
            if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
                return
            }
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

    var pre *Board
    if h.store != nil && h.events == nil {
        ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
        b, err := h.store.Load(ctx, room)
        cancel()
        if err != nil {
            log.Println("load:", err)
            http.Error(w, "storage unavailable", http.StatusServiceUnavailable)
            return
        }
        pre = b
    }

    conn, err := upgrader.Upgrade(w, r, nil)
    if err != nil {
        log.Println("upgrade:", err)
        return
    }

    c := &Client{
        hub:     h,
        conn:    conn,
        room:    room,
        send:    make(chan []byte, 64),
        preload: pre,
    }
    select {
    case h.join <- c:
    case <-h.done:
        conn.Close()
        return
    }

    go c.writePump()
    c.readPump() // blocks until the connection closes
}
