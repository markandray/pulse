package hub

import (
    "log"
    "net/http"
	"time"
    "encoding/json"

    "github.com/gorilla/websocket"
)

const (
    writeWait      = 10 * time.Second
    pongWait       = 30 * time.Second
    pingPeriod     = pongWait * 9 / 10
    maxMessageSize = 8 * 1024
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
        msg := Message{Room: c.room, Data: data, From: c}
        switch ev.Type {
        case "add":
            if ev.Shape == nil || ev.Shape.ID == "" {
                continue
            }
            msg.Event = &ev
        case "move", "delete":
            if ev.ID == "" {
                continue
            }
            msg.Event = &ev
        case "seg":
            // ephemeral: relayed as raw bytes
        default:
            continue
        }
        c.hub.broadcast <- msg
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