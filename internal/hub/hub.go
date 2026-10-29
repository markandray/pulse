package hub

import (
    "context"
    "encoding/json"
    "log"
    "time"
)

type Message struct {
    Room  string
    Data  []byte
    From  *Client
    Event *Event // non-nil for add/move/delete; nil means relay raw (e.g. seg)
}

type Store interface {
    Load(ctx context.Context, room string) (*Board, error)
    Save(ctx context.Context, b *Board) error
}

type Hub struct {
    rooms     map[string]map[*Client]struct{}
    boards    map[string]*Board
    join      chan *Client
    leave     chan *Client
    broadcast chan Message

    store    Store
    interval time.Duration
    persist  chan *Board
    dirty    map[string]struct{}
}

func New() *Hub {
    return &Hub{
        rooms:     map[string]map[*Client]struct{}{},
        boards:    map[string]*Board{},
        join:      make(chan *Client),
        leave:     make(chan *Client),
        broadcast: make(chan Message, 256),
    }
}

func (h *Hub) board(room string) *Board {
    b, ok := h.boards[room]
    if !ok {
        b = NewBoard(room)
        h.boards[room] = b
    }
    return b
}

// Call before Run.
func (h *Hub) EnablePersistence(s Store, every time.Duration) {
    h.store = s
    h.interval = every
    h.persist = make(chan *Board, 64)
    h.dirty = map[string]struct{}{}
    go h.persister()
}

func (h *Hub) persister() {
    for b := range h.persist {
        ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
        if err := h.store.Save(ctx, b); err != nil {
            log.Println("save:", err)
        }
        cancel()
    }
}

func (h *Hub) flush() {
    for room := range h.dirty {
        b := h.boards[room]
        cp := &Board{Room: b.Room, Seq: b.Seq, Shapes: make(map[string]Shape, len(b.Shapes))}
        for k, v := range b.Shapes {
            cp.Shapes[k] = v
        }
        select {
        case h.persist <- cp:
            delete(h.dirty, room)
        default: // persister busy, retry next tick
        }
    }
}

func (h *Hub) remove(c *Client) {
    if clients, ok := h.rooms[c.room]; ok {
        if _, ok := clients[c]; ok {
            delete(clients, c)
            close(c.send)
            if len(clients) == 0 {
                delete(h.rooms, c.room) // the board stays in h.boards
            }
        }
    }
}

func (h *Hub) Run() {
    var tick <-chan time.Time
    if h.store != nil {
        t := time.NewTicker(h.interval)
        defer t.Stop()
        tick = t.C
    }
    for {
        select {
        case c := <-h.join:
            if _, ok := h.boards[c.room]; !ok && c.preload != nil {
                h.boards[c.room] = c.preload
            }
            c.preload = nil
            if h.rooms[c.room] == nil {
                h.rooms[c.room] = map[*Client]struct{}{}
            }
            h.rooms[c.room][c] = struct{}{}
            // snapshot first, before any live event can reach this client
            select {
            case c.send <- h.board(c.room).Snapshot():
            default:
                h.remove(c)
            }
        case c := <-h.leave:
            h.remove(c)
        case m := <-h.broadcast:
            data := m.Data
            toSender := false
            if m.Event != nil {
                b := h.board(m.Room)
                b.Seq++
                m.Event.Seq = b.Seq
                b.Apply(m.Event)
                if h.dirty != nil {
                    h.dirty[m.Room] = struct{}{}
                }
                var err error
                if data, err = json.Marshal(m.Event); err != nil {
                    continue
                }
                toSender = true // everyone, sender included, sees the one agreed order
            }
            for c := range h.rooms[m.Room] {
                if c == m.From && !toSender {
                    continue
                }
                select {
                case c.send <- data:
                default:
                    h.remove(c)
                }
            }
        case <-tick:
            h.flush()
        }
    }
}
