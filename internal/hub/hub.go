package hub

import "encoding/json"

type Message struct {
    Room  string
    Data  []byte
    From  *Client
    Event *Event // non-nil for add/move/delete; nil means relay raw (e.g. seg)
}

type Hub struct {
    rooms     map[string]map[*Client]struct{}
    boards    map[string]*Board
    join      chan *Client
    leave     chan *Client
    broadcast chan Message
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
    for {
        select {
        case c := <-h.join:
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
        }
    }
}