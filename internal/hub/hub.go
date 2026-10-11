package hub

type Message struct {
    Room string
    Data []byte
    From *Client
}

type Hub struct {
    rooms     map[string]map[*Client]struct{}
    join      chan *Client
    leave     chan *Client
    broadcast chan Message
}

func New() *Hub {
    return &Hub{
        rooms:     map[string]map[*Client]struct{}{},
        join:      make(chan *Client),
        leave:     make(chan *Client),
        broadcast: make(chan Message, 256),
    }
}

func (h *Hub) remove(c *Client) {
    if clients, ok := h.rooms[c.room]; ok {
        if _, ok := clients[c]; ok {
            delete(clients, c)
            close(c.send) // makes writePump exit
            if len(clients) == 0 {
                delete(h.rooms, c.room)
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
        case c := <-h.leave:
            h.remove(c)
        case m := <-h.broadcast:
            for c := range h.rooms[m.Room] {
                if c == m.From {
                    continue
                }
                select {
                case c.send <- m.Data:
                default: // client too slow, drop it
                    h.remove(c)
                }
            }
        }
    }
}