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

// EventLog is the durable, ordered log of shape events (Kafka in production).
type EventLog interface {
    Publish(room string, data []byte)
}

// record is one event read back from the log.
type record struct {
    room      string
    data      []byte
    partition int32 // unused until step 3, when snapshots store them
    offset    int64
}

type Hub struct {
    rooms     map[string]map[*Client]struct{}
    boards    map[string]*Board
    join      chan *Client
    leave     chan *Client
    broadcast chan Message
    records   chan record

    events EventLog // nil means "no log": events are applied directly

    store       Store
    interval    time.Duration
    persist     chan *Board
    dirty       map[string]struct{}
    quit        chan chan struct{}
    done        chan struct{} // closed when Run has exited
    persistDone chan struct{}
}

func New() *Hub {
    return &Hub{
        rooms:     map[string]map[*Client]struct{}{},
        boards:    map[string]*Board{},
        join:      make(chan *Client),
        leave:     make(chan *Client),
        broadcast: make(chan Message, 256),
        records:   make(chan record), // unbuffered: Deliver returns once the hub has taken the record
        quit:      make(chan chan struct{}),
        done:      make(chan struct{}),
    }
}

// SetEventLog switches the hub to log mode. Call before Run.
func (h *Hub) SetEventLog(l EventLog) { h.events = l }

// EnablePersistence turns on Mongo snapshots (non-log mode). Call before Run.
func (h *Hub) EnablePersistence(s Store, every time.Duration) {
    h.store = s
    h.interval = every
    h.persist = make(chan *Board, 64)
    h.persistDone = make(chan struct{})
    h.dirty = map[string]struct{}{}
    go h.persister()
}

func (h *Hub) persister() {
    defer close(h.persistDone)
    for b := range h.persist {
        ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
        if err := h.store.Save(ctx, b); err != nil {
            log.Println("save:", err)
        }
        cancel()
    }
}

func (h *Hub) snapshotOf(room string) *Board {
    b := h.boards[room]
    cp := &Board{Room: b.Room, Seq: b.Seq, Shapes: make(map[string]Shape, len(b.Shapes))}
    for k, v := range b.Shapes {
        cp.Shapes[k] = v
    }
    return cp
}

func (h *Hub) flush() {
    for room := range h.dirty {
        select {
        case h.persist <- h.snapshotOf(room):
            delete(h.dirty, room)
        default: // persister busy, retry next tick
        }
    }
}

// flushAll blocks until every dirty board is handed to the persister.
func (h *Hub) flushAll() {
    for room := range h.dirty {
        h.persist <- h.snapshotOf(room)
        delete(h.dirty, room)
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

// sendTo queues data for a client, dropping the client if it is too slow.
func (h *Hub) sendTo(c *Client, data []byte) {
    select {
    case c.send <- data:
    default:
        h.remove(c)
    }
}

// applyEvent assigns the room's next seq, applies the event to the board and
// sends it to every client in the room, sender included.
func (h *Hub) applyEvent(room string, ev *Event) {
    b := h.board(room)
    b.Seq++
    ev.Seq = b.Seq
    b.Apply(ev)
    if h.dirty != nil {
        h.dirty[room] = struct{}{}
    }
    clients := h.rooms[room]
    if len(clients) == 0 {
        return // nobody to tell (e.g. during replay): skip the marshal
    }
    data, err := json.Marshal(ev)
    if err != nil {
        return
    }
    for c := range clients {
        h.sendTo(c, data)
    }
}

// relay forwards raw bytes to everyone in the room except the sender.
func (h *Hub) relay(m Message) {
    for c := range h.rooms[m.Room] {
        if c != m.From {
            h.sendTo(c, m.Data)
        }
    }
}

// submit sends a validated shape event on its way: to the log if there is one,
// otherwise straight to the hub.
func (h *Hub) submit(room string, ev *Event, from *Client) {
    if h.events != nil {
        ev.Seq = 0 // never trust a client-supplied seq; it is assigned on consume
        data, err := json.Marshal(ev)
        if err != nil {
            return
        }
        h.events.Publish(room, data)
        return
    }
    select {
    case h.broadcast <- Message{Room: room, From: from, Event: ev}:
    case <-h.done:
    }
}

// Deliver hands the hub one record read from the log. It blocks until the hub
// has taken it, so records are applied in the order they were delivered.
func (h *Hub) Deliver(room string, data []byte, partition int32, offset int64) {
    select {
    case h.records <- record{room: room, data: data, partition: partition, offset: offset}:
    case <-h.done:
    }
}

func (h *Hub) Shutdown(ctx context.Context) error {
    ack := make(chan struct{})
    select {
    case h.quit <- ack:
    case <-h.done: // already shut down
        return nil
    case <-ctx.Done():
        return ctx.Err()
    }
    select {
    case <-ack:
        return nil
    case <-ctx.Done():
        return ctx.Err()
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
            h.sendTo(c, h.board(c.room).Snapshot())

        case c := <-h.leave:
            h.remove(c)

        case m := <-h.broadcast:
            if m.Event != nil {
                h.applyEvent(m.Room, m.Event)
            } else {
                h.relay(m)
            }

        case rec := <-h.records:
            var ev Event
            if err := json.Unmarshal(rec.data, &ev); err != nil || !ev.Valid() {
                // A poison pill in a permanent log must never crash the hub.
                log.Printf("skipping bad log record (room %q, partition %d, offset %d)",
                    rec.room, rec.partition, rec.offset)
                continue
            }
            h.applyEvent(rec.room, &ev)

        case <-tick:
            h.flush()

        case ack := <-h.quit:
            for _, clients := range h.rooms {
                for c := range clients {
                    h.remove(c) // closes send, so writePump sends a close frame
                }
            }
            if h.persist != nil {
                h.flushAll()
                close(h.persist)
                <-h.persistDone
            }
            close(h.done)
            close(ack)
            return
        }
    }
}