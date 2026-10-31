package hub_test

import (
    "context"
    "net/http"
    "net/http/httptest"
    "sort"
    "strings"
    "sync"
    "testing"
    "time"

    "github.com/gorilla/websocket"
    "github.com/markandray/pulse/internal/hub"
)

// bus simulates Kafka: one totally ordered log, delivered to every attached
// instance in log order, plus an ephemeral channel that skips the sender.
type bus struct {
    mu      sync.Mutex
    log     []busRecord
    members map[string]*hub.Hub
}

type busRecord struct {
    room string
    data []byte
}

func newBus() *bus { return &bus{members: map[string]*hub.Hub{}} }

// attach replays the existing log into h, then registers it for new records.
// Both happen under the lock, so no record is missed or seen twice.
func (b *bus) attach(id string, h *hub.Hub) {
    b.mu.Lock()
    defer b.mu.Unlock()
    for off, r := range b.log {
        h.Deliver(r.room, r.data, 0, int64(off))
    }
    b.members[id] = h
}

func (b *bus) detach(id string) {
    b.mu.Lock()
    defer b.mu.Unlock()
    delete(b.members, id)
}

// member is one instance's handle on the bus (its hub's EventLog).
type member struct {
    b  *bus
    id string
}

func (m *member) Publish(room string, data []byte) {
    m.b.mu.Lock()
    defer m.b.mu.Unlock()
    off := int64(len(m.b.log))
    m.b.log = append(m.b.log, busRecord{room, data})
    for _, h := range m.b.members {
        h.Deliver(room, data, 0, off)
    }
}

func (m *member) PublishEphemeral(room string, data []byte) {
    m.b.mu.Lock()
    defer m.b.mu.Unlock()
    for id, h := range m.b.members {
        if id != m.id {
            h.DeliverEphemeral(room, data)
        }
    }
}

type instance struct {
    id   string
    h    *hub.Hub
    srv  *httptest.Server
    bus  *bus
    once sync.Once
}

func (i *instance) url() string {
    return "ws" + strings.TrimPrefix(i.srv.URL, "http") + "/ws"
}

func startInstance(t *testing.T, b *bus, id string) *instance {
    t.Helper()
    h := hub.New()
    h.SetEventLog(&member{b: b, id: id})
    go h.Run()
    b.attach(id, h) // replay whatever the cluster has already logged
    i := &instance{id: id, h: h, bus: b, srv: httptest.NewServer(http.HandlerFunc(h.ServeWS))}
    t.Cleanup(i.stop)
    return i
}

func (i *instance) stop() {
    i.once.Do(func() {
        i.bus.detach(i.id)
        i.srv.Close()
        ctx, cancel := context.WithTimeout(context.Background(), time.Second)
        defer cancel()
        i.h.Shutdown(ctx)
    })
}

func shapeIDs(f frame) string {
    ids := make([]string, 0, len(f.Shapes))
    for _, s := range f.Shapes {
        ids = append(ids, s.ID)
    }
    sort.Strings(ids)
    return strings.Join(ids, ",")
}

// expectQuiet fails if anything arrives within d. Use it as the last read on a conn.
func expectQuiet(t *testing.T, c *websocket.Conn, d time.Duration, who string) {
    t.Helper()
    c.SetReadDeadline(time.Now().Add(d))
    if _, data, err := c.ReadMessage(); err == nil {
        t.Fatalf("%s: unexpected frame %s", who, data)
    }
}

func TestTwoInstancesStayInSync(t *testing.T) {
    b := newBus()
    i1 := startInstance(t, b, "i1")
    i2 := startInstance(t, b, "i2")

    a := dial(t, i1.url(), "r1")
    c := dial(t, i2.url(), "r1")
    join(t, a)
    join(t, c)

    // An add on instance 1 reaches the client on instance 2 with the same seq.
    send(t, a, add("s1"))
    fa, fc := read(t, a), read(t, c)
    if fa.Seq != 1 || fc.Seq != 1 || fa.Shape.ID != "s1" || fc.Shape.ID != "s1" {
        t.Fatalf("add: a=%+v c=%+v", fa, fc)
    }

    // A move on instance 2 reaches instance 1.
    send(t, c, hub.Event{Type: "move", ID: "s1", X: 50, Y: 60})
    fa, fc = read(t, a), read(t, c)
    if fa.Seq != 2 || fc.Seq != 2 || fa.X != 50 || fc.X != 50 || fa.Y != 60 || fc.Y != 60 {
        t.Fatalf("move: a=%+v c=%+v", fa, fc)
    }

    // Simultaneous edits from both instances are totally ordered, identically.
    send(t, a, add("x1"))
    send(t, c, add("x2"))
    var orderA, orderC []string
    for want := uint64(3); want <= 4; want++ {
        fa, fc := read(t, a), read(t, c)
        if fa.Seq != want || fc.Seq != want {
            t.Fatalf("seq a=%d c=%d, want %d", fa.Seq, fc.Seq, want)
        }
        orderA = append(orderA, fa.Shape.ID)
        orderC = append(orderC, fc.Shape.ID)
    }
    if strings.Join(orderA, ",") != strings.Join(orderC, ",") {
        t.Fatalf("instances disagree on order: %v vs %v", orderA, orderC)
    }

    // A late joiner on either instance sees the same board.
    s1 := join(t, dial(t, i1.url(), "r1"))
    s2 := join(t, dial(t, i2.url(), "r1"))
    if s1.Seq != 4 || s2.Seq != 4 || shapeIDs(s1) != shapeIDs(s2) || len(s1.Shapes) != 3 {
        t.Fatalf("snapshots differ: %+v vs %+v", s1, s2)
    }
}

func TestStrokesCrossInstancesExactlyOnce(t *testing.T) {
    b := newBus()
    i1 := startInstance(t, b, "i1")
    i2 := startInstance(t, b, "i2")

    a := dial(t, i1.url(), "r1")
    sameInstance := dial(t, i1.url(), "r1")
    otherInstance := dial(t, i2.url(), "r1")
    join(t, a)
    join(t, sameInstance)
    join(t, otherInstance)

    seg := []byte(`{"type":"seg","color":"red","x0":1,"y0":2,"x1":3,"y1":4}`)
    if err := a.WriteMessage(websocket.TextMessage, seg); err != nil {
        t.Fatal(err)
    }
    send(t, a, add("s1")) // an event that follows the stroke

    // The sender sees its own add, but never its own stroke.
    if f := read(t, a); f.Type != "add" {
        t.Fatalf("sender's first frame = %q, want add", f.Type)
    }
    expectQuiet(t, a, 150*time.Millisecond, "sender")

    for name, cl := range map[string]*websocket.Conn{
        "peer on the same instance":  sameInstance,
        "peer on the other instance": otherInstance,
    } {
        segs, adds := 0, 0
        // The order between a stroke and an event is not defined, only that both arrive.
        for segs == 0 || adds == 0 {
            switch f := read(t, cl); f.Type {
            case "seg":
                segs++
            case "add":
                adds++
            }
        }
        if segs != 1 || adds != 1 {
            t.Fatalf("%s: got %d strokes and %d adds", name, segs, adds)
        }
        expectQuiet(t, cl, 150*time.Millisecond, name) // a duplicate would show up here
    }
}

func TestRestartedInstanceRebuildsFromTheLog(t *testing.T) {
    b := newBus()
    i1 := startInstance(t, b, "i1")
    i2 := startInstance(t, b, "i2")

    a := dial(t, i1.url(), "r1")
    join(t, a)
    send(t, a, add("s1"))
    read(t, a)
    send(t, a, add("s2"))
    read(t, a)
    send(t, a, hub.Event{Type: "delete", ID: "s1"})
    read(t, a)

    i2.stop() // instance 2 goes away

    send(t, a, add("s3")) // the cluster carries on without it
    read(t, a)

    i3 := startInstance(t, b, "i3") // a fresh instance replays the whole log
    fromNew := join(t, dial(t, i3.url(), "r1"))
    fromOld := join(t, dial(t, i1.url(), "r1"))

    if fromNew.Seq != 4 || fromOld.Seq != 4 {
        t.Fatalf("seq: new=%d old=%d, want 4", fromNew.Seq, fromOld.Seq)
    }
    if shapeIDs(fromNew) != "s2,s3" || shapeIDs(fromOld) != "s2,s3" {
        t.Fatalf("boards differ: new=%q old=%q, want s2,s3", shapeIDs(fromNew), shapeIDs(fromOld))
    }
}