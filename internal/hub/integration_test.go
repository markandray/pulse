package hub_test

import (
    "bytes"
    "context"
    "encoding/json"
    "net/http"
    "net/http/httptest"
    "strings"
    "testing"
    "time"

    "github.com/gorilla/websocket"
    "github.com/markandray/pulse/internal/hub"
)

type frame struct {
    Type   string      `json:"type"`
    Seq    uint64      `json:"seq"`
    ID     string      `json:"id"`
    X      float64     `json:"x"`
    Y      float64     `json:"y"`
    Shape  *hub.Shape  `json:"shape"`
    Shapes []hub.Shape `json:"shapes"`
}

// startServer runs the real handler on a random local port.
func startServer(t *testing.T) (*hub.Hub, string) {
    t.Helper()
    h := hub.New()
    go h.Run()
    srv := httptest.NewServer(http.HandlerFunc(h.ServeWS))
    t.Cleanup(func() {
        srv.Close()
        ctx, cancel := context.WithTimeout(context.Background(), time.Second)
        defer cancel()
        h.Shutdown(ctx)
    })
    return h, "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws"
}

func dial(t *testing.T, base, room string) *websocket.Conn {
    t.Helper()
    c, _, err := websocket.DefaultDialer.Dial(base+"?room="+room, nil)
    if err != nil {
        t.Fatalf("dial: %v", err)
    }
    t.Cleanup(func() { c.Close() })
    return c
}

func read(t *testing.T, c *websocket.Conn) frame {
    t.Helper()
    c.SetReadDeadline(time.Now().Add(2 * time.Second))
    _, data, err := c.ReadMessage()
    if err != nil {
        t.Fatalf("read: %v", err)
    }
    var f frame
    if err := json.Unmarshal(data, &f); err != nil {
        t.Fatalf("bad json %q: %v", data, err)
    }
    return f
}

func send(t *testing.T, c *websocket.Conn, v any) {
    t.Helper()
    data, err := json.Marshal(v)
    if err != nil {
        t.Fatal(err)
    }
    if err := c.WriteMessage(websocket.TextMessage, data); err != nil {
        t.Fatalf("write: %v", err)
    }
}

func add(id string) hub.Event {
    return hub.Event{Type: "add", Shape: &hub.Shape{ID: id, Kind: "rect", W: 10, H: 10}}
}

// join reads the snapshot that proves the client is registered in its room.
func join(t *testing.T, c *websocket.Conn) frame {
    t.Helper()
    f := read(t, c)
    if f.Type != "snapshot" {
        t.Fatalf("first frame = %q, want snapshot", f.Type)
    }
    return f
}

func TestTwoClientsSeeTheSameOrder(t *testing.T) {
    _, base := startServer(t)
    a := dial(t, base, "r1")
    b := dial(t, base, "r1")
    if s := join(t, a); s.Seq != 0 || len(s.Shapes) != 0 {
        t.Fatalf("a snapshot = %+v", s)
    }
    join(t, b)

    send(t, a, add("s1"))
    send(t, b, add("s2"))

    var orderA, orderB []string
    for i := 1; i <= 2; i++ {
        fa, fb := read(t, a), read(t, b)
        if fa.Seq != uint64(i) || fb.Seq != uint64(i) {
            t.Fatalf("seq a=%d b=%d, want %d", fa.Seq, fb.Seq, i)
        }
        orderA = append(orderA, fa.Shape.ID)
        orderB = append(orderB, fb.Shape.ID)
    }
    if strings.Join(orderA, ",") != strings.Join(orderB, ",") {
        t.Fatalf("clients disagree on order: a=%v b=%v", orderA, orderB)
    }
    if len(orderA) != 2 || orderA[0] == orderA[1] {
        t.Fatalf("expected both shapes exactly once, got %v", orderA)
    }
}

func TestLateJoinerGetsSnapshotThenContinuousEvents(t *testing.T) {
    _, base := startServer(t)
    a := dial(t, base, "r1")
    join(t, a)

    send(t, a, add("s1"))
    send(t, a, add("s2"))
    send(t, a, hub.Event{Type: "move", ID: "s1", X: 50, Y: 60})
    for i := 0; i < 3; i++ {
        read(t, a)
    }

    b := dial(t, base, "r1")
    snap := join(t, b)
    if snap.Seq != 3 || len(snap.Shapes) != 2 {
        t.Fatalf("snapshot seq=%d shapes=%d, want 3 and 2", snap.Seq, len(snap.Shapes))
    }
    moved := false
    for _, s := range snap.Shapes {
        if s.ID == "s1" && s.X == 50 && s.Y == 60 {
            moved = true
        }
    }
    if !moved {
        t.Fatalf("snapshot did not include the move: %+v", snap.Shapes)
    }

    send(t, a, hub.Event{Type: "delete", ID: "s2"})
    ev := read(t, b)
    if ev.Type != "delete" || ev.ID != "s2" || ev.Seq != 4 {
        t.Fatalf("got %+v, want delete s2 at seq 4 (no gap after snapshot)", ev)
    }
}

func TestRoomsAreIsolated(t *testing.T) {
    _, base := startServer(t)
    a := dial(t, base, "r1")
    c := dial(t, base, "r2")
    join(t, a)
    join(t, c)

    send(t, a, add("in-r1"))
    read(t, a) // echo: the hub has processed r1's event

    send(t, c, add("in-r2"))
    got := read(t, c)
    // If r1's event had leaked into r2, it would have arrived first.
    if got.Seq != 1 || got.Shape == nil || got.Shape.ID != "in-r2" {
        t.Fatalf("r2 client got %+v, want its own event at seq 1", got)
    }
}

func TestReconnectResyncsFromSnapshot(t *testing.T) {
    _, base := startServer(t)
    a := dial(t, base, "r1")
    join(t, a)
    send(t, a, add("s1"))
    read(t, a)
    a.Close()

    a2 := dial(t, base, "r1")
    snap := join(t, a2)
    if snap.Seq != 1 || len(snap.Shapes) != 1 || snap.Shapes[0].ID != "s1" {
        t.Fatalf("resync snapshot = %+v", snap)
    }
}

func TestMalformedMessagesAreIgnored(t *testing.T) {
    _, base := startServer(t)
    a := dial(t, base, "r1")
    b := dial(t, base, "r1")
    join(t, a)
    join(t, b)

    for _, bad := range []string{
        "not json",
        `{"type":"add"}`,
        `{"type":"move"}`,
        `{"type":"bogus"}`,
    } {
        if err := a.WriteMessage(websocket.TextMessage, []byte(bad)); err != nil {
            t.Fatal(err)
        }
    }
    send(t, a, add("s1"))

    // The first event b sees must be the valid one, still at seq 1:
    // bad input neither crashed the hub nor consumed a sequence number.
    got := read(t, b)
    if got.Type != "add" || got.Seq != 1 || got.Shape.ID != "s1" {
        t.Fatalf("got %+v", got)
    }
}

func TestOversizedMessageDropsClient(t *testing.T) {
    _, base := startServer(t)
    a := dial(t, base, "r1")
    join(t, a)

    big := bytes.Repeat([]byte("x"), 16*1024) // limit is 8 KB
    if err := a.WriteMessage(websocket.TextMessage, big); err != nil {
        t.Fatal(err)
    }
    a.SetReadDeadline(time.Now().Add(2 * time.Second))
    if _, _, err := a.ReadMessage(); err == nil {
        t.Fatal("expected the server to close the connection")
    }
}

func TestShutdownClosesClients(t *testing.T) {
    h, base := startServer(t)
    a := dial(t, base, "r1")
    join(t, a)

    ctx, cancel := context.WithTimeout(context.Background(), time.Second)
    defer cancel()
    if err := h.Shutdown(ctx); err != nil {
        t.Fatal(err)
    }

    a.SetReadDeadline(time.Now().Add(2 * time.Second))
    if _, _, err := a.ReadMessage(); err == nil {
        t.Fatal("expected a close after shutdown")
    }
}