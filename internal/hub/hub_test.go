package hub

import (
    "context"
    "encoding/json"
    "testing"
    "time"
)

func newTestClient(h *Hub, room string, buf int) *Client {
    return &Client{hub: h, room: room, send: make(chan []byte, buf)}
}

func recv(t *testing.T, c *Client) []byte {
    t.Helper()
    select {
    case d, ok := <-c.send:
        if !ok {
            t.Fatal("channel closed")
        }
        return d
    case <-time.After(time.Second):
        t.Fatal("timed out waiting for message")
    }
    return nil
}

func TestBroadcastRoomIsolation(t *testing.T) {
    h := New()
    go h.Run()

    a := newTestClient(h, "r1", 4)
    b := newTestClient(h, "r1", 4)
    c := newTestClient(h, "r2", 4)
    h.join <- a
    h.join <- b
    h.join <- c
    for _, cl := range []*Client{a, b, c} {
        recv(t, cl) // snapshot
    }

    h.broadcast <- Message{Room: "r1", Data: []byte("hi"), From: a}

    if got := recv(t, b); string(got) != "hi" {
        t.Fatalf("got %q", got)
    }
    select {
    case <-a.send:
        t.Fatal("sender received its own raw message")
    case <-c.send:
        t.Fatal("other room received message")
    case <-time.After(50 * time.Millisecond):
    }
}

func TestSlowClientIsDropped(t *testing.T) {
    h := New()
    go h.Run()

    sender := newTestClient(h, "r1", 8)
    slow := newTestClient(h, "r1", 2) // snapshot + m1 fill it, m2 overflows
    probe := newTestClient(h, "r1", 8)
    h.join <- sender
    h.join <- slow
    h.join <- probe
    recv(t, sender)
    recv(t, probe)

    for _, m := range []string{"m1", "m2", "m3"} {
        h.broadcast <- Message{Room: "r1", Data: []byte(m), From: sender}
    }
    for i := 0; i < 3; i++ {
        recv(t, probe) // barrier: hub has processed all three
    }

    recv(t, slow) // its snapshot
    if got := recv(t, slow); string(got) != "m1" {
        t.Fatalf("got %q, want m1", got)
    }
    if _, ok := <-slow.send; ok {
        t.Fatal("expected slow client's channel to be closed")
    }
}

func TestEventsGetSequenceAndEchoToSender(t *testing.T) {
    h := New()
    go h.Run()

    a := newTestClient(h, "r1", 8)
    b := newTestClient(h, "r1", 8)
    h.join <- a
    h.join <- b
    recv(t, a)
    recv(t, b)

    h.broadcast <- Message{Room: "r1", From: a,
        Event: &Event{Type: "add", Shape: &Shape{ID: "s1", Kind: "rect"}}}
    h.broadcast <- Message{Room: "r1", From: a,
        Event: &Event{Type: "move", ID: "s1", X: 5, Y: 6}}

    for _, cl := range []*Client{a, b} {
        for want := uint64(1); want <= 2; want++ {
            var ev Event
            if err := json.Unmarshal(recv(t, cl), &ev); err != nil {
                t.Fatal(err)
            }
            if ev.Seq != want {
                t.Fatalf("seq = %d, want %d", ev.Seq, want)
            }
        }
    }
}

type fakeStore struct {
    saved chan *Board
}

func (f *fakeStore) Load(ctx context.Context, room string) (*Board, error) { return nil, nil }
func (f *fakeStore) Save(ctx context.Context, b *Board) error {
    f.saved <- b
    return nil
}

func TestDirtyBoardIsPersisted(t *testing.T) {
    h := New()
    fs := &fakeStore{saved: make(chan *Board, 4)}
    h.EnablePersistence(fs, 10*time.Millisecond)
    go h.Run()

    a := newTestClient(h, "r1", 8)
    h.join <- a
    recv(t, a)
    h.broadcast <- Message{Room: "r1", From: a,
        Event: &Event{Type: "add", Shape: &Shape{ID: "s1", Kind: "rect"}}}

    select {
    case b := <-fs.saved:
        if b.Seq != 1 || len(b.Shapes) != 1 {
            t.Fatalf("saved seq=%d shapes=%d", b.Seq, len(b.Shapes))
        }
    case <-time.After(time.Second):
        t.Fatal("board never persisted")
    }
}

func TestPreloadedBoardIsServedOnJoin(t *testing.T) {
    h := New()
    go h.Run()

    pre := NewBoard("r1")
    pre.Seq = 5
    pre.Shapes["s1"] = Shape{ID: "s1", Kind: "rect"}

    a := newTestClient(h, "r1", 8)
    a.preload = pre
    h.join <- a

    var snap struct {
        Type   string  `json:"type"`
        Seq    uint64  `json:"seq"`
        Shapes []Shape `json:"shapes"`
    }
    if err := json.Unmarshal(recv(t, a), &snap); err != nil {
        t.Fatal(err)
    }
    if snap.Seq != 5 || len(snap.Shapes) != 1 {
        t.Fatalf("snapshot seq=%d shapes=%d", snap.Seq, len(snap.Shapes))
    }
}