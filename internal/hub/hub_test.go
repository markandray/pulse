package hub

import (
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