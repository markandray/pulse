package hub

import (
    "testing"
    "time"
)

func newTestClient(h *Hub, room string, buf int) *Client {
    return &Client{hub: h, room: room, send: make(chan []byte, buf)}
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

    h.broadcast <- Message{Room: "r1", Data: []byte("hi"), From: a}

    select {
    case got := <-b.send:
        if string(got) != "hi" {
            t.Fatalf("got %q", got)
        }
    case <-time.After(time.Second):
        t.Fatal("b never received message")
    }

    select {
    case <-a.send:
        t.Fatal("sender received its own message")
    case <-c.send:
        t.Fatal("other room received message")
    case <-time.After(50 * time.Millisecond):
    }
}

func TestSlowClientIsDropped(t *testing.T) {
    h := New()
    go h.Run()

    sender := newTestClient(h, "r1", 1)
    slow := newTestClient(h, "r1", 1) // buffer of 1, nobody reading
    probe := newTestClient(h, "r1", 8) // healthy, used as a barrier
    h.join <- sender
    h.join <- slow
    h.join <- probe

    for _, m := range []string{"m1", "m2", "m3"} {
        h.broadcast <- Message{Room: "r1", Data: []byte(m), From: sender}
    }

    // Wait until the hub has processed all three (messages are handled in order).
    for i := 0; i < 3; i++ {
        select {
        case <-probe.send:
        case <-time.After(time.Second):
            t.Fatal("probe did not receive all messages")
        }
    }

    // Now slow's buffer overflowed on m2, so it holds m1 and is closed.
    if got := <-slow.send; string(got) != "m1" {
        t.Fatalf("got %q, want m1", got)
    }
    if _, ok := <-slow.send; ok {
        t.Fatal("expected slow client's channel to be closed")
    }
}