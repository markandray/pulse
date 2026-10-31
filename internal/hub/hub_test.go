package hub

import (
    "context"
    "encoding/json"
    "errors"
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

type checkpointCall struct {
    boards  []*Board
    offsets map[int32]int64
}

type fakeStore struct {
    saved       chan *Board
    checkpoints chan checkpointCall
    failures    int
}

func (f *fakeStore) Load(ctx context.Context, room string) (*Board, error) { return nil, nil }

func (f *fakeStore) Save(ctx context.Context, b *Board) error {
    if f.failures > 0 {
        f.failures--
        return errors.New("injected failure")
    }
    f.saved <- b
    return nil
}

func (f *fakeStore) SaveCheckpoint(ctx context.Context, boards []*Board, offsets map[int32]int64) error {
    f.checkpoints <- checkpointCall{boards: boards, offsets: offsets}
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

func TestRejoinGetsCurrentSnapshot(t *testing.T) {
    h := New()
    go h.Run()

    a := newTestClient(h, "r1", 8)
    h.join <- a
    recv(t, a) // snapshot
    h.broadcast <- Message{Room: "r1", From: a,
        Event: &Event{Type: "add", Shape: &Shape{ID: "s1", Kind: "rect"}}}
    recv(t, a)   // echo of its own add
    h.leave <- a // room is now empty

    b := newTestClient(h, "r1", 8)
    h.join <- b

    var snap struct {
        Type   string  `json:"type"`
        Seq    uint64  `json:"seq"`
        Shapes []Shape `json:"shapes"`
    }
    if err := json.Unmarshal(recv(t, b), &snap); err != nil {
        t.Fatal(err)
    }
    if snap.Type != "snapshot" || snap.Seq != 1 || len(snap.Shapes) != 1 {
        t.Fatalf("got %+v", snap)
    }
}

func TestShutdownFlushesDirtyBoards(t *testing.T) {
    h := New()
    fs := &fakeStore{saved: make(chan *Board, 4)}
    h.EnablePersistence(fs, time.Hour)
    go h.Run()

    a := newTestClient(h, "r1", 8)
    h.join <- a
    recv(t, a)
    h.broadcast <- Message{Room: "r1", From: a,
        Event: &Event{Type: "add", Shape: &Shape{ID: "s1", Kind: "rect"}}}
    recv(t, a)

    ctx, cancel := context.WithTimeout(context.Background(), time.Second)
    defer cancel()
    if err := h.Shutdown(ctx); err != nil {
        t.Fatal(err)
    }

    if _, ok := <-a.send; ok {
        t.Fatal("expected client channel to be closed")
    }
    select {
    case b := <-fs.saved:
        if b.Seq != 1 || len(b.Shapes) != 1 {
            t.Fatalf("saved seq=%d shapes=%d", b.Seq, len(b.Shapes))
        }
    default:
        t.Fatal("dirty board was not persisted during shutdown")
    }
}

type published struct {
    room string
    data []byte
}

type fakeLog struct{ published chan published }

func (f *fakeLog) Publish(room string, data []byte) {
    f.published <- published{room, data}
}

func mustJSON(t *testing.T, v any) []byte {
    t.Helper()
    b, err := json.Marshal(v)
    if err != nil {
        t.Fatal(err)
    }
    return b
}

func addJSON(t *testing.T, id string) []byte {
    return mustJSON(t, Event{Type: "add", Shape: &Shape{ID: id, Kind: "rect"}})
}

type snapshotMsg struct {
    Type   string  `json:"type"`
    Seq    uint64  `json:"seq"`
    Shapes []Shape `json:"shapes"`
}

func joinAndSnapshot(t *testing.T, h *Hub, room string) snapshotMsg {
    t.Helper()
    c := newTestClient(h, room, 8)
    h.join <- c
    var s snapshotMsg
    if err := json.Unmarshal(recv(t, c), &s); err != nil {
        t.Fatal(err)
    }
    return s
}

func TestSubmitPublishesAndAppliesOnlyWhenDelivered(t *testing.T) {
    h := New()
    fl := &fakeLog{published: make(chan published, 4)}
    h.SetEventLog(fl)
    go h.Run()

    a := newTestClient(h, "r1", 8)
    h.join <- a
    recv(t, a) // snapshot

    h.submit("r1", &Event{Type: "add", Seq: 99, Shape: &Shape{ID: "s1", Kind: "rect"}}, a)

    var p published
    select {
    case p = <-fl.published:
    case <-time.After(time.Second):
        t.Fatal("event was not published to the log")
    }

    var sent Event
    if err := json.Unmarshal(p.data, &sent); err != nil {
        t.Fatal(err)
    }
    if sent.Seq != 0 {
        t.Fatalf("published seq = %d; a client must not choose its own seq", sent.Seq)
    }

    // Nothing was applied yet, so a new client sees an empty board.
    if s := joinAndSnapshot(t, h, "r1"); s.Seq != 0 || len(s.Shapes) != 0 {
        t.Fatalf("event applied before it came back from the log: %+v", s)
    }

    h.Deliver(p.room, p.data, 0, 0)
    var ev Event
    if err := json.Unmarshal(recv(t, a), &ev); err != nil {
        t.Fatal(err)
    }
    if ev.Type != "add" || ev.Seq != 1 {
        t.Fatalf("got %+v, want add at seq 1", ev)
    }
}

func TestReplayRebuildsBoardBeforeAnyClientJoins(t *testing.T) {
    h := New()
    go h.Run()

    h.Deliver("r1", mustJSON(t, Event{Type: "add", Shape: &Shape{ID: "s1", Kind: "rect"}}), 0, 0)
    h.Deliver("r1", mustJSON(t, Event{Type: "add", Shape: &Shape{ID: "s2", Kind: "rect"}}), 0, 1)
    h.Deliver("r1", mustJSON(t, Event{Type: "move", ID: "s1", X: 7, Y: 8}), 0, 2)
    h.Deliver("r1", mustJSON(t, Event{Type: "delete", ID: "s2"}), 0, 3)

    s := joinAndSnapshot(t, h, "r1")
    if s.Seq != 4 || len(s.Shapes) != 1 || s.Shapes[0].ID != "s1" ||
        s.Shapes[0].X != 7 || s.Shapes[0].Y != 8 {
        t.Fatalf("replayed board = %+v", s)
    }
}

func TestBadLogRecordsAreSkippedWithoutConsumingSeq(t *testing.T) {
    h := New()
    go h.Run()

    h.Deliver("r1", []byte("garbage"), 0, 0)
    h.Deliver("r1", mustJSON(t, Event{Type: "add"}), 0, 1) // add with no shape
    h.Deliver("r1", mustJSON(t, Event{Type: "add", Shape: &Shape{ID: "s1", Kind: "rect"}}), 0, 2)

    s := joinAndSnapshot(t, h, "r1")
    if s.Seq != 1 || len(s.Shapes) != 1 {
        t.Fatalf("board = %+v; poison records should be skipped and seq stay dense", s)
    }
}

func TestRestoreSkipsEventsAlreadyInSnapshot(t *testing.T) {
    h := New()
    b := NewBoard("r1")
    b.Seq, b.Partition, b.Offset = 3, 0, 5
    b.Shapes["s1"] = Shape{ID: "s1", Kind: "rect"}
    // The checkpoint is older than the board: a crash happened after the board
    // was written but before the checkpoint advanced.
    h.Restore([]*Board{b}, map[int32]int64{0: 3})
    go h.Run()

    h.Deliver("r1", addJSON(t, "s2"), 0, 4) // already reflected in the snapshot
    h.Deliver("r1", addJSON(t, "s3"), 0, 5) // already reflected in the snapshot
    h.Deliver("r1", addJSON(t, "s4"), 0, 6) // genuinely new

    s := joinAndSnapshot(t, h, "r1")
    ids := map[string]bool{}
    for _, sh := range s.Shapes {
        ids[sh.ID] = true
    }
    if s.Seq != 4 || len(ids) != 2 || !ids["s1"] || !ids["s4"] {
        t.Fatalf("board = seq %d, shapes %v; want seq 4 with s1 and s4 only", s.Seq, ids)
    }
}

func TestLogModeWritesBoardsAndOffsetsTogether(t *testing.T) {
    h := New()
    h.SetEventLog(&fakeLog{published: make(chan published, 1)})
    fs := &fakeStore{saved: make(chan *Board, 4), checkpoints: make(chan checkpointCall, 16)}
    h.EnablePersistence(fs, 10*time.Millisecond)
    go h.Run()

    h.Deliver("r1", addJSON(t, "s1"), 2, 0)
    h.Deliver("r1", addJSON(t, "s2"), 2, 1)

    deadline := time.After(time.Second)
    for {
        select {
        case cp := <-fs.checkpoints:
            if cp.offsets[2] != 1 {
                continue // an earlier tick, before both events were applied
            }
            if len(cp.boards) != 1 {
                t.Fatalf("got %d boards in the batch, want 1", len(cp.boards))
            }
            b := cp.boards[0]
            if b.Room != "r1" || b.Seq != 2 || b.Partition != 2 || b.Offset != 1 {
                t.Fatalf("saved board = %+v", b)
            }
            return
        case <-deadline:
            t.Fatal("no checkpoint covering offset 1 was written")
        }
    }
}

func TestFailedSaveIsRetried(t *testing.T) {
    h := New()
    fs := &fakeStore{saved: make(chan *Board, 4), failures: 1}
    h.EnablePersistence(fs, 10*time.Millisecond)
    go h.Run()

    a := newTestClient(h, "r1", 8)
    h.join <- a
    recv(t, a)
    h.broadcast <- Message{Room: "r1", From: a,
        Event: &Event{Type: "add", Shape: &Shape{ID: "s1", Kind: "rect"}}}
    recv(t, a) // applied and marked dirty

    select {
    case b := <-fs.saved:
        if b.Seq != 1 {
            t.Fatalf("saved seq %d, want 1", b.Seq)
        }
    case <-time.After(time.Second):
        t.Fatal("board was never saved after the first save failed")
    }
}

func TestUndoIsJustMoreEvents(t *testing.T) {
    h := New()
    go h.Run()

    orig := Shape{ID: "s1", Kind: "rect", X: 10, Y: 20}
    h.Deliver("r1", mustJSON(t, Event{Type: "add", Shape: &orig}), 0, 0)
    h.Deliver("r1", mustJSON(t, Event{Type: "move", ID: "s1", X: 50, Y: 60}), 0, 1)
    h.Deliver("r1", mustJSON(t, Event{Type: "move", ID: "s1", X: 10, Y: 20}), 0, 2) // undo the move
    h.Deliver("r1", mustJSON(t, Event{Type: "delete", ID: "s1"}), 0, 3)
    h.Deliver("r1", mustJSON(t, Event{Type: "add", Shape: &orig}), 0, 4) // undo the delete

    s := joinAndSnapshot(t, h, "r1")
    if s.Seq != 5 || len(s.Shapes) != 1 || s.Shapes[0].X != 10 || s.Shapes[0].Y != 20 {
        t.Fatalf("board = %+v; want seq 5 and s1 back at (10,20)", s)
    }
}

func TestStaleUndoIsHarmless(t *testing.T) {
    h := New()
    go h.Run()

    h.Deliver("r1", addJSON(t, "s1"), 0, 0)
    h.Deliver("r1", mustJSON(t, Event{Type: "delete", ID: "s1"}), 0, 1) // another user deletes it
    h.Deliver("r1", mustJSON(t, Event{Type: "move", ID: "s1", X: 1, Y: 2}), 0, 2) // a late undo of my move

    s := joinAndSnapshot(t, h, "r1")
    if len(s.Shapes) != 0 {
        t.Fatalf("a stale move resurrected or changed something: %+v", s.Shapes)
    }
    if s.Seq != 3 {
        t.Fatalf("seq = %d, want 3 (a valid event always consumes a seq, even if it is a no-op)", s.Seq)
    }
}