package store

import (
    "context"
    "fmt"
    "testing"
    "time"

    "github.com/markandray/pulse/internal/hub"
)

func testStore(t *testing.T) *Store {
    t.Helper()
    ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
    defer cancel()
    name := fmt.Sprintf("pulse_test_%d", time.Now().UnixNano())
    s, err := NewDB(ctx, "mongodb://localhost:27017", name)
    if err != nil {
        t.Skipf("mongo not reachable: %v", err)
    }
    t.Cleanup(func() {
        bg := context.Background()
        s.client.Database(name).Drop(bg)
        s.Close(bg)
    })
    return s
}

func TestSaveCheckpointNeverMovesBackward(t *testing.T) {
    s := testStore(t)
    ctx := context.Background()

    at := func(seq uint64, off int64, shapes ...string) *hub.Board {
        b := &hub.Board{Room: "r1", Seq: seq, Partition: 0, Offset: off, Shapes: map[string]hub.Shape{}}
        for _, id := range shapes {
            b.Shapes[id] = hub.Shape{ID: id}
        }
        return b
    }
    load := func() (*hub.Board, map[int32]int64) {
        boards, offsets, err := s.LoadCheckpoint(ctx)
        if err != nil || len(boards) != 1 {
            t.Fatalf("load: %d boards, err %v", len(boards), err)
        }
        return boards[0], offsets
    }

    // A fast instance saves offset 100.
    if err := s.SaveCheckpoint(ctx, []*hub.Board{at(10, 100, "a")}, map[int32]int64{0: 100}); err != nil {
        t.Fatal(err)
    }
    // A slower instance arrives later with an older view. It must not win.
    if err := s.SaveCheckpoint(ctx, []*hub.Board{at(9, 90)}, map[int32]int64{0: 90}); err != nil {
        t.Fatalf("a stale save should be ignored, not fail: %v", err)
    }
    b, offsets := load()
    if b.Offset != 100 || b.Seq != 10 || len(b.Shapes) != 1 || offsets[0] != 100 {
        t.Fatalf("stale write won: board %+v, offsets %v", b, offsets)
    }

    // A newer save still goes through.
    if err := s.SaveCheckpoint(ctx, []*hub.Board{at(11, 110, "a", "b")}, map[int32]int64{0: 110}); err != nil {
        t.Fatal(err)
    }
    b, offsets = load()
    if b.Offset != 110 || b.Seq != 11 || len(b.Shapes) != 2 || offsets[0] != 110 {
        t.Fatalf("newer write was rejected: board %+v, offsets %v", b, offsets)
    }
}