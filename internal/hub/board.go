package hub

import "encoding/json"

type Shape struct {
    ID    string  `json:"id" bson:"id"`
    Kind  string  `json:"kind" bson:"kind"`
    X     float64 `json:"x" bson:"x"`
    Y     float64 `json:"y" bson:"y"`
    W     float64 `json:"w" bson:"w"`
    H     float64 `json:"h" bson:"h"`
    Color string  `json:"color" bson:"color"`
}

type Event struct {
    Type  string  `json:"type"`
    Seq   uint64  `json:"seq,omitempty"`
    Shape *Shape  `json:"shape,omitempty"`
    ID    string  `json:"id,omitempty"`
    X     float64 `json:"x"`
    Y     float64 `json:"y"`
}

type Board struct {
    Room   string           `bson:"_id"`
    Seq    uint64           `bson:"seq"`
    Shapes map[string]Shape `bson:"shapes"`
}

func NewBoard(room string) *Board {
    return &Board{Room: room, Shapes: map[string]Shape{}}
}

func (b *Board) Apply(e *Event) {
    switch e.Type {
    case "add":
        b.Shapes[e.Shape.ID] = *e.Shape
    case "move":
        if s, ok := b.Shapes[e.ID]; ok {
            s.X, s.Y = e.X, e.Y
            b.Shapes[e.ID] = s
        }
    case "delete":
        delete(b.Shapes, e.ID)
    }
}

func (b *Board) Snapshot() []byte {
    shapes := make([]Shape, 0, len(b.Shapes))
    for _, s := range b.Shapes {
        shapes = append(shapes, s)
    }
    data, _ := json.Marshal(struct {
        Type   string  `json:"type"`
        Seq    uint64  `json:"seq"`
        Shapes []Shape `json:"shapes"`
    }{"snapshot", b.Seq, shapes})
    return data
}

func (e *Event) Valid() bool {
    switch e.Type {
    case "add":
        return e.Shape != nil && e.Shape.ID != ""
    case "move", "delete":
        return e.ID != ""
    }
    return false
}