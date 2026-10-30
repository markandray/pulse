package store

import (
    "context"
    "errors"
    "strconv"
    "time"

    "github.com/markandray/pulse/internal/hub"
    "go.mongodb.org/mongo-driver/v2/bson"
    "go.mongodb.org/mongo-driver/v2/mongo"
    "go.mongodb.org/mongo-driver/v2/mongo/options"
)

type Store struct {
    client     *mongo.Client
    boards     *mongo.Collection // non-log mode snapshots
    logBoards  *mongo.Collection // log mode: boards plus the offset each reflects
    checkpoint *mongo.Collection // log mode: one doc, partition -> offset
}

func New(ctx context.Context, uri string) (*Store, error) {
    client, err := mongo.Connect(options.Client().ApplyURI(uri))
    if err != nil {
        return nil, err
    }
    pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
    defer cancel()
    if err := client.Ping(pingCtx, nil); err != nil {
        return nil, err
    }
        db := client.Database("pulse")
    return &Store{
        client:     client,
        boards:     db.Collection("boards"),
        logBoards:  db.Collection("log_boards"),
        checkpoint: db.Collection("log_checkpoint"),
    }, nil
}

func (s *Store) Close(ctx context.Context) error {
    return s.client.Disconnect(ctx)
}

var _ hub.Store = (*Store)(nil)

func (s *Store) Load(ctx context.Context, room string) (*hub.Board, error) {
    var b hub.Board
    err := s.boards.FindOne(ctx, bson.M{"_id": room}).Decode(&b)
    if errors.Is(err, mongo.ErrNoDocuments) {
        return nil, nil
    }
    if err != nil {
        return nil, err
    }
    if b.Shapes == nil {
        b.Shapes = map[string]hub.Shape{}
    }
    return &b, nil
}

func (s *Store) Save(ctx context.Context, b *hub.Board) error {
    _, err := s.boards.ReplaceOne(ctx, bson.M{"_id": b.Room}, b,
        options.Replace().SetUpsert(true))
    return err
}

type checkpointDoc struct {
    ID      string           `bson:"_id"`
    Offsets map[string]int64 `bson:"offsets"` // BSON keys must be strings
}

// SaveCheckpoint writes the boards first and the checkpoint last. The order is
// the whole point: the checkpoint may only advance once every board it covers
// is on disk.
func (s *Store) SaveCheckpoint(ctx context.Context, boards []*hub.Board, offsets map[int32]int64) error {
    for _, b := range boards {
        if _, err := s.logBoards.ReplaceOne(ctx, bson.M{"_id": b.Room}, b,
            options.Replace().SetUpsert(true)); err != nil {
            return err
        }
    }
    doc := checkpointDoc{ID: "offsets", Offsets: map[string]int64{}}
    for p, o := range offsets {
        doc.Offsets[strconv.Itoa(int(p))] = o
    }
    _, err := s.checkpoint.ReplaceOne(ctx, bson.M{"_id": "offsets"}, doc,
        options.Replace().SetUpsert(true))
    return err
}

// LoadCheckpoint returns every saved board and the per-partition offsets to
// resume the log from. An empty store means "replay everything".
func (s *Store) LoadCheckpoint(ctx context.Context) ([]*hub.Board, map[int32]int64, error) {
    offsets := map[int32]int64{}

    var cp checkpointDoc
    err := s.checkpoint.FindOne(ctx, bson.M{"_id": "offsets"}).Decode(&cp)
    switch {
    case errors.Is(err, mongo.ErrNoDocuments):
        // first run
    case err != nil:
        return nil, nil, err
    default:
        for k, v := range cp.Offsets {
            p, err := strconv.Atoi(k)
            if err != nil {
                return nil, nil, err
            }
            offsets[int32(p)] = v
        }
    }

    cur, err := s.logBoards.Find(ctx, bson.M{})
    if err != nil {
        return nil, nil, err
    }
    var boards []*hub.Board
    if err := cur.All(ctx, &boards); err != nil {
        return nil, nil, err
    }
    for _, b := range boards {
        if b.Shapes == nil {
            b.Shapes = map[string]hub.Shape{}
        }
    }
    return boards, offsets, nil
}