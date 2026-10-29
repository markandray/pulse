package store

import (
    "context"
    "errors"
    "time"

    "github.com/markandray/pulse/internal/hub"
    "go.mongodb.org/mongo-driver/v2/bson"
    "go.mongodb.org/mongo-driver/v2/mongo"
    "go.mongodb.org/mongo-driver/v2/mongo/options"
)

type Store struct {
    client *mongo.Client
    boards *mongo.Collection
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
    return &Store{
        client: client,
        boards: client.Database("pulse").Collection("boards"),
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
