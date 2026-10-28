package store

import (
    "context"
    "time"

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