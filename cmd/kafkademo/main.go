package main

import (
    "context"
    "fmt"
    "log"
    "os"
    "os/signal"

    "github.com/twmb/franz-go/pkg/kgo"
)

const (
    topic   = "pulse-events"
    brokers = "localhost:9094"
)

func main() {
    ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
    defer stop()

    switch {
    case len(os.Args) == 4 && os.Args[1] == "produce":
        produce(ctx, os.Args[2], os.Args[3])
    case len(os.Args) == 3 && os.Args[1] == "consume":
        consume(ctx, os.Args[2])
    default:
        log.Fatal("usage: kafkademo produce <room> <text> | kafkademo consume <group>")
    }
}

func produce(ctx context.Context, room, text string) {
    cl, err := kgo.NewClient(kgo.SeedBrokers(brokers))
    if err != nil {
        log.Fatal(err)
    }
    defer cl.Close()

    res := cl.ProduceSync(ctx, &kgo.Record{
        Topic: topic,
        Key:   []byte(room),
        Value: []byte(text),
    })
    if err := res.FirstErr(); err != nil {
        log.Fatal(err)
    }
    r := res[0].Record
    fmt.Printf("room=%s -> partition=%d offset=%d\n", room, r.Partition, r.Offset)
}

func consume(ctx context.Context, group string) {
    cl, err := kgo.NewClient(
        kgo.SeedBrokers(brokers),
        kgo.ConsumeTopics(topic),
        kgo.ConsumerGroup(group),
        kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
    )
    if err != nil {
        log.Fatal(err)
    }
    defer cl.Close()

    fmt.Printf("[%s] waiting for events...\n", group)
    for {
        fetches := cl.PollFetches(ctx)
        if ctx.Err() != nil {
            return
        }
        for _, e := range fetches.Errors() {
            log.Printf("fetch error: %v", e.Err)
        }
        fetches.EachRecord(func(r *kgo.Record) {
            fmt.Printf("[%s] partition=%d offset=%d room=%s value=%s\n",
                group, r.Partition, r.Offset, r.Key, r.Value)
        })
    }
}