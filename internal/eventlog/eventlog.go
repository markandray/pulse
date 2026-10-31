package eventlog

import (
    "context"
    "crypto/rand"
    "encoding/hex"
    "errors"
    "fmt"
    "log"

    "github.com/twmb/franz-go/pkg/kadm"
    "github.com/twmb/franz-go/pkg/kerr"
    "github.com/twmb/franz-go/pkg/kgo"
)

// Log is the durable event log, backed by one Kafka topic keyed by room.
//
// Consumption uses no consumer group: every instance reads every partition,
// starting from its own checkpoint, which is both fan-out and replay.
type Log struct {
    brokers  []string
    topic    string
    ephTopic string // short-retention topic for freehand strokes
    instance string // random id of this process
    cl       *kgo.Client
    ready    chan struct{}
}

func New(brokers []string, topic string) (*Log, error) {
    cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
    if err != nil {
        return nil, err
    }

    ephTopic := topic + "-ephemeral"
    responses, err := kadm.NewClient(cl).CreateTopics(
        context.Background(), -1, -1,
        map[string]*string{"retention.ms": kadm.StringPtr("60000")},
        ephTopic,
    )
    if err != nil {
        cl.Close()
        return nil, fmt.Errorf("create ephemeral topic %q: %w", ephTopic, err)
    }
    if err := responses.Error(); err != nil && !errors.Is(err, kerr.TopicAlreadyExists) {
        cl.Close()
        return nil, fmt.Errorf("create ephemeral topic %q: %w", ephTopic, err)
    }

    return &Log{
        brokers: brokers, topic: topic, ephTopic: ephTopic,
        instance: newInstanceID(), cl: cl, ready: make(chan struct{}),
    }, nil
}

func newInstanceID() string {
    b := make([]byte, 6)
    rand.Read(b)
    return hex.EncodeToString(b)
}

func (l *Log) Instance() string { return l.instance }

// Publish queues an event for the room's partition and returns immediately.
// Records from one goroutine for one key stay in order.
func (l *Log) Publish(room string, data []byte) {
    l.cl.Produce(context.Background(),
        &kgo.Record{Topic: l.topic, Key: []byte(room), Value: data},
        func(_ *kgo.Record, err error) {
            if err != nil {
                log.Printf("eventlog: publish failed: %v", err)
            }
        })
}

// PublishEphemeral sends a stroke to the other instances. The record carries
// this instance's id so the sender can skip its own.
func (l *Log) PublishEphemeral(room string, data []byte) {
    l.cl.Produce(context.Background(), &kgo.Record{
        Topic:   l.ephTopic,
        Key:     []byte(room),
        Value:   data,
        Headers: []kgo.RecordHeader{{Key: "src", Value: []byte(l.instance)}},
    }, func(_ *kgo.Record, err error) {
        if err != nil {
            log.Printf("eventlog: ephemeral publish failed: %v", err)
        }
    })
}

// Ready is closed once everything that was in the log when Consume started
// has been delivered.
func (l *Log) Ready() <-chan struct{} { return l.ready }

func (l *Log) Flush(ctx context.Context) error { return l.cl.Flush(ctx) }

func (l *Log) Close() { l.cl.Close() }

// Consume delivers every record after the checkpoint (start[partition] is the
// last offset already reflected in the snapshots), in per-partition order,
// until ctx ends.
func (l *Log) Consume(ctx context.Context, start map[int32]int64,
    deliver func(room string, data []byte, partition int32, offset int64)) error {

    ends, err := kadm.NewClient(l.cl).ListEndOffsets(ctx, l.topic)
    if err != nil {
        return fmt.Errorf("list end offsets: %w", err)
    }

    parts := map[int32]kgo.Offset{}
    pending := map[int32]int64{} // partition -> last offset we must deliver before we are "ready"
    var bad error
    ends.Each(func(o kadm.ListedOffset) {
        if bad != nil {
            return
        }
        if o.Err != nil {
            bad = o.Err
            return
        }
        from := int64(0)
        if s, ok := start[o.Partition]; ok {
            from = s + 1
        }
        if from > o.Offset {
            bad = fmt.Errorf("partition %d: checkpoint is at offset %d but the log ends at %d; "+
                "was the topic recreated? drop the log_boards and log_checkpoint collections",
                o.Partition, from-1, o.Offset)
            return
        }
        parts[o.Partition] = kgo.NewOffset().At(from)
        if o.Offset > from {
            pending[o.Partition] = o.Offset - 1
        }
    })
    if bad != nil {
        return bad
    }

    cons, err := kgo.NewClient(
        kgo.SeedBrokers(l.brokers...),
        kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{l.topic: parts}),
    )
    if err != nil {
        return err
    }
    defer cons.Close()

    replayed := 0
    if len(pending) == 0 {
        log.Println("eventlog: replayed 0 records (nothing after the checkpoint)")
        close(l.ready)
    }

    for {
        fetches := cons.PollFetches(ctx)
        if ctx.Err() != nil || fetches.IsClientClosed() {
            return nil
        }
        for _, fe := range fetches.Errors() {
            log.Printf("eventlog: fetch error: %v", fe.Err)
        }
        fetches.EachRecord(func(r *kgo.Record) {
            deliver(string(r.Key), r.Value, r.Partition, r.Offset)
            replayed++
            if last, ok := pending[r.Partition]; ok && r.Offset >= last {
                delete(pending, r.Partition)
                if len(pending) == 0 {
                    log.Printf("eventlog: replayed %d records", replayed)
                    close(l.ready)
                }
            }
        })
    }
}

// ConsumeEphemeral delivers strokes from OTHER instances, starting from now.
// Nothing is replayed: a stroke you were not connected for is gone, by design.
func (l *Log) ConsumeEphemeral(ctx context.Context, deliver func(room string, data []byte)) error {
    cons, err := kgo.NewClient(
        kgo.SeedBrokers(l.brokers...),
        kgo.ConsumeTopics(l.ephTopic),
        kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
    )
    if err != nil {
        return err
    }
    defer cons.Close()

    for {
        fetches := cons.PollFetches(ctx)
        if ctx.Err() != nil || fetches.IsClientClosed() {
            return nil
        }
        for _, fe := range fetches.Errors() {
            log.Printf("eventlog: ephemeral fetch error: %v", fe.Err)
        }
        fetches.EachRecord(func(r *kgo.Record) {
            for _, h := range r.Headers {
                if h.Key == "src" && string(h.Value) == l.instance {
                    return // our own stroke: we already relayed it locally
                }
            }
            deliver(string(r.Key), r.Value)
        })
    }
}