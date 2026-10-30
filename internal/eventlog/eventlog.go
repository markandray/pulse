package eventlog

import (
    "context"
    "fmt"
    "log"

    "github.com/twmb/franz-go/pkg/kadm"
    "github.com/twmb/franz-go/pkg/kgo"
)

// Log is the durable event log, backed by one Kafka topic keyed by room.
//
// Consumption uses no consumer group: every instance reads every partition,
// starting from its own checkpoint, which is both fan-out and replay.
type Log struct {
    brokers []string
    topic   string
    cl      *kgo.Client // produces and administers; consuming uses its own client
    ready   chan struct{}
}

func New(brokers []string, topic string) (*Log, error) {
    cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
    if err != nil {
        return nil, err
    }
    return &Log{brokers: brokers, topic: topic, cl: cl, ready: make(chan struct{})}, nil
}

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