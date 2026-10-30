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
// It consumes WITHOUT a consumer group: every instance reads every partition
// from the beginning, which is both the fan-out and the replay mechanism.
type Log struct {
    topic string
    cl    *kgo.Client
    ready chan struct{}
}

func New(brokers []string, topic string) (*Log, error) {
    cl, err := kgo.NewClient(
        kgo.SeedBrokers(brokers...),
        kgo.ConsumeTopics(topic),
        kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
    )
    if err != nil {
        return nil, err
    }
    return &Log{topic: topic, cl: cl, ready: make(chan struct{})}, nil
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

// Consume delivers every record, in per-partition order, until ctx ends.
func (l *Log) Consume(ctx context.Context,
    deliver func(room string, data []byte, partition int32, offset int64)) error {

    // Remember where the log ended when we started: that is the replay target.
    ends, err := kadm.NewClient(l.cl).ListEndOffsets(ctx, l.topic)
    if err != nil {
        return fmt.Errorf("list end offsets: %w", err)
    }
    pending := map[int32]int64{} // partition -> last offset we must deliver
    var listErr error
    ends.Each(func(o kadm.ListedOffset) {
        if o.Err != nil {
            listErr = o.Err
            return
        }
        if o.Offset > 0 {
            pending[o.Partition] = o.Offset - 1
        }
    })
    if listErr != nil {
        return fmt.Errorf("list end offsets: %w", listErr)
    }
    if len(pending) == 0 {
        close(l.ready) // empty log: nothing to replay
    }

    for {
        fetches := l.cl.PollFetches(ctx)
        if ctx.Err() != nil || fetches.IsClientClosed() {
            return nil
        }
        for _, fe := range fetches.Errors() {
            log.Printf("eventlog: fetch error: %v", fe.Err)
        }
        fetches.EachRecord(func(r *kgo.Record) {
            deliver(string(r.Key), r.Value, r.Partition, r.Offset)
            if last, ok := pending[r.Partition]; ok && r.Offset >= last {
                delete(pending, r.Partition)
                if len(pending) == 0 {
                    close(l.ready)
                }
            }
        })
    }
}