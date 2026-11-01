package eventlog

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

var mPublishErrors = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "pulse_kafka_publish_errors_total",
	Help: "Kafka publishes that failed, by kind (event or ephemeral).",
}, []string{"kind"})

var mPublishAck = promauto.NewHistogram(prometheus.HistogramOpts{
	Name:    "pulse_kafka_publish_ack_seconds",
	Help:    "Time from handing an event to the Kafka client until the broker acknowledged it.",
	Buckets: []float64{.001, .002, .005, .01, .015, .02, .03, .05, .1, .25, .5, 1},
})

// headerCarrier lets the OpenTelemetry propagator read and write a Kafka
// record's headers, which is how a trace crosses the log.
type headerCarrier struct{ r *kgo.Record }

func (c headerCarrier) Get(key string) string {
	for _, h := range c.r.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func (c headerCarrier) Set(key, value string) {
	for i, h := range c.r.Headers {
		if h.Key == key {
			c.r.Headers[i].Value = []byte(value)
			return
		}
	}
	c.r.Headers = append(c.r.Headers, kgo.RecordHeader{Key: key, Value: []byte(value)})
}

func (c headerCarrier) Keys() []string {
	keys := make([]string, 0, len(c.r.Headers))
	for _, h := range c.r.Headers {
		keys = append(keys, h.Key)
	}
	return keys
}

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
func (l *Log) Publish(ctx context.Context, room string, data []byte) {
	ctx, span := otel.Tracer("pulse/eventlog").Start(ctx, "kafka.publish",
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String("messaging.destination.name", l.topic),
			attribute.String("room", room),
		))

	rec := &kgo.Record{Topic: l.topic, Key: []byte(room), Value: data}
	otel.GetTextMapPropagator().Inject(ctx, headerCarrier{rec}) // the trace context rides in a Kafka header

	start := time.Now()
	l.cl.Produce(context.Background(), rec, func(r *kgo.Record, err error) {
		mPublishAck.Observe(time.Since(start).Seconds())
		if err != nil {
			mPublishErrors.WithLabelValues("event").Inc()
			log.Printf("eventlog: publish failed: %v", err)
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		} else {
			span.SetAttributes(
				attribute.Int("partition", int(r.Partition)),
				attribute.Int64("offset", r.Offset),
			)
		}
		span.End() // the span lasts until the broker has acknowledged the record
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
			mPublishErrors.WithLabelValues("ephemeral").Inc()
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
	deliver func(context.Context, string, []byte, int32, int64)) error {

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
	replaying := len(pending) > 0
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
			// Records read during replay may carry sampled trace headers from long
			// ago; starting spans on them would flood Jaeger on every restart.
			rctx := context.Background()
			if !replaying {
				rctx = otel.GetTextMapPropagator().Extract(context.Background(), headerCarrier{r})
			}
			deliver(rctx, string(r.Key), r.Value, r.Partition, r.Offset)
			replayed++
			if last, ok := pending[r.Partition]; ok && r.Offset >= last {
				delete(pending, r.Partition)
				if len(pending) == 0 {
					replaying = false
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
