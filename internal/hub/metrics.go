package hub

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	mConnections = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "pulse_ws_connections_active",
		Help: "WebSocket connections currently attached to this instance.",
	})
	mMessagesIn = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "pulse_messages_received_total",
		Help: "Messages received from clients, by type. Unparseable or invalid ones count as invalid.",
	}, []string{"type"})
	mMessagesOut = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pulse_messages_sent_total",
		Help: "Messages queued for delivery to clients (fan-out).",
	})
	mEventsApplied = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "pulse_events_applied_total",
		Help: "Shape events applied to a board, by type.",
	}, []string{"type"})
	mSlowDropped = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pulse_slow_clients_dropped_total",
		Help: "Clients disconnected because their send queue was full.",
	})
	mLogRecords = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pulse_log_records_total",
		Help: "Records consumed from the event log, including replay.",
	})
	mBadRecords = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pulse_bad_log_records_total",
		Help: "Log records skipped because they were malformed.",
	})
	mPublishToApply = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "pulse_publish_to_apply_seconds",
		Help:    "Time from publishing a shape event to applying it on this instance (live events only).",
		Buckets: []float64{.001, .002, .005, .01, .015, .02, .03, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60},
	})
	mSendQueueMax = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "pulse_send_queue_max",
		Help: "Length of the fullest per-client send queue on this instance (capacity 64).",
	})
	mSendQueueTotal = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "pulse_send_queue_total",
		Help: "Messages waiting in all per-client send queues on this instance.",
	})
	mBroadcastSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "pulse_broadcast_seconds",
		Help:    "Time the hub spends sending one event to the local clients of its room.",
		Buckets: prometheus.ExponentialBuckets(0.00001, 4, 8),
	})
	mSaveSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "pulse_snapshot_save_seconds",
		Help:    "Duration of one snapshot batch write to MongoDB.",
		Buckets: prometheus.DefBuckets,
	})
	mSaveErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pulse_snapshot_save_errors_total",
		Help: "Snapshot batches that failed to save.",
	})
	mReplaySeconds = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "pulse_replay_duration_seconds",
		Help: "How long startup replay of the log took.",
	})
)

// RecordReplay stores how long startup replay took.
func RecordReplay(d time.Duration) { mReplaySeconds.Set(d.Seconds()) }
