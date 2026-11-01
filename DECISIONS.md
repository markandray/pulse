# Decisions

Architecture decisions for Pulse, a realtime collaborative whiteboard.
Each entry: context, decision, alternatives considered, trade-off.

Status: Week 2 in progress (persistence done, reconnect/resync next).

---

## 1. Hub goroutine owns all room state

**Context:** Many connection goroutines need to join, leave, and broadcast to
shared rooms.

**Decision:** A single hub goroutine owns the `rooms` and `boards` maps.
Connections talk to it only through channels (`join`, `leave`, `broadcast`),
so no `sync.Mutex` is needed.

**Alternatives:** A mutex-guarded shared map.

**Trade-off:** All room operations serialize through one goroutine. That is
fine at this scale, but it is the first thing to check under load in week 5,
and it becomes a real constraint when multiple instances run in week 3.

## 2. One reader and one writer goroutine per connection

**Context:** gorilla/websocket allows only one concurrent writer per
connection.

**Decision:** Each client has a `readPump` and a `writePump`. The hub never
writes to a connection directly; it pushes bytes into the client's buffered
`send` channel (64), and `writePump` drains it.

**Alternatives:** Locking around `conn.WriteMessage`.

**Trade-off:** Two goroutines per client and a buffer to size. In exchange,
writes are serialized for free and a slow client is detectable (decision 3).

## 3. Drop slow clients instead of blocking

**Context:** If the hub blocks on one client's full `send` channel, every
room freezes.

**Decision:** Broadcast uses a non-blocking send. If a client's buffer is
full, the hub removes it and closes its channel.

**Alternatives:** Block until the client catches up; drop only the message;
use an unbounded queue per client.

**Trade-off:** A client that stalls briefly gets disconnected and must
reconnect and resync, which is why reconnect/resync is a week 2 goal.
`remove` checks room membership first, so a client dropped for being slow
is not closed twice when its `readPump` later calls `leave`.

## 4. Heartbeats and read limits

**Context:** A client that vanishes without closing (lid closed, wifi lost)
would hold goroutines and a hub slot until TCP times out, which can take
minutes.

**Decision:** The server pings every 27 seconds. Each pong extends the read
deadline to 30 seconds. Writes have a 10-second deadline. Incoming messages
are capped at 8 KB.

**Alternatives:** Rely on TCP keepalive; application-level heartbeat
messages.

**Trade-off:** Some constant tuning, and the 8 KB cap must be revisited if
messages grow (for example batched strokes or large shapes).

## 5. Server sequences events (replaces "server relays opaque bytes")

**Status:** Supersedes the week 1 design where the server relayed raw bytes
without parsing.

**Context:** With a pure relay, ordering depended on which message happened
to arrive last, different clients could see different orders, and the server
could not persist or replay anything.

**Decision:** The server parses `add`, `move`, and `delete`, assigns each a
per-room sequence number (`seq`) when the event is consumed from the log (see
decision 18), applies it to its own `Board`, and broadcasts it to everyone,
including the sender. Clients apply shape changes only when the server echoes
them.

**Alternatives:** Client-side timestamps; CRDTs (out of scope for this
project).

**Trade-off:** Conflicts are still last-write-wins, but "last" now means last
consumed from the log, which is the same order for every instance.
Shape actions wait for a server round trip (invisible on localhost). Undo is a
client-local stack of inverse events for the user's own shape actions. A
multi-event move is one undo step; undoing a move restores its starting
position and can therefore override a later remote move under the same
last-write-wins rule. Undo skips entries for shapes another user has deleted,
and undoing a delete re-adds the shape with its original ID.

## 6. Freehand strokes are ephemeral

**Context:** Pen strokes are very high volume (one message per mouse move)
and are not objects with identity.

**Decision:** `seg` messages are relayed as raw bytes to others in the room.
They are not sequenced, applied to the board, or persisted.

**Alternatives:** Persist every segment; group a stroke into one object when
the pen is released.

**Trade-off:** Strokes disappear on refresh and are not visible to late
joiners. Fixing this properly means treating a whole stroke as one shape.

## 7. Client-generated shape IDs

**Context:** Shapes need stable identity so `move` and `delete` can refer to
them.

**Decision:** The client creates IDs with `crypto.randomUUID()`.

**Alternatives:** Server-assigned IDs.

**Trade-off:** No round trip is needed to create an ID, but the server cannot
enforce uniqueness and trusts the client. A move or delete for a shape that
no longer exists is ignored, not treated as an error.

## 8. Validate events before they reach the hub

**Context:** A panic inside the hub goroutine would take down every room, for
example `add` with no shape.

**Decision:** `readPump` unmarshals and validates each message. Malformed
messages and unknown types are dropped silently. Only valid events reach the
hub.

**Alternatives:** Validate inside the hub.

**Trade-off:** Parsing runs in parallel in each client's goroutine, which
keeps work off the hub, but the validation rules live in the client file
rather than next to `Board.Apply`.

## 9. Snapshot on join

**Context:** Late joiners and reconnecting clients saw a blank board.

**Decision:** When a client joins, the hub sends one `snapshot` message
(current `seq` plus all shapes) as the first message on its `send` channel,
before any live event can reach it.

**Alternatives:** Replay the full event history; let the client request state
on demand.

**Trade-off:** A full snapshot is simple, but its size grows with the board.
Shape order comes from map iteration, so z-order is not stable across
reloads (see known gaps).

## 10. Load persisted boards when clients join

When persistence is enabled, `ServeWS` loads the requested room before
upgrading the WebSocket and passes the result to the hub as a preload. The
hub uses it only if the room is not already in memory, because memory is
newer than the database.

If the load fails, the connection is rejected with 503. Otherwise an empty
board could take over the room and overwrite the real saved board on the
next save.

The load happens before the upgrade so the hub goroutine never waits on a
database read.

This currently performs one storage read for every join, including rooms
whose boards are already in memory. That is acceptable at the current scale;
a future optimization could avoid repeated reads for in-memory rooms.

## 11. Persist debounced board snapshots

The hub saves dirty board snapshots on a one-second interval instead of
writing every event to MongoDB. This reduces database load, but a crash can
lose up to one interval of edits. Week 3's Kafka log is the intended durable
event-log solution.

The hub sends the persister a copy of the board, not the live one. The hub
keeps mutating its own maps, so sharing them with the persister would be a
data race.

Failed saves are logged but not retried until the room changes again. A small
future improvement is to mark the room dirty again after a failed save.

Graceful shutdown now flushes dirty boards on SIGINT/SIGTERM. A hard kill, OOM,
or crash can still lose edits from the last interval.

The persister processes snapshots in one goroutine, serializing writes so an
older snapshot cannot overwrite a newer one.

## 12. Hub depends on a `Store` interface

**Context:** `store` needs the `Board` and `Shape` types from `hub`, so `hub`
importing `store` would be an import cycle.

**Decision:** `hub` declares a small `Store` interface (`Load`, `Save`) and
the MongoDB implementation satisfies it, with a compile-time check in
`store`.

**Alternatives:** Move shared types into a third package.

**Trade-off:** The interface lives where it is used (idiomatic Go), and tests
use a fake store with no database. Persistence is optional: with no store the
hub works in memory only.

## 13. Testing concurrent code without sleeps

**Context:** An early hub test passed or failed depending on goroutine
scheduling (the broadcast channel is buffered, so the test raced the hub).

**Decision:** Tests use a healthy "probe" client as a barrier. The hub handles
messages in FIFO order, so once the probe has received the last message, the
earlier ones are guaranteed to be processed. The suite runs with
`go test -race -count=50`.

**Alternatives:** `time.Sleep` in tests.

**Trade-off:** Slightly more test setup, but no timing guesses and no flaky
tests.

## 14. Reconnect with full-snapshot resync

**Context:** Clients get disconnected (server restart, dropped network, slow
client dropped by the hub) and must recover without a manual refresh.

**Decision:** The browser reconnects automatically with exponential backoff
(0.5s doubling to a 10s cap) and jitter. A reconnecting client is treated as
a new client, so the hub sends it a full snapshot, which replaces its state.
The client tracks the last `seq` and forces a reconnect if it sees a gap.
Backoff resets when a snapshot arrives, not when the socket opens.

**Alternatives:** Client sends its last `seq` and the server replays only the
missing events; this needs a bounded event history per room. This delta-resync
idea was realised in step 3 using the event log.

**Trade-off:** A full snapshot costs more bandwidth than a delta, and its
size grows with the board. In exchange there is no history to retain and no
catch-up edge cases. The event log now supports delta resync, realised in
step 3. Input is ignored while offline (no queue), so actions taken during an
outage are lost, not replayed.

## 15. Containerized with a multi-stage build and env-based config

**Context:** The service must run the same way on a laptop, in compose, and
(week 4) in Kubernetes, where `localhost` no longer reaches the database.

**Decision:** Configuration comes from environment variables (`MONGO_URI`,
`ADDR`, `WEB_DIR`) with local-development defaults. The Dockerfile builds a
static binary in a Go image and copies it with the web folder into a distroless
non-root image. Compose waits for a Mongo healthcheck before starting the
service, and Mongo data lives in a named volume.

**Alternatives:** A single-stage image on a full Go base (about 1 GB, with the
toolchain in production); config files baked into the image.

**Trade-off:** The distroless image has no shell, so debugging inside the
running container is harder. The static web files are baked into the image,
so a UI change requires a rebuild.

## 16. Graceful shutdown with a final flush

**Context:** Deploys and `docker stop` send SIGTERM. Without handling it, the
last interval of edits is lost, and clients see an abrupt TCP drop.

**Decision:** On SIGINT/SIGTERM the process first stops the HTTP listener, then
shuts down the hub. The hub closes every client (so each receives a WebSocket
close frame), synchronously hands all dirty boards to the persister, and waits
for the persister to finish its last save. The process uses a shared 8-second
shutdown deadline, below Docker's default 10-second stop grace period. After the hub exits, its
`done` channel releases any goroutine still trying to send to it.

**Alternatives:** Rely on the periodic flush and accept losing up to one
interval on every deploy; write every event to the database synchronously.

**Trade-off:** `http.Server.Shutdown` does not wait for hijacked WebSocket
connections, so the hub needs its own shutdown path and ordering has to be
explicit. A hard kill, OOM, or crash still loses up to one interval of edits,
which only the week 3 event log can fix. The flush interval is configurable
(`PERSIST_INTERVAL`), so the test can prove the final flush is the one that
saved the data.

## 17. Two layers of tests

**Context:** Concurrency bugs live in the interaction between goroutines, but
unit tests with fake clients cannot see the WebSocket handler, the pumps, or
the JSON validation.

**Decision:** Hub unit tests use fake clients and a fake store to check
broadcast, backpressure, sequencing, persistence and shutdown deterministically.
Integration tests run the real `ServeWS` on a random port through
`httptest` with real gorilla clients, and cover ordering, late-join snapshots,
room isolation, reconnect, malformed input, the message-size limit, and
shutdown. The integration tests live in an external `hub_test` package, so they
can only use the exported API. Everything runs with `-race`.

**Alternatives:** Only end-to-end tests against the Docker stack; mocking the
WebSocket connection.

**Trade-off:** The tests avoid timing assumptions by treating the join
snapshot as a "you are registered" signal and by proving isolation through
event ordering instead of waiting for silence. The cost is that a few tests
depend on the exact join protocol, so a change to it means updating them.
Mongo itself is not covered here: persistence is tested against a fake store,
so the real driver path is only exercised by the manual Docker checks.

## 18. The Kafka log is the source of truth; seq is assigned on consume

**Context:** One hub instance assigning `seq` works, but two instances would
each assign their own, and snapshot persistence can lose recent edits.

**Decision:** Validated shape events are published to a single Kafka topic keyed
by room, so each room's events land in one partition and keep their order. The
hub applies an event only when it comes back from the log: it assigns the
room's next `seq`, applies it, and broadcasts to every client in the room,
including the sender. Because every instance reads the same ordered stream
and counts the same way, instances agree on `seq` without coordinating. Each
instance consumes without a consumer group, from the beginning of every
partition, so it both receives every event (fan-out) and rebuilds its boards
by replay on startup. An instance does not accept connections until replay has
reached the log's end offsets as they were at startup. The topic has infinite
retention. Records are validated again when consumed, so a malformed record is
skipped and cannot crash the hub or consume a `seq`.

**Alternatives:** Instance-assigned `seq` (breaks with multiple instances); a
shared consumer group (each event goes to only one instance, which is
work-sharing, not fan-out); Redis pub/sub (fan-out only, no durable log).

**Trade-off:** Anything a client has seen echoed is already in the log, so it
survives a crash, and the log doubles as the replay source for undo. The costs
are an extra round trip on every shape action, startup time that grows with the
log (step 3 adds snapshots that record the offset they cover so only the tail
is replayed), unbounded topic growth, and a failed publish that simply drops
the action (it is logged, and the client never sees an echo). Freehand strokes
still use a local relay, so they do not reach other instances yet.

## 19. Snapshots with log offsets bound the replay; failed saves retry

**Context:** Replaying the entire log on startup gets slower with every event
ever drawn.

**Decision:** Boards are still snapshotted to MongoDB on the debounce
interval, and each saved board records the partition and offset it reflects.
A batch writes the changed boards first and a single checkpoint document
(partition -> offset) last. On startup the hub restores the boards, and the
consumer resumes each partition just after its checkpoint. Replayed events at or
below a board's own offset are skipped, so a crash between the board writes and
the checkpoint write can never apply an event twice. Boards and offsets are
copied at the same instant inside the hub goroutine, so each snapshot is
consistent. A failed save keeps the checkpoint where it was and marks the
rooms dirty again, so it is retried on the next tick. If the checkpoint is
ahead of the log (for example the topic was recreated), startup fails loudly.

**Alternatives:** Always replay from the start; MongoDB transactions (not
available on a standalone server); snapshots keyed by a single global offset
(offsets are per partition).

**Trade-off:** Startup time depends on the time since the last checkpoint, not
on the age of the log, and the log stays the source of truth: a lost or stale
snapshot only costs replay time. The price is more moving parts and an
ordering argument to maintain. The partition count of the topic must never
change, because it would remap room keys to different partitions and break
both ordering and the per-board offsets. Snapshots assume a single writer;
with two instances an older snapshot can overwrite a newer one, which step 5
must fix.

## 20. Undo is a client-side compensating event

**Context:** Users need to undo their own actions in a shared room. The event
log is append-only and every instance must derive identical state from it.

**Decision:** Each client keeps a history of the inverse of its own completed
actions (the inverse of an add is a delete, of a move is a move back to where
the drag started, of a delete is an add of the full shape under its original
ID). One drag is one history entry. Undo sends the most recent inverse as an
ordinary event; entries that someone else has already made pointless are
skipped without sending anything. The server and the log are unchanged: an
undo is indistinguishable from a normal edit and gets its own seq.

**Alternatives:** Global undo of the room's last event (lets users erase each
other's work); server-side per-user undo stacks (they affect how events are
applied, so they would have to be stored in the snapshots and kept identical
across instances, or boards diverge).

**Trade-off:** History is lost on refresh, there is no redo, and undoing a move
restores the position the user last saw, overriding any later move by someone
else (last-write-wins). Undone deletes return a shape to the top of the
z-order. In exchange there is no new server state, so replay, snapshots and
multi-instance fan-out are unaffected.

## 21. Freehand strokes fan out over a short-retention topic

**Context:** With two instances, a stroke from a client on one instance never
reached clients on the other. Strokes are high-volume, disposable, and have no
need for ordering or replay.

**Decision:** An instance relays a stroke to its own clients immediately and also
publishes it to a second topic with one-minute retention, tagged with the
instance's id. Each instance consumes that topic from "now" with no replay and
ignores records carrying its own id, so a stroke is delivered exactly once to
each client. Strokes are never sequenced, persisted or replayed.

**Alternatives:** Put strokes in the main log (permanent, replayed and
checkpointed for no benefit); Redis pub/sub, the usual tool for pure fan-out,
which would add another component.

**Trade-off:** A stroke is lost if the receiving instance is down or
disconnected, and it is not ordered relative to shape events. That is acceptable
for ephemeral drawing, and strokes vanish on refresh, as before. Reusing Kafka
avoids a fourth component, but Redis would be the more natural choice for this
traffic.

## 22. Snapshot writes are monotonic, so any instance may write

**Context:** Every instance consumes the whole log, so every instance saves
snapshots. Without care, a slower instance can overwrite a newer board and the
checkpoint can then run ahead of that board, so a restart would silently skip
events.

**Decision:** A board document is replaced only if its stored offset is lower
than the incoming one, and each partition's checkpoint advances with `$max`.
Neither can move backward whichever instance writes, and a rejected write only
means a newer snapshot already exists. All instances compute identical state
from the same log, so any of their snapshots is valid.

**Alternatives:** Leader election with a lease so one instance writes; one
checkpoint document per instance.

**Trade-off:** Every instance writes to MongoDB, so writes are duplicated by the
number of instances, and leader-only snapshots would remove that. Correctness
also depends on all instances running the same code, because an older version
that applied events differently would produce incompatible snapshots.

## 23. Multi-instance behaviour is tested on a simulated log

**Context:** Kafka is slow and awkward to start in unit tests.

**Decision:** An in-memory bus implements the same interface as the Kafka
log: one total order, delivery to every attached instance, replay on attach, and
an ephemeral channel that skips the sender. Two or three real hubs with real
WebSocket servers run on it, and tests check shared ordering, the same seq on
both instances, stroke delivery exactly once, and a fresh instance rebuilding
from the full log. The Kafka client itself is covered by the manual Docker
checks.

**Trade-off:** The tests are fast and deterministic, but the real Kafka
producer, consumer and offset handling are not covered automatically.

## 24. Liveness and readiness are separate, and shutdown drains first

**Context:** Replay takes time that grows with the log, and Kubernetes removes
a terminating pod from its Service at about the same moment it signals the
pod, so new connections can still arrive for a few seconds.

**Decision:** The HTTP server starts immediately. `/healthz` is a liveness
check that answers as soon as the process runs; `/readyz`, `/ws` and the web
page return 503 until replay is complete. On SIGTERM the instance flips to
not-ready, waits `SHUTDOWN_DELAY` (5 s in the chart) for proxies to notice,
then runs the normal shutdown (stop listener, flush the log, close clients
with a close frame so they reconnect to another pod). The pod's termination
grace period (30 s) covers the delay plus the shutdown budget.

**Alternatives:** A startup probe with a long failure threshold around a
single probe endpoint; a `preStop` sleep in the pod spec instead of in the app.

**Trade-off:** Liveness only proves the process answers HTTP, so a deadlocked
hub would not be detected (a probe that round-trips through the hub would
fix that). The drain delay lengthens every deploy slightly.

## 25. Helm for the service, plain manifests for the infrastructure

**Decision:** Mongo and Kafka run as single-replica StatefulSets from plain
manifests, since they are dependencies and not the thing being built. The Pulse
service is a Helm chart with three replicas, a rolling update that never drops
below full ready capacity (`maxUnavailable: 0`), replicas spread across nodes
where possible, and a PodDisruptionBudget of two. Configuration is passed
through environment variables in `values.yaml`.

**Trade-off:** Single-replica Kafka and Mongo are single points of failure,
which is acceptable for a local cluster but would be the first thing to change
in production. Every replica writes snapshots, which monotonic writes
(decision 22) make safe.

## 26. NGINX as a plain reverse proxy, not an ingress controller

**Context:** The plan called for an NGINX ingress. The community ingress-nginx
controller was retired in March 2026: the repository is archived and no
security fixes are published.

**Decision:** NGINX runs as a normal Deployment (two replicas, non-root image)
with its configuration in a ConfigMap, exposed through a NodePort mapped to
localhost by kind. The configuration forwards the WebSocket upgrade headers
over HTTP/1.1, sets explicit long idle timeouts, and retries a 503 from a pod
that is starting or draining.

**Alternatives:** The retired controller; a Gateway API implementation or
another ingress controller.

**Trade-off:** There is no Ingress resource, TLS termination, or hostname
routing, which matters on a real cluster, where a Gateway API implementation
would be the migration path. In exchange, the proxy configuration is fully
visible and is the part that actually has to be understood for WebSockets.

## 27. No session affinity; any pod can serve any room

**Context:** Stickiness is usually needed when pods hold private state.

**Decision:** NGINX forwards to the Service's virtual IP and kube-proxy picks a
ready pod per new connection, so there is no affinity. A WebSocket stays on its
pod for its lifetime because it is one TCP connection. When that pod goes away,
a client reconnects to whichever pod it reaches and receives a snapshot. Every
pod derives its state from the same log, so which pod it is does not matter.

**Alternatives:** Cookie or IP-hash affinity; consistent routing of a room to a
pod.

**Trade-off:** Pods consume every partition, so each carries every room's board
in memory regardless of where its clients are, and memory grows with total
data rather than with local load. Routing rooms to specific pods would shrink
that, at the cost of the simplicity that makes a pod kill a non-event.
Connections are balanced per connection, not per load, so a pod can end up
with more clients than another.

## 28. A small, bounded metric set that answers specific questions

**Decision:** Each instance exposes Prometheus metrics for connections,
messages in and out, events applied, publish-to-apply latency, hub broadcast
time, dropped slow clients, log and snapshot health, and startup replay time.
Label values come from a fixed set; anything client-supplied that is not
recognised is counted as `unknown` or `invalid`, so a client cannot create an
unbounded number of series. Publish-to-apply latency is measured with a
timestamp carried in the event itself and is recorded only after replay has
finished, so replayed history cannot pollute it.

**Alternatives:** OpenTelemetry metrics through a Prometheus exporter, which
translates metric names; a larger metric set.

**Trade-off:** The latency number depends on publisher and consumer clocks
agreeing, which holds in a single-host cluster but not in general. Because every
pod applies every event, event-rate panels use a max across pods, not a sum.

## 29. Metrics on a private port; plain Prometheus and Grafana

**Decision:** Metrics are served on port 9100, separate from the public port and
never routed through NGINX. Prometheus finds pods through Kubernetes service
discovery and scrapes them directly. Prometheus and Grafana run as plain
single-replica Deployments with data in `emptyDir`, and Grafana is provisioned
from ConfigMaps with anonymous access, for local use only.

**Alternatives:** The kube-prometheus-stack chart with the Prometheus operator.

**Trade-off:** Much lighter and easier to read, but there is no Alertmanager,
no persistence (metrics vanish when Prometheus restarts), and no authentication,
all of which a real deployment needs. NGINX is not instrumented.

## 30. Traces follow an event through the log via Kafka headers

**Context:** An event's latency is spread over a WebSocket read, a Kafka
produce, the broker, and consumption on every pod. Metrics show the total, not
where the time went.

**Decision:** The root span starts when a valid shape event is received and the
sampling decision is made there; everything downstream follows the parent. The
publisher injects the W3C trace context into the Kafka record headers, and each
consuming pod extracts it and starts `hub.apply` and `hub.broadcast` spans as
children, so one trace shows the same event being applied on every pod. Records
read during replay are never given a trace context, because stored headers can
refer to traces from long ago and would flood the backend on every restart.
Freehand strokes are not traced. Tracing is off unless
`OTEL_EXPORTER_OTLP_ENDPOINT` is set, and the sample ratio is configurable.

**Alternatives:** Putting a trace id in the event body; tracing every event; a
link between spans instead of parent-child.

**Trade-off:** Consume-side spans exist on every pod, so trace volume scales
with the number of replicas. Span timestamps come from each pod's own clock, so
cross-pod durations are only trustworthy where clocks agree, as on a single
host. Jaeger runs with in-memory storage, so traces disappear when it restarts.

## 31. How the load test measures, and what it can and cannot say

**Decision:** A Go client swarm opens real WebSocket connections through NGINX
and sends a mix of adds, moves and deletes (plus optional strokes) at a fixed
per-client rate, spread over a configurable number of rooms. Latency is
measured end to end by embedding the send time in the id of each added shape and
subtracting it when the shape is echoed back from the log. Room size is varied
on purpose, because outbound traffic is events per second multiplied by clients
per room. Clients delete their shapes on exit, since the log is permanent.
Tracing is sampled at 1% during the test.

**Trade-off:** The generator, the cluster, Docker's VM and the browser share one
laptop, so absolute numbers describe this machine and are only meaningful next
to its CPU and memory figures. Server-side metrics are compared with the
generator's latency to tell whether a slowdown is inside the app. A run adds
its events to the permanent log, which is itself a measure of replay cost.

## 32. Overload behaviour: ingress is unbounded (known gap)

**Observation:** Three 60-second runs at 200 clients had p95 echo latency of
35-36 ms; two runs at 300 clients had p95 around 135 ms. At 400 clients, six
runs ranged from 149 ms to 16.1 s p95: two coped (0.15 s and 0.31 s) and four
fell behind (3.5-16.1 s). Four runs were confirmed to start with measured lag
below 10 records; they produced p95 values of 0.15 s, 3.54 s, 9.52 s, and
16.10 s. The backlog state at the start of the other two runs was not recorded.
This is consistent with the system sitting at its capacity, where small
differences in available CPU can determine whether backlog grows, but the cause
of the spread has not been established. The edge for this setup is roughly
600-800 events/s.

Across three 800-client runs, p50 echo latency ranged from 6.2 to 11.6 s and
p95 from 17.6 to 31.1 s; generator-reported drops ranged from 6 to 71. In an
instrumented 800-client run, 28.2% of adds had not echoed at run end. Echo
percentiles include only adds echoed before the loadgen stops; queued events
are censored, making overload latency a lower bound. A prior 800-client run
showed every pod's consumer lag at about 56-61k records, while the fullest
per-client send queue stayed below 29 of 64 slots (sampled once per second) and
each pod used about 2 CPU cores. One 800-client run dropped 71 clients, so slow
receivers are a secondary effect.

A separate single-room verification run had 61 generator-reported drops while
the server slow-client counter increased by 69; the counts did not match
exactly. Over the six-hour Prometheus window, resident memory peaked at about
104 MiB per pod.

**Consequence:** The lag, queue, and CPU measurements point to the per-pod
consume/apply path as the primary overload limit, with slow receivers a
secondary effect. Each pod applies every event, so adding replicas is expected
to increase connection and fan-out capacity but not apply capacity; this scaling
effect was not tested. No single result establishes an exact stable capacity:
the measured edge is roughly 600-800 events/s on this setup.

**Not done, and why:** Per-connection rate limits and a bounded publish queue
that sheds events with a "busy" response remain the first changes to make
before real use. Room-aware routing could scale the apply rate, but would add
complexity. No CPU profile was captured, so the evidence identifies the
consume/apply path but not its function-level hotspot.

---

## Known gaps and future work

- Z-order is unstable after reload because snapshots come from map iteration.
  Fix: store a creation `seq` on each shape and sort by it.
- One message per mouse move, and a full redraw on every event. Candidates:
  batching, and `requestAnimationFrame` rendering.
- Boards stay in the hub's memory after their room empties, so memory grows.
  Fix: evict after a final save.
- NGINX itself is not instrumented; add an exporter if proxy-level metrics are
  needed.
