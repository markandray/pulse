package main

import (
    "bytes"
    "encoding/json"
    "flag"
    "fmt"
    "math/rand"
    "net/url"
    "os"
    "os/signal"
    "sort"
    "strconv"
    "strings"
    "sync"
    "sync/atomic"
    "time"

    "github.com/gorilla/websocket"
)

type config struct {
    url      string
    clients  int
    rooms    int
    rate     float64 // shape events per second per client
    strokes  float64 // freehand strokes per second per client
    duration time.Duration
    ramp     time.Duration
}

type stats struct {
    connected atomic.Int64
    failed    atomic.Int64 // connections that could not be opened
    dropped   atomic.Int64 // connections that died during the run
    sent      atomic.Int64
    received  atomic.Int64

    mu     sync.Mutex
    window []time.Duration // echo latencies since the last report
    all    []time.Duration
}

func (s *stats) observe(d time.Duration) {
    s.mu.Lock()
    s.window = append(s.window, d)
    s.all = append(s.all, d)
    s.mu.Unlock()
}

func (s *stats) takeWindow() []time.Duration {
    s.mu.Lock()
    w := s.window
    s.window = nil
    s.mu.Unlock()
    return w
}

func summarize(d []time.Duration) string {
    if len(d) == 0 {
        return "no samples"
    }
    sort.Slice(d, func(i, j int) bool { return d[i] < d[j] })
    at := func(p float64) time.Duration { return d[int(float64(len(d)-1)*p)].Round(100 * time.Microsecond) }
    return fmt.Sprintf("p50=%v p95=%v p99=%v max=%v (n=%d)",
        at(0.50), at(0.95), at(0.99), d[len(d)-1].Round(100*time.Microsecond), len(d))
}

var addPrefix = []byte(`{"type":"add"`)

func runClient(id int, room string, cfg config, st *stats, stop <-chan struct{}, wg *sync.WaitGroup) {
    defer wg.Done()

    dialer := websocket.Dialer{HandshakeTimeout: 10 * time.Second}
    conn, _, err := dialer.Dial(cfg.url+"?room="+url.QueryEscape(room), nil)
    if err != nil {
        st.failed.Add(1)
        return
    }
    st.connected.Add(1)
    defer st.connected.Add(-1)
    defer conn.Close()

    prefix := fmt.Sprintf("c%d-", id)
    closing := make(chan struct{}) // closed by the reader when the connection ends

    go func() {
        defer close(closing)
        for {
            _, data, err := conn.ReadMessage()
            if err != nil {
                return
            }
            st.received.Add(1)
            if !bytes.HasPrefix(data, addPrefix) {
                continue
            }
            var ev struct {
                Shape struct {
                    ID string `json:"id"`
                } `json:"shape"`
            }
            if json.Unmarshal(data, &ev) != nil || !strings.HasPrefix(ev.Shape.ID, prefix) {
                continue // someone else's shape
            }
            // my own shape coming back: id = c<client>-<n>-<unix nanos at send>
            if i := strings.LastIndexByte(ev.Shape.ID, '-'); i >= 0 {
                if ns, err := strconv.ParseInt(ev.Shape.ID[i+1:], 10, 64); err == nil {
                    st.observe(time.Since(time.Unix(0, ns)))
                }
            }
        }
    }()

    rng := rand.New(rand.NewSource(int64(id) + time.Now().UnixNano()))
    interval := time.Duration(float64(time.Second) / cfg.rate)
    time.Sleep(time.Duration(rng.Int63n(int64(interval)))) // desynchronise the clients
    tick := time.NewTicker(interval)
    defer tick.Stop()

    var segC <-chan time.Time
    if cfg.strokes > 0 {
        segTick := time.NewTicker(time.Duration(float64(time.Second) / cfg.strokes))
        defer segTick.Stop()
        segC = segTick.C
    }

    n := 0
    var mine []string // shapes this client currently has on the board
    for {
        select {
        case <-stop:
            // leave the board clean: the event log keeps everything forever
            for _, sid := range mine {
                data, _ := json.Marshal(map[string]any{"type": "delete", "id": sid})
                conn.WriteMessage(websocket.TextMessage, data)
            }
            conn.WriteMessage(websocket.CloseMessage,
                websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
            return
        case <-closing:
            st.dropped.Add(1)
            return
        case <-segC:
            seg := []byte(`{"type":"seg","color":"#c33","x0":10,"y0":10,"x1":20,"y1":25}`)
            if err := conn.WriteMessage(websocket.TextMessage, seg); err != nil {
                st.dropped.Add(1)
                return
            }
            st.sent.Add(1)
            continue
        case <-tick.C:
        }

        var msg map[string]any
        switch {
        case len(mine) < 3: // add a shape; its id carries the send time
            n++
            sid := fmt.Sprintf("c%d-%d-%d", id, n, time.Now().UnixNano())
            mine = append(mine, sid)
            msg = map[string]any{"type": "add", "shape": map[string]any{
                "id": sid, "kind": "rect", "color": "#3a7",
                "x": float64(rng.Intn(700)), "y": float64(rng.Intn(500)), "w": 40, "h": 30,
            }}
        case rng.Intn(4) == 0: // remove the oldest
            msg = map[string]any{"type": "delete", "id": mine[0]}
            mine = mine[1:]
        default: // move a random one
            msg = map[string]any{"type": "move", "id": mine[rng.Intn(len(mine))],
                "x": float64(rng.Intn(800)), "y": float64(rng.Intn(600))}
        }
        data, _ := json.Marshal(msg)
        if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
            st.dropped.Add(1)
            return
        }
        st.sent.Add(1)
    }
}

func main() {
    var cfg config
    flag.StringVar(&cfg.url, "url", "ws://localhost:8080/ws", "WebSocket endpoint")
    flag.IntVar(&cfg.clients, "clients", 100, "number of simulated clients")
    flag.IntVar(&cfg.rooms, "rooms", 10, "clients are spread evenly over this many rooms")
    flag.Float64Var(&cfg.rate, "rate", 2, "shape events per second per client")
    flag.Float64Var(&cfg.strokes, "strokes", 0, "freehand stroke messages per second per client")
    flag.DurationVar(&cfg.duration, "duration", 60*time.Second, "how long to run")
    flag.DurationVar(&cfg.ramp, "ramp", 10*time.Second, "time over which clients connect")
    flag.Parse()
    if cfg.clients < 1 || cfg.rooms < 1 || cfg.rate <= 0 || cfg.ramp >= cfg.duration {
        fmt.Fprintln(os.Stderr, "need clients>=1, rooms>=1, rate>0 and ramp < duration")
        os.Exit(2)
    }

    st := &stats{}
    stop := make(chan struct{})
    var wg sync.WaitGroup

    fmt.Printf("clients=%d rooms=%d (%d per room) rate=%.1f/s strokes=%.1f/s duration=%v\n",
        cfg.clients, cfg.rooms, cfg.clients/cfg.rooms, cfg.rate, cfg.strokes, cfg.duration)

    start := time.Now()
    rampDone := make(chan struct{})
    go func() {
        defer close(rampDone)
        gap := cfg.ramp / time.Duration(cfg.clients)
        for i := 0; i < cfg.clients; i++ {
            select {
            case <-stop:
                return
            default:
            }
            wg.Add(1)
            go runClient(i, fmt.Sprintf("load-%d", i%cfg.rooms), cfg, st, stop, &wg)
            time.Sleep(gap)
        }
    }()

    sigs := make(chan os.Signal, 1)
    signal.Notify(sigs, os.Interrupt)
    report := time.NewTicker(5 * time.Second)
    deadline := time.After(cfg.duration)
    lastSent, lastRecv, lastT := int64(0), int64(0), start

loop:
    for {
        select {
        case now := <-report.C:
            sent, recv := st.sent.Load(), st.received.Load()
            dt := now.Sub(lastT).Seconds()
            fmt.Printf("t=%3.0fs conns=%d failed=%d dropped=%d sent=%.0f/s recv=%.0f/s echo: %s\n",
                now.Sub(start).Seconds(), st.connected.Load(), st.failed.Load(), st.dropped.Load(),
                float64(sent-lastSent)/dt, float64(recv-lastRecv)/dt, summarize(st.takeWindow()))
            lastSent, lastRecv, lastT = sent, recv, now
        case <-deadline:
            break loop
        case <-sigs:
            break loop
        }
    }
    close(stop)
    <-rampDone
    wg.Wait()

    sent, recv := st.sent.Load(), st.received.Load()
    fmt.Println("---- summary ----")
    fmt.Printf("sent=%d received=%d fan-out=%.1fx failed=%d dropped=%d\n",
        sent, recv, float64(recv)/float64(max(sent, 1)), st.failed.Load(), st.dropped.Load())
    fmt.Println("echo latency over the whole run:", summarize(st.all))
    if st.failed.Load() > 0 {
        fmt.Println("hint: failed connections usually mean the file-descriptor limit; run `ulimit -n 10240` first")
    }
}