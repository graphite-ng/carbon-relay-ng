package route

import (
	"bytes"
	"crypto/tls"
	"hash/fnv"
	"io/ioutil"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Dieterbe/go-metrics"
	"github.com/golang/snappy"
	dest "github.com/graphite-ng/carbon-relay-ng/destination"
	"github.com/graphite-ng/carbon-relay-ng/matcher"
	"github.com/graphite-ng/carbon-relay-ng/stats"
	"github.com/graphite-ng/carbon-relay-ng/util"
	"github.com/jpillora/backoff"

	"github.com/lomik/go-carbon/persister"
	"gopkg.in/raintank/schema.v1"
	"gopkg.in/raintank/schema.v1/msg"
)

type GrafanaNet struct {
	baseRoute
	addr    string
	apiKey  string
	buf     chan *util.Point
	schemas persister.WhisperSchemas

	bufSize      int // amount of messages we can buffer up. each message is about 100B. so 1e7 is about 1GB.
	flushMaxNum  int
	flushMaxWait time.Duration
	timeout      time.Duration
	sslVerify    bool
	blocking     bool
	dispatch     func(chan *util.Point, *util.Point, metrics.Gauge, metrics.Counter)
	concurrency  int
	orgId        int
	writeQueues  []chan []byte
	shutdown     chan struct{}
	wg           *sync.WaitGroup
	client       *http.Client

	numErrFlush       metrics.Counter
	numOut            metrics.Counter   // metrics successfully written to our buffered conn (no flushing yet)
	numDropBuffFull   metrics.Counter   // metric drops due to queue full
	durationTickFlush metrics.Timer     // only updated after successful flush
	durationManuFlush metrics.Timer     // only updated after successful flush. not implemented yet
	tickFlushSize     metrics.Histogram // only updated after successful flush
	manuFlushSize     metrics.Histogram // only updated after successful flush. not implemented yet
	numBuffered       metrics.Gauge
	bufferSize        metrics.Gauge
}

// NewGrafanaNet creates a special route that writes to a grafana.net datastore
// We will automatically run the route and the destination
// ignores spool for now
func NewGrafanaNet(key, prefix, sub, regex, addr, apiKey, schemasFile string, spool, sslVerify, blocking bool, bufSize, flushMaxNum, flushMaxWait, timeout, concurrency, orgId int) (Route, error) {
	m, err := matcher.New(prefix, sub, regex)
	if err != nil {
		return nil, err
	}
	schemas, err := getSchemas(schemasFile)
	if err != nil {
		return nil, err
	}

	cleanAddr := util.AddrToPath(addr)

	r := &GrafanaNet{
		baseRoute: baseRoute{sync.Mutex{}, atomic.Value{}, key},
		addr:      addr,
		apiKey:    apiKey,
		buf:       make(chan *util.Point, bufSize), // takes about 228MB on 64bit
		schemas:   schemas,

		bufSize:      bufSize,
		flushMaxNum:  flushMaxNum,
		flushMaxWait: time.Duration(flushMaxWait) * time.Millisecond,
		timeout:      time.Duration(timeout) * time.Millisecond,
		sslVerify:    sslVerify,
		blocking:     blocking,
		concurrency:  concurrency,
		orgId:        orgId,
		writeQueues:  make([]chan []byte, concurrency),
		shutdown:     make(chan struct{}),
		wg:           new(sync.WaitGroup),

		numErrFlush:       stats.Counter("dest=" + cleanAddr + ".unit=Err.type=flush"),
		numOut:            stats.Counter("dest=" + cleanAddr + ".unit=Metric.direction=out"),
		durationTickFlush: stats.Timer("dest=" + cleanAddr + ".what=durationFlush.type=ticker"),
		durationManuFlush: stats.Timer("dest=" + cleanAddr + ".what=durationFlush.type=manual"),
		tickFlushSize:     stats.Histogram("dest=" + cleanAddr + ".unit=B.what=FlushSize.type=ticker"),
		manuFlushSize:     stats.Histogram("dest=" + cleanAddr + ".unit=B.what=FlushSize.type=manual"),
		numBuffered:       stats.Gauge("dest=" + cleanAddr + ".unit=Metric.what=numBuffered"),
		bufferSize:        stats.Gauge("dest=" + cleanAddr + ".unit=Metric.what=bufferSize"),
		numDropBuffFull:   stats.Counter("dest=" + cleanAddr + ".unit=Metric.action=drop.reason=queue_full"),
	}

	r.bufferSize.Update(int64(bufSize))

	if blocking {
		r.dispatch = dispatchBlocking
	} else {
		r.dispatch = dispatchNonBlocking
	}

	for i := 0; i < r.concurrency; i++ {
		r.writeQueues[i] = make(chan []byte)
	}
	r.config.Store(baseConfig{*m, make([]*dest.Destination, 0)})

	// start off with a transport the same as Go's DefaultTransport
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
			DualStack: true,
		}).DialContext,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}

	// disable http 2.0 because there seems to be a compatibility problem between nginx hosts and the golang http2 implementation
	// which would occasionally result in bogus `400 Bad Request` errors.
	transport.TLSNextProto = make(map[string]func(authority string, c *tls.Conn) http.RoundTripper)

	if !r.sslVerify {
		transport.TLSClientConfig = &tls.Config{
			InsecureSkipVerify: true,
		}
	}

	r.client = &http.Client{
		Timeout:   r.timeout,
		Transport: transport,
	}

	go r.run()
	return r, nil
}

func (route *GrafanaNet) run() {
	metrics := make([][]*schema.MetricData, route.concurrency)
	route.wg.Add(route.concurrency)
	for i := 0; i < route.concurrency; i++ {
		metrics[i] = make([]*schema.MetricData, 0, route.flushMaxNum)

		// start up our goroutines for writing data to tsdb-gw
		go route.flush(i)
	}

	flush := func(shard int) {
		if len(metrics[shard]) == 0 {
			return
		}
		mda := schema.MetricDataArray(metrics[shard])
		data, err := msg.CreateMsg(mda, 0, msg.FormatMetricDataArrayMsgp)
		if err != nil {
			panic(err)
		}
		route.writeQueues[shard] <- data
		route.numOut.Inc(int64(len(metrics[shard])))
		metrics[shard] = metrics[shard][:0]

	}

	hasher := fnv.New32a()

	ticker := time.NewTicker(route.flushMaxWait)
	for {
		select {
		case point := <-route.buf:
			route.numBuffered.Dec(1)
			md, err := parseMetric(point, route.schemas, route.orgId)
			if err != nil {
				log.Error("RouteGrafanaNet: %s", err)
				continue
			}
			md.SetId()

			hasher.Reset()
			hasher.Write([]byte(md.Name))
			shard := int(hasher.Sum32() % uint32(route.concurrency))
			metrics[shard] = append(metrics[shard], md)
			if len(metrics[shard]) == route.flushMaxNum {
				flush(shard)
			}
		case <-ticker.C:
			for shard := 0; shard < route.concurrency; shard++ {
				flush(shard)
			}
		case <-route.shutdown:
			for shard := 0; shard < route.concurrency; shard++ {
				flush(shard)
				close(route.writeQueues[shard])
			}
			return
		}
	}
}

func (route *GrafanaNet) flush(shard int) {
	b := &backoff.Backoff{
		Min:    100 * time.Millisecond,
		Max:    time.Minute,
		Factor: 1.5,
		Jitter: true,
	}
	body := new(bytes.Buffer)
	for data := range route.writeQueues[shard] {
		for {
			pre := time.Now()
			body.Reset()
			snappyBody := snappy.NewWriter(body)
			snappyBody.Write(data)
			snappyBody.Close()
			req, err := http.NewRequest("POST", route.addr, body)
			if err != nil {
				panic(err)
			}
			req.Header.Add("Authorization", "Bearer "+route.apiKey)
			req.Header.Add("Content-Type", "rt-metric-binary-snappy")
			resp, err := route.client.Do(req)
			diff := time.Since(pre)
			if err == nil && resp.StatusCode >= 200 && resp.StatusCode < 300 {
				b.Reset()
				log.Info("GrafanaNet sent metrics in %s -msg size %d", diff, len(data))

				route.tickFlushSize.Update(int64(len(data)))
				route.durationTickFlush.Update(diff)

				ioutil.ReadAll(resp.Body)
				resp.Body.Close()
				break
			}
			route.numErrFlush.Inc(1)
			dur := b.Duration()
			if err != nil {
				log.Warning("GrafanaNet failed to submit data: %s will try again in %s (this attempt took %s)", err, dur, diff)
			} else {
				buf := make([]byte, 300)
				n, _ := resp.Body.Read(buf)
				log.Warning("GrafanaNet failed to submit data: http %d - %s will try again in %s (this attempt took %s)", resp.StatusCode, buf[:n], dur, diff)

				ioutil.ReadAll(resp.Body)
				resp.Body.Close()
			}

			time.Sleep(dur)
		}
	}
	route.wg.Done()
}

func (route *GrafanaNet) Dispatch(point *util.Point) {
	//conf := route.config.Load().(Config)
	// should return as quickly as possible
	log.Info("route %s sending to dest %s: %s", route.key, route.addr, point)
	route.dispatch(route.buf, point, route.numBuffered, route.numDropBuffFull)
}

func (route *GrafanaNet) Flush() error {
	//conf := route.config.Load().(Config)
	// no-op. Flush() is currently not called by anything.
	return nil
}

func (route *GrafanaNet) Shutdown() error {
	//conf := route.config.Load().(Config)

	// trigger all of our queues to be flushed to the tsdb-gw
	route.shutdown <- struct{}{}

	// wait for all tsdb-gw writes to complete.
	route.wg.Wait()
	return nil
}

func (route *GrafanaNet) Snapshot() Snapshot {
	snapshot := makeSnapshot(&route.baseRoute, "GrafanaNet")
	snapshot.Addr = route.addr
	return snapshot
}
