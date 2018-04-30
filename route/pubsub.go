package route

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"io/ioutil"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/Dieterbe/go-metrics"
	dest "github.com/graphite-ng/carbon-relay-ng/destination"
	"github.com/graphite-ng/carbon-relay-ng/matcher"
	"github.com/graphite-ng/carbon-relay-ng/stats"
	"github.com/graphite-ng/carbon-relay-ng/util"
)

// gzipPool provides a sync.Pool of initialized gzip.Writer's to avoid
// the allocation overhead of repeatedly calling gzip.NewWriter
var gzipPool sync.Pool

func init() {
	gzipPool.New = func() interface{} {
		gz, err := gzip.NewWriterLevel(ioutil.Discard, gzip.BestSpeed)
		if err != nil {
			log.Fatal(err)
		}
		return gz
	}
}

// PubSub publishes metrics to a Google PubSub topic
type PubSub struct {
	baseRoute
	project  string
	topic    string
	format   string
	codec    string
	psClient *pubsub.Client
	psTopic  *pubsub.Topic
	buf      chan *util.Point
	blocking bool
	dispatch func(chan *util.Point, *util.Point, metrics.Gauge, metrics.Counter)

	bufSize      int // amount of messages we can buffer up. each message is about 100B. so 1e7 is about 1GB.
	flushMaxSize int
	flushMaxWait time.Duration

	numOut            metrics.Counter // metrics successfully written to google pubsub
	numPubSubMessages metrics.Counter // number of pubsub.Messages submitted to google pubsub
	numErrFlush       metrics.Counter
	numDropBuffFull   metrics.Counter   // metric drops due to queue full
	numParseError     metrics.Counter   // metrics that failed destination.ParseDataPoint()
	durationTickFlush metrics.Timer     // only updated after successful flush
	tickFlushSize     metrics.Histogram // only updated after successful flush
	numBuffered       metrics.Gauge
	bufferSize        metrics.Gauge
}

// NewPubSub creates a route that writes metrics to a Google PubSub topic
// We will automatically run the route and the destination
func NewPubSub(key, prefix, sub, regex, project, topic, format, codec string, bufSize, flushMaxSize, flushMaxWait int, blocking bool) (Route, error) {
	m, err := matcher.New(prefix, sub, regex)
	if err != nil {
		return nil, err
	}

	r := &PubSub{
		baseRoute: baseRoute{sync.Mutex{}, atomic.Value{}, key},
		project:   project,
		topic:     topic,
		format:    format,
		codec:     codec,
		buf:       make(chan *util.Point, bufSize),
		blocking:  blocking,

		bufSize:      bufSize,
		flushMaxSize: flushMaxSize,
		flushMaxWait: time.Duration(flushMaxWait) * time.Millisecond,

		numOut:            stats.Counter("dest=" + topic + ".unit=Metric.direction=out"),
		numPubSubMessages: stats.Counter("dest=" + topic + "unit.Metric.what=pubsubMessagesPublished"),
		numErrFlush:       stats.Counter("dest=" + topic + ".unit=Err.type=flush"),
		numParseError:     stats.Counter("dest=" + topic + ".unit.Err.type=parse"),
		durationTickFlush: stats.Timer("dest=" + topic + ".what=durationFlush.type=ticker"),
		tickFlushSize:     stats.Histogram("dest=" + topic + ".unit=B.what=FlushSize.type=ticker"),
		numBuffered:       stats.Gauge("dest=" + topic + ".unit=Metric.what=numBuffered"),
		bufferSize:        stats.Gauge("dest=" + topic + ".unit=Metric.what=bufferSize"),
		numDropBuffFull:   stats.Counter("dest=" + topic + ".unit=Metric.action=drop.reason=queue_full"),
	}
	r.bufferSize.Update(int64(bufSize))

	if blocking {
		r.dispatch = dispatchBlocking
	} else {
		r.dispatch = dispatchNonBlocking
	}

	// create pubsub client
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, project)
	if err != nil {
		log.Fatalf("pubsub(%s) failed to create google pubsub client: %v", r.Key(), err)
	}
	r.psClient = client

	psTopic := client.Topic(topic)
	exists, err := psTopic.Exists(ctx)
	if err != nil {
		log.Fatalf("pubsub(%s) failed to connect to pubsub topic: %v", r.Key(), err)
	}
	if !exists {
		log.Fatalf("pubsub(%s) topic %s does not exist. You need to create it before running this app.", r.Key(), topic)
	}
	r.psTopic = psTopic

	r.config.Store(baseConfig{*m, make([]*dest.Destination, 0)})
	go r.run()
	return r, nil
}

func (r *PubSub) run() {
	ticker := time.NewTicker(r.flushMaxWait)
	var cnt int // number of metrics written to the payload buffer
	var msgbuf bytes.Buffer
	msgbuf.Grow(r.flushMaxSize)

	flush := func() {
		r.publish(&msgbuf, cnt)
		msgbuf.Reset()
		cnt = 0
	}

	for {
		select {
		case point, ok := <-r.buf:
			if !ok {
				flush()
				return
			}
			r.numBuffered.Dec(1)

			// flush first if this new buf is likely to breach our size limit (compression is not considered so it won't be exact)
			if msgbuf.Len()+len(point.String())+1 >= r.flushMaxSize {
				flush()
			}

			switch r.format {
			case "plain":
				// buf is already in graphite line format so write it directly into the buf
				msgbuf.WriteString(point.String())
				msgbuf.WriteByte('\n')
			case "pickle":
				msgbuf.Write(dest.Pickle(point))
			}
			cnt++
		case _ = <-ticker.C:
			flush()
		}
	}
}

// compressWrite compresses the contents of src into the dst writer according to the
// specified codec
// func compressWrite(dst, src *bytes.Buffer, codec string) error {
func compressWrite(dst io.Writer, src []byte, codec string) error {
	switch codec {
	case "gzip":
		writer := gzipPool.Get().(*gzip.Writer)
		defer gzipPool.Put(writer)
		writer.Reset(dst)
		if _, err := writer.Write(src); err != nil {
			return err
		}
		if err := writer.Close(); err != nil {
			return err
		}
	default:
		// "none", no compression
		dst.Write(src)
	}
	return nil
}

// publish publishes a buffer to google pubsub. The buffer may be sent as-is
// or compressed first. The following attributes are added to published messages:
// - "content-type": "application/text" (graphite line format), "application/python-pickle"
// - "codec": "none" (uncompressed), "gzip"
func (r *PubSub) publish(buf *bytes.Buffer, cnt int) {
	if cnt == 0 {
		return
	}

	start := time.Now()
	var payload bytes.Buffer     // pubsub Message "Data"
	attrs := map[string]string{} // pubsub Message "Attributes"

	switch r.format {
	case "plain":
		attrs["content-type"] = "application/text"
	case "pickle":
		attrs["content-type"] = "application/python-pickle"
	}

	// write a compressed (maybe) copy of buf into the payload buffer. The content may not be
	// compressed if r.codec == "none"
	if err := compressWrite(&payload, buf.Bytes(), r.codec); err != nil {
		log.Error("pubsub(%s) failed compressing message: %s", r.Key(), err)
		r.numErrFlush.Inc(1)
		return
	}
	attrs["codec"] = r.codec

	// publish message and wait to Get() confirmation from the server before recording stats and returning
	res := r.psTopic.Publish(context.Background(), &pubsub.Message{
		Data:       payload.Bytes(),
		Attributes: attrs,
	})
	if id, err := res.Get(context.Background()); err != nil {
		log.Error("pubsub(%s) publish failure: %s", r.Key(), err)
		r.numErrFlush.Inc(1)
	} else {
		dur := time.Since(start)
		r.numOut.Inc(int64(cnt))
		r.numPubSubMessages.Inc(1)
		r.durationTickFlush.Update(dur)
		r.tickFlushSize.Update(int64(payload.Len()))
		// ex: "pubsub(pubsub) publish success, msgID: 27624288224257, count: 50000, size: 2139099, time: 1.288598 seconds"
		log.Info("pubsub(%s) publish success, msgID: %s, count: %d, size: %d, time: %f ms",
			r.Key(), id, cnt, payload.Len(), float64(dur)/float64(time.Millisecond))
	}
}

// Dispatch is called to submit metrics. They will be in graphite 'plain' format no matter how they arrived.
func (r *PubSub) Dispatch(point *util.Point) {
	r.dispatch(r.buf, point, r.numBuffered, r.numDropBuffFull)
}

// Flush is not currently implemented
func (r *PubSub) Flush() error {
	// no-op. Flush() is currently not called by anything.
	return nil
}

// Shutdown stops the pubsub publisher and returns with the publisher has finished in-flight work
func (r *PubSub) Shutdown() error {
	close(r.buf)
	r.psTopic.Stop()
	return nil
}

// Snapshot clones the current config for update operations
func (r *PubSub) Snapshot() Snapshot {
	return makeSnapshot(&r.baseRoute, "pubsub")
}
