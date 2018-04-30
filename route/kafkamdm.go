package route

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Dieterbe/go-metrics"
	dest "github.com/graphite-ng/carbon-relay-ng/destination"
	"github.com/graphite-ng/carbon-relay-ng/matcher"
	"github.com/graphite-ng/carbon-relay-ng/stats"
	"github.com/graphite-ng/carbon-relay-ng/util"

	"github.com/Shopify/sarama"
	"github.com/lomik/go-carbon/persister"
	"github.com/raintank/metrictank/cluster/partitioner"
	"gopkg.in/raintank/schema.v1"
)

type KafkaMdm struct {
	baseRoute
	saramaCfg   *sarama.Config
	producer    sarama.SyncProducer
	topic       string
	brokers     []string
	buf         chan *util.Point
	partitioner *partitioner.Kafka
	schemas     persister.WhisperSchemas
	blocking    bool
	dispatch    func(chan *util.Point, *util.Point, metrics.Gauge, metrics.Counter)

	orgId int // organisation to publish data under

	bufSize      int // amount of messages we can buffer up. each message is about 100B. so 1e7 is about 1GB.
	flushMaxNum  int
	flushMaxWait time.Duration

	numErrFlush       metrics.Counter
	numOut            metrics.Counter   // metrics successfully written to kafka
	numDropBuffFull   metrics.Counter   // metric drops due to queue full
	durationTickFlush metrics.Timer     // only updated after successful flush
	durationManuFlush metrics.Timer     // only updated after successful flush. not implemented yet
	tickFlushSize     metrics.Histogram // only updated after successful flush
	manuFlushSize     metrics.Histogram // only updated after successful flush. not implemented yet
	numBuffered       metrics.Gauge
	bufferSize        metrics.Gauge
}

// NewKafkaMdm creates a special route that writes to a grafana.net datastore
// We will automatically run the route and the destination
func NewKafkaMdm(key, prefix, sub, regex, topic, codec, schemasFile, partitionBy string, brokers []string, bufSize, orgId, flushMaxNum, flushMaxWait, timeout int, blocking bool) (Route, error) {
	m, err := matcher.New(prefix, sub, regex)
	if err != nil {
		return nil, err
	}
	schemas, err := getSchemas(schemasFile)
	if err != nil {
		return nil, err
	}

	cleanAddr := util.AddrToPath(brokers[0])

	r := &KafkaMdm{
		baseRoute: baseRoute{sync.Mutex{}, atomic.Value{}, key},
		topic:     topic,
		brokers:   brokers,
		buf:       make(chan *util.Point, bufSize),
		schemas:   schemas,
		blocking:  blocking,
		orgId:     orgId,

		bufSize:      bufSize,
		flushMaxNum:  flushMaxNum,
		flushMaxWait: time.Duration(flushMaxWait) * time.Millisecond,

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

	r.partitioner, err = partitioner.NewKafka(partitionBy)
	if err != nil {
		log.Fatal(4, "kafkaMdm %q: failed to initialize partitioner. %s", r.Key, err)
	}

	// We are looking for strong consistency semantics.
	// Because we don't change the flush settings, sarama will try to produce messages
	// as fast as possible to keep latency low.
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll // Wait for all in-sync replicas to ack the message
	config.Producer.Retry.Max = 10                   // Retry up to 10 times to produce the message
	config.Producer.Compression, err = getCompression(codec)
	if err != nil {
		log.Fatal(5, "kafkaMdm %q: %s", r.key, err)
	}
	config.Producer.Return.Successes = true
	config.Producer.Timeout = time.Duration(timeout) * time.Millisecond
	err = config.Validate()
	if err != nil {
		log.Fatal(4, "kafkaMdm %q: failed to validate kafka config. %s", r.Key, err)
	}
	r.saramaCfg = config

	r.config.Store(baseConfig{*m, make([]*dest.Destination, 0)})
	go r.run()
	return r, nil
}

func (r *KafkaMdm) run() {
	metrics := make([]*schema.MetricData, 0, r.flushMaxNum)
	ticker := time.NewTicker(r.flushMaxWait)
	var err error

	for r.producer == nil {
		r.producer, err = sarama.NewSyncProducer(r.brokers, r.saramaCfg)
		if err == sarama.ErrOutOfBrokers {
			log.Warning("kafkaMdm %q: %s", r.key, err)
			// sleep before trying to connect again.
			time.Sleep(time.Second)
		} else if err != nil {
			log.Fatal(4, "kafkaMdm %q: failed to initialize kafka producer. %s", r.key, err)
		}
	}
	log.Notice("kafkaMdm %q: now connected to kafka", r.key)

	// flushes the data to kafka and resets buffer.  blocks until it succeeds
	flush := func() {
		for {
			pre := time.Now()
			size := 0

			payload := make([]*sarama.ProducerMessage, len(metrics))

			for i, metric := range metrics {
				var data []byte
				data, err = metric.MarshalMsg(data[:])
				if err != nil {
					panic(err)
				}
				size += len(data)

				key, err := r.partitioner.GetPartitionKey(metric, nil)
				if err != nil {
					panic(err)
				}
				payload[i] = &sarama.ProducerMessage{
					Key:   sarama.ByteEncoder(key),
					Topic: r.topic,
					Value: sarama.ByteEncoder(data),
				}
			}
			err = r.producer.SendMessages(payload)

			diff := time.Since(pre)
			if err == nil {
				log.Info("KafkaMdm %q: sent %d metrics in %s - msg size %d", r.key, len(metrics), diff, size)
				r.numOut.Inc(int64(len(metrics)))
				r.tickFlushSize.Update(int64(size))
				r.durationTickFlush.Update(diff)
				metrics = metrics[:0]
				break
			}

			errors := make(map[error]int)
			for _, e := range err.(sarama.ProducerErrors) {
				errors[e.Err] += 1
			}
			for k, v := range errors {
				log.Warning("KafkaMdm %q: seen %d times: %s", r.key, v, k)
			}

			r.numErrFlush.Inc(1)
			log.Warning("KafkaMdm %q: failed to submit data: %s will try again in 100ms. (this attempt took %s)", r.key, err, diff)

			time.Sleep(100 * time.Millisecond)
		}
	}
	for {
		select {
		case point, ok := <-r.buf:
			if !ok {
				if len(metrics) != 0 {
					flush()
				}
				return
			}
			r.numBuffered.Dec(1)
			md, err := parseMetric(point, r.schemas, r.orgId)
			if err != nil {
				log.Error("KafkaMdm %q: %s", r.key, err)
				continue
			}
			md.SetId()
			metrics = append(metrics, md)
			if len(metrics) == r.flushMaxNum {
				flush()
			}
		case <-ticker.C:
			if len(metrics) != 0 {
				flush()
			}
		}
	}
}

func (r *KafkaMdm) Dispatch(point *util.Point) {
	log.Info("kafkaMdm %q: sending to dest %v: %s", r.key, r.brokers, point)
	r.dispatch(r.buf, point, r.numBuffered, r.numDropBuffFull)
}

func (r *KafkaMdm) Flush() error {
	//conf := r.config.Load().(Config)
	// no-op. Flush() is currently not called by anything.
	return nil
}

func (r *KafkaMdm) Shutdown() error {
	//conf := r.config.Load().(Config)
	close(r.buf)
	return nil
}

func (r *KafkaMdm) Snapshot() Snapshot {
	return makeSnapshot(&r.baseRoute, "KafkaMdm")
}

func getCompression(codec string) (sarama.CompressionCodec, error) {
	switch codec {
	case "none":
		return sarama.CompressionNone, nil
	case "gzip":
		return sarama.CompressionGZIP, nil
	case "snappy":
		return sarama.CompressionSnappy, nil
	}
	return 0, fmt.Errorf("unknown compression codec %q", codec)
}
