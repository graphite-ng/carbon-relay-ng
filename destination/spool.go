package destination

import (
	"time"

	"github.com/Dieterbe/go-metrics"
	"github.com/graphite-ng/carbon-relay-ng/nsqd"
	"github.com/graphite-ng/carbon-relay-ng/stats"
	"github.com/graphite-ng/carbon-relay-ng/util"
)

// sits in front of nsqd diskqueue.
// provides buffering (to accept input while storage is slow / sync() runs -every 1000 items- etc)
// QoS (RT vs Bulk) and controllable i/o rates
type Spool struct {
	key          string
	InRT         chan []byte
	InBulk       chan []byte
	Out          chan []byte
	spoolSleep   time.Duration // how long to wait between stores to spool
	unspoolSleep time.Duration // how long to wait between loads from spool

	queue       *nsqd.DiskQueue
	queueBuffer chan []byte // buffer metrics into queue because it can block

	durationWrite  metrics.Timer
	durationBuffer metrics.Timer
	numBuffered    metrics.Gauge // track watermark on read and write
	// metrics we could do but i don't think that useful: diskqueue depth, amount going in/out diskqueue
	numIncomingBulk metrics.Counter // sync channel, no need to track watermark, instead we track number seen on read
	numIncomingRT   metrics.Counter // more or less sync (small buff). we track number of drops in dest so no need for watermark, instead we track num seen on read

	shutdownWriter chan bool
	shutdownBuffer chan bool
}

// parameters should be tuned so that:
// can buffer packets for the duration of 1 sync
// buffer no more then needed, esp if we know the queue is slower then the ingest rate
func NewSpool(key, spoolDir string, bufSize int, maxBytesPerFile, syncEvery int64, syncPeriod, spoolSleep, unspoolSleep time.Duration) *Spool {
	dqName := "spool_" + key
	// bufSize should be tuned to be able to hold the max amount of metrics that can be received
	// while the disk subsystem is doing a write/sync. Basically set it to the amount of metrics
	// you receive in a second.
	queue := nsqd.NewDiskQueue(dqName, spoolDir, maxBytesPerFile, syncEvery, syncPeriod).(*nsqd.DiskQueue)
	s := Spool{
		key:             key,
		InRT:            make(chan []byte, 10),
		InBulk:          make(chan []byte),
		Out:             NewSlowChan(queue.ReadChan(), unspoolSleep),
		spoolSleep:      spoolSleep,
		unspoolSleep:    unspoolSleep,
		queue:           queue,
		queueBuffer:     make(chan []byte, bufSize),
		durationWrite:   stats.Timer("spool=" + key + ".operation=write"),
		durationBuffer:  stats.Timer("spool=" + key + ".operation=buffer"),
		numBuffered:     stats.Gauge("spool=" + key + ".unit=Metric.status=buffered"),
		numIncomingRT:   stats.Counter("spool=" + key + ".unit=Metric.status=incomingRT"),
		numIncomingBulk: stats.Counter("spool=" + key + ".unit=Metric.status=incomingBulk"),
		shutdownWriter:  make(chan bool),
		shutdownBuffer:  make(chan bool),
	}
	go s.Writer()
	go s.Buffer()
	return &s
}

// provides a channel based api to the queue
func (s *Spool) Writer() {
	// we always try to serve realtime traffic as much as we can
	// because that's an inputstream at a fixed rate, it won't slow down
	// but if no realtime traffic is coming in, then we can use spare capacity
	// to read from the Bulk input, which is used to offload a known (potentially large) set of data
	// that could easily exhaust the capacity of our disk queue.  But it doesn't require RT processing,
	// so just handle this to the extent we can
	// note that this still allows for channel ops to come in on InRT and to be starved, resulting
	// in some realtime traffic to be dropped, but that shouldn't be too much of an issue. experience will tell..
	for {
		select {
		case <-s.shutdownWriter:
			return
		case buf := <-s.InRT: // wish we could somehow prioritize this higher
			s.numIncomingRT.Inc(1)
			//pre = time.Now()
			log.Debug("spool %v satisfying spool RT", s.key)
			log.Info("spool %s %s Writer -> queue.Put\n", s.key, buf)
			s.durationBuffer.Time(func() { s.queueBuffer <- buf })
			s.numBuffered.Inc(1)
			//post = time.Now()
			//fmt.Println("queueBuffer duration RT:", post.Sub(pre).Nanoseconds())
		case buf := <-s.InBulk:
			s.numIncomingBulk.Inc(1)
			//pre = time.Now()
			log.Debug("spool %v satisfying spool BULK", s.key)
			log.Info("spool %s %s Writer -> queue.Put\n", s.key, buf)
			s.durationBuffer.Time(func() { s.queueBuffer <- buf })
			s.numBuffered.Inc(1)
			//post = time.Now()
			//fmt.Println("queueBuffer duration BULK:", post.Sub(pre).Nanoseconds())
		}
	}
}

func (s *Spool) Ingest(bulkData []*util.Point) {
	for _, buf := range bulkData {
		s.InBulk <- []byte(buf.String())
		time.Sleep(s.spoolSleep)
	}
}
func (s *Spool) Buffer() {
	for {
		select {
		case <-s.shutdownBuffer:
			return
		case buf := <-s.queueBuffer:
			s.numBuffered.Dec(1)
			//pre := time.Now()
			s.durationWrite.Time(func() { s.queue.Put(buf) })
			//post := time.Now()
			//fmt.Println("PUT DURATION", post.Sub(pre).Nanoseconds())
		}
	}
}

func (s *Spool) Close() {
	s.shutdownWriter <- true
	s.shutdownBuffer <- true
	// we don't need to close Out, our user should just not read from it anymore. destination does this
	s.queue.Close()
}
