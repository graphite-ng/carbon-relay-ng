package destination

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Dieterbe/go-metrics"
	"github.com/graphite-ng/carbon-relay-ng/matcher"
	"github.com/graphite-ng/carbon-relay-ng/stats"
	"github.com/graphite-ng/carbon-relay-ng/util"
)

func addrInstanceSplit(addr string) (string, string) {
	var instance string
	// The address may be specified as server, server:port or server:port:instance.
	if strings.Count(addr, ":") == 2 {
		addrComponents := strings.Split(addr, ":")
		addr = strings.Join(addrComponents[0:2], ":")
		instance = addrComponents[2]
	}
	return addr, instance
}

type Destination struct {
	// basic properties in init and copy
	lockMatcher sync.Mutex
	Matcher     matcher.Matcher `json:"matcher"`

	Addr         string `json:"address"`  // tcp dest
	Instance     string `json:"instance"` // Optional carbon instance name, useful only with consistent hashing
	SpoolDir     string // where to store spool files (if enabled)
	Key          string // unique key per destination, based on routeName and destination addr/port combination
	Spool        bool   `json:"spool"`        // spool metrics to disk while dest down?
	Pickle       bool   `json:"pickle"`       // send in pickle format?
	Online       bool   `json:"online"`       // state of connection online/offline.
	SlowNow      bool   `json:"slowNow"`      // did we have to drop packets in current loop
	SlowLastLoop bool   `json:"slowLastLoop"` // "" last loop
	periodFlush  time.Duration
	periodReConn time.Duration
	connBufSize  int // in metrics. (each metric line is typically about 70 bytes). default 30k. to make sure writes to In are fast until conn flushing can't keep up
	ioBufSize    int // conn io buffer in bytes. 4096 is go default. 2M is our default

	SpoolBufSize         int
	SpoolMaxBytesPerFile int64
	SpoolSyncEvery       int64
	SpoolSyncPeriod      time.Duration
	SpoolSleep           time.Duration // how long to wait between stores to spool
	UnspoolSleep         time.Duration // how long to wait between loads from spool
	RouteName            string

	// set in/via Run()
	In                  chan *util.Point   `json:"-"` // incoming metrics
	shutdown            chan bool          // signals shutdown internally
	spool               *Spool             // queue used if spooling enabled
	connUpdates         chan *Conn         // when the dest changes (possibly nil)
	inConnUpdate        chan bool          // to signal when we start a new conn and when we finish
	setSignalConnOnline chan chan struct{} // the provided chan will be closed when the conn comes online (internal implementation detail)
	flush               chan bool
	flushErr            chan error
	tasks               sync.WaitGroup

	numDropNoConnNoSpool metrics.Counter
	numDropSlowSpool     metrics.Counter
	numDropSlowConn      metrics.Counter
}

// New creates a destination object. Note that it still needs to be told to run via Run().
func New(routeName, prefix, sub, regex, addr, spoolDir string, spool, pickle bool, periodFlush, periodReConn time.Duration, connBufSize, ioBufSize, spoolBufSize int, spoolMaxBytesPerFile, spoolSyncEvery int64, spoolSyncPeriod, spoolSleep, unspoolSleep time.Duration) (*Destination, error) {
	m, err := matcher.New(prefix, sub, regex)
	if err != nil {
		return nil, err
	}
	addr, instance := addrInstanceSplit(addr)
	key := util.Key(routeName, addr)
	dest := &Destination{
		Matcher:              *m,
		Addr:                 addr,
		Instance:             instance,
		SpoolDir:             spoolDir,
		Key:                  key,
		Spool:                spool,
		Pickle:               pickle,
		periodFlush:          periodFlush,
		periodReConn:         periodReConn,
		connBufSize:          connBufSize,
		ioBufSize:            ioBufSize,
		SpoolBufSize:         spoolBufSize,
		SpoolMaxBytesPerFile: spoolMaxBytesPerFile,
		SpoolSyncEvery:       spoolSyncEvery,
		SpoolSyncPeriod:      spoolSyncPeriod,
		SpoolSleep:           spoolSleep,
		UnspoolSleep:         unspoolSleep,
		RouteName:            routeName,
	}
	dest.setMetrics()
	return dest, nil
}

func (dest *Destination) setMetrics() {
	dest.numDropNoConnNoSpool = stats.Counter("dest=" + dest.Key + ".unit=Metric.action=drop.reason=conn_down_no_spool")
	dest.numDropSlowSpool = stats.Counter("dest=" + dest.Key + ".unit=Metric.action=drop.reason=slow_spool")
	dest.numDropSlowConn = stats.Counter("dest=" + dest.Key + ".unit=Metric.action=drop.reason=slow_conn")
}

func (dest *Destination) Match(s []byte) bool {
	dest.lockMatcher.Lock()
	defer dest.lockMatcher.Unlock()
	return dest.Matcher.Match(s)
}

// can't be changed yet: pickle, spool, flush, reconn
func (dest *Destination) Update(opts map[string]string) error {
	match := dest.GetMatcher()
	prefix := match.Prefix
	sub := match.Sub
	regex := match.Regex
	updateMatcher := false
	addr := ""

	for name, val := range opts {
		switch name {
		case "addr":
			addr = val
		case "prefix":
			prefix = val
			updateMatcher = true
		case "sub":
			sub = val
			updateMatcher = true
		case "regex":
			regex = val
			updateMatcher = true
		default:
			return errors.New("no such option: " + name)
		}
	}
	if addr != "" {
		dest.updateConn(addr)
	}
	if updateMatcher {
		match, err := matcher.New(prefix, sub, regex)
		if err != nil {
			return err
		}
		dest.UpdateMatcher(*match)
	}
	return nil
}

func (dest *Destination) UpdateMatcher(matcher matcher.Matcher) {
	dest.lockMatcher.Lock()
	defer dest.lockMatcher.Unlock()
	dest.Matcher = matcher
}

func (dest *Destination) GetMatcher() matcher.Matcher {
	dest.lockMatcher.Lock()
	defer dest.lockMatcher.Unlock()
	return dest.Matcher
}

// a "basic" static copy of the dest, not actually running
func (dest *Destination) Snapshot() *Destination {
	return &Destination{
		Matcher:  dest.GetMatcher(),
		Addr:     dest.Addr,
		SpoolDir: dest.SpoolDir,
		Spool:    dest.Spool,
		Pickle:   dest.Pickle,
		Online:   dest.Online,
		Key:      dest.Key,
	}
}

func (dest *Destination) Run() {
	if dest.In != nil {
		panic(fmt.Sprintf("Run() called on already running dest '%s'", dest.Key))
	}
	dest.In = make(chan *util.Point)
	dest.shutdown = make(chan bool)
	dest.connUpdates = make(chan *Conn)
	dest.inConnUpdate = make(chan bool)
	dest.flush = make(chan bool)
	dest.flushErr = make(chan error)
	dest.setSignalConnOnline = make(chan chan struct{})
	if dest.Spool {
		// TODO better naming for spool, because it won't update when addr changes
		dest.spool = NewSpool(
			dest.Key,
			dest.SpoolDir,
			dest.SpoolBufSize,
			dest.SpoolMaxBytesPerFile,
			dest.SpoolSyncEvery,
			dest.SpoolSyncPeriod,
			dest.SpoolSleep,
			dest.UnspoolSleep,
		)
	}
	dest.tasks = sync.WaitGroup{}
	go dest.relay()
}

func (dest *Destination) Flush() error {
	dest.flush <- true
	return <-dest.flushErr
}

func (dest *Destination) Shutdown() error {
	if dest.shutdown == nil {
		return errors.New("not running yet")
	}
	dest.shutdown <- true
	dest.tasks.Wait()
	return nil
}

func (dest *Destination) updateConn(addr string) {
	log.Debug("dest %v (re)connecting to %v\n", dest.Key, addr)
	dest.inConnUpdate <- true
	defer func() { dest.inConnUpdate <- false }()
	addr, instance := addrInstanceSplit(addr)
	conn, err := NewConn(addr, dest, dest.periodFlush, dest.Pickle, dest.connBufSize, dest.ioBufSize)
	if err != nil {
		log.Debug("dest %v: %v\n", dest.Key, err.Error())
		return
	}
	log.Debug("dest %v connected to %v\n", dest.Key, addr)
	if addr != dest.Key {
		log.Notice("dest %v update address to %v)\n", dest.Key, addr)
		dest.Addr = addr
		dest.Instance = instance
		dest.Key = util.Key(dest.RouteName, addr)
		dest.setMetrics()
	}
	dest.connUpdates <- conn
	return
}

func (dest *Destination) collectRedo(conn *Conn) {
	dest.tasks.Add(1)
	bulkData := conn.getRedo()
	dest.spool.Ingest(bulkData)
	dest.tasks.Done()
}

func (dest *Destination) WaitOnline() chan struct{} {
	signalConnOnline := make(chan struct{})
	dest.setSignalConnOnline <- signalConnOnline
	return signalConnOnline
}

// TODO func (l *TCPListener) SetDeadline(t time.Time)
// TODO Decide when to drop this buffer and move on.
func (dest *Destination) relay() {
	ticker := time.NewTicker(dest.periodReConn)
	var toUnspool chan []byte
	var conn *Conn

	// try to send the data on the buffered tcp conn
	// if that's slow or down, discard the data
	nonBlockingSend := func(point *util.Point) {
		select {
		// this op won't succeed as long as the conn is busy processing/flushing
		case conn.In <- point:
			conn.numBuffered.Inc(1)
		default:
			log.Debug("dest %s %s nonBlockingSend -> dropping due to slow conn\n", dest.Key, point)
			// TODO check if it was because conn closed
			// we don't want to just buffer everything in memory,
			// it would probably keep piling up until OOM.  let's just drop the traffic.
			dest.numDropSlowConn.Inc(1)
			dest.SlowNow = true
		}
	}

	// try to send the data to the spool
	// if slow or down, drop and move on
	nonBlockingSpool := func(point *util.Point) {
		select {
		case dest.spool.InRT <- []byte(point.String()):
			log.Debug("dest %s %s nonBlockingSpool -> added to spool\n", dest.Key, point)
		default:
			log.Debug("dest %s %s nonBlockingSpool -> dropping due to slow spool\n", dest.Key, point)
			dest.numDropSlowSpool.Inc(1)
		}
	}

	numConnUpdates := 0
	go dest.updateConn(dest.Addr)
	var signalConnOnline chan struct{}

	// this loop/select should never block, we can't hang dest.In or the route & table locks up
	for {
		if conn != nil {
			if !conn.isAlive() {
				if dest.Spool {
					go dest.collectRedo(conn)
				}
				conn = nil
			}
		}
		// only process spool queue if we have an outbound connection and we haven't needed to drop packets in a while
		if conn != nil && dest.Spool && !dest.SlowLastLoop && !dest.SlowNow {
			toUnspool = dest.spool.Out
		} else {
			toUnspool = nil
		}
		log.Debug("dest %v entering select. conn: %v spooling: %v slowLastloop: %v, slowNow: %v spoolQueue: %v", dest.Key, conn != nil, dest.Spool, dest.SlowLastLoop, dest.SlowNow, toUnspool != nil)
		select {
		case sig := <-dest.setSignalConnOnline:
			signalConnOnline = sig
		case inConnUpdate := <-dest.inConnUpdate:
			if inConnUpdate {
				numConnUpdates += 1
			} else {
				numConnUpdates -= 1
			}
		// note: new conn can be nil and that's ok (it means we had to [re]connect but couldn't)
		case conn = <-dest.connUpdates:
			dest.Online = conn != nil
			log.Notice("dest %s updating conn. online: %v\n", dest.Key, dest.Online)
			if dest.Online {
				// new conn? start with a clean slate!
				dest.SlowLastLoop = false
				dest.SlowNow = false
				if signalConnOnline != nil {
					close(signalConnOnline)
				}
			}
		case <-ticker.C: // periodically try to bring connection (back) up, if we have to, and no other connect is happening
			if conn == nil && numConnUpdates == 0 {
				go dest.updateConn(dest.Addr)
			}
			dest.SlowLastLoop = dest.SlowNow
			dest.SlowNow = false
		case <-dest.flush:
			if conn != nil {
				dest.flushErr <- conn.Flush()
			} else {
				dest.flushErr <- nil
			}
		case <-dest.shutdown:
			log.Notice("dest %v shutting down. flushing and closing conn\n", dest.Key)
			if conn != nil {
				conn.Flush()
				conn.Close()
			}
			if dest.spool != nil {
				dest.spool.Close()
			}
			return
		case buf := <-toUnspool:
			// we know that conn != nil here because toUnspool is set above
			log.Debug("dest %v %s received from spool -> nonBlockingSend\n", dest.Key, buf)
			point, err := ParseDataPoint(buf)
			if err != nil {
				log.Error("error parsing %s received from spool: %s", buf, err)
			} else {
				nonBlockingSend(point)
			}
		case point := <-dest.In:
			if conn != nil {
				log.Debug("dest %v %s received from In -> nonBlockingSend\n", dest.Key, point)
				nonBlockingSend(point)
			} else if dest.Spool {
				log.Debug("dest %v %s received from In -> nonBlockingSpool\n", dest.Key, point)
				nonBlockingSpool(point)
			} else {
				log.Debug("dest %v %s received from In -> no conn no spool -> drop\n", dest.Key, point)
				dest.numDropNoConnNoSpool.Inc(1)
			}
		}
	}
}
