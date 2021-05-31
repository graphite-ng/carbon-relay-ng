// carbon-relay-ng
// route traffic to anything that speaks the Graphite Carbon protocol (text or pickle)
// such as Graphite's carbon-cache.py, influxdb, ...
package main

import (
	"flag"
	"fmt"
	"net"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"syscall"

	"github.com/Dieterbe/go-metrics"
	"github.com/grafana/carbon-relay-ng/aggregator"
	"github.com/grafana/carbon-relay-ng/badmetrics"
	"github.com/grafana/carbon-relay-ng/cfg"
	"github.com/grafana/carbon-relay-ng/input"
	"github.com/grafana/carbon-relay-ng/input/manager"
	"github.com/grafana/carbon-relay-ng/logger"
	"github.com/grafana/carbon-relay-ng/stats"
	"github.com/grafana/carbon-relay-ng/statsmt"
	tbl "github.com/grafana/carbon-relay-ng/table"
	"github.com/grafana/carbon-relay-ng/ui/telnet"
	"github.com/grafana/carbon-relay-ng/ui/web"
	log "github.com/sirupsen/logrus"

	"strconv"
	"strings"
	"time"
)

var (
	config_file      string
	to_dispatch      = make(chan []byte)
	inputs           []input.Plugin
	shutdownTimeout  = time.Second * 30 // how long to wait for shutdown
	table            *tbl.Table
	cpuprofile       = flag.String("cpuprofile", "", "write cpu profile to file")
	blockProfileRate = flag.Int("block-profile-rate", 0, "see https://golang.org/pkg/runtime/#SetBlockProfileRate")
	memProfileRate   = flag.Int("mem-profile-rate", 512*1024, "0 to disable. 1 for max precision (expensive!) see https://golang.org/pkg/runtime/#pkg-variables")
	enablePprof      = flag.Bool("enable-pprof", false, "Will enable debug endpoints on /debug/pprof/")
	badMetrics       *badmetrics.BadMetrics
	Version          = "unknown"
)

func usage() {
	header := `Usage:
        carbon-relay-ng version
        carbon-relay-ng <path-to-config>
	`
	fmt.Fprintln(os.Stderr, header)
	flag.PrintDefaults()
}

func main() {

	flag.Usage = usage
	flag.Parse()
	runtime.SetBlockProfileRate(*blockProfileRate)
	runtime.MemProfileRate = *memProfileRate

	config_file = "/etc/carbon-relay-ng.ini"
	if 1 == flag.NArg() {
		val := flag.Arg(0)
		if val == "version" {
			fmt.Printf("carbon-relay-ng %s (built with %s)\n", Version, runtime.Version())
			return
		}
		config_file = val
	}

	config, meta, err := cfg.NewFromFile(config_file)
	if err != nil {
		log.Fatal(err)
	}
	//runtime.SetBlockProfileRate(1) // to enable block profiling. in my experience, adds 35% overhead.

	formatter := &logger.TextFormatter{}
	formatter.TimestampFormat = "2006-01-02 15:04:05.000"
	log.SetFormatter(formatter)
	lvl, err := log.ParseLevel(config.Log_level)
	if err != nil {
		log.Fatalf("failed to parse log-level %q: %s", config.Log_level, err.Error())
	}
	log.SetLevel(lvl)

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	if len(config.Instance) == 0 {
		log.Error("instance identifier cannot be empty")
		os.Exit(1)
	}

	log.Infof("===== carbon-relay-ng instance '%s' starting. (version %s) =====", config.Instance, Version)

	if os.Getenv("GOMAXPROCS") == "" && config.Max_procs >= 1 {
		log.Debugf("setting GOMAXPROCS to %d", config.Max_procs)
		runtime.GOMAXPROCS(config.Max_procs)
	}

	stats.New(config.Instance)

	if config.Pid_file != "" {
		f, err := os.Create(config.Pid_file)
		if err != nil {
			log.Fatalf("error creating pidfile: %s", err.Error())
		}
		_, err = f.Write([]byte(strconv.Itoa(os.Getpid())))
		if err != nil {
			log.Fatalf("error writing to pidfile: %s", err.Error())
		}
		f.Close()
	}

	aggregator.InitMetrics()

	go func() {
		sys := stats.Gauge("what=virtual_memory.unit=Byte")
		alloc := stats.Gauge("what=memory_allocated.unit=Byte")
		ticker := time.NewTicker(time.Second)
		var memstats runtime.MemStats
		for range ticker.C {
			runtime.ReadMemStats(&memstats)
			sys.Update(int64(memstats.Sys))
			alloc.Update(int64(memstats.Alloc))

		}
	}()

	if config.Instrumentation.Graphite_addr != "" {
		addr, err := net.ResolveTCPAddr("tcp", config.Instrumentation.Graphite_addr)
		if err != nil {
			log.Fatal(err)
		}
		go metrics.Graphite(metrics.DefaultRegistry, time.Duration(config.Instrumentation.Graphite_interval)*time.Millisecond, "", addr)

		// we use a copy of metrictank's stats library for some extra process/memory related stats
		// note: they follow a different naming scheme, and have their own reporter.

		statsmt.NewMemoryReporter()
		_, err = statsmt.NewProcessReporter()
		if err != nil {
			// ProcessReporter depends on /proc which does not exists/is not mounted by all platforms (Windows/OSX/FreeBSD)
			if os.IsNotExist(err) {
				log.Warnf("stats: could not initialize process - unsupported platform: %v", err)
			} else {
				log.Fatalf("stats: could not initialize process reporter: %v", err)
			}
		}
		aggregator.NewAggregatorReporter()
		statsmt.NewGraphite("carbon-relay-ng.stats."+config.Instance, config.Instrumentation.Graphite_addr, config.Instrumentation.Graphite_interval/1000, 1000, time.Second*10)
	}

	log.Info("initializing routing table...")

	table, err := tbl.InitFromConfig(config, meta)
	if err != nil {
		log.Error(err.Error())
		os.Exit(1)
	}

	tablePrinted := table.Print()
	log.Info("===========================")
	log.Info("========== TABLE ==========")
	log.Info("===========================")
	for _, line := range strings.Split(tablePrinted, "\n") {
		log.Info(line)
	}

	if config.Listen_addr != "" {
		inputs = append(inputs, input.NewListener(config.Listen_addr, config.Plain_read_timeout.Duration, input.NewPlain(table)))
	}

	if config.Pickle_addr != "" {
		inputs = append(inputs, input.NewListener(config.Pickle_addr, config.Pickle_read_timeout.Duration, input.NewPickle(table)))
	}

	if config.Amqp.Amqp_enabled == true {
		inputs = append(inputs, input.NewAMQP(config, table, input.AMQPConnector))
	}

	for _, in := range inputs {
		err := in.Start()
		if err != nil {
			log.Error(err.Error())
			os.Exit(1)
		}
	}

	if config.Admin_addr != "" {
		go func() {
			err := telnet.Start(config.Admin_addr, table)
			if err != nil {
				log.Fatalf("Error listening: %s", err.Error())
			}
		}()
	}

	if config.Http_addr != "" {
		go web.Start(config.Http_addr, config, table, *enablePprof)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-sigChan:
		log.Infof("Received signal %q. Shutting down", sig)
	}
	if !manager.Stop(inputs, shutdownTimeout) {
		os.Exit(1)
	}
}
