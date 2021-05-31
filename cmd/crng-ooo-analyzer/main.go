package main

import (
	"bufio"
	"flag"
	"fmt"
	_ "net/http/pprof"
	"os"
	"regexp"
	"runtime"

	"github.com/grafana/carbon-relay-ng/cfg"
	"github.com/grafana/carbon-relay-ng/logger"
	log "github.com/sirupsen/logrus"
)

var Version = "unknown"

func usage() {
	header := `Usage:
        crng-ooo-analyzer version
        crng-ooo-analyzer <path-to-config>
	`
	fmt.Fprintln(os.Stderr, header)
	flag.PrintDefaults()
}

func main() {

	flag.Usage = usage
	flag.Parse()

	config_file := "/etc/carbon-relay-ng.ini"
	if 1 == flag.NArg() {
		val := flag.Arg(0)
		if val == "version" {
			fmt.Printf("crng-ooo-analyzer %s (built with %s)\n", Version, runtime.Version())
			return
		}
		config_file = val
	}

	config, _, err := cfg.NewFromFile(config_file)
	if err != nil {
		log.Fatal(err)
	}

	formatter := &logger.TextFormatter{}
	formatter.TimestampFormat = "2006-01-02 15:04:05.000"
	log.SetFormatter(formatter)
	lvl, err := log.ParseLevel(config.Log_level)
	if err != nil {
		log.Fatalf("failed to parse log-level %q: %s", config.Log_level, err.Error())
	}
	log.SetLevel(lvl)

	var revRe []*regexp.Regexp

	for _, agg := range config.Aggregation {
		r, err := reverseRegex(agg)
		if err != nil {
			log.Fatal(err)
		}
		revRe = append(revRe, r)
	}

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		fmt.Printf("%s", scanner.Text())
		for i, rev := range revRe {
			if rev.MatchString(scanner.Text()) {
				fmt.Printf(" %d", i)
			}
		}
		fmt.Println()
	}

	if scanner.Err() != nil {
		log.Fatal(scanner.Err())
	}
}
