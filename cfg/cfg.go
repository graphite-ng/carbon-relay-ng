package cfg

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/grafana/carbon-relay-ng/validate"
	m20 "github.com/metrics20/go-metrics20/carbon20"
)

type Config struct {
	Listen_addr             string
	Plain_read_timeout      Duration
	Pickle_addr             string
	Pickle_read_timeout     Duration
	Admin_addr              string
	Http_addr               string
	Spool_dir               string
	Amqp                    Amqp
	Max_procs               int
	First_only              bool
	Init                    Init
	Instance                string
	Log_level               string
	Instrumentation         instrumentation
	Bad_metrics_max_age     string
	Pid_file                string
	Validation_level_legacy validate.LevelLegacy
	Validation_level_m20    validate.LevelM20
	Validate_order          bool
	BlackList               []string
	Aggregation             []Aggregation
	Route                   []Route
	Rewriter                []Rewriter
}

func New() Config {
	return Config{
		Plain_read_timeout: Duration{
			2 * time.Minute,
		},
		Pickle_read_timeout: Duration{
			2 * time.Minute,
		},
		Validation_level_legacy: validate.LevelLegacy{m20.MediumLegacy},
		Validation_level_m20:    validate.LevelM20{m20.MediumM20},
	}
}

func NewFromFile(path string) (Config, toml.MetaData, error) {
	config := New()
	var meta toml.MetaData
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return config, meta, fmt.Errorf("Couldn't read config file %q: %s", path, err.Error())
	}

	dataStr := os.Expand(string(data), expandVars)
	meta, err = toml.Decode(dataStr, &config)
	if err != nil {
		return config, meta, fmt.Errorf("Invalid config file %q: %s", path, err.Error())
	}
	return config, meta, nil
}

func expandVars(in string) (out string) {
	switch in {
	case "HOST":
		hostname, _ := os.Hostname()
		// in case hostname is an fqdn or has dots, only take first part
		parts := strings.SplitN(hostname, ".", 2)
		return parts[0]
	case "GRAFANA_NET_ADDR":
		return os.Getenv("GRAFANA_NET_ADDR")
	case "GRAFANA_NET_API_KEY":
		return os.Getenv("GRAFANA_NET_API_KEY")
	case "GRAFANA_NET_USER_ID":
		return os.Getenv("GRAFANA_NET_USER_ID")
	default:
		return "$" + in
	}
}

type Duration struct {
	time.Duration
}

func (d *Duration) UnmarshalText(text []byte) error {
	var err error
	d.Duration, err = time.ParseDuration(string(text))
	return err
}

type Aggregation struct {
	Function  string
	Regex     string
	NotRegex  string
	Prefix    string
	NotPrefix string
	Substr    string
	Sub       string
	NotSub    string
	Format    string
	Cache     bool
	Interval  int
	Wait      int
	DropRaw   bool
}

type Route struct {
	Key          string
	Type         string
	Prefix       string
	NotPrefix    string
	Substr       string
	Sub          string
	NotSub       string
	Regex        string
	NotRegex     string
	Destinations []string

	// grafanaNet & kafkaMdm & Google PubSub
	SchemasFile  string
	OrgId        int
	BufSize      int
	FlushMaxNum  int // also used by CloudWatch
	FlushMaxWait int // also used by CloudWatch
	Timeout      int
	Blocking     bool

	// grafanaNet
	Addr        string
	ApiKey      string
	Spool       bool
	SslVerify   bool
	Concurrency int

	// kafkaMdm
	Brokers       []string
	Topic         string // also used by Google PubSub
	Codec         string // also used by Google PubSub
	PartitionBy   string
	TLSEnabled    bool
	TLSSkipVerify bool
	TLSClientCert string
	TLSClientKey  string
	SASLEnabled   bool
	SASLMechanism string
	SASLUsername  string
	SASLPassword  string

	// Google PubSub
	Project      string
	Format       string
	FlushMaxSize int

	// CloudWatch
	Profile           string // For local development
	Region            string
	Namespace         string     // For now fixed in config
	Dimensions        [][]string // For now fixed in config
	StorageResolution int64
}

type Rewriter struct {
	Old string
	New string
	Not string
	Max int
}

type Amqp struct {
	Amqp_enabled   bool
	Amqp_host      string
	Amqp_port      int
	Amqp_vhost     string
	Amqp_user      string
	Amqp_password  string
	Amqp_exchange  string
	Amqp_queue     string
	Amqp_key       string
	Amqp_durable   bool
	Amqp_exclusive bool
}

type Init struct {
	Cmds []string
}

type instrumentation struct {
	Graphite_addr     string
	Graphite_interval int
}
