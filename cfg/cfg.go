package cfg

import (
	"github.com/graphite-ng/carbon-relay-ng/validate"
)

type Config struct {
	Listen_addr             string
	Pickle_addr             string
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

type Aggregation struct {
	Function string
	Regex    string
	Prefix   string
	Substr   string
	Format   string
	Cache    bool
	Interval uint32
	Wait     uint32
	DropRaw  bool
}

type Route struct {
	Key          string
	Type         string
	Prefix       string
	Substr       string
	Regex        string
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
	Brokers     []string
	Topic       string // also used by Google PubSub
	Codec       string // also used by Google PubSub
	PartitionBy string

	// Google PubSub
	Project      string
	Format       string
	FlushMaxSize int

	// CloudWatch
	Profile    string // For local development
	Region     string
	Namespace  string     // For now fixed in config
	Dimensions [][]string // For now fixed in config
}

type Rewriter struct {
	Old string
	New string
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
