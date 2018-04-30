package route

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/Dieterbe/go-metrics"
	dest "github.com/graphite-ng/carbon-relay-ng/destination"
	"github.com/graphite-ng/carbon-relay-ng/matcher"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatch"
	"github.com/graphite-ng/carbon-relay-ng/stats"
	"github.com/graphite-ng/carbon-relay-ng/util"
)

// Publishes data points to the native AWS metrics service: CloudWatch
type CloudWatch struct {
	awsProfile         string
	awsRegion          string
	awsNamespace       string
	awsDimensions      []*cloudwatch.Dimension
	session            *session.Session
	client             *cloudwatch.CloudWatch
	putMetricDataInput cloudwatch.PutMetricDataInput

	baseRoute
	buf      chan *util.Point
	blocking bool
	dispatch func(chan *util.Point, *util.Point, metrics.Gauge, metrics.Counter)

	bufSize      int // amount of messages we can buffer up. each message is about 100B. so 1e7 is about 1GB.
	flushMaxSize int
	flushMaxWait time.Duration

	numOut                metrics.Counter // metrics successfully written to CloudWatch
	numCloudWatchMessages metrics.Counter // number of messages submitted to CloudWatch
	numErrFlush           metrics.Counter
	numDropBuffFull       metrics.Counter   // metric drops due to queue full
	numParseError         metrics.Counter   // metrics that failed destination.ParseDataPoint()
	durationTickFlush     metrics.Timer     // only updated after successful flush
	tickFlushSize         metrics.Histogram // only updated after successful flush
	numBuffered           metrics.Gauge
	bufferSize            metrics.Gauge
}

// NewCloudWatch creates a route that writes metrics to the AWS service CloudWatch
// We will automatically run the route and the destination
func NewCloudWatch(key, prefix, sub, regex, awsProfile, awsRegion, awsNamespace string, awsDimensions [][]string, bufSize, flushMaxSize, flushMaxWait int, blocking bool) (Route, error) {
	m, err := matcher.New(prefix, sub, regex)
	if err != nil {
		return nil, err
	}

	r := &CloudWatch{
		awsProfile:         awsProfile,
		awsRegion:          awsRegion,
		awsNamespace:       awsNamespace,
		putMetricDataInput: cloudwatch.PutMetricDataInput{Namespace: aws.String(awsNamespace)},
		baseRoute:          baseRoute{sync.Mutex{}, atomic.Value{}, key},
		buf:                make(chan *util.Point, bufSize),
		blocking:           blocking,
		bufSize:            bufSize,
		flushMaxSize:       flushMaxSize,
		flushMaxWait:       time.Duration(flushMaxWait) * time.Millisecond,

		numOut:                stats.Counter("dest=cloudwatch" + ".unit=Metric.direction=out"),
		numCloudWatchMessages: stats.Counter("dest=cloudwatch" + "unit.Metric.what=CloudWatchMessagesPublished"),
		numErrFlush:           stats.Counter("dest=cloudwatch" + ".unit=Err.type=flush"),
		numParseError:         stats.Counter("dest=cloudwatch" + ".unit.Err.type=parse"),
		durationTickFlush:     stats.Timer("dest=cloudwatch" + ".what=durationFlush.type=ticker"),
		tickFlushSize:         stats.Histogram("dest=cloudwatch" + ".unit=B.what=FlushSize.type=ticker"),
		numBuffered:           stats.Gauge("dest=cloudwatch" + ".unit=Metric.what=numBuffered"),
		bufferSize:            stats.Gauge("dest=cloudwatch" + ".unit=Metric.what=bufferSize"),
		numDropBuffFull:       stats.Counter("dest=cloudwatch" + ".unit=Metric.action=drop.reason=queue_full"),
	}
	r.bufferSize.Update(int64(bufSize))

	if blocking {
		r.dispatch = dispatchBlocking
	} else {
		r.dispatch = dispatchNonBlocking
	}

	for _, dim := range awsDimensions {
		if len(dim) < 2 {
			log.Error("RouteCloudWatch: Dimension needs exactly 2 fields: name and val", dim)
			continue
		}
		r.awsDimensions = append(r.awsDimensions, &cloudwatch.Dimension{
			Name:  aws.String(dim[0]),
			Value: aws.String(dim[1])})
	}

	// Initialize a session at AWS
	r.session = session.Must(session.NewSessionWithOptions(session.Options{
		// For local development:
		// credentials from the shared credentials file ~/.aws/credentials
		// and configuration from the shared configuration file ~/.aws/config.
		// SharedConfigState: session.SharedConfigEnable,
		// AssumeRoleTokenProvider: stscreds.StdinTokenProvider,
		// Profile: r.awsProfile,
		Config: aws.Config{
			Region: aws.String(r.awsRegion),
		},
	}))

	// Create new CloudWatch client.
	r.client = cloudwatch.New(r.session)

	r.config.Store(baseConfig{*m, make([]*dest.Destination, 0)})
	go r.run()
	return r, nil
}

func (r *CloudWatch) run() {
	ticker := time.NewTicker(r.flushMaxWait)
	var cnt int // number of metrics written to the payload buffer

	flush := func() {
		r.publish(r.putMetricDataInput, cnt)
		r.putMetricDataInput.MetricData = nil
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

			// Write new metric data to slice
			newDatum := &cloudwatch.MetricDatum{
				MetricName: aws.String(string(point.Key)),
				Timestamp:  aws.Time(time.Unix(int64(point.TS), 0)),
				Value:      aws.Float64(point.Val),
			}
			if len(r.awsDimensions) > 0 {
				newDatum.Dimensions = r.awsDimensions
			}
			r.putMetricDataInput.MetricData = append(r.putMetricDataInput.MetricData, newDatum)

			// flush if slice is likely to breach our size limit
			if len(r.putMetricDataInput.MetricData) >= r.flushMaxSize {
				flush()
			}

			cnt++
		case _ = <-ticker.C:
			if len(r.putMetricDataInput.MetricData) > 0 {
				flush()
			}
		}
	}
}

// publishes a batch to CloudWatch.
func (r *CloudWatch) publish(metricData cloudwatch.PutMetricDataInput, cnt int) {
	if cnt == 0 {
		return
	}
	start := time.Now()

	// Publish to CloudWatch!
	result, err := r.client.PutMetricData(&metricData)
	if err != nil {
		log.Error("RouteCloudWatch: failed sending metric data", err)
		r.numErrFlush.Inc(1)
		return
	}

	dataLength := int64(len(metricData.MetricData))
	dur := time.Since(start)
	r.numOut.Inc(int64(cnt))
	r.numCloudWatchMessages.Inc(1)
	r.durationTickFlush.Update(dur)
	r.tickFlushSize.Update(dataLength)
	// ex: "CloudWatch(key) publish success, count: 50000, size: 2139099, time: 1.288598 seconds"
	log.Info("CloudWatch(%s) publish success, count: %d, size: %d, time: %f ms, result: %s",
		r.Key(), cnt, dataLength, float64(dur)/float64(time.Millisecond), result)

}

// Dispatch is called to submit metrics. They will be in graphite 'plain' format no matter how they arrived.
func (r *CloudWatch) Dispatch(point *util.Point) {
	r.dispatch(r.buf, point, r.numBuffered, r.numDropBuffFull)
}

// Flush is not currently implemented
func (r *CloudWatch) Flush() error {
	// no-op. Flush() is currently not called by anything.
	return nil
}

// Shutdown stops the CloudWatch publisher and returns with the publisher has finished in-flight work
func (r *CloudWatch) Shutdown() error {
	close(r.buf)
	return nil
}

// Snapshot clones the current config for update operations
func (r *CloudWatch) Snapshot() Snapshot {
	return makeSnapshot(&r.baseRoute, "CloudWatch")
}
