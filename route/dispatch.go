package route

import (
	"github.com/Dieterbe/go-metrics"
	"github.com/graphite-ng/carbon-relay-ng/util"
)

// DispatchNonBlocking will dispatch in to buf.
// if buf is full, will discard the data
func dispatchNonBlocking(buf chan *util.Point, in *util.Point, gauge metrics.Gauge, drops metrics.Counter) {
	select {
	case buf <- in:
		gauge.Inc(1)
	default:
		drops.Inc(1)
	}
}

// DispatchBlocking will dispatch in to buf.
// If buf is full, the call will block
// note that in this case, numBuffered will contain size of buffer + number of waiting entries,
// and hence could be > bufSize
func dispatchBlocking(buf chan *util.Point, in *util.Point, gauge metrics.Gauge, drops metrics.Counter) {
	gauge.Inc(1)
	buf <- in
}
