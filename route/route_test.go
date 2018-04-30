package route

import (
	"testing"

	"github.com/graphite-ng/carbon-relay-ng/destination"
	"github.com/graphite-ng/carbon-relay-ng/util"
)

// just sending into route, no matching or sending to dest
func BenchmarkRouteDispatchMetric(b *testing.B) {
	route, err := NewSendAllMatch("", "", "", "", make([]*destination.Destination, 0))
	if err != nil {
		b.Fatal(err)
	}
	metric70 := &util.Point{
		[]byte("abcde_fghij.klmnopqrst.uv_wxyz.1234567890abcdefg"),
		float64(12345.6789),
		uint32(1234567890),
	}
	for i := 0; i < b.N; i++ {
		route.Dispatch(metric70)
	}
}
