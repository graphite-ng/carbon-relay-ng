package aggregator

import (
	"bytes"
	"testing"
	"time"

	"github.com/graphite-ng/carbon-relay-ng/util"
)

func TestScanner(t *testing.T) {
	cases := []struct {
		in    []float64
		ts    []uint32
		avg   float64
		delta float64
		last  float64
		max   float64
		min   float64
		stdev float64
		sum   float64
		deriv float64
		p25   float64
		p50   float64
		p75   float64
		p90   float64
		p95   float64
		p99   float64
	}{
		{
			[]float64{1, 2, 5, 4, 3},
			[]uint32{1, 2, 3, 4, 5},
			3,
			4,
			3,
			5,
			1,
			1.4142135623730951,
			15,
			0.5,
			1.5,
			3,
			4.5,
			5,
			5,
			5,
		},
		{
			[]float64{5, 4, 7, 4, 2, 5, 4, 9},
			[]uint32{1, 2, 3, 4, 5, 6, 7, 8},
			5,
			7,
			9,
			9,
			2,
			2,
			40,
			float64(4) / float64(7),
			4,
			4.5,
			6.5,
			9,
			9,
			9,
		},
		{
			[]float64{6, 2, 3, 1},
			[]uint32{1, 2, 3, 4},
			3,
			5,
			1,
			6,
			1,
			1.8708286933869707,
			12,
			float64(-5) / float64(3),
			1.25,
			2.5,
			5.25,
			6,
			6,
			6,
		},
		// test out of order. this is the same dataset as the first one, but a bit shuffled
		{
			[]float64{7, 4, 5, 4, 9, 5, 4, 2},
			[]uint32{3, 2, 1, 4, 8, 6, 7, 5},
			5,
			7,
			2, // last is the last received one, not the one with last timestamp. we could handle that better perhaps
			9,
			2,
			2,
			40,
			float64(4) / float64(7),
			4,
			4.5,
			6.5,
			9,
			9,
			9,
		},
		// Testing percentiles against NIST example from https://www.itl.nist.gov/div898/handbook/prc/section2/prc262.htm
		{
			[]float64{95.1772, 95.1567, 95.1937, 95.1959, 95.1442, 95.0610, 95.1591, 95.1195, 95.1065, 95.0925, 95.1990, 95.1682},
			[]uint32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12},
			95.14779166666666,
			0.13799999999999102,
			95.1682,
			95.1990,
			95.0610,
			0.04246679994091972,
			1141.7735,
			-0.0008181818181818492,
			95.10974999999999,
			95.1579,
			95.189575,
			95.19807,
			95.199,
			95.199,
		},
	}
	testCase := func(i int, name string, in []float64, ts []uint32, exp map[string]float64) {
		procConstr, err := GetProcessorConstructor(name)
		if err != nil {
			t.Fatalf("got err %q", err)
		}
		p := procConstr(in[0], ts[0])
		for i, v := range in[1:] {
			p.Add(v, ts[i+1])
		}
		results, ok := p.Flush()
		if !ok {
			t.Fatalf("case %d %s - expected valid output, got null", i, name)
		}
		for fcn, expVal := range exp {
			fcnFound := false
			for _, result := range results {
				if result.fcnName == fcn {
					fcnFound = true
					if result.val != expVal {
						t.Fatalf("case %d %s %s - expected %v, actual %v", i, name, result.fcnName, expVal, result.val)
					}
				}
			}
			if !fcnFound {
				t.Fatalf("case %d %s - expected result of fcn %s not returned in results slice", i, name, fcn)
			}
		}

	}
	for i, e := range cases {
		testCase(i, "avg", e.in, e.ts, map[string]float64{"avg": e.avg})
		testCase(i, "delta", e.in, e.ts, map[string]float64{"delta": e.delta})
		testCase(i, "last", e.in, e.ts, map[string]float64{"last": e.last})
		testCase(i, "max", e.in, e.ts, map[string]float64{"max": e.max})
		testCase(i, "min", e.in, e.ts, map[string]float64{"min": e.min})
		testCase(i, "stdev", e.in, e.ts, map[string]float64{"stdev": e.stdev})
		testCase(i, "sum", e.in, e.ts, map[string]float64{"sum": e.sum})
		testCase(i, "derive", e.in, e.ts, map[string]float64{"derive": e.deriv})
		testCase(i, "percentiles", e.in, e.ts, map[string]float64{
			"p25": e.p25,
			"p50": e.p50,
			"p75": e.p75,
			"p90": e.p90,
			"p95": e.p95,
			"p99": e.p99,
		})
	}
}

var r float64

func BenchmarkProcessorMax(b *testing.B) {
	procConstr, _ := GetProcessorConstructor("max")
	proc := procConstr(3, 0)
	for i := 0; i < b.N; i++ {
		for j := 0; j < 10; j++ {
			proc.Add(float64(j), uint32(j))
		}
		results, ok := proc.Flush()
		if !ok {
			panic("why would max produce an invalid output here?")
		}
		r = results[0].val
	}
}

// an operation here is an aggregation, comprising of 2*aggregates*pointsPerAggregate points,
// (with just as many points ignored each time)
func BenchmarkAggregator1Aggregates2PointsPerAggregate(b *testing.B) {
	benchmarkAggregator(1, 2, "4.0000", false, b)
}
func BenchmarkAggregator5Aggregates10PointsPerAggregate(b *testing.B) {
	benchmarkAggregator(5, 10, "20.000", false, b)
}
func BenchmarkAggregator5Aggregates100PointsPerAggregate(b *testing.B) {
	benchmarkAggregator(5, 100, "200.00", false, b)
}

func BenchmarkAggregator1Aggregates2PointsPerAggregateWithReCache(b *testing.B) {
	benchmarkAggregator(1, 2, "4.0000", true, b)
}
func BenchmarkAggregator5Aggregates10PointsPerAggregateWithReCache(b *testing.B) {
	benchmarkAggregator(5, 10, "20.000", true, b)
}
func BenchmarkAggregator5Aggregates100PointsPerAggregateWithReCache(b *testing.B) {
	benchmarkAggregator(5, 100, "200.00", true, b)
}

// we purposely keep the regex relatively simple because regex performance is up to the carbon-relay-ng user,
// so we want to focus on the carbon-relay-ng features, not regex performance
// b.N is how many points we generate (each based on 100 inputs)

func benchmarkAggregator(aggregates, pointsPerAggregate int, match string, cache bool, b *testing.B) {
	//fmt.Println("BenchmarkAggregator", aggregates, pointsPerAggregate, "with b.N", b.N)
	out := make(chan *util.Point)
	done := make(chan struct{})
	go func(match string) {
		count := 0
		for v := range out {
			count += 1
			if bytes.HasPrefix(v.Key, []byte("aggregated.totals.abc.ignoreme")) {
				continue
			}
			if string(v.Key[33:39]) != match {
				b.Fatalf("expected 'aggregated.totals.abc.<10 random chars> %s... <ts>'. got: %q", match, v)
			}
			//	if count%100 == 0 {
			//fmt.Println("got", string(v), "count is now", count)
			//	}
			if count == aggregates*b.N {
				close(done)
				return
			}
		}
	}(match)

	// at each timestamp we will send all the inputs we predeclare here, with as many matching as non-matching.
	// (2*aggregates*pointsPerAggregate in total are sent at each timestamp)
	// (2*aggregates*pointsPerAggregate matching points go into each aggregation, since each input is used twice see below)
	var inputs [][]byte
	for i := 0; i < aggregates; i++ {
		key := "raw.abc." + RandString(10)
		for j := 0; j < pointsPerAggregate; j++ {
			inputs = append(inputs, []byte(key))
		}
	}
	for i := 0; i < aggregates*pointsPerAggregate; i++ {
		inputs = append(inputs, []byte("nomatch.foo.bar"))
	}

	// time starts at 1000. increase by 5
	// we will do b.N aggregations (each 10s apart) and comprising 2 sets of points (5s apart)
	// so there is 2*b.N input timestamps
	// before adding the values, we set the clock to 12 after the ts of the points so it can flush prev point
	// this means we can allow the aggregator to buffer only 2*aggregates*pointsPerAggregate points, otherwise buffering would be unsafe
	// at the speed that our benchmark runs (as it could trigger flush before it takes in the points)
	// e.g. :
	// pointTS - outputTs - wall
	// 1000    - 1000      1012
	// 1005    - 1000      1017
	// 1010    - 1010      1022 -> flush point with outputTs 990
	// 1015    - 1010      1027
	// 1020    - 1020      1032 -> flush point with outputTs 1000
	// 1025    - 1020      1037
	// 1030    - 1030      1042 -> flush point with outputTs 1010
	// 1035    - 1030      1047
	// 1040    - 1040      1052 -> flush point with outputTs 1020

	// tinfo holds all data to represent a point in time during the bench run
	type tinfo struct {
		ts   uint32 // the timestamp for the data at each point
		wall int64  // the fake wall clock time at each point
	}
	var tinfos []tinfo
	for t := uint32(1000); t < uint32(1000+(10*b.N)); t += 5 {
		tinfos = append(tinfos, tinfo{
			ts:   t,
			wall: int64(t + 12),
		})
	}
	val := 1

	regex := `^raw\.(...)\.([A-Za-z0-9_-]+)$`
	outFmt := "aggregated.totals.$1.$2"

	clock := NewMockClock(0)
	tick := NewMockTick(10)
	clock.AddTick(tick)
	bufSize := 2 * aggregates * pointsPerAggregate

	agg, err := NewMocked("sum", regex, "", "", outFmt, cache, 10, 30, false, out, bufSize, clock.Now, tick.C)
	if err != nil {
		b.Fatalf("couldn't create aggregation: %q", err)
	}

	b.ResetTimer()
	for _, t := range tinfos {
		//	fmt.Println("setting clock to", wall[i], "and sending", len(inputs)/2, "will go through. for ts", string(ts))
		clock.Set(t.wall)
		for _, input := range inputs {
			buf := &util.Point{
				input,
				float64(val),
				t.ts,
			}
			agg.AddMaybe(buf)
		}
	}
	// we must make sure to get the final aggregated point.
	// first of all, fill up the aggregator input buffer with metrics that will match and will go into the queue
	// but can't affect any results we care about.
	// making sure that all values we care about are pushed out of the buffer and processed.
	//fmt.Println("adding", bufSize, "more to push through buffer")
	for i := 0; i < bufSize; i++ {
		buf := &util.Point{
			[]byte("raw.abc.ignoreme"),
			float64(val),
			tinfos[0].ts,
		}
		agg.AddMaybe(buf)
	}
	// then, we keep updating the clock, triggering ticks, with high enough timestamps to surely flush the last aggregates.
	// this so that if the aggregator is busy and doesn't read ticks, we keep trying until it isn't and does.
	lastFlushTs := tinfos[len(tinfos)-1].wall + 1000
	almostFinished := time.Now()
	for {
		select {
		case <-done:
			return
		default:
			if time.Since(almostFinished) > 2*time.Second {
				b.Fatalf("waited 2 seconds for all results to come in. giving up")
			}
			clock.Set(lastFlushTs)
			lastFlushTs += 10
			time.Sleep(10 * time.Microsecond)
		}
	}
}
