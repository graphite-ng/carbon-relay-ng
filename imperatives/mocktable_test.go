package imperatives

import (
	"github.com/graphite-ng/carbon-relay-ng/aggregator"
	"github.com/graphite-ng/carbon-relay-ng/matcher"
	"github.com/graphite-ng/carbon-relay-ng/rewriter"
	"github.com/graphite-ng/carbon-relay-ng/route"
	"github.com/graphite-ng/carbon-relay-ng/util"
)

type mockTable struct {
}

func (m *mockTable) AddAggregator(agg *aggregator.Aggregator)                              {}
func (m *mockTable) AddRewriter(rw rewriter.RW)                                            {}
func (m *mockTable) AddBlacklist(matcher *matcher.Matcher)                                 {}
func (m *mockTable) AddRoute(route route.Route)                                            {}
func (m *mockTable) DelRoute(key string) error                                             { return nil }
func (m *mockTable) UpdateDestination(key string, index int, opts map[string]string) error { return nil }
func (m *mockTable) UpdateRoute(key string, opts map[string]string) error                  { return nil }
func (m *mockTable) GetAggIn() chan *util.Point                                            { return nil }
func (m *mockTable) GetSpoolDir() string                                                   { return "fake-spool-dir" }
