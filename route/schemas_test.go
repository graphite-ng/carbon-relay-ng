package route

import (
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/grafana/carbon-relay-ng/persister"
	"github.com/grafana/metrictank/schema"
)

func getMatchEverythingSchemas() persister.WhisperSchemas {
	schema := persister.WhisperSchemas{
		persister.Schema{
			Name:         "everything",
			RetentionStr: "10:8640",
			Priority:     10,
		},
	}
	schema[0].Retentions, _ = persister.ParseRetentionDefs(schema[0].RetentionStr)
	schema[0].Pattern, _ = regexp.Compile(".*")
	return schema
}

func TestParseMetricErrors(t *testing.T) {
	schemas := getMatchEverythingSchemas()
	cases := []struct {
		in     string
		expErr bool
		expOut schema.MetricData
	}{
		{
			"foo 123 123",
			false,
			schema.MetricData{
				Name:     "foo",
				Metric:   "foo",
				Interval: 10,
				Value:    123,
				Unit:     "unknown",
				Time:     123,
				Mtype:    "gauge",
				Tags:     []string{},
				OrgId:    1,
			},
		},
		{
			".foo 123 123",
			false,
			schema.MetricData{
				Name:     "foo",
				Metric:   "foo",
				Interval: 10,
				Value:    123,
				Unit:     "unknown",
				Time:     123,
				Mtype:    "gauge",
				Tags:     []string{},
				OrgId:    1,
			},
		},
		{
			"......f 123 123",
			false,
			schema.MetricData{
				Name:     "f",
				Metric:   "f",
				Interval: 10,
				Value:    123,
				Unit:     "unknown",
				Time:     123,
				Mtype:    "gauge",
				Tags:     []string{},
				OrgId:    1,
			},
		},
		{
			in:     "...... 123 123",
			expErr: true,
		},
	}
	for i, c := range cases {
		out, err := parseMetric([]byte(c.in), schemas, 1)
		if (err == nil) == c.expErr {
			t.Fatalf("test %d exp err %t, got err %v", i, c.expErr, err)
		}
		if err == nil {
			if !reflect.DeepEqual(c.expOut, *out) {
				t.Fatalf("test %d exp out %v, got %v", i, c.expOut, *out)
			}
		}
	}
}
func TestParseMetricWithTags(t *testing.T) {
	schemas := getMatchEverythingSchemas()
	tags := []string{"tag2=value2", "tag1=value1"}
	time := int64(200)
	value := float64(100)
	name := "a.b.c"
	nameWithTags := fmt.Sprintf("%s;%s", name, strings.Join(tags, ";"))
	line := []byte(fmt.Sprintf("%s %f %d", nameWithTags, value, time))
	md, _ := parseMetric(line, schemas, 1)
	sort.Strings(tags)
	expectedMd := &schema.MetricData{
		Name:     name,
		Interval: 10,
		Value:    value,
		Unit:     "unknown",
		Time:     time,
		Mtype:    "gauge",
		Tags:     tags,
		OrgId:    1,
	}
	if !reflect.DeepEqual(md, expectedMd) {
		t.Fatalf("Returned MetricData is not as expected:\nGot:\n%+v\nExpected:\n%+v\n", md, expectedMd)
	}
}

func TestParseMetricWithoutTags(t *testing.T) {
	schemas := getMatchEverythingSchemas()
	time := int64(200)
	value := float64(100)
	name := "a.b.c"
	line := []byte(fmt.Sprintf("%s %f %d", name, value, time))
	md, _ := parseMetric(line, schemas, 1)
	expectedMd := &schema.MetricData{
		Name:     name,
		Interval: 10,
		Value:    value,
		Unit:     "unknown",
		Time:     time,
		Mtype:    "gauge",
		Tags:     []string{},
		OrgId:    1,
	}
	if !reflect.DeepEqual(md, expectedMd) {
		t.Fatalf("Returned MetricData is not as expected:\nGot:\n%+v\nExpected:\n%+v\n", md, expectedMd)
	}
}
