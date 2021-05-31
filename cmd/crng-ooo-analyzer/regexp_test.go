package main

import (
	"regexp/syntax"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/grafana/carbon-relay-ng/cfg"
)

func TestGroupsEmpty(t *testing.T) {
	testGroups("", nil, t)
}

func TestGroupsNone(t *testing.T) {
	testGroups("foo.bar.baz", nil, t)
}

func TestGroupsBasic(t *testing.T) {
	want := []group{
		{
			Num:  1,
			Patt: `[^\.]+`,
		},
		{
			Num:  2,
			Patt: "test",
		},
		{
			Num:  3,
			Name: "last",
			Patt: "[A-Za-z]+",
		},
	}
	testGroups(`^foo-([^\.]+)\.(test)\.otherstuff(?P<last>[a-zA-Z]+)`, want, t)
}

func testGroups(input string, want []group, t *testing.T) {
	re, err := syntax.Parse(input, syntax.Perl)
	if err != nil {
		t.Error(err)
	}
	got := groups(re)
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("groups() mismatch (-want +got):\n%s", diff)
	}
}

func TestReverseRegex(t *testing.T) {
	agg := cfg.Aggregation{
		Regex:  "([a-d]+).(irrelevant).(?P<named>[A-Za-z]+).foobar",
		Format: "foo.$1.$3.$named.ok",
	}
	want := `foo\.[a-d]+\.[A-Za-z]+\.[A-Za-z]+\.ok`

	got, err := reverseRegex(agg)
	if err != nil {
		t.Fatal(err)
	}
	if diff := cmp.Diff(want, got.String()); diff != "" {
		t.Errorf("expand() mismatch (-want +got):\n%s", diff)
	}
}

func TestExpandMissing(t *testing.T) {
	template := "foo.$1.$3.$named.ok"
	groups := []group{
		{
			Num:  1,
			Patt: `[a-d]+`,
		},
	}

	_, err := expand(nil, template, groups)
	if err == nil {
		t.Fatal("expected error for missing variable")
	}
}
