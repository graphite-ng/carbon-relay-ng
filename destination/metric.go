package destination

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/graphite-ng/carbon-relay-ng/util"
)

func ParseDataPoint(buf []byte) (*util.Point, error) {
	str := strings.TrimSpace(string(buf))
	elements := strings.Fields(str)
	if len(elements) != 3 {
		return nil, fmt.Errorf("%q doesn't have three fields", str)
	}
	name := elements[0]
	val, err := strconv.ParseFloat(elements[1], 64)
	if err != nil {
		return nil, err
	}
	timestamp, err := strconv.ParseUint(elements[2], 10, 32)
	if err != nil {
		return nil, err
	}
	return &util.Point{[]byte(name), val, uint32(timestamp)}, nil
}
