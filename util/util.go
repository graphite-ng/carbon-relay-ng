package util

import (
	"fmt"
	"strconv"
	"strings"
)

// some.host:2003 -> some_host_2003
// http://some.host:8080 -> http_some_host_8080
func AddrToPath(s string) string {
	s = strings.Replace(s, ".", "_", -1)
	s = strings.Replace(s, ":", "_", -1)
	return strings.Replace(s, "/", "", -1)
}

// some.host:2003, kafkaRoute -> kafkaRoute_some_host_2003
// http://some.host:8080, kafkaRoute -> kafkaRoute_http_some_host_8080
func Key(routeName, addr string) string {
	return routeName + "_" + AddrToPath(addr)
}

type Point struct {
	Key []byte
	Val float64
	TS  uint32
}

func (point *Point) String() string {
	return fmt.Sprintf("%s %s %d", point.Key, strconv.FormatFloat(point.Val, 'f', -1, 64), point.TS)
}
