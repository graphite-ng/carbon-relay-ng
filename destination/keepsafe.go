package destination

import (
	"sync"
	"time"

	"github.com/graphite-ng/carbon-relay-ng/util"
)

type keepSafe struct {
	initialCap int
	safeOld    []*util.Point
	safeRecent []*util.Point
	periodKeep time.Duration // period to keep at least.  you get this period + a bit more
	sync.Mutex
}

func NewKeepSafe(initialCap int, periodKeep time.Duration) *keepSafe {
	k := keepSafe{
		initialCap: initialCap,
		safeOld:    make([]*util.Point, 0, initialCap),
		safeRecent: make([]*util.Point, 0, initialCap),
		periodKeep: periodKeep,
	}
	go k.keepClean()
	return &k
}

func (k *keepSafe) keepClean() {
	tick := time.NewTicker(k.periodKeep)
	for range tick.C {
		k.Lock()
		k.safeOld = k.safeRecent
		k.safeRecent = make([]*util.Point, 0, k.initialCap)
		k.Unlock()
	}
}

func (k *keepSafe) Add(point *util.Point) {
	k.Lock()
	k.safeRecent = append(k.safeRecent, point)
	k.Unlock()
}

func (k *keepSafe) GetAll() []*util.Point {
	k.Lock()
	ret := append(k.safeOld, k.safeRecent...)
	k.safeOld = make([]*util.Point, 0, k.initialCap)
	k.safeRecent = make([]*util.Point, 0, k.initialCap)
	k.Unlock()
	return ret
}
