package route

import (
	"bytes"
	"crypto/md5"
	"encoding/binary"
	"sort"
	"strconv"
	"strings"

	dest "github.com/graphite-ng/carbon-relay-ng/destination"
)

type hashRingEntry struct {
	Position         uint16
	Hostname         string
	Instance         string
	DestinationIndex int
}

// hashRing represents the ring of elements and implements sort.Interface
// comparing `Position`, `Hostname`, and `Instance`, in that order.
type hashRing []hashRingEntry

// Len, Swap, and Less make up sort.Interface.
func (r hashRing) Len() int {
	return len(r)
}
func (r hashRing) Swap(i, j int) {
	r[i], r[j] = r[j], r[i]
}
func (r hashRing) Less(i, j int) bool {
	return r[i].Position < r[j].Position ||
		(r[i].Position == r[j].Position && r[i].Hostname < r[j].Hostname) ||
		(r[i].Position == r[j].Position && r[i].Hostname == r[j].Hostname && r[i].Instance < r[j].Instance)
}

type ConsistentHasher struct {
	Ring         hashRing
	destinations []*dest.Destination
	replicaCount int
}

func computeRingPosition(key []byte) uint16 {
	var Position uint16
	hash := md5.Sum(key)
	buf := bytes.NewReader(hash[0:2])
	binary.Read(buf, binary.BigEndian, &Position)
	return Position
}

func NewConsistentHasher(destinations []*dest.Destination) ConsistentHasher {
	return NewConsistentHasherReplicaCount(destinations, 100)
}

func NewConsistentHasherReplicaCount(destinations []*dest.Destination, replicaCount int) ConsistentHasher {
	hashRing := ConsistentHasher{replicaCount: replicaCount}
	for _, d := range destinations {
		hashRing.AddDestination(d)
	}
	return hashRing
}

func (h *ConsistentHasher) AddDestination(d *dest.Destination) {
	newDestinationIndex := len(h.destinations)
	h.destinations = append(h.destinations, d)
	newRingEntries := make(hashRing, h.replicaCount)
	for i := 0; i < h.replicaCount; i++ {
		var keyBuf bytes.Buffer
		// The part of the key prior to the ':' is actually the Python
		// string representation of the tuple (server, instance) in the
		// original Carbon code. Note that the server component excludes
		// the port.
		server := strings.Split(d.Addr, ":")
		keyBuf.WriteString("('")
		keyBuf.WriteString(server[0])
		keyBuf.WriteString("', ")
		if d.Instance != "" {
			keyBuf.WriteString("'")
			keyBuf.WriteString(d.Instance)
			keyBuf.WriteString("'")
		} else {
			keyBuf.WriteString("None")
		}
		keyBuf.WriteString(")")
		keyBuf.WriteString(":")
		keyBuf.WriteString(strconv.Itoa(i))
		position := computeRingPosition(keyBuf.Bytes())
		//Align with https://github.com/graphite-project/carbon/commit/024f9e67ca47619438951c59154c0dec0b0518c7#diff-1486787206e06af358b8d935577e76f5
	outer:
		for {
			for i := 0; i < len(h.Ring); i++ {
				if position == h.Ring[i].Position {
					position++
					continue outer
				}
			}
			break
		}

		newRingEntries[i].Position = position
		newRingEntries[i].Hostname = server[0]
		newRingEntries[i].Instance = d.Instance
		newRingEntries[i].DestinationIndex = newDestinationIndex
	}
	h.Ring = append(h.Ring, newRingEntries...)
	sort.Sort(h.Ring)
}

// GetDestinationIndex returns the index of the destination corresponding
// to the provided key.
func (h *ConsistentHasher) GetDestinationIndex(key []byte) int {
	position := computeRingPosition(key)
	// Find the index where we would insert a server entry with the same
	// position field as the position for the specified key.
	// This is equivalent to bisect_left in the Python implementation.
	index := sort.Search(len(h.Ring), func(i int) bool { return h.Ring[i].Position >= position }) % len(h.Ring)
	return h.Ring[index].DestinationIndex
}
