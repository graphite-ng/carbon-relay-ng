package input

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math/big"
	"net"

	"github.com/graphite-ng/carbon-relay-ng/badmetrics"
	"github.com/graphite-ng/carbon-relay-ng/cfg"
	"github.com/graphite-ng/carbon-relay-ng/table"
	"github.com/graphite-ng/carbon-relay-ng/validate"
	ogorek "github.com/kisielk/og-rek"
	m20 "github.com/metrics20/go-metrics20/carbon20"
)

type Pickle struct {
	config cfg.Config
	bad    *badmetrics.BadMetrics
	table  *table.Table
}

func NewPickle(config cfg.Config, addr string, tbl *table.Table, bad *badmetrics.BadMetrics) (net.Listener, error) {
	l, err := listen(addr, &Pickle{config, bad, tbl})
	if err != nil {
		return nil, err
	}
	return l, nil
}

func (p *Pickle) Handle(c net.Conn) {
	defer c.Close()
	// TODO c.SetTimeout(60e9)
	r := bufio.NewReaderSize(c, 4096)
	log.Debug("pickle.go: entering ReadLoop...")
ReadLoop:
	for {

		// Note that everything in this loop should proceed as fast as it can
		// so we're not blocked and can keep processing
		// so the validation, the pipeline initiated via table.Dispatch(), etc
		// must never block.

		log.Debug("pickle.go: detecting payload length with binary.Read...")
		var length uint32
		err := binary.Read(r, binary.BigEndian, &length)
		if err != nil {
			if io.EOF != err {
				log.Error("couldn't read payload length: " + err.Error())
			}
			log.Debug("pickle.go: detected EOF while detecting payload length with binary.Read, nothing more to read, breaking")
			break
		}
		log.Debug(fmt.Sprintf("pickle.go: done detecting payload length with binary.Read, length is %d", int(length)))

		lengthTotal := int(length)
		lengthRead := 0
		payload := make([]byte, lengthTotal, lengthTotal)
		for {
			log.Debug("pickle.go: reading payload...")
			tmpLengthRead, err := r.Read(payload[lengthRead:])
			if err != nil {
				log.Error("couldn't read payload: " + err.Error())
				break ReadLoop
			}
			lengthRead += tmpLengthRead
			if lengthRead == lengthTotal {
				log.Debug("pickle.go: done reading payload")
				break
			}
			if lengthRead > lengthTotal {
				log.Error(fmt.Sprintf("expected to read %d bytes, but read %d", length, lengthRead))
				break ReadLoop
			}
		}

		decoder := ogorek.NewDecoder(bytes.NewBuffer(payload))

		log.Debug("pickle.go: decoding pickled data...")
		rawDecoded, err := decoder.Decode()
		if err != nil {
			if io.ErrUnexpectedEOF != err {
				log.Error("error reading pickled data " + err.Error())
			}
			log.Debug("pickle.go: detected ErrUnexpectedEOF while decoding pickled data, nothing more to decode, breaking")
			break
		}
		log.Debug("pickle.go: done decoding pickled data")

		log.Debug("pickle.go: checking the type of pickled data...")
		decoded, ok := rawDecoded.([]interface{})
		if !ok {
			log.Error(fmt.Sprintf("Unrecognized type %T for pickled data", rawDecoded))
			break
		}
		log.Debug("pickle.go: done checking the type of pickled data")

		log.Debug("pickle.go: entering ItemLoop...")

	ItemLoop:
		for _, rawItem := range decoded {
			numIn.Inc(1)

			log.Debug("pickle.go: doing high-level validation of unpickled item and data...")
			item, ok := rawItem.(ogorek.Tuple)
			if !ok {
				log.Error(fmt.Sprintf("Unrecognized type %T for item", rawItem))
				numInvalid.Inc(1)
				continue
			}
			if len(item) != 2 {
				log.Error(fmt.Sprintf("item length must be 2, got %d", len(item)))
				numInvalid.Inc(1)
				continue
			}

			metric, ok := item[0].(string)
			if !ok {
				log.Error(fmt.Sprintf("item metric must be a string, got %T", item[0]))
				numInvalid.Inc(1)
				continue
			}

			data, ok := item[1].(ogorek.Tuple)
			if !ok {
				log.Error(fmt.Sprintf("item data must be an array, got %T", item[1]))
				numInvalid.Inc(1)
				continue
			}
			if len(data) != 2 {
				log.Error(fmt.Sprintf("item data length must be 2, got %d", len(data)))
				numInvalid.Inc(1)
				continue
			}
			log.Debug("pickle.go: done doing high-level validation of unpickled item and data")

			var value string
			switch data[1].(type) {
			case string:
				value = data[1].(string)
			case uint8, uint16, uint32, uint64, int8, int16, int32, int64:
				value = fmt.Sprintf("%d", data[1])
			case float32, float64:
				value = fmt.Sprintf("%f", data[1])
			default:
				log.Error(fmt.Sprintf("Unrecognized type %T for value", data[1]))
				numInvalid.Inc(1)
				continue ItemLoop
			}

			var timestamp string
			switch data[0].(type) {
			case string:
				timestamp = data[0].(string)
			case uint8, uint16, uint32, uint64, int8, int16, int32, int64, (*big.Int):
				timestamp = fmt.Sprintf("%d", data[0])
			case float32, float64:
				timestamp = fmt.Sprintf("%.0f", data[0])
			default:
				log.Error(fmt.Sprintf("Unrecognized type %T for timestamp", data[0]))
				numInvalid.Inc(1)
				continue ItemLoop
			}

			buf := []byte(metric + " " + value + " " + timestamp)

			log.Debug("pickle.go: passing unpickled metric to m20 Packet validator...")
			key, val, ts, err := m20.ValidatePacket(buf, p.config.Validation_level_legacy.Level, p.config.Validation_level_m20.Level)
			if err != nil {
				log.Debug("pickle.go: metric failed to pass m20 Packet validation!")
				p.bad.Add(key, buf, err)
				numInvalid.Inc(1)
				continue
			}

			if p.config.Validate_order {
				log.Debug("pickle.go: order validation enabled, performing order validation...")
				err = validate.Ordered(key, ts)
				if err != nil {
					log.Debug("pickle.go: order validation failed!")
					p.bad.Add(key, buf, err)
					numOutOfOrder.Inc(1)
					continue
				}
			}

			log.Debug("pickle.go: all good, dispatching metrics buffer")
			p.table.Dispatch(key, val, ts)

			log.Debug("pickle.go: exiting ItemLoop")
		}
		log.Debug("pickle.go: exiting ReadLoop")
	}
}
