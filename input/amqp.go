package input

import (
	"bufio"
	"bytes"
	"io"
	"sync"
	"time"

	"github.com/graphite-ng/carbon-relay-ng/cfg"
	"github.com/jpillora/backoff"
	"github.com/streadway/amqp"
)

// amqpConnector is a function that connects an instance of *Amqp so
// it will receive messages
type amqpConnector func(a *Amqp) (<-chan amqp.Delivery, error)

// Closables are things that have a .Close() method (channels & connections)
type Closable interface {
	Close() error
}

type Amqp struct {
	wg         sync.WaitGroup
	uri        amqp.URI
	conn       Closable
	channel    Closable
	config     cfg.Config
	dispatcher Dispatcher
	connect    amqpConnector
	shutdown   chan struct{}
}

func (a *Amqp) close() {
	a.channel.Close()
	a.conn.Close()
}

func StartAMQP(config cfg.Config, dispatcher Dispatcher, connect amqpConnector) *Amqp {
	a := NewAMQP(config, dispatcher, connect)
	go a.Start()
	return a
}

func NewAMQP(config cfg.Config, dispatcher Dispatcher, connect amqpConnector) *Amqp {
	uri := amqp.URI{
		Scheme:   "amqp",
		Host:     config.Amqp.Amqp_host,
		Port:     config.Amqp.Amqp_port,
		Username: config.Amqp.Amqp_user,
		Password: config.Amqp.Amqp_password,
		Vhost:    config.Amqp.Amqp_vhost,
	}

	return &Amqp{
		uri:        uri,
		config:     config,
		dispatcher: dispatcher,
		connect:    connect,
		shutdown:   make(chan struct{}),
	}
}

// connects an instance of Amqp and returns the message channel
func AMQPConnector(a *Amqp) (<-chan amqp.Delivery, error) {
	log.Notice("dialing AMQP: %v", a.uri)
	conn, err := amqp.Dial(a.uri.String())
	if err != nil {
		return nil, err
	}
	a.conn = conn

	amqpChan, err := conn.Channel()
	if err != nil {
		a.conn.Close()
		return nil, err
	}
	a.channel = amqpChan

	// queue name will be random, as in the python implementation
	q, err := amqpChan.QueueDeclare(a.config.Amqp.Amqp_queue, a.config.Amqp.Amqp_durable, false, a.config.Amqp.Amqp_exclusive, false, nil)
	if err != nil {
		a.close()
		return nil, err
	}

	err = amqpChan.QueueBind(q.Name, a.config.Amqp.Amqp_key, a.config.Amqp.Amqp_exchange, false, nil)
	if err != nil {
		a.close()
		return nil, err
	}

	c, err := amqpChan.Consume(q.Name, "carbon-relay-ng", true, a.config.Amqp.Amqp_exclusive, true, false, nil)
	if err != nil {
		a.close()
		return nil, err
	}

	return c, nil
}

func (a *Amqp) Start() {
	b := &backoff.Backoff{
		Min: 500 * time.Millisecond,
	}
	a.wg.Add(1)
	defer a.wg.Done()

	for {
		c, err := a.connect(a)
		if err != nil {

			select {
			case <-a.shutdown:
				log.Info("shutting down AMQP client")
				return
			default:
			}
			dur := b.Duration()
			log.Error("connectAMQP: %v. retrying in %s", err, dur)
			<-time.After(dur)
		} else {
			// connected successfully; reset backoff
			b.Reset()

			// blocks until channel is closed
			a.consumeAMQP(c)
			log.Notice("consumeAMQP: channel closed")

			// reconnect immediately
			a.close()

			select {
			case <-a.shutdown:
				log.Info("shutting down AMQP client")
				return
			default:
			}
		}
	}
}

func (a *Amqp) Name() string {
	return "amqp"
}

func (a *Amqp) Stop() bool {
	close(a.shutdown)
	a.wg.Wait()
	return true
}

func (a *Amqp) consumeAMQP(c <-chan amqp.Delivery) {
	log.Notice("consuming AMQP messages")
	for {
		select {
		case m := <-c:
			// note that we don't support lines longer than 4096B. that seems very reasonable..
			r := bufio.NewReaderSize(bytes.NewReader(m.Body), 4096)
			for {
				buf, _, err := r.ReadLine()

				if err != nil {
					if io.EOF != err {
						log.Error(err.Error())
					}
					break
				}

				a.dispatcher.Dispatch(buf)
			}
		case <-a.shutdown:
			return
		}
	}
}
