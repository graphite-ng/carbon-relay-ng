package input

import (
	"bytes"
	"net"
	"sync"
	"time"

	"github.com/jpillora/backoff"
)

type Listener struct {
	wg       sync.WaitGroup
	name     string
	tcpList  *net.TCPListener
	udpConn  *net.UDPConn
	handler  Handler
	shutdown chan struct{}
}

func NewListener(name string, handler Handler) *Listener {
	return &Listener{
		handler:  handler,
		shutdown: make(chan struct{}),
	}
}

func (l *Listener) listen(addr string) error {
	// listeners are set up outside of accept* here so they can interrupt startup
	err := l.listenTcp(addr)
	if err != nil {
		return err
	}

	err = l.listenUdp(addr)
	if err != nil {
		return err
	}

	l.wg.Add(2)
	go l.accept(addr, "tcp", l.acceptTcp, l.listenTcp, l.tcpList)
	go l.accept(addr, "udp", l.consumeUdp, l.listenUdp, l.udpConn)

	return nil
}

func (l *Listener) accept(addr, proto string, consume func(string), reconnect func(string) error, listener Closable) {
	defer l.wg.Done()

	backoffCounter := &backoff.Backoff{
		Min: 500 * time.Millisecond,
		Max: time.Minute,
	}

	go func() {
		<-l.shutdown
		log.Info("shutting down %v/%s, closing socket", addr, proto)
		listener.Close()
	}()

	for {
		log.Notice("listening on %v/%s", addr, proto)

		consume(addr)

		select {
		case <-l.shutdown:
			return
		default:
			for {
				log.Notice("reopening %v/%s", addr, proto)
				err := reconnect(addr)
				if err == nil {
					backoffCounter.Reset()
					break
				}

				backoffDuration := backoffCounter.Duration()
				log.Error("error listening on %v/%s, retrying after %v: %s", addr, proto, backoffDuration, err)
				select {
				case <-l.shutdown:
					log.Info("shutting down %v/%s, closing socket", addr, proto)
					return
				case <-time.After(backoffDuration):
					// retry to connect
				}
			}
		}
	}
}

func (l *Listener) listenTcp(addr string) error {
	laddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		return err
	}
	l.tcpList, err = net.ListenTCP("tcp", laddr)
	if err != nil {
		return err
	}
	return nil
}

func (l *Listener) acceptTcp(addr string) {
	for {
		c, err := l.tcpList.AcceptTCP()
		if err != nil {
			select {
			// socket has been closed due to shut down, log and return
			case <-l.shutdown:
				return
			default:
				log.Error("error accepting on %v/tcp, closing connection: %s", addr, err)
				l.tcpList.Close()
				return
			}
		}

		// handle the connection
		log.Debug("listen.go: tcp connection from %v", c.RemoteAddr())
		l.wg.Add(1)
		go l.acceptTcpConn(c)
	}
}

func (l *Listener) acceptTcpConn(c net.Conn) {
	defer l.wg.Done()

	go func() {
		<-l.shutdown
		c.Close()
	}()

	l.handler.Handle(c)
}

func (l *Listener) listenUdp(addr string) error {
	udp_addr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return err
	}
	l.udpConn, err = net.ListenUDP("udp", udp_addr)
	if err != nil {
		return err
	}
	return nil
}

func (l *Listener) consumeUdp(addr string) {
	buffer := make([]byte, 65535)
	for {
		// read a packet into buffer
		b, src, err := l.udpConn.ReadFrom(buffer)
		if err != nil {
			select {
			// socket has been closed due to shut down, log and return
			case <-l.shutdown:
				return
			default:
				log.Error("error reading packet on %v/udp, closing connection: %s", addr, err)
				l.udpConn.Close()
				return
			}
		}

		// handle the packet
		log.Debug("listen.go: udp packet from %v (length: %d)", src, b)
		l.handler.Handle(bytes.NewReader(buffer[:b]))
	}
}

func (l *Listener) Name() string {
	return l.name
}

func (l *Listener) Stop() bool {
	close(l.shutdown)
	l.wg.Wait()
	return true
}
