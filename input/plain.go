package input

import (
	"bufio"
	"io"
)

type Plain struct {
	dispatcher Dispatcher
}

func NewPlain(addr string, dispatcher Dispatcher) (*Listener, error) {
	listener := NewListener("plain", &Plain{dispatcher})
	return listener, listener.listen(addr)
}

func (p *Plain) Handle(c io.Reader) {
	scanner := bufio.NewScanner(c)
	for scanner.Scan() {
		// Note that everything in this loop should proceed as fast as it can
		// so we're not blocked and can keep processing
		// so the validation, the pipeline initiated via dispatcher.Dispatch(), etc
		// must never block.

		buf := scanner.Bytes()
		log.Debug("plain.go: Received Line: %q", buf)

		p.dispatcher.Dispatch(buf)
	}
	if err := scanner.Err(); err != nil {
		log.Error(err.Error())
	}
}
