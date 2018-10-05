package input

import (
	"io"

	logging "github.com/op/go-logging"
)

var (
	log = logging.MustGetLogger("input") // for tests. overridden by main
)

func SetLogger(l *logging.Logger) {
	log = l
}

type Plugin interface {
	Name() string
	Stop() bool
}

type Handler interface {
	Handle(io.Reader)
}

type Dispatcher interface {
	Dispatch(buf []byte)
	IncNumInvalid()
}
