package rlog

import (
	"github.com/op/go-logging"
	"os"
)

var Logger = logging.MustGetLogger("relay")

var format = logging.MustStringFormatter(
	`%{color}%{time:2006-01-02 15:04:05.000} %{shortfile} > %{level:.4s} %{id:03x}%{color:reset} %{message}`,
)

func init() {
	// For demo purposes, create two backend for os.Stderr.
	backend1 := logging.NewLogBackend(os.Stdout, "", 0)
	backend2 := logging.NewLogBackend(os.Stderr, "", 0)

	// For messages written to backend2 we want to add some additional
	// information to the output, including the used log level and the name of
	// the function.
	backend1Formatter := logging.NewBackendFormatter(backend1, format)

	// Only errors and more severe messages should be sent to backend2
	backend2Leveled := logging.AddModuleLevel(backend2)
	backend2Leveled.SetLevel(logging.ERROR, "")

	// Set the backends to be used.
	logging.SetBackend(backend2Leveled, backend1Formatter)
}
