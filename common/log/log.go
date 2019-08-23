package log

import (
	"log"
	"os"
)

var (
	Info    *log.Logger
	Warning *log.Logger
	Error   *log.Logger
)

func init() {

	Info = log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lshortfile)
	Warning = log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lshortfile)
	Error = log.New(os.Stderr, "", log.Ldate|log.Ltime|log.Lshortfile)

}
