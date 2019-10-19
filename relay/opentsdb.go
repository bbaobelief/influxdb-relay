package relay

import (
	"fmt"
	logger "influxdb-relay/common/log"
	"influxdb-relay/config"
	"io"
	"net"
	"sync/atomic"
)

type OpenTSDB struct {
	addr string
	name string

	closing int64
	ln      net.Listener

	backends []*telnetBackend
}

func (g *OpenTSDB) Name() string {
	if g.name == "" {
		return fmt.Sprintf("INFO tcp://%s \n", g.addr)
	}

	return g.name
}

func NewTSDBRelay(cfg config.TSDBonfig) (Relay, error) {
	t := new(OpenTSDB)

	t.addr = cfg.Addr
	t.name = cfg.Name

	listener, err := net.Listen("tcp", t.addr)
	if err != nil {
		return nil, err
	}

	t.ln = listener

	for i := range cfg.Outputs {
		cfg := &cfg.Outputs[i]
		if cfg.Name == "" {
			cfg.Name = cfg.Location
		}

		backend, err := newTelnetBackend(cfg)
		if err != nil {
			return nil, err
		}

		t.backends = append(t.backends, backend)

	}

	return t, nil
}

func (t *OpenTSDB) Run() error {
	logger.Info.Printf("INFO Starting opentsdb relay %q on %v \n", t.Name(), t.addr)

	for {
		conn, err := t.ln.Accept()
		if opErr, ok := err.(*net.OpError); ok && !opErr.Temporary() {
			logger.Info.Println("INFO Opentsdb tcp listener closed")
			return opErr
		} else if err != nil {
			logger.Error.Println("ERROR accepting OpenTSDB", err)
			continue
		}

		go t.handleConn(conn)
	}
}

func (t *OpenTSDB) handleConn(conn net.Conn) {
	defer conn.Close()

	writers := []io.Writer{}

	for _, b := range t.backends {
		if b == nil {
			continue
		}

		remote, err := b.Pool.Get()
		if err != nil {
			continue
		}

		writers = append(writers, remote)
	}

	mw := io.MultiWriter(writers...)

	_, err := io.Copy(mw, conn)
	if err != nil {
		logger.Error.Printf("ERROR writing to multiwriter: %s\n", err.Error())
	}
}

//func (t *OpenTSDB) Send(line []byte) {
//	var wg sync.WaitGroup
//
//	if len(line) == 0 {
//		return
//	}
//
//	for _, b := range t.backends {
//		if b == nil {
//			continue
//		}
//
//		wg.Add(1)
//		go func(b *telnetBackend) {
//			defer wg.Done()
//
//			var err error
//			sendOk := false
//			for i := 0; i < b.Cfg.Retry; i++ {
//				err := b.WriteBackend(line)
//				if err == nil {
//					sendOk = true
//					break
//				}
//				time.Sleep(time.Millisecond * 10)
//			}
//
//			if !sendOk {
//				logger.Error.Println("ERROR send influxdb %s fail: %v", b.Cfg.Location, err)
//			}
//
//			return
//		}(b)
//
//	}
//
//	wg.Wait()
//}

func (t *OpenTSDB) Stop() error {
	atomic.StoreInt64(&t.closing, 1)
	return t.ln.Close()
}
