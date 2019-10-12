package relay

import (
	"bufio"
	"fmt"
	logger "influxdb-relay/common/log"
	"influxdb-relay/config"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

const (
	DefaultRetry       = 3
	DefaultInterval    = 5 * time.Second
	DefaultDialTimeout = 5 * time.Second
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
			logger.Info.Println("OpenTSDB TCP listener closed")
			return opErr
		} else if err != nil {
			logger.Error.Println("Error accepting OpenTSDB", err)
			continue
		}

		go t.handleConn(conn)
	}
}

func (t *OpenTSDB) handleConn(conn net.Conn) {
	defer conn.Close()

	reader := bufio.NewReader(conn)
	for {

		line, err := reader.ReadBytes('\n')
		if err != nil {
			if err != io.EOF {
				logger.Info.Printf("ERROR Reading from OpenTSDB connection %v \n", err)
			}
			return
		}

		t.Send(line)
	}
}

func (t *OpenTSDB) Send(line []byte) {
	var wg sync.WaitGroup

	if len(line) == 0 {
		return
	}

	for _, b := range t.backends {
		if b == nil {
			continue
		}

		wg.Add(1)
		go func(b *telnetBackend) {
			defer wg.Done()

			var err error
			sendOk := false
			for i := 0; i < b.Retry; i++ {
				err := b.WriteBackend(line)
				if err == nil {
					sendOk = true
					break
				}
				time.Sleep(time.Millisecond * 10)
			}

			if !sendOk {
				logger.Error.Println("send influxdb %s fail: %v", b.Cfg.Location, err)
			}

			return
		}(b)

	}

	wg.Wait()
}

func (t *OpenTSDB) Stop() error {
	atomic.StoreInt64(&t.closing, 1)
	return t.ln.Close()
}
