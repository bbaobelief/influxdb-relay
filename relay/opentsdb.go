package relay

import (
	"bufio"
	"fmt"
	"influxdb-relay/common/log"
	"influxdb-relay/config"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

const (
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
	log.Info.Printf("INFO Starting opentsdb relay %q on %v \n", t.Name(), t.addr)

	for {
		conn, err := t.ln.Accept()
		if err != nil {
			return err
		}

		log.Info.Printf("INFO Transfer connected: %v \n", conn.RemoteAddr().String())

		go t.handleConn(conn)
	}
}

func (t *OpenTSDB) handleConn(conn net.Conn) {
	ipAddr := conn.RemoteAddr().String()

	defer func() {
		log.Warning.Printf("WARN Transfer disconnected: %s", ipAddr)
		_ = conn.Close()
	}()

	reader := bufio.NewReader(conn)
	for {

		line, err := reader.ReadBytes('\n')
		if err != nil {
			if err != io.EOF {
				log.Info.Printf("ERROR Reading from OpenTSDB connection %v \n", err)
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
		if !b.Active || b == nil {
			continue
		}
		wg.Add(1)
		go func(b *telnetBackend) {
			defer wg.Done()

			v, err := b.Pool.Get()
			if err != nil {
				return
			}

			conn := v.(net.Conn)
			_, err = conn.Write(line)
			if err != nil {
				log.Warning.Println(err)
				return
			}


			fmt.Println(b.Pool.Len())
			_ = b.Pool.Close
			_ = v.Close()
			fmt.Println(b.Pool.Len())
			//log.Info.Printf("write to %s done", b.Name)
		}(b)

	}

	wg.Wait()
}

func (t *OpenTSDB) Stop() error {
	atomic.StoreInt64(&t.closing, 1)
	return t.ln.Close()
}
