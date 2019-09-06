package relay

import (
	"bufio"
	"bytes"
	"fmt"
	"log"

	//"influxdb-relay/common/log"
	//"influxdb-relay/common/pool"
	"gopkg.in/fatih/pool.v3"
	"influxdb-relay/config"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

var (
	DialTimeout     = 5 * time.Second
	IdleTimeout     = 60 * time.Second
	ReadTimeout     = 5 * time.Second
	WriteTimeout    = 5 * time.Second
	DeadlineTimeout = 30 * time.Second
)

type OpenTSDB struct {
	addr      string
	name      string
	precision string

	closing int64
	ln      net.Listener
	wg      sync.WaitGroup

	backends []*telnetBackend
}

func (g *OpenTSDB) Name() string {
	if g.name == "" {
		return fmt.Sprintf("INFO tcp://%s", g.addr)
	}

	return g.name
}

func NewTSDBRelay(cfg config.TSDBonfig) (Relay, error) {
	t := new(OpenTSDB)

	t.addr = cfg.Addr
	t.name = cfg.Name
	t.precision = cfg.Precision

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
	log.Printf("INFO Starting opentsdb relay %q on %v", t.Name(), t.addr)

	for {
		conn, err := t.ln.Accept()
		if err != nil {
			return err
		}

		fmt.Printf("INFO Transfer connected: " + conn.RemoteAddr().String())

		go t.handleConn(conn)
	}
}

type readerConn struct {
	net.Conn
	r io.Reader
}

func (t *OpenTSDB) handleConn(conn net.Conn) {
	var buf bytes.Buffer
	bufr := bufio.NewReader(io.MultiReader(&buf, conn))
	conn = &readerConn{Conn: conn, r: bufr}

	t.wg.Add(1)
	t.handleTelnetConn(conn)
	t.wg.Done()
}

func (t *OpenTSDB) handleTelnetConn(conn net.Conn) {
	defer conn.Close()

	reader := bufio.NewReader(conn)
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			if err != io.EOF {
				fmt.Println("ERROR reading from OpenTSDB connection", err)
			}
			return
		}

		t.Send(line)
	}
}

func (t *OpenTSDB) Send(data []byte) {
	for _, b := range t.backends {
		if err := b.post(data); err != nil {
			log.Printf("ERROR Writing points in relay %q to backend %q: %v", t.Name(), b.name, err)
		}
	}
}

func (t *OpenTSDB) Stop() error {
	atomic.StoreInt64(&t.closing, 1)
	//t.wg.Wait()
	return t.ln.Close()
}

// todo backend
type telnetBackend struct {
	name string
	pool pool.Pool
}

func newTelnetBackend(cfg *config.TSDBOutputConfig) (*telnetBackend, error) {
	if cfg.Name == "" {
		cfg.Name = cfg.Location
	}

	factory := func() (net.Conn, error) { return net.Dial("tcp", cfg.Location) }

	p, err := pool.NewChannelPool(5, 30, factory)

	if err != nil {
		log.Fatalf("ERROR Creating InfluxDB Client: %s", err)
	}

	return &telnetBackend{
		name: cfg.Name,
		pool: p,
	}, nil
}

func (b *telnetBackend) post(data []byte) error {
	//fmt.Printf("%s: %d %s\n", b.name, b.pool.Len(), data)
	v, err := b.pool.Get()
	if err != nil {
		return err
	}

	conn := v.(net.Conn)
	_, err = conn.Write(data)
	if err != nil {
		return err
	}

	_ = v.Close()
	return err
}
