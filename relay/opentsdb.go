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

const (
	//DefaultTimeout      = 10 * time.Second
	DefaultInterval    = 5 * time.Second
	DefaultDialTimeout = 5 * time.Second
	//DefaultBatchSizeKB      = 512

	//KB = 1024
	//MB = 1024 * KB
)

//var (
//	DialTimeout     = 5 * time.Second
//	IdleTimeout     = 60 * time.Second
//	ReadTimeout     = 5 * time.Second
//	WriteTimeout    = 5 * time.Second
//	DeadlineTimeout = 30 * time.Second
//)

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
		return fmt.Sprintf("INFO tcp://%s \n", g.addr)
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

		fmt.Printf("INFO Transfer connected: \n" + conn.RemoteAddr().String())

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
			log.Printf("ERROR Writing points in relay %q to backend %q: %v \n", t.Name(), b.name, err)
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
	name        string
	pool        pool.Pool
	Active      bool
	Location    string
	DialTimeout time.Duration
	Ticker      *time.Ticker
}

func newTelnetBackend(cfg *config.TSDBOutputConfig) (*telnetBackend, error) {
	if cfg.Name == "" {
		cfg.Name = cfg.Location
	}

	interval := DefaultInterval
	if cfg.DelayInterval != "" {
		i, err := time.ParseDuration(cfg.DelayInterval)
		if err != nil {
			return nil, fmt.Errorf("error parsing tcp delay interval '%v' \n", err)
		}
		interval = i
	}

	dialTimeout := DefaultDialTimeout
	if cfg.DialTimeout != "" {
		d, err := time.ParseDuration(cfg.DialTimeout)
		if err != nil {
			return nil, fmt.Errorf("error parsing tcp dial timeout '%v' \n", err)
		}
		dialTimeout = d
	}

	factory := func() (net.Conn, error) { return net.Dial("tcp", cfg.Location) }

	p, err := pool.NewChannelPool(cfg.InitCap, cfg.MaxCap, factory)

	if err != nil {
		log.Fatalf("ERROR Creating InfluxDB Client: %s \n", err)
	}

	tb := &telnetBackend{
		name:        cfg.Name,
		pool:        p,
		Active:      true,
		Location:    cfg.Location,
		DialTimeout: dialTimeout,
		Ticker:      time.NewTicker(interval),
	}

	go tb.CheckActive()

	return tb, nil
}

func (b *telnetBackend) post(data []byte) error {
	//fmt.Printf("%s: %d %s\n", b.name, b.pool.Len(), data)
	v, err := b.pool.Get()
	if err != nil {
		return err
	}

	if b.IsActive() {

		conn := v.(net.Conn)
		_, err = conn.Write(data)
		if err != nil {
			return err
		}

		_ = v.Close()
	}
	return err
}

func (t *telnetBackend) CheckActive() {
	for range t.Ticker.C {
		_, err := t.Ping()
		if err != nil {
			t.Active = false
			log.Printf("%s inactive. \n", t.name)
		} else {
			t.Active = true
		}
	}
}

func (t *telnetBackend) IsActive() bool {
	return t.Active
}

func (t *telnetBackend) Ping() (version string, err error) {

	conn, err := net.DialTimeout("tcp", t.Location, t.DialTimeout)
	if err != nil {
		log.Println("tcp ping error: \n", err)
		return
	}

	defer conn.Close()

	return
}
