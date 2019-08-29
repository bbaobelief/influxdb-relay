package relay

import (
	"bufio"
	"fmt"
	"strings"

	//"influxdb-relay/common/heartbeat"
	"influxdb-relay/common/log"
	"influxdb-relay/common/pool"
	"influxdb-relay/config"
	"net"
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

type OpentsdbRelay struct {
	addr     string
	name     string
	protocol string

	closing int64
	ln      net.Listener

	backends []*telnetBackend
}

func (g *OpentsdbRelay) Name() string {
	if g.name == "" {
		return fmt.Sprintf("INFO tcp://%s", g.addr)
	}

	return g.name
}

func NewOpentsdbRelay(cfg config.OpentsdbConfig) (Relay, error) {
	t := new(OpentsdbRelay)

	t.addr = cfg.Addr
	t.name = cfg.Name

	ln, err := net.Listen("tcp", t.addr)
	if err != nil {
		return nil, err
	}

	t.ln = ln

	// 连接influxdb后端
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

func (t *OpentsdbRelay) Run() error {
	log.Info.Printf("INFO Starting opentsdb relay %q on %v", t.Name(), t.addr)
	for {
		conn, err := t.ln.Accept()
		if err != nil {
			if atomic.LoadInt64(&t.closing) == 0 {
				log.Error.Printf("ERROR Reading packet in relay %q from %v: %v", t.name, conn.RemoteAddr().String(), err)
			} else {
				err = nil
			}
			return err
		}

		//conn.SetKeepAlive(true)
		//conn.SetKeepAlivePeriod(15 * time.Second)

		log.Info.Printf("INFO Transfer connected: " + conn.RemoteAddr().String())
		go t.Receive(conn)
	}
}

// 从transfer接收opentsdb协议数据
func (t *OpentsdbRelay) Receive(conn net.Conn) {
	ipAddr := conn.RemoteAddr().String()
	defer func() {
		log.Warning.Printf("WARN Transfer disconnected: %s", ipAddr)
		_ = conn.Close()
	}()

	//messnager := make(chan byte)
	reader := bufio.NewReader(conn)
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			return
		}

		//line := strings.TrimSpace(string(buf))
		b := strings.ContainsRune(string(line), '\n')
		fmt.Println(b) // false
		//fmt.Printf("%#v", line)


		// 发送数据至influxdb backend
		t.Send(line)

		// 心跳计时
		// heartbeat.HeartBeating(conn, message, DeadlineTimeout)

	}
}

// 使用opentsdb协议发送数据至后端influxdb
func (t *OpentsdbRelay) Send(data []byte) {
	for _, b := range t.backends {

		if err := b.post(data); err != nil {
			log.Error.Printf("ERROR Writing points in relay %q to backend %q: %v", t.Name(), b.name, err)
		}
	}
}

func (t *OpentsdbRelay) Stop() error {
	atomic.StoreInt64(&t.closing, 1)
	return t.ln.Close()
}

// todo backend
type telnetBackend struct {
	name string
	pool *pool.TCPPool
}

func newTelnetBackend(cfg *config.OpentsdbOutputConfig) (*telnetBackend, error) {
	if cfg.Name == "" {
		cfg.Name = cfg.Location
	}

	if cfg.IdleTimeout != "" {
		t, err := time.ParseDuration(cfg.IdleTimeout)
		if err != nil {
			return nil, fmt.Errorf("ERROR Parsing Telnet timeout '%v'", err)
		}
		IdleTimeout = t
	}

	options := &pool.Options{
		InitTargets:  []string{cfg.Location},
		InitCap:      cfg.InitCap,
		MaxCap:       cfg.MaxCap,
		DialTimeout:  DialTimeout,
		IdleTimeout:  IdleTimeout,
		ReadTimeout:  ReadTimeout,
		WriteTimeout: WriteTimeout,
	}

	p, err := pool.NewTCPPool(options)

	if err != nil {
		log.Error.Fatalf("ERROR %s %s\n", cfg.Name, err)
	}

	return &telnetBackend{
		name: cfg.Name,
		pool: p,
	}, nil
}

func (b *telnetBackend) post(data []byte) error {
	// fmt.Printf("%s: %d %s\n", b.name, b.pool.Len(), data)
	v, err := b.pool.Get()
	if err != nil {
		return err
	}

	conn := v.(net.Conn)
	_, err = conn.Write(data)
	if err != nil {
		return err
	}

	err = b.pool.Put(v)
	return err
}
