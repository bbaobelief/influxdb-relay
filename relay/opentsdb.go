package relay

import (
	"bufio"
	"fmt"
	"influxdb-relay/config"
	"log"
	"net"
	"sync/atomic"
	"time"
)

const (
	defaultTelnetTimeout = 10 * time.Second
)

type OpentsdbRelay struct {
	addr string
	name string

	schema string

	closing int64
	l       net.Listener
	c       *net.TCPConn

	backends []*telnetBackend
}

func (g *OpentsdbRelay) Name() string {
	if g.name == "" {
		return fmt.Sprintf("%s://%s", g.schema, g.addr)
	}

	return g.name
}

func (t *OpentsdbRelay) Run() error {

	log.Printf("Starting Opentsdb relay %q on %v", t.Name(), t.addr)

	for {
		conn, err := t.l.Accept()
		if err != nil {
			if atomic.LoadInt64(&t.closing) == 0 {
				log.Printf("Error reading packet in relay %q from %v: %v", t.name, conn.RemoteAddr().String(), err)
			} else {
				err = nil
			}
			return err
		}

		log.Printf("A client connected: " + conn.RemoteAddr().String())

		go t.Receive(conn)
	}
}

// 从transfer接收opentsdb协议数据
func (t *OpentsdbRelay) Receive(conn net.Conn) {
	ipAddr := conn.RemoteAddr().String()
	defer func() {
		fmt.Println("disconnected :" + ipAddr)
		_ = conn.Close()
	}()

	reader := bufio.NewReader(conn)
	for {
		message, err := reader.ReadString('\n')
		if err != nil {
			return
		}
		t.Send(message)
	}
}

// 使用opentsdb协议发送数据至后端influxdb
func (t *OpentsdbRelay) Send(s string) {
	for _, b := range t.backends {
		if err := b.post(s); err != nil {
			log.Printf("Error writing points in relay %q to backend %q: %v", t.Name(), b.name, err)
		}
	}
}

func (t *OpentsdbRelay) Stop() error {
	atomic.StoreInt64(&t.closing, 1)
	return t.l.Close()
}

func NewOpentsdbRelay(cfg config.OpentsdbConfig) (Relay, error) {
	t := new(OpentsdbRelay)

	t.addr = cfg.Addr
	t.name = cfg.Name

	t.schema = "tcp"

	listener, err := net.Listen("tcp", t.addr)
	if err != nil {
		return nil, err
	}

	t.l = listener

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

func newTelnetBackend(cfg *config.OpentsdbOutputConfig) (*telnetBackend, error) {
	if cfg.Name == "" {
		cfg.Name = cfg.Location
	}

	timeout := defaultTelnetTimeout
	if cfg.Timeout != "" {
		t, err := time.ParseDuration(cfg.Timeout)
		if err != nil {
			return nil, fmt.Errorf("error parsing Telnet timeout '%v'", err)
		}
		timeout = t
	}

	_, err := net.ResolveTCPAddr("tcp", cfg.Location)
	if err != nil {
		return nil, err
	}

	conn, err := net.DialTimeout("tcp", cfg.Location, timeout)
	if err != nil {
		return nil, err
	}

	return &telnetBackend{
		cli:  conn,
		name: cfg.Name,
	}, nil
}

type telnetBackend struct {
	cli  net.Conn
	name string
}

func (b *telnetBackend) post(data string) error {
	var err error
	_, err = b.cli.Write([]byte(data))
	return err
}
