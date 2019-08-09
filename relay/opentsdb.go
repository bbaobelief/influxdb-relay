package relay

import (
	"bufio"
	"fmt"
	"influxdb-relay/config"
	"log"
	"net"
	"sync/atomic"
)

type OpentsdbRelay struct {
	addr string
	name string

	schema string

	closing int64
	l       net.Listener

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

		go Receive(conn)
	}
}

func Receive(conn net.Conn) {
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
		fmt.Println(string(message))
		//msg := "---> " + string(message) + "\n"
		//b := []byte(msg)
		//conn.Write(b)
		//time.Sleep(time.Duration(3) * time.Second)
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

	listener, err := net.Listen("tcp", "127.0.0.1:14242")
	if err != nil {
		return nil, err
	}

	t.l = listener

	return t, nil
}
