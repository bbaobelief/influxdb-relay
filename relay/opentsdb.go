package relay

import (
	"bufio"
	"fmt"
	"influxdb-relay/common/log"
	"influxdb-relay/config"
	"io"
	"net"
	"regexp"
	"strings"
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

		line, err := reader.ReadString('\n')
		if err != nil {
			if err != io.EOF {
				log.Info.Printf("ERROR Reading from OpenTSDB connection %v \n", err)
			}
			return
		}

		lineStr := tsdb_to_line(line)
		t.Send(lineStr)
	}
}

// put g1 old generation.gc.count 1567743820 -1.000 jmxport=8888 endpoint=tx1-inf-base-kafka01.us01
// put consul.monitor.count 1567748657 1.000 evnet=consul.serf.snapshot.appendLine.count monitortag=consul_monitor endpoint=tx3-ops-influxdb01.bj project=
// put event.total 1567738055 0.000 endpoint=tx4-cj-base-maidian01.bj = event=cj.link.ua.conn.sample.version project=link.maidian.analysizer
// 转换非标准opentsdb格式
func tsdb_to_line(lineStr string) []byte {

	lineStr = strings.TrimRight(lineStr, "= \r\n")

	re := regexp.MustCompile(`(?P<method>(put)) (?P<metric>(.+)) (?P<ts>(\d+)) (?P<value>(-?\d.?\d*)) (?P<tags>(.*))`)
	match := re.FindStringSubmatch(lineStr)

	items := make(map[string]string)

	for i, name := range re.SubexpNames() {
		if i != 0 && name != "" && len(match) != 0 {
			tagStr := []string{}
			if name == "tags" {
				tags := strings.Split(match[i], " ")
				for _, t := range tags {
					parts := strings.Split(t, "=")
					if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
						log.Warning.Printf("Malformed tag data tag: %s | %s", t, lineStr)
						continue
					}
					tagStr = append(tagStr, t)
				}
			}
			items[name] = match[i]
			items["tags"] = strings.Join(tagStr, " ")
		}
	}

	metricStr := items["metric"]
	metric := strings.Split(metricStr, " ")
	if len(metric) > 1 {
		metricStr = strings.Join(metric, ".")
	}

	if items["method"] != "" && metricStr != "" && items["ts"] != "" && items["value"] != "" {
		itemStr := fmt.Sprintf("%s %s %s %s %s", items["method"], metricStr, items["ts"], items["value"], items["tags"])
		return []byte(itemStr)
	}
	return nil
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
				fmt.Println(err)
				return
			}

			_ = v.Close()
			//log.Info.Printf("write to %s done", b.Name)
		}(b)

	}

	wg.Wait()
}

func (t *OpenTSDB) Stop() error {
	atomic.StoreInt64(&t.closing, 1)
	return t.ln.Close()
}
