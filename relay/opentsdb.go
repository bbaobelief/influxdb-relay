package relay

import (
	"bufio"
	"bytes"
	"fmt"
	nlist "github.com/toolkits/container/list"
	"influxdb-relay/common/gpool"
	logger "influxdb-relay/common/log"
	"influxdb-relay/config"
	"io"
	"net"
	"net/textproto"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

var (
	SenderQueue = nlist.NewSafeListLimited(DefaultSendQueueMaxSize)
)

const (
	DefaultSendSleepInterval   = time.Millisecond * 50 //默认睡眠间隔为50ms
	DefaultSendQueueMaxSize    = 1024000               //102.4w
	DefaultDefaultBatchMaxSize = 200
)

type OpenTSDB struct {
	addr  string
	name  string
	batch int

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

	t.batch = cfg.Batch
	if cfg.Batch == 0 {
		t.batch = DefaultDefaultBatchMaxSize
	}

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

	wg := sync.WaitGroup{}

	// write
	wg.Add(1)
	go func() { defer wg.Done(); t.sendTask() }()

	// read
	wg.Add(1)
	go func() { defer wg.Done(); t.serveTelnet() }()

	wg.Wait()
	return nil
}

func (t *OpenTSDB) serveTelnet() {
	for {
		conn, err := t.ln.Accept()
		if opErr, ok := err.(*net.OpError); ok && !opErr.Temporary() {
			logger.Info.Println("INFO Opentsdb tcp listener closed")
			return
		} else if err != nil {
			logger.Error.Println("ERROR accepting OpenTSDB", err)
			continue
		}

		go t.handleTelnetConn(conn)
	}
}

func (t *OpenTSDB) handleTelnetConn(conn net.Conn) {
	defer conn.Close()

	r := textproto.NewReader(bufio.NewReader(conn))
	for {
		line, err := r.ReadLine()
		if err != nil {
			if err != io.EOF {
				logger.Info.Println("Error reading from OpenTSDB connection", err)
			}

			return
		}

		isSuccess := SenderQueue.PushFront(line)
		if !isSuccess {
			logger.Error.Printf("ERROR sender queue overflow: %d \n", (DefaultSendQueueMaxSize - SenderQueue.Len()))
		}
	}

}
func (t *OpenTSDB) sendTask() {
	pool := gpool.New(1000)
	println(runtime.NumGoroutine())

	for {

		if atomic.LoadInt64(&t.closing) == 1 {
			break
		}

		items := SenderQueue.PopBackBy(t.batch)
		count := len(items)

		fmt.Printf("len: %d, count: %d \n", SenderQueue.Len(), count)

		if count == 0 {
			time.Sleep(DefaultSendSleepInterval)
			continue
		}

		var tsdbBuffer bytes.Buffer
		for i := 0; i < count; i++ {
			tsdbItem := items[i].(string)
			tsdbBuffer.WriteString(tsdbItem)
			tsdbBuffer.WriteString("\n")
		}

		pool.Add(1)
		go func(batchItems []byte) {
			// Send multiple backend
			for _, b := range t.backends {
				if b == nil {
					continue
				}
				// Retry write
				Send(b, batchItems)
			}

			println(runtime.NumGoroutine())
			pool.Done()
		}(tsdbBuffer.Bytes())
	}

	pool.Wait()
}

func Send(b *telnetBackend, line []byte) {

	var err error
	sendOk := false
	for i := 0; i < b.Cfg.Retry; i++ {
		err := b.WriteBackend(line)
		if err == nil {
			sendOk = true
			break
		}
		time.Sleep(time.Millisecond * 10)
	}

	if !sendOk {
		logger.Error.Println("ERROR send influxdb %s fail: %v", b.Cfg.Location, err)
	}
}

func (t *OpenTSDB) Stop() error {
	atomic.StoreInt64(&t.closing, 1)
	return t.ln.Close()
}
