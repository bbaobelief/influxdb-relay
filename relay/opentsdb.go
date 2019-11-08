package relay

import (
	"bufio"
	"bytes"
	"fmt"
	nlist "github.com/toolkits/container/list"
	"influxdb-relay/common/gpool"
	"influxdb-relay/common/rlog"
	"influxdb-relay/config"
	"io"
	"net"
	"net/textproto"
	"sync"
	"sync/atomic"
	"time"
)

var (
	SenderQueue = nlist.NewSafeListLimited(DefaultSendQueueMaxSize)
)

const (
	DefaultSendSleepInterval = 50 * time.Millisecond //默认睡眠间隔为50ms
	DefaultTCPTimeout        = 10 * time.Second      //默认超时时间为10s
	DefaultSendQueueMaxSize  = 1024000               //102.4w
	DefaultBatchMaxSize      = 200
	DefaultConcurrentMaxSize = 100
	DefaultRetry             = 3
)

type OpenTSDB struct {
	addr       string
	name       string
	batch      int
	concurrent int

	closing int64
	ln      net.Listener

	backends []*telnetBackend
}

func (t *OpenTSDB) Name() string {
	if t.name == "" {
		return fmt.Sprintf("INFO tcp://%s \n", t.addr)
	}

	return t.name
}

func NewTSDBRelay(cfg config.TSDBonfig) (Relay, error) {
	t := new(OpenTSDB)

	t.addr = cfg.Addr
	t.name = cfg.Name

	t.batch = cfg.Batch
	if cfg.Batch == 0 {
		t.batch = DefaultBatchMaxSize
	}

	t.concurrent = cfg.Concurrent
	if cfg.Concurrent == 0 {
		t.concurrent = DefaultConcurrentMaxSize
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
	rlog.Logger.Infof("Starting opentsdb relay %q on %v \n", t.Name(), t.addr)

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
			rlog.Logger.Notice("Opentsdb tcp listener closed")
			return
		} else if err != nil {
			rlog.Logger.Errorf("Accepting OpenTSDB", err)
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
				rlog.Logger.Warningf("Reading from OpenTSDB connection", err)
			}

			return
		}

		isSuccess := SenderQueue.PushFront(line)
		if !isSuccess {
			rlog.Logger.Critical(line)
		}
	}
}

func (t *OpenTSDB) sendTask() {
	pool := gpool.New(t.concurrent)

	for {

		if atomic.LoadInt64(&t.closing) == 1 {
			break
		}

		items := SenderQueue.PopBackBy(t.batch)
		count := len(items)

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
		startTime := time.Now()
		go func(batchItems []byte) {
			// Send multiple backend
			for _, b := range t.backends {
				if b == nil {
					continue
				}
				// Retry write
				Send(b, batchItems)

				endTime := time.Now()
				sub := endTime.Sub(startTime)
				rlog.Logger.Debugf("SenderQueue %s |len: %6d |put: %6d |time: %12d |\n", b.Cfg.Name, SenderQueue.Len(), count, sub.Nanoseconds())
			}
			pool.Done()
		}(tsdbBuffer.Bytes())
	}

	pool.Wait()
}

func Send(b *telnetBackend, line []byte) {

	sendOk := false
	for i := 0; i < b.Cfg.Retry; i++ {
		err := b.WriteBackend(line)
		if err == nil {
			sendOk = true
			break
		}
		time.Sleep(time.Millisecond * 10)
		rlog.Logger.Noticef("Write failed retry %s, %s", b.Cfg.Location, err)
	}

	if !sendOk {
		rlog.Logger.Error("Send influxdb %s fail", b.Cfg.Location)
	}
}

func (t *OpenTSDB) Stop() error {
	atomic.StoreInt64(&t.closing, 1)
	return t.ln.Close()
}
