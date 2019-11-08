package relay

import (
	"gopkg.in/fatih/pool.v3"
	"influxdb-relay/common/rlog"
	"influxdb-relay/config"
	"net"
	"time"
)

type telnetBackend struct {
	Pool    pool.Pool
	Timeout time.Duration
	Cfg     *config.TSDBOutputConfig
}

func newTelnetBackend(cfg *config.TSDBOutputConfig) (*telnetBackend, error) {
	if cfg.Name == "" {
		cfg.Name = cfg.Location
	}

	cfg.Retry = DefaultRetry
	if cfg.Retry == 0 {
		cfg.Retry = DefaultRetry
	}

	timeout := DefaultTCPTimeout
	if cfg.Timeout != "" {
		t, err := time.ParseDuration(cfg.Timeout)
		if err != nil {
			rlog.Logger.Fatalf("error parsing TCP timeout '%v' \n", err)
		}
		timeout = t
	}

	factory := func() (net.Conn, error) { return net.DialTimeout("tcp", cfg.Location, timeout) }

	p, err := pool.NewChannelPool(cfg.InitCap, cfg.MaxCap, factory)
	if err != nil {
		rlog.Logger.Fatalf("Creating InfluxDB Client: %s \n", err)
	}

	tb := &telnetBackend{
		Cfg:     cfg,
		Pool:    p,
		Timeout: timeout,
	}

	return tb, nil
}

func (t *telnetBackend) Len() int { return t.Pool.Len() }

func (t *telnetBackend) WriteBackend(b []byte) (err error) {

	conn, err := t.Pool.Get()
	if err != nil {
		return
	}

	//err = conn.SetWriteDeadline(time.Now().Add(t.Timeout))

	_, err = conn.Write(b)
	if err != nil {
		//rlog.Logger.Errorf("%s: %s", t.Cfg.Location, err)

		if pc, ok := conn.(*pool.PoolConn); ok {
			pc.MarkUnusable()
			_ = pc.Close()
		}

		return
	}

	// put
	conn.Close()

	return
}
