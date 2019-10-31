package relay

import (
	"gopkg.in/fatih/pool.v3"
	"influxdb-relay/common/rlog"
	"influxdb-relay/config"
	"net"
)

const (
	DefaultRetry = 3
)

type telnetBackend struct {
	Pool pool.Pool
	Cfg  *config.TSDBOutputConfig
}

func newTelnetBackend(cfg *config.TSDBOutputConfig) (*telnetBackend, error) {
	if cfg.Name == "" {
		cfg.Name = cfg.Location
	}

	cfg.Retry = DefaultRetry
	if cfg.Retry == 0 {
		cfg.Retry = DefaultRetry
	}

	factory := func() (net.Conn, error) { return net.Dial("tcp", cfg.Location) }

	p, err := pool.NewChannelPool(cfg.InitCap, cfg.MaxCap, factory)
	if err != nil {
		rlog.Logger.Fatalf("Creating InfluxDB Client: %s \n", err)
	}

	tb := &telnetBackend{
		Cfg:  cfg,
		Pool: p,
	}

	return tb, nil
}

func (t *telnetBackend) Len() int { return t.Pool.Len() }

func (t *telnetBackend) WriteBackend(b []byte) (err error) {

	conn, err := t.Pool.Get()
	if err != nil {
		return
	}

	_, err = conn.Write(b)
	if err != nil {
		rlog.Logger.Errorf("%s: %s", t.Cfg.Location, err)

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
