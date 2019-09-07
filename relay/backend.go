package relay

import (
	"fmt"
	"gopkg.in/fatih/pool.v3"
	"influxdb-relay/config"
	"log"
	"net"
	"time"
)

type telnetBackend struct {
	Name        string
	Pool        pool.Pool
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
		Name:        cfg.Name,
		Pool:        p,
		Active:      true,
		Location:    cfg.Location,
		DialTimeout: dialTimeout,
		Ticker:      time.NewTicker(interval),
	}

	go tb.CheckActive()

	return tb, nil
}

func (t *telnetBackend) CheckActive() {
	for range t.Ticker.C {
		_, err := t.Ping()
		if err != nil {
			t.Active = false
			log.Printf("%s inactive. \n", t.Name)
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
		return
	}

	defer conn.Close()

	return
}
