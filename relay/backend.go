package relay

import (
	"fmt"
	"gopkg.in/fatih/pool.v3"
	rlog "influxdb-relay/common/log"
	"influxdb-relay/config"
	"log"
	"net"
	"time"
)

type telnetBackend struct {
	Pool        pool.Pool
	Active      bool
	DialTimeout time.Duration
	Ticker      *time.Ticker
	Cfg         *config.TSDBOutputConfig
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
		Cfg:         cfg,
		Pool:        p,
		Active:      true,
		DialTimeout: dialTimeout,
		Ticker:      time.NewTicker(interval),
	}

	go tb.CheckActive()

	return tb, nil
}

func (t *telnetBackend) CheckActive() {
	for range t.Ticker.C {
		err := t.Ping()
		if err != nil {
			t.Active = false
			rlog.Error.Printf("%s inactive, %s. \n", t.Cfg.Name, err)
		} else {
			t.Active = true
		}
	}
}

func (t *telnetBackend) IsActive() bool {
	return t.Active
}

func (t *telnetBackend) Ping() (err error) {

	fmt.Println("start:",t.Cfg.Location,t.Pool.Len())

	v, err := t.Pool.Get()
	if err != nil {
		fmt.Println("a:",t.Pool.Len(), err)
		return
	}

	_, err = v.Write([]byte("ping \n"))
	if err != nil {
		fmt.Println("b:",t.Pool.Len(), err)
		return
	}

	fmt.Println("get:",t.Pool.Len())


	err = v.Close()

	fmt.Println("end:",t.Pool.Len())

	return
}


func (t *telnetBackend) Len() int { return t.Pool.Len() }

func (t *telnetBackend) Close() { t.Pool.Close() }
