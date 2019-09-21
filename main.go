package main

import (
	"flag"
	"influxdb-relay/common/log"
	"influxdb-relay/config"
	"influxdb-relay/relay"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
)

var (
	configFile = flag.String("config", "", "Configuration file to use")
)

func main() {
	flag.Parse()

	if *configFile == "" {
		log.Error.Println("Missing configuration file")
		flag.PrintDefaults()
		os.Exit(1)
	}

	cfg, err := config.LoadConfigFile(*configFile)
	if err != nil {
		log.Error.Println("Problem loading config file:", err)
	}

	r, err := relay.New(cfg)
	if err != nil {
		log.Error.Fatal(err)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt)

	go func() {
		log.Info.Println(http.ListenAndServe("localhost:19096", nil))
	}()

	go func() {
		<-sigChan
		r.Stop()
	}()

	log.Info.Println("INFO starting relays...")
	r.Run()
}
