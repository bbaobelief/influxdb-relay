package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"influxdb-relay/common/log"
	v "influxdb-relay/common/version"
	"influxdb-relay/config"
	"influxdb-relay/relay"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
)

var (
	cfg     = flag.String("config", "", "Configuration file to use")
	version = flag.Bool("version", false, "show version info")
)

func main() {
	flag.Parse()

	if *version {
		v := v.Get()
		marshalled, err := json.MarshalIndent(&v, "", "  ")
		if err != nil {
			fmt.Printf("%v\n", err)
			os.Exit(1)
		}

		fmt.Println(string(marshalled))
		return
	}

	if *cfg == "" {
		log.Error.Println("Missing configuration file")
		flag.PrintDefaults()
		os.Exit(1)
	}

	cfg, err := config.LoadConfigFile(*cfg)
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
		log.Info.Println(http.ListenAndServe(":19096", nil))
	}()

	go func() {
		<-sigChan
		r.Stop()
	}()

	log.Info.Println("INFO starting relays...")
	r.Run()
}
