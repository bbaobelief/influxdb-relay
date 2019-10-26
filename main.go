package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"influxdb-relay/common/log"
	"influxdb-relay/config"
	"influxdb-relay/relay"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
)

var (
	cfg     = flag.String("config", "", "Configuration file to use")
	version = flag.Bool("version", false, "show version info")

	gitTag       string = ""
	gitCommit    string = "$Format:%H$"
	gitTreeState string = "not a git tree"
	buildDate    string = "1970-01-01T00:00:00Z"
)

type Info struct {
	GitTag       string `json:"gitTag"`
	GitCommit    string `json:"gitCommit"`
	GitTreeState string `json:"gitTreeState"`
	BuildDate    string `json:"buildDate"`
	GoVersion    string `json:"goVersion"`
	Compiler     string `json:"compiler"`
	Platform     string `json:"platform"`
}

func (info Info) String() string {
	return info.GitTag
}

func versionInfo() Info {
	return Info{
		GitTag:       gitTag,
		GitCommit:    gitCommit,
		GitTreeState: gitTreeState,
		BuildDate:    buildDate,
		GoVersion:    runtime.Version(),
		Compiler:     runtime.Compiler,
		Platform:     fmt.Sprintf("%s/%s", runtime.GOOS, runtime.GOARCH),
	}
}

func main() {
	flag.Parse()

	if *version {
		v := versionInfo()
		marshalled, err := json.MarshalIndent(&v, "", "  ")
		if err != nil {
			log.Info.Println(err)
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
