package relay

import (
	"bufio"
	"fmt"
	"influxdb-relay/config"
	"log"
	"net"
	"sync/atomic"
	"time"

)

type OpentsdbRelay struct {
	addr string
	name string

	schema string

	closing int64
	l       *net.TCPListener
	//c       *net.TCPConn

	//enableMetering bool

	//dropUnauthorized bool

	//backends []*opentsdbBackend
}

func (g *OpentsdbRelay) Name() string {
	if g.name == "" {
		return fmt.Sprintf("%s://%s", g.schema, g.addr)
	}

	return g.name
}

func (t *OpentsdbRelay) Run() error {

	var tcpAddr *net.TCPAddr

	tcpAddr, _ = net.ResolveTCPAddr("tcp", t.addr)
	listener, _ := net.ListenTCP("tcp", tcpAddr)

	log.Printf("Starting Opentsdb relay %q on %v", t.Name(), t.addr)

	t.l = listener

	//defer listener.Close()

	//queue := make(chan string, 1024)
	//go func() {
	//	for p := range queue {
	//		//u.post(&p)
	//		fmt.Println(p)
	//		//wg.Done()
	//	}
	//}()

	for {
		conn, err := listener.AcceptTCP()
		if err != nil {
			continue
		}

		log.Printf("Opentsdb connected: %s", conn.RemoteAddr().String())

		//go Receive(conn, &wg)

		func(conn *net.TCPConn) {
			//defer conn.Close()

			fmt.Println(66666666666)
			for {

				reader := bufio.NewReader(conn)
				content, err := reader.ReadString('\n')
				//
				//buf := make([]byte, 512)
				//content, err := conn.Read(buf)

				if err != nil {
					log.Printf("Listener: Read error: %s", err)
				}

				log.Printf("Listener: Received content: %v\n", content)
				time.Sleep(time.Duration(1) * time.Second)

				//queue <- content

			}
		}(conn)

	}
}

//func Receive(conn *net.TCPConn, wg *sync.WaitGroup) {
//	defer wg.Done()
//	//defer conn.Close()
//
//	reader := bufio.NewReader(conn)
//
//	for {
//		msg, err := reader.ReadString('\n')
//		if err != nil {
//			return
//		}
//		fmt.Println(string(msg))
//	}
//}

func (o *OpentsdbRelay) Stop() error {
	atomic.StoreInt64(&o.closing, 1)
	return o.l.Close()
}

func NewOpentsdbRelay(cfg config.OpentsdbConfig) (Relay, error) {
	t := new(OpentsdbRelay)

	t.addr = cfg.Addr
	t.name = cfg.Name

	t.schema = "tcp"
	//if g.cert != "" {
	//	g.schema = "https"
	//}


	//for i := range cfg.Outputs {
	//	backend, err := NewOpentsdbBackend(&cfg.Outputs[i])
	//	if err != nil {
	//		return nil, err
	//	}
	//
	//	g.backends = append(g.backends, backend)
	//}

	return t, nil
}

//func NewOpentsdbBackend(cfg *config.OpentsdbOutputConfig) (*OpentsdbBackend, error) {
//	if cfg.Name == "" {
//		cfg.Name = cfg.Location
//	}
//
//	return &opentsdbBackend{
//		name:     cfg.Name,
//		location: cfg.Location,
//	}, nil
//}

//type opentsdbBackend struct {
//	name string
//	location string
//}
//
//func (g *OpentsdbRelay) ServeHTTP(w http.ResponseWriter, r *http.Request) {
//	start := time.Now()
//
//	opentsdbServers := make([]string, len(g.backends))
//	for i := range g.backends {
//		opentsdbServers = append(opentsdbServers, g.backends[i].location)
//	}
//	opentsdbClient := &opentsdb.Opentsdb{
//		Servers: opentsdbServers,
//		Prefix:  "bucky",
//	}
//
//	conErr := opentsdbClient.Connect()
//	if conErr != nil {
//		jsonError(w, http.StatusInternalServerError, "unable to connect to opentsdb")
//		log.Fatalf("Could not connect to opentsdb: %s", conErr)
//	}
//
//	queryParams := r.URL.Query()
//
//	// if r.URL.Path == "/metrics" {
//	// 	resp := ""
//	// 	t := time.Now().UnixNano()
//	// 	mu.RLock()
//	// 	for orgID, machineMap := range metering {
//	// 		for machineID, samples := range machineMap {
//	// 			resp += fmt.Sprintf("gocky_samples_total{relay=opentsdb,orgID=%s,machineID=%s} %d %d\n", orgID, machineID, samples, t)
//	// 		}
//	// 	}
//	// 	mu.RUnlock()
//	// 	io.WriteString(w, resp)
//	// 	return
//	// }
//
//	if r.URL.Path != "/write" {
//		jsonError(w, 204, "Dummy response for db creation")
//		return
//	}
//
//	var body = r.Body
//	if r.Header.Get("Content-Encoding") == "gzip" {
//		b, err := gzip.NewReader(r.Body)
//		if err != nil {
//			jsonError(w, http.StatusBadRequest, "unable to decode gzip body")
//		}
//		defer b.Close()
//		body = b
//	}
//
//	bodyBuf := getBuf()
//	_, err := bodyBuf.ReadFrom(body)
//	if err != nil {
//		putBuf(bodyBuf)
//		jsonError(w, http.StatusInternalServerError, "problem reading request body")
//		return
//	}
//
//	precision := queryParams.Get("precision")
//	points, err := models.ParsePointsWithPrecision(bodyBuf.Bytes(), start, precision)
//	if err != nil {
//		putBuf(bodyBuf)
//		jsonError(w, http.StatusBadRequest, "unable to parse points")
//		return
//	}
//
//	machineID := ""
//	if r.Header["X-Gocky-Tag-Machine-Id"] != nil {
//		machineID = r.Header["X-Gocky-Tag-Machine-Id"][0]
//	} else {
//		if g.dropUnauthorized {
//			log.Println("Gocky Headers are missing. Dropping packages...")
//			jsonError(w, http.StatusForbidden, "cannot find Gocky headers")
//			return
//		}
//	}
//
//	sourceType := "unix"
//
//	if r.Header["X-Gocky-Tag-Source-Type"][0] == "windows" {
//		sourceType = "windows"
//	}
//
//	go pushToOpentsdb(points, opentsdbClient, machineID, sourceType)
//
//	if g.enableMetering {
//		orgID := "Unauthorized"
//		if r.Header["X-Gocky-Tag-Org-Id"] != nil {
//			orgID = r.Header["X-Gocky-Tag-Org-Id"][0]
//		}
//		machineID := ""
//		if r.Header["X-Gocky-Tag-Machine-Id"] != nil {
//			machineID = r.Header["X-Gocky-Tag-Machine-Id"][0]
//		}
//
//		mu.Lock()
//
//		_, orgExists := metering[orgID]
//		if !orgExists {
//			metering[orgID] = make(map[string]int)
//		}
//
//		_, machExists := metering[orgID][machineID]
//		if !machExists {
//			metering[orgID][machineID] = len(points)
//		} else {
//			metering[orgID][machineID] += len(points)
//		}
//
//		mu.Unlock()
//	}
//
//	// telegraf expects a 204 response on write
//	w.WriteHeader(204)
//}

//func pushToOpentsdb(points []models.Point, g *opentsdb.Opentsdb, machineID, sourceType string) {
//	for _, p := range points {
//		tags := make(map[string]string)
//		for _, v := range p.Tags() {
//			tags[string(v.Key)] = string(v.Value)
//		}
//		if machineID == "" {
//			machineID = "Unknown." + tags["machine_id"]
//		}
//		tags["machine_id"] = machineID
//		opentsdbMetrics := make([]telegraf.Metric, 0, len(points))
//		fi := p.FieldIterator()
//		for fi.Next() {
//			switch fi.Type() {
//			case models.Float:
//				v, _ := fi.FloatValue()
//				if utf8.ValidString(string(fi.FieldKey())) {
//					switch sourceType {
//					case "windows":
//						grphPoint := OpentsdbWindowsMetric(string(p.Name()), tags, p.UnixNano(), v, string(fi.FieldKey()))
//						if grphPoint != nil {
//							opentsdbMetrics = append(opentsdbMetrics, grphPoint)
//						}
//					default:
//						grphPoint := OpentsdbMetric(string(p.Name()), tags, p.UnixNano(), v, string(fi.FieldKey()))
//						if grphPoint != nil {
//							opentsdbMetrics = append(opentsdbMetrics, grphPoint)
//						}
//					}
//				}
//			case models.Integer:
//				v, _ := fi.IntegerValue()
//				if utf8.ValidString(string(fi.FieldKey())) {
//					switch sourceType {
//					case "windows":
//						grphPoint := OpentsdbWindowsMetric(string(p.Name()), tags, p.UnixNano(), v, string(fi.FieldKey()))
//						if grphPoint != nil {
//							opentsdbMetrics = append(opentsdbMetrics, grphPoint)
//						}
//					default:
//						grphPoint := OpentsdbMetric(string(p.Name()), tags, p.UnixNano(), v, string(fi.FieldKey()))
//						if grphPoint != nil {
//							opentsdbMetrics = append(opentsdbMetrics, grphPoint)
//						}
//					}
//				}
//				// case models.String:
//				// 	log.Println("String values not supported")
//				// case models.Boolean:
//				// 	log.Println("Boolean values not supported")
//				// case models.Empty:
//				// 	log.Println("Empry values not supported")
//				// default:
//				// 	log.Println("Unknown value type")
//			}
//		}
//
//		err := g.Write(opentsdbMetrics)
//		if err != nil {
//			log.Println(err)
//		}
//	}
//
//}
