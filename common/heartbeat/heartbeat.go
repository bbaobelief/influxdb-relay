package heartbeat

import (
	"influxdb-relay/common/log"
	"net"
	"time"
)

func HeartBeating(conn net.Conn, message string, timeout time.Duration) {

	ch := make(chan string)
	go func() {
		ch <- message
	}()

	select {

	case <-ch:
		_ = conn.SetReadDeadline(time.Now().Add(time.Duration(timeout)))
		break
	case <-time.After(time.Second * 5):
		log.Warning.Println("WARN The heart stops beating...")
		_ = conn.Close()
	}
}
