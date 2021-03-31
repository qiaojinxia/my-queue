package main

import (
	"awesomeProject4/pack"
	"fmt"
	"net"
	"os"
	"time"
)

type SysFunc int

const (
	Publish   SysFunc = 1
	Subscribe SysFunc = 2
)

type PublishC struct {
	Content       string
	Channel       string
	TimeOut       int64
	DeliverCounts int
}

func NewPublishContent(channel string, content string, timeOut int64) *PublishC {
	return &PublishC{Content: content, Channel: channel, TimeOut: timeOut}
}

func (s *PublishC) Packet() []byte {
	p1 := append(pack.IntToBytes(int(Publish)), ' ')
	p1 = append(p1, []byte(s.Channel)...)
	p1 = append(p1, ' ')
	p1 = append(pack.IntToBytes(int(s.TimeOut)), ' ')
	p1 = append(p1, ' ')
	p1 = append(pack.IntToBytes(int(s.DeliverCounts)), ' ')
	p1 = append(p1, ' ')
	p1 = append(p1, s.Content...)
	return pack.Packet(p1)
}

func sender(conn net.Conn) {
	for i := 0; i < 10; i++ {
		conn.Write(NewPublishContent("1", "caonima", 0).Packet())
	}
	fmt.Println("send over")
}

func main() {
	server := "127.0.0.1:7777"
	tcpAddr, err := net.ResolveTCPAddr("tcp4", server)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error: %s", err.Error())
		os.Exit(1)
	}
	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error: %s", err.Error())
		os.Exit(1)
	}
	defer conn.Close()
	fmt.Println("connect success")
	go sender(conn)
	for {
		time.Sleep(1 * 1e9)
	}
}
