package main

import (
	"awesomeProject4/pack"
	"fmt"
	log "github.com/sirupsen/logrus"
	"net"
	"os"
	"time"
)

type SysFunc int

const (
	Publish   SysFunc = 1
	Subscribe SysFunc = 2
)

type SubscribeC struct {
	Channel string
}

func NewSubscribeContent(channel string) *SubscribeC {
	return &SubscribeC{Channel: channel}
}

func (s *SubscribeC) Packet() []byte {
	p1 := append(pack.IntToBytes(int(Subscribe)), ' ')
	p1 = append(p1, []byte(s.Channel)...)
	p1 = append(p1, ' ')
	p1 = append(p1, '.')
	return pack.Packet(p1)

}

func sender(conn net.Conn) {
	conn.Write(NewSubscribeContent("1").Packet())
	go func() {
		buff := make([]byte, 1024)
		for {
			conn.Read(buff)
			log.Infof("收到订阅消息:%s", string(buff))
			//switch buff[0] {
			//case '0':
			//	fmt.Println("asdasd")
			//}
		}

	}()

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
