package main

import (
	"bufio"
	"fmt"
	log "github.com/sirupsen/logrus"
	"net"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"time"
)

type IServer interface {
	Close() error
}

type ClientManger struct {
	Clients map[string]*Client
}

func (c *ClientManger) Close() error {
	for _, Client := range c.Clients {
		err := Client.Close()
		if err != nil {
			log.Errorf("关闭客户端  %s 错误 %v!", Client.Ip, err.Error())
			continue
		}
	}
	return nil
}

var waitGroup *WaitGroupWrapper
var Qserver *QServer
var allQueue *Subscription

type QServer struct {
	clientManger *ClientManger
	IsClose      int32
	sync.RWMutex
}

func init() {
	clientManger := &ClientManger{Clients: make(map[string]*Client)}
	Qserver = &QServer{
		clientManger: clientManger,
		RWMutex:      sync.RWMutex{},
	}
	allQueue = &Subscription{queue: make(map[string]*Queue)}

}

func (q *QServer) StartServer(ip, port string) error {
	quit := make(chan os.Signal) // 接收系统中断信号
	signal.Notify(quit, os.Kill, os.Interrupt)
	waitGroup = &WaitGroupWrapper{
		WaitGroup: sync.WaitGroup{},
	}
	listen, err := net.Listen("tcp", fmt.Sprintf("%s:%s", ip, port))
	if err != nil {
		log.Errorf("Server Start Error %v!", err)
		return err
	}
	go func() {
		<-quit
		log.Warn("Get Stop Command. Now Stoping...")
		q.clientManger.Close()
		if err := listen.Close(); err != nil {
			log.Error(err)
		}
		atomic.AddInt32(&q.IsClose, 1)
		return
	}()
	defer func() {
		for atomic.LoadInt32(&q.IsClose) == 0 {
			time.Sleep(time.Second * 1)
		}
	}()
	for {
		conn, err := listen.Accept()
		if err != nil {
			log.Errorf("Accept Conn Error %v!", err)
			break
		}
		waitGroup.Wrap(func() {
			HandlerConn(q, conn)
		})
	}
	waitGroup.Wait()
	return nil
}

func HandlerConn(q *QServer, conn net.Conn) {
	defer func() {
		if err := recover(); err != nil {
			log.Errorf("出现错误%v", err)
		}
	}()
	log.Infof("监听到一个链接 %s", conn.RemoteAddr())
	client, ok := q.clientManger.Clients[conn.RemoteAddr().String()]
	if !ok {
		client = &Client{
			Id:           len(q.clientManger.Clients) + 1,
			Ip:           conn.RemoteAddr().String(),
			Conn:         conn,
			Writer:       bufio.NewWriter(conn),
			Reader:       bufio.NewReader(conn),
			userChannels: make(map[string]*Channel),
		}
		q.clientManger.Clients[client.Ip] = client
		go client.ReadMsg()
	}

}
