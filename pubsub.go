package main

import (
	"awesomeProject4/pack"
	"bufio"
	"encoding/json"
	"errors"
	log "github.com/sirupsen/logrus"
	"io"
	"net"
	"strings"
	"sync"
	"time"
)

type Client struct {
	Id      int
	Ip      string
	msgChan chan *Message
	*bufio.Reader
	*bufio.Writer
	Conn         net.Conn
	lk           sync.RWMutex
	userChannels map[string]*Channel
}

func (c *Client) unpackMsg(readerChan chan []byte, exit chan struct{}) {
	for {
		select {
		case <-exit:
			_, ok := <-exit
			if ok {
				close(exit)
			}
			break
		case msg := <-readerChan:
			command := strings.Split(string(msg[4:]), " ")
			msgType := SysFunc(pack.BytesToInt(msg[:4]))
			if len(command) == 2 {
				command = append(command, "")
			}
			channelName := command[1]
			switch msgType {
			case Subscribe:
				UserServer.Subscribe(c, channelName)
			case Publish:
				timeout := pack.BytesToInt(msg[5:9])
				allQueue.RLock()
				queue, ok := allQueue.queue[channelName]
				allQueue.RUnlock()
				if !ok {
					queue = &Queue{
						ChannelName: channelName,
						MsgChan:     make(chan *Message, 12),
						SaveChan:    make(chan *Message, 12),
						CloseFlag:   make(chan bool),
					}
					go queue.Persistent()
					go queue.Consume()
					allQueue.Lock()
					allQueue.queue[channelName] = queue
					allQueue.Unlock()
				}
				type DeliverMsg struct {
					ConsumeNum int
					NeedAck    bool
					MsgID      int32
					Content    string
				}
				msg := &DeliverMsg{
					ConsumeNum: 0,
					NeedAck:    false,
					MsgID:      0,
					Content:    command[2],
				}
				data, err := json.Marshal(msg)
				if err != nil {
					log.Println("序列化失败")
				}
				queue.AddMsg(&Message{
					MsgType: msgType,
					Timeout: time.Now().Add(time.Second * time.Duration(timeout)).Unix(),
					ID:      GetId(),
					Content: data,
					Client:  c,
				})
			case ACK:

			default:
				log.Warnf("未知的协议号!")
			}

		}
	}
}

func (c *Client) ReadMsg() {
	tmpBuffer := make([]byte, 0)
	readerChannel := make(chan []byte, 16)
	exit := make(chan struct{}, 1)
	go c.unpackMsg(readerChannel, exit)
	for {
		buffer := make([]byte, 1024)
		n, err := c.Read(buffer)
		if err != nil {
			exit <- struct{}{}
			if err == io.EOF {
				log.Errorf("%s 连接关闭 ", c.Ip)
				return
			}
			log.Errorf("读取字节错误 %v", err)
			return
		}
		tmpBuffer = pack.Unpack(append(tmpBuffer, buffer[:n]...), readerChannel)
	}
}

func (c *Client) Close() error {
	return c.Conn.Close()
}

type Server struct {
	Dict map[string]*Channel //map[Channel.Name]*Channel
	sync.RWMutex
}

var UserServer *Server

func init() {
	UserServer = NewServer()
}
func NewServer() *Server {
	s := &Server{}
	s.Dict = make(map[string]*Channel) //所有channel
	return s
}

//订阅
func (srv *Server) Subscribe(client *Client, channelName string) {
	// 客户是否在Channel的客户列表中
	srv.RLock()
	ch, found := srv.Dict[channelName]
	srv.RUnlock()
	if !found {
		ch = NewChannel(channelName)
		ch.AddClient(client)
		srv.Lock()
		srv.Dict[channelName] = ch
		srv.Unlock()
	} else {
		ch.AddClient(client)
	}
	client.lk.RLock()
	if _, ok := client.userChannels[ch.Name]; !ok {
		client.userChannels[ch.Name] = ch
	}
	client.lk.RUnlock()
}

//取消订阅
func (srv *Server) Unsubscribe(client *Client, channelName string) {
	srv.RLock()
	ch, found := srv.Dict[channelName]
	srv.RUnlock()
	if found {
		if ch.DeleteClient(client) == 0 {
			ch.Exit()
			srv.Lock()
			delete(srv.Dict, channelName)
			srv.Unlock()
		}
	}
}

//发布消息
func (srv *Server) PublishMessage(channelName, message string) (bool, error) {
	srv.RLock()
	ch, found := srv.Dict[channelName]
	if !found {
		srv.RUnlock()
		return false, errors.New("channelName不存在!")
	}
	srv.RUnlock()

	ch.Notify(message)
	ch.Wait()
	return true, nil
}
