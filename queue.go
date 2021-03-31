package main

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/syndtr/goleveldb/leveldb"
	"runtime"
	"sync"
)

type Subscription struct {
	sync.RWMutex
	queue map[string]*Queue
	max   uint64
}

type Queue struct {
	ChannelName string
	MsgChan     chan *Message
	SaveChan    chan *Message
	db          *leveldb.DB
	CloseFlag   chan bool
}

type Message struct {
	MsgType    SysFunc //消息类型
	ID         int64
	Version    []byte
	Key        []byte
	delivered  []string
	Content    []byte
	Timeout    int64
	maxConsume int
	Client     *Client
}

func (q *Queue) AddMsg(msg *Message) {
	q.SaveChan <- msg
}

func (q *Queue) Close() error {
	close(q.MsgChan)
	close(q.SaveChan)
	return q.db.Close()
}

func (q *Queue) Persistent() {
	for {
		select {
		case <-q.CloseFlag:
			log.Info("队列被关闭!")
			return
		case msg := <-q.SaveChan:
			log.Infof("缓存消息到本地 %v", msg)
			//向db中插入键值对
			err := levelDb.Put([]byte(fmt.Sprintf("%s:%s:%d", q.ChannelName, msg.Key, msg.ID)), msg.Content, nil)
			if err != nil {
				log.Errorf("保存数据错误 %v", err.Error())
			}
			q.MsgChan <- msg
		}
	}
}

func (q *Queue) Consume() {
	for {
		select {
		case q_msg := <-q.MsgChan:
			switch q_msg.MsgType {
			case Publish:
				_, err := UserServer.PublishMessage(q.ChannelName, string(q_msg.Content))
				if err != nil {
					log.Errorf("错误 %v", err)
					continue
				}
				if q_msg.maxConsume <= 0 {
					q.DeleMsg(q_msg)
				}
			default:
				log.Warnf("未知的功能号 %d!", q_msg.MsgType)
			}
		default:
			iter := levelDb.NewIterator(nil, nil)
			for iter.Next() {
				key := iter.Key()
				value := iter.Value()
				log.Info(key, value)
				levelDb.Delete(key, nil)
			}
			iter.Release()
			err := iter.Error()
			if err != nil {
				log.Errorf("扫描错误 %v!")
			}
			//从 leveldb 扫描 消息
			runtime.Gosched()
		}
	}
}

func (q *Queue) ConsumeMsg() *Message {
	return <-q.MsgChan
}

func (q *Queue) DeleMsg(msg *Message) error {
	return levelDb.Delete([]byte(fmt.Sprintf("%s:%s", q.ChannelName, msg.Key, msg.ID)), nil)
}
