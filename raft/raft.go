package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"

	"log"
)

//对每个节点id和端口的封装类型
type nodeInfo struct {
	id       string
	port     string
	httpPort string
}

type NodeStatus int

const (
	UNINIT    NodeStatus = -1
	FOLLOWER  NodeStatus = 0
	CANDIDATE NodeStatus = 1
	LEADER    NodeStatus = 2
)

//声明节点对象类型Raft
type Raft struct {
	node nodeInfo
	mu   sync.Mutex
	//当前节点编号
	me          int
	currentTerm int
	votedFor    int
	state       NodeStatus

	heartbeatTimeout *time.Timer
	electionTimeout  *time.Timer
	currentLeader    int
	//该节点最后一次处理数据的时间
	lastMessageTime int64

	message   chan bool
	eclectCh  chan bool
	heartbeat chan bool
	//子节点给主节点返回心跳信号
	heartbeatRe chan bool
}

//声明leader对象
type Leader struct {
	//任期
	Term int
	//leader 编号
	LeaderId int
}

//设置节点个数
const raftCount = 2

var leader = Leader{0, -1}

//存储缓存信息
var bufferMessage = make(map[string]string)

//处理数据库信息
var mysqlMessage = make(map[string]string)

//操作消息数组下标
var messageId = 1

//用nodeTable存储每个节点中的键值对
var nodeTable map[string]string

var nodehttpTable map[string]string

func init() {
	nodeTable = make(map[string]string)
	nodeTable["1"] = ":7777"
	nodeTable["2"] = ":7778"
	nodeTable["3"] = ":7779"

	nodehttpTable = make(map[string]string)
	nodehttpTable["1"] = ":8080"
	nodehttpTable["2"] = ":8081"
	nodehttpTable["3"] = ":8082"
}

func main() {
	host := flag.String("h", "127.0.0.1", "the host address of raft server")
	//port := flag.String("p","7777","the port of raft server")
	id := flag.String("id", "1", "the server no")
	flag.Parse()

	//终端接收来的是数组
	if len(os.Args) > 1 {
		//字符串转换整型
		nodeID, _ := strconv.Atoi(*id)
		//封装nodeInfo对象
		node := nodeInfo{id: *host, port: nodeTable[*id], httpPort: nodehttpTable[*id]}
		//创建节点对象
		rf := Make(nodeID)
		//确保每个新建立的节点都有端口对应
		//127.0.0.1:8000
		rf.node = node
		delete(nodeTable, *id)
		//注册rpc
		go func() {
			//注册rpc，为了实现远程链接
			rf.raftRegisterRPC(node.port)
		}()
		go func() {
			//回调方法
			http.HandleFunc("/req", rf.getRequest)
			http.HandleFunc("/getvalues", rf.getKeyValues)
			fmt.Printf("监听Http请求%s", node.httpPort)
			if err := http.ListenAndServe(node.httpPort, nil); err != nil {
				fmt.Println(err)
				return
			}
		}()
	}
	select {}
}

var clientWriter http.ResponseWriter

func (rf *Raft) getRequest(writer http.ResponseWriter, request *http.Request) {
	request.ParseForm()
	if len(request.Form["age"]) > 0 {
		clientWriter = writer
		fmt.Println("主节点广播客户端请求age:", request.Form["age"][0])

		param := Param{Msg: request.Form["age"][0], MsgId: strconv.Itoa(messageId)}
		messageId++
		if leader.LeaderId == rf.me {
			rf.sendMessageToOtherNodes(param)
		} else {
			//将消息转发给leader
			leaderId := nodeTable[strconv.Itoa(leader.LeaderId)]
			//连接远程rpc服务
			rpc, err := rpc.DialHTTP("tcp", "127.0.0.1"+leaderId)
			if err != nil {
				log.Fatal("\nrpc转发连接server错误:", leader.LeaderId, err)
			}
			var bo = false
			//首先给leader传递
			err = rpc.Call("Raft.ForwardingMessage", param, &bo)
			if err != nil {
				log.Fatal("\nrpc转发调用server错误:", leader.LeaderId, err)
			}
		}
	}
}

func (rf *Raft) getKeyValues(writer http.ResponseWriter, request *http.Request) {
	data, err := json.Marshal(bufferMessage)
	if err != nil {
		writer.Write([]byte(err.Error()))
		writer.WriteHeader(200)
		return
	}
	writer.Write(data)
}

func (rf *Raft) sendMessageToOtherNodes(param Param) {
	//存储 log 日志
	bufferMessage[param.MsgId] = param.Msg
	// 只有领导才能给其它服务器发送消息
	if rf.currentLeader == rf.me {
		var success_count = 0
		fmt.Printf("领导者发送数据中 。。。\n")
		go func() {
			rf.broadcast(param, "Raft.LogDataCopy", func(ok bool) {
				//需要其它服务端回应
				rf.message <- ok
			})
		}()
		//数据 等待 ack 消息 如果其他一半以上节点都 收到消息 就返回
		for i := 0; i < raftCount-1; i++ {
			fmt.Println("等待其它服务端回应")
			select {
			case ok := <-rf.message:
				if ok {
					success_count++
					if success_count >= raftCount/2 {
						rf.mu.Lock()
						rf.lastMessageTime = milliseconds()
						mysqlMessage[param.MsgId] = bufferMessage[param.MsgId]
						delete(bufferMessage, param.MsgId)
						if clientWriter != nil {
							fmt.Fprintf(clientWriter, "OK")
						}
						fmt.Printf("\n领导者发送数据结束\n")
						rf.mu.Unlock()
					}
				}
			}
		}
	}
}

//注册Raft对象，注册后的目的为确保每个节点（raft) 可以远程接收
func (node *Raft) raftRegisterRPC(port string) {
	//注册一个服务器
	rpc.Register(node)
	//把服务绑定到http协议上
	rpc.HandleHTTP()
	err := http.ListenAndServe(port, nil)
	if err != nil {
		fmt.Println("注册rpc服务失败", err)
	}
}

//创建节点对象
func Make(me int) *Raft {
	rf := &Raft{}
	rf.me = me
	rf.votedFor = -1
	rf.state = FOLLOWER

	rf.currentLeader = -1
	rf.setTerm(0)
	rf.electionTimeout = time.NewTimer(time.Millisecond * time.Duration(randomRange(1500, 3000)))
	rf.heartbeatTimeout = time.NewTimer(time.Millisecond * time.Duration(randomRange(500, 1500)))
	//初始化通道
	rf.message = make(chan bool)
	rf.heartbeat = make(chan bool)
	rf.heartbeatRe = make(chan bool)
	rf.eclectCh = make(chan bool)

	//每个节点都有选举权
	go rf.election()
	//每个节点都有心跳功能
	go rf.sendLeaderHeartBeat()

	return rf
}

//选举成功后，应该广播所有的节点，本节点成为了leader
func (rf *Raft) sendLeaderHeartBeat() {
	for {
		select {
		case <-rf.heartbeat:
			rf.sendAppendEntriesImpl()
		}
	}
}

//发送 投票消息给 其他
func (rf *Raft) sendAppendEntriesImpl() {
	if rf.currentLeader == rf.me {
		var success_count = 0
		go func() {
			param := Param{Msg: "leader heartbeat",
				Arg: Leader{rf.currentTerm, rf.me}}
			rf.broadcast(param, "Raft.Heartbeat", func(ok bool) {
				rf.heartbeatRe <- ok
			})
		}()

		for i := 0; i < raftCount-1; i++ {
			select {
			case ok := <-rf.heartbeatRe:
				if ok {
					success_count++
					if success_count >= raftCount/2 {
						rf.mu.Lock()
						rf.lastMessageTime = milliseconds()
						fmt.Println("接收子节点心跳成功!")
						rf.heartbeatTimeout.Reset(time.Duration(randomRange(500, 1500)) * time.Millisecond)
						rf.mu.Unlock()
					} else {
						rf.mu.Lock()
						rf.lastMessageTime = milliseconds()
						rf.state = FOLLOWER
						rf.currentLeader = -1
						rf.mu.Unlock()
					}
				}
			}
		}
	}
}

func randomRange(min, max int64) int64 {
	//设置随机时间
	rand.Seed(time.Now().UnixNano())
	return rand.Int63n(max-min) + min
}

//获得当前时间（毫秒）
func milliseconds() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}

func (rf *Raft) election() {
	var result bool
	//每隔一段时间发一次心跳
	for {
		//设置该节点最有一次处理消息的时间
		rf.lastMessageTime = milliseconds()
		switch rf.state {
		//超时 重新选举
		case LEADER:
			select {
			//间隔时间为1500-3000ms的随机值
			case <-rf.heartbeatTimeout.C:
				rf.heartbeat <- true
			}
		case FOLLOWER:
			select {
			case <-rf.electionTimeout.C:
				result = false
				for !result {
					//选择leader
					result = rf.election_one_round(&leader)
				}
			}

		}
	}
}

func (rf *Raft) election_one_round(args *Leader) bool {
	////已经有了leader，并且不是自己，那么return
	//if args.LeaderId > -1 && args.LeaderId != rf.me {
	//	if rf.heartbeatTimeout != nil{
	//		select {
	//		case <- rf.heartbeatTimeout.C:
	//			goto ELECTION
	//		}
	//
	//	}
	//	fmt.Printf("%d已是leader，终止%d选举\n", args.LeaderId, rf.me)
	//	return true
	//}
	var timeout int64
	var vote int
	var triggerHeartbeat bool
	timeout = 2000
	last := milliseconds()
	success := false
	rf.mu.Lock()
	rf.becomeCandidate()
	rf.mu.Unlock()
	fmt.Printf("candidate=%d start electing leader\n", rf.me)
	for {
		fmt.Printf("candidate=%d send request vote to server\n", rf.me)
		go func() {
			rf.broadcast(Param{Msg: "send request vote"}, "Raft.ElectingLeader", func(ok bool) {
				//无论成功失败都需要发送到通道 避免堵塞
				rf.eclectCh <- ok
			})
		}()

		vote = 0
		triggerHeartbeat = false
		for i := 0; i < raftCount-1; i++ {
			fmt.Printf("选举服务器:%d 等待选举 i=%d\n", rf.me, i)
			select {
			case ok := <-rf.eclectCh:
				if ok {
					vote++
					success = vote >= raftCount/2 || rf.currentLeader > -1
					if success && !triggerHeartbeat {
						fmt.Println("okok", args)
						triggerHeartbeat = true
						rf.mu.Lock()
						rf.becomeLeader()
						args.Term = rf.currentTerm + 1
						args.LeaderId = rf.me
						rf.mu.Unlock()
						fmt.Printf("选举服务器:%d 成为leader\n", rf.currentLeader)
						rf.heartbeat <- true
					}
				}
			}
			fmt.Printf("选举服务器:%d 完成选举 i:%d\n", rf.me, i)
		}
		//选举超时 或者 投票数 已经大于一般 或者 当前节点已经有领导者 结束本次拉票
		if (timeout+last < milliseconds()) || (vote >= raftCount/2 || rf.currentLeader > -1) {
			break
		} else {
			select {
			case <-time.After(time.Duration(5000) * time.Millisecond):
			}
		}
	}
	fmt.Printf("选举服务器ID:%d 选举是否成功:%t\n", rf.me, success)
	return success
}

func (rf *Raft) becomeLeader() {
	rf.state = LEADER
	fmt.Println(rf.me, "成为了leader")
	rf.currentLeader = rf.me
}

//设置发送参数的数据类型
type Param struct {
	Msg   string
	MsgId string
	Arg   Leader
}

func (rf *Raft) broadcast(msg Param, path string, fun func(ok bool)) {
	//设置不要自己给自己广播
	for nodeID, port := range nodeTable {
		if nodeID == rf.node.id {
			continue
		}
		//链接远程rpc
		rp, err := rpc.DialHTTP("tcp", "127.0.0.1"+port)
		if err != nil {
			fun(false)
			continue
		}

		var bo = false
		err = rp.Call(path, msg, &bo)
		if err != nil {
			fun(false)
			continue
		}
		fun(bo)
	}

}

func (rf *Raft) becomeCandidate() {
	if rf.state == FOLLOWER || rf.currentLeader == -1 {
		//候选人状态
		rf.state = CANDIDATE
		rf.votedFor = rf.me
		rf.setTerm(rf.currentTerm + 1)
		rf.currentLeader = -1

	}
}

func (rf *Raft) setTerm(term int) {
	rf.currentTerm = term
}

//Rpc处理
func (rf *Raft) ElectingLeader(param Param, a *bool) error {
	//给leader投票
	*a = true
	rf.votedFor = param.Arg.LeaderId
	rf.lastMessageTime = milliseconds()
	return nil
}

//同步心跳包
func (rf *Raft) Heartbeat(param Param, a *bool) error {
	fmt.Println("\n rpc消息:leader心跳包:", rf.me, param.Msg)
	if param.Arg.Term < rf.currentTerm {
		*a = false
	} else {
		leader = param.Arg
		fmt.Printf("收到当前leader:%d 心跳包 \n", rf.currentLeader)
		*a = true
		rf.mu.Lock()
		rf.currentLeader = leader.LeaderId
		rf.votedFor = leader.LeaderId
		rf.state = FOLLOWER
		rf.lastMessageTime = milliseconds()
		if rf.electionTimeout != nil {
			rf.electionTimeout.Reset(time.Duration(randomRange(1500, 3000)) * time.Millisecond)
		} else {
			rf.electionTimeout = time.NewTimer(time.Duration(randomRange(1500, 3000)) * time.Millisecond)
		}
		fmt.Printf("当前服务器ID:%d 更新leader为 %d \n", rf.me, rf.currentLeader)
		rf.mu.Unlock()
	}
	return nil
}

//连接到leader节点
func (rf *Raft) ForwardingMessage(param Param, a *bool) error {
	fmt.Println("\nrpc消息:转发消息:", rf.me, param.Msg)

	rf.sendMessageToOtherNodes(param)

	*a = true
	rf.lastMessageTime = milliseconds()

	return nil
}

//接收leader传过来的日志
func (r *Raft) LogDataCopy(param Param, a *bool) error {
	fmt.Println("\n rpc:日志消息拷贝:", r.me, param.Msg)
	bufferMessage[param.MsgId] = param.Msg
	*a = true
	return nil
}
