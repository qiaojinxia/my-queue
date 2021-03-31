package main

func main() {
	//c1 := &Client{Id:100,Ip:"172.18.1.1"}
	//c3:=  &Client{Id:300,Ip:"172.18.1.3"}
	//
	//srv := NewServer()
	//
	//srv.Subscribe(c1,"Topic")
	//srv.Subscribe(c3,"Topic")
	//
	//srv.PublishMessage("Topic","测试信息1")
	//
	//srv.Unsubscribe(c3,"Topic")
	//srv.PublishMessage("Topic","测试信息2222")
	//
	//srv.Subscribe(c1,"Topic2")
	//srv.Subscribe(c3,"Topic2")
	//srv.PublishMessage("Topic2"," Topic2的测试信息")

	Qserver.StartServer("127.0.0.1", "7777")

}
