package common

import (
	"net/rpc"
	"net"
	"log"
	"fmt"
)

type Args struct{
	Address string
}



func StartRpcServer(rcvr interface{}, port int, kind string){
	fmt.Printf("starting %s on port %d", kind, port)
	fmt.Println()
	server := rpc.NewServer()
	server.Register(rcvr)
	address := fmt.Sprintf(":%d", port)
	listener, error := net.Listen("tcp", address)
	if error != nil {
		log.Fatal("listen error:", error)
	}
	go server.Accept(listener)
	fmt.Printf("%s server started", kind)
	fmt.Println()
}

func StartRpcCli(location string, kind string) net.Conn {
	fmt.Printf("starting client %s for srv location %s" ,kind, location )
	fmt.Println()
	conn, err := net.Dial("tcp", location)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	fmt.Print("client started listen'g")
	fmt.Println()
	return conn
}