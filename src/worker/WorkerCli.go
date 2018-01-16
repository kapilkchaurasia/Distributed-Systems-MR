package worker

import (
	"log"
	"MapReduce/src/common"
	"net/rpc"
)

type WorkerCliOperation struct {
	client *rpc.Client
}

var clientRegistry = make(map[string]WorkerCliOperation)

func StartWorkerCli(masterSrvAddr string,workerSrvAddrs []string) {
	for _, workerSrvAddr := range workerSrvAddrs {
		conn := common.StartRpcCli(masterSrvAddr, "workerCli") //master server location
		workerCliOperation := &WorkerCliOperation{client: rpc.NewClient(conn)}
		clientRegistry[workerSrvAddr]= *workerCliOperation
		workerCliOperation.Registry(workerSrvAddr) //worker server location location
	}
}
func (t *WorkerCliOperation) Registry(address string){
	var reply string
	args := &common.Args{ address}
	if err := t.client.Call("MasterOperation.Registry", args, &reply); err != nil{
		log.Fatalf("error:", err)
	}
}
func (t *WorkerCliOperation) FileTakeLock(hash string){
	if err := t.client.Call("MasterOperation.FileTakeLock", hash, nil); err != nil{
		log.Fatal("error:", err)
	}
}

func (t *WorkerCliOperation) FileReleaseLock(hash string){
	if err := t.client.Call("MasterOperation.FileReleaseLock", hash, nil); err != nil{
		log.Fatal("error:", err)
	}
}