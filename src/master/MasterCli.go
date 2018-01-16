package master

import (
	"net/rpc"
	"MapReduce/src/common"
	"log"
	"MapReduce/src/worker"
	"fmt"
)

type MasterCliOperation struct {
	client []*rpc.Client
	operationMap map[string]string
}
var masterCliOperation = MasterCliOperation{client:nil,operationMap:make(map[string]string)}

func StartMasterCli() { // location : 9091,9092,9093
	client := make([]*rpc.Client,0)
    for _, addr := range WorkerSrvAddr {
		conn := common.StartRpcCli(addr, "masterCli")
		client = append(client, rpc.NewClient(conn))
	}
	opMap := fillOperationMap()
    masterCliOperation = MasterCliOperation{client:client, operationMap:opMap}
}

func DoOperation(filePath []string,kind string ) ([]string, error){
	value,ok := masterCliOperation.operationMap["kind"]
	if !ok {
		return nil,fmt.Errorf("Operation is not valid")
	}
	fmt.Println("kicking %s phase\n", value)
	var mapOutput []string
	var reply string
	if(kind == "Reduce"){
		args := &worker.MutliFileholder{filePath}
		if err := masterCliOperation.client[0].Call(value, args, &reply); err != nil {
			log.Fatal("error:", err)
		}
		return []string{reply}, nil
	}else {
		for idx, filePath := range filePath {
			args := &worker.SingleFileHolder{filePath}
			if err := masterCliOperation.client[idx%len(masterCliOperation.client)].Call(value, args, &reply); err != nil {
				log.Fatal("error:", err)
			}
			fmt.Printf("map output is in %s processed by %d", reply, idx%len(masterCliOperation.client))
			mapOutput = append(mapOutput, reply)
		}
		return mapOutput, nil
	}
	return []string{""},nil
}

func fillOperationMap() map[string]string {
	operationMap := make(map[string]string)
	operationMap["Map"]= "WorkerSrvOperation.DoMap"
	operationMap["LocalAggregation"]= "WorkerSrvOperation.DoLocalAggregation"
	operationMap["LocalAggregation"]= "WorkerSrvOperation.DoLocalAggregation"
	operationMap["Shuffing"]= "WorkerSrvOperation.DoShuffing"
	operationMap["Reduce"]= "WorkerSrvOperation.DoReduce"
	return operationMap
}