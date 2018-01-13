package main

import (
	"MapReduce/src/master"
	"MapReduce/src/worker"
	"fmt"
)
//TODO: Allow users to pass custom functions
func main() {

    //bring up the services
	masterSrvAddr := master.StartMasterSrv(9090) //9090
	workerSrvAddr1 := worker.StartWorkerSrv(9091); //9091 ,9092, 9093
	workerSrvAddr2 := worker.StartWorkerSrv(9092);
	worker.StartWorkerCli(masterSrvAddr, []string{workerSrvAddr1,workerSrvAddr2});
	master.StartMasterCli();

	//distributed map-reduce flow
	mapOutput,err := master.DoOperation([]string{"/Users/k0c00nc/go/src/MapReduce/res/input.txt", "/Users/k0c00nc/go/src/distributedDb" +
		"/res/input1.txt"},"Map")
	if err !=nil{
		fmt.Printf("map phase failed with err %s ", err.Error())
	}

	localAggregation,err :=master.DoOperation(mapOutput,"LocalAggregation")
	if err !=nil{
		fmt.Printf("localAggregation phase failed with err %s ", err.Error())
	}

	shuffing,err :=master.DoOperation(localAggregation,"Shuffing")
	if err !=nil{
		fmt.Printf("shuffing phase failed with err %s ", err.Error())
	}

	reduce,err :=master.DoOperation(shuffing,"Reduce")
	if err !=nil{
		fmt.Printf("reduce phase failed with err %s ", err.Error())
	}

    fmt.Println("MR output are in file", reduce[0])

}
