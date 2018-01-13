package master

import (
	"MapReduce/src/common"
	"fmt"
	"sync/atomic"
)

type MasterOperation struct{
	shuffleRegistry map[string]uint64
}

var masterOperation = make(map[string]uint64,0)
var WorkerSrvAddr = make([]string, 0)
func StartMasterSrv(port int ) string{
	op := new(MasterOperation)
	common.StartRpcServer(op,port, "master-server")
	return fmt.Sprintf(":%d",port)
}
func (m *MasterOperation) Registry(arg *common.Args, reply *string) error {
	fmt.Printf("Registing worker server %s", arg.Address)
	fmt.Println()
	WorkerSrvAddr = append(WorkerSrvAddr, arg.Address)
	return nil
}
func (m *MasterOperation) FileTakeLock(hash *string, reply *string) error {
	fmt.Printf("looking for hash file for lock %s\n", hash)
	value,ok := masterOperation[*hash]
	if !ok{
		masterOperation[*hash] = atomic.AddUint64(&value, 1)
		return nil
	}
	for{
		if(masterOperation[*hash] != 1){
			return nil
		}
	}
	}

func (m *MasterOperation) FileReleaseLock(hash *string, reply *string) error {
	fmt.Printf("looking for hash file for release %s\n", hash)
	value,_ := masterOperation[*hash]
	masterOperation[*hash] = atomic.AddUint64(&value, 0)
	return nil
}

