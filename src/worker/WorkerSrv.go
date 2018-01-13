package worker

import (
	"MapReduce/src/common"
	"os"
	"bufio"
	"fmt"
	"strings"
	"time"
	"strconv"
)

type WorkerSrvOperation int

type SingleFileHolder struct{
	FileLocation string
}

type MutliFileholder struct{
	fileLocations []string
}

func StartWorkerSrv(port int ) string {
	op := new(WorkerSrvOperation)
	common.StartRpcServer(op,port,"worker-server") //self location :9091 ||9092 || 9093
	return  fmt.Sprintf( ":%d",port)
}

func (m *WorkerSrvOperation) DoMap(arg *SingleFileHolder, reply *string) error {
	*reply = Map(arg.FileLocation)
	return nil
}

func (m *WorkerSrvOperation) DoLocalAggregation(arg *SingleFileHolder, newFileLocation *string) error {
	*newFileLocation = LocalAggregation(arg.FileLocation)
	return nil
}

func (m *WorkerSrvOperation) DoShuffing(arg *SingleFileHolder, newFileLocation *string) error {
	Shuffing(arg.FileLocation)
	return nil
}

func (m *WorkerSrvOperation) DoReduce(arg *MutliFileholder, newFileLocation *string) error {
	*newFileLocation = MergeIntoSingle(arg.fileLocations)
	return nil
}
func MergeIntoSingle(filePath []string) string {
	//copy all files into a single file
	return ""
}

func Shuffing(fileLocation string)  {
	fileDescpt := common.GiveFileDescpt(fileLocation)
	defer fileDescpt.Close()
	scanner := bufio.NewScanner(fileDescpt)
	for scanner.Scan() {
		line :=scanner.Text()
		clientRegistry["mycli"].FileTakeLock(strings.Split(line,",")[0])
		outputFilePath :=common.DirectoryPath(fileLocation)+"/"+fmt.Sprintf("shuffle_output_%d.txt", time.Now().UnixNano())
		outputFile, _ := os.Open(outputFilePath)
		defer outputFile.Close()
		outputFile.WriteString(fmt.Sprintf("%s\n",scanner.Text()))
		clientRegistry["mycli"].FileReleaseLock(strings.Split(line,",")[0])
	}
}

func Map(fileLocation string) string {
	fileDescpt := common.GiveFileDescpt(fileLocation)
	defer fileDescpt.Close()
	outputFilePath :=common.DirectoryPath(fileLocation)+"/"+fmt.Sprintf("output_%d.txt", time.Now().UnixNano())
	outputFile, _ := os.Create(outputFilePath)
	defer outputFile.Close()
	scanner := bufio.NewScanner(fileDescpt)
	for scanner.Scan() {
		//fmt.Println(scanner.Text())
		outputFile.WriteString(fmt.Sprintf("%s,%d\n",scanner.Text(),1))
	}
	return outputFilePath
}

func LocalAggregation(fileLocation string) string {
	fileFreqMap := make(map[string]int)
	fileDescpt := common.GiveFileDescpt(fileLocation)
	defer fileDescpt.Close()
	outputFilePath :=common.DirectoryPath(fileLocation)+"/"+fmt.Sprintf("%s_output_%d.txt",fileLocation, time.Now().UnixNano())
	outputFile, _ := os.Create(outputFilePath)
	defer outputFile.Close()
	scanner := bufio.NewScanner(fileDescpt)
	for scanner.Scan() {
		//fmt.Println(scanner.Text())
		line :=scanner.Text()
		fields := strings.Split(line,",")
		value, _ := strconv.Atoi(fields[1])
		fileFreqMap[fields[0]] += value
	}
	for k,v :=range fileFreqMap{
		outputFile.WriteString(fmt.Sprintf("%s,%d\n",k,v))
	}
	return outputFilePath
}

