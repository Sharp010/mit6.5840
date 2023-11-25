package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
//	maptask
//
func mapTask(task RpcReply, mapf func(string, string) []KeyValue) {
	filename := task.FileName
	file, err := os.Open(filename)
	// open input file
	if err != nil {
		log.Fatalf("cannot open %v\n", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v\n", filename)
	}
	file.Close()
	// do map task   get kv pair like "get":"1"
	kvs := mapf(filename, string(content))
	kvamap := make(map[int][]KeyValue)

	// kva  == intermediate kv pairs
	// divide kv pairs to different intermediate file
	for _, kv := range kvs {
		hash := ihash(kv.Key) % task.NReduce
		kvamap[hash] = append(kvamap[hash], kv)
	}
	// write intermediate kvs to temp file, atomic rename temp file
	for i := 0; i < task.NReduce; i++ {
		// if ith hash bucket no empty
		if len(kvamap[i]) == 0 {
			continue
		}
		onamei := fmt.Sprintf("mr-%d-%d", task.TaskNum, i)
		ofile, err := ioutil.TempFile("", "tmpfile")
		if err != nil {
			fmt.Printf("map worker create temp file error!\n")
		}
		enc := json.NewEncoder(ofile)
		for _, kv := range kvamap[i] {
			err = enc.Encode(kv)
			if err != nil {
				fmt.Printf("map encode file error!\n")
			}
		}
		// atomic rename temp file to intermediate file
		ofile.Close()
		os.Rename(ofile.Name(), onamei)
	}
	args := RpcArgs{
		TaskType: 0,
		TaskNum:  task.TaskNum,
	}
	//fmt.Printf("[map done] filename %v taskid %v\n", task.FileName, task.TaskNum)
	reply := RpcReply{}
	call("Coordinator.Handler", &args, &reply)
}

func reduceTask(task RpcReply, reducef func(string, []string) string) {
	// reducer -> output  use fprintf
	// fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
	pattern := fmt.Sprintf("mr-*-%d", task.TaskNum)
	files, err := filepath.Glob(pattern)
	if err != nil {
		fmt.Println(err)
	}
	// collect all intermediate file's kv pairs
	var kvs []KeyValue
	for _, file := range files {
		ofile, err := os.Open(file)
		if err != nil {
			fmt.Printf("reduce worker open file error! taskid %v filename %s\n", task.TaskNum, file)
		}
		dec := json.NewDecoder(ofile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kvs = append(kvs, kv)
		}
		ofile.Close()
	}
	// reduce all kv pairs
	sort.Sort(ByKey(kvs))
	oname := fmt.Sprintf("mr-out-%d", task.TaskNum)
	ofile, err := ioutil.TempFile("", "tmpfile")
	defer ofile.Close()
	if err != nil {
		fmt.Printf("reduce worker create temp file error! taskid %v\n", task.TaskNum)
	}

	i := 0
	for i < len(kvs) {
		j := i + 1
		for j < len(kvs) && kvs[j].Key == kvs[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kvs[k].Value)
		}
		output := reducef(kvs[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kvs[i].Key, output)

		i = j
	}
	os.Rename(ofile.Name(), oname)
	args := RpcArgs{
		TaskType: 1,
		TaskNum:  task.TaskNum,
	}
	reply := RpcReply{}
	//fmt.Printf("[reduce done] taskid %v\n", task.TaskNum)
	call("Coordinator.Handler", &args, &reply)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.
	// 1.request a task from coordinator
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
eventLoop:
	for {
		args := RpcArgs{
			TaskType: -1,
			TaskNum:  -1,
		}
		reply := RpcReply{}
		//ok := call("Coordinator.Handler", &args, &reply)
		call("Coordinator.Handler", &args, &reply)
		//fmt.Printf("reply nReduce %v\n", reply.NReduce)
		switch reply.WorkType {
		case 0:
			// 1.1.1 mapper request input file
			// 1.1.2 mapper produce nReduce intermediate file named mr-X-Y   Y is computed by ihash()
			mapTask(reply, mapf)
		case 1:
			// 1.2.1 reducer request intermediate file
			// 1.2.2 produce one output file  -- one line per reduce task (Reduce() at wc.go)
			reduceTask(reply, reducef)
		case -1:
			args := RpcArgs{
				TaskType: -1,
				TaskNum:  -1,
			}
			reply := RpcReply{}
			// trigger to end coordinator's loop
			call("Coordinator.Handler", &args, &reply)
			break eventLoop
		default:
			//fmt.Printf("reply.WorkType:%d\n", reply.WorkType)
			time.Sleep(2 * time.Second)
		}
		time.Sleep(time.Second)
	}

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	//args := ExampleArgs{}

	// fill in the argument(s).
	//args.X = 99

	// declare a reply structure.
	//reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	//
	//ok := call("Coordinator.Example", &args, &reply)
	//if ok {
	//	// reply.Y should be 100.
	//	fmt.Printf("reply.Y %v\n", reply.Y)
	//} else {
	//	fmt.Printf("call failed!\n")
	//}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
