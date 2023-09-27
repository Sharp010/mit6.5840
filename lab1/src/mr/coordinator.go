package mr

import (
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"

type Coordinator struct {
	// 0 unstart  1 doing 2 done
	mapTaskState  map[string]int
	mapTask       []string // taskId - inputFile
	mapDone       bool     // read write   lock?
	mapTaskExpire []bool

	mu sync.Mutex

	nReduce int // read
	// 0 unstart 1 doing 2 done
	reduceTaskState  []int //  taskId - State   read write
	reduceTaskExpire []bool
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
//func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
//	reply.Y = args.X + 1
//	return nil
//}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) Handler(args *RpcArgs, reply *RpcReply) error {
	if c.Done() {
		reply.WorkType = -1
		reply.NReduce = 0
		reply.TaskNum = 0
		reply.FileName = ""
		//reply = &RpcReply{
		//	WorkType: -1,
		//	TaskNum:  0,
		//	FileName: "",
		//	NReduce:  0,
		//}
		return nil
	}
	//fmt.Printf("[rpc] Get taskType %v !\n", args.TaskType)
	switch args.TaskType {
	// request a task
	case -1:
		//c.mu.Lock()
		//defer c.mu.Unlock()
		if !c.mapDone {
			// allocate a Map task
			taskIdx := -1
			filename := ""
			c.mu.Lock()

			for k, v := range c.mapTask {
				if c.mapTaskState[v] == 0 || (c.mapTaskState[v] == 1 && c.mapTaskExpire[k]) {
					filename = v
					taskIdx = k
					break
				}
			}
			WorkType := 0
			if taskIdx == -1 {
				WorkType = 1024
			}
			reply.WorkType = WorkType
			reply.NReduce = c.nReduce
			reply.TaskNum = taskIdx
			reply.FileName = filename
			//reply = &RpcReply{
			//	WorkType: WorkType,
			//	TaskNum:  taskIdx,
			//	FileName: filename,
			//	NReduce:  c.nReduce,
			//}
			if WorkType == 0 {
				c.mapTaskState[c.mapTask[taskIdx]] = 1
				fmt.Printf("[map alloc] %v\n", taskIdx)
			}
			c.mu.Unlock()

			//fmt.Printf("[map] filename %v taskIdx %v\n", filename, taskIdx)
			if WorkType == 0 {
				go func() {
					time.Sleep(5 * time.Second)
					c.mapTaskExpire[taskIdx] = true
				}()
			}
			return nil
		} else {
			// allocate a Reduce task
			taskIdx := -1
			c.mu.Lock()

			for k, v := range c.reduceTaskState {
				if v == 0 || v == 1 && c.reduceTaskExpire[k] {
					taskIdx = k
					break
				}
			}
			WorkType := 1
			if taskIdx == -1 {
				WorkType = 1024
			}
			reply.WorkType = WorkType
			reply.NReduce = c.nReduce
			reply.TaskNum = taskIdx
			reply.FileName = ""
			//reply = &RpcReply{
			//	WorkType: WorkType,
			//	TaskNum:  taskIdx,
			//	FileName: "",
			//	NReduce:  c.nReduce,
			//}
			if WorkType == 1 {
				c.reduceTaskState[taskIdx] = 1
				fmt.Printf("[reduce alloc] %v\n", taskIdx)
			}
			c.mu.Unlock()
			//fmt.Printf("[reduce] taskIdx %v\n", taskIdx)
			if WorkType == 1 {
				go func() {
					time.Sleep(5 * time.Second)
					c.reduceTaskExpire[taskIdx] = true
				}()
			}
			return nil
		}
	// map task done
	case 0:
		//fmt.Printf("[map done] taskid %v\n", args.TaskNum)
		c.mu.Lock()
		defer c.mu.Unlock()
		if c.mapTaskState[c.mapTask[args.TaskNum]] == 1 {
			c.mapTaskState[c.mapTask[args.TaskNum]] = 2
			fmt.Printf("[map done!] %v\n", args.TaskNum)
		} else if c.mapTaskState[c.mapTask[args.TaskNum]] == 2 {
			return nil
		}
		for _, v := range c.mapTask {
			if c.mapTaskState[v] != 2 {
				return nil
			}
		}
		c.mapDone = true
	// reduce task done
	case 1:
		//fmt.Printf("[reduce done] taskid %v\n", args.TaskNum)
		c.mu.Lock()
		defer c.mu.Unlock()
		if c.reduceTaskState[args.TaskNum] == 1 {
			c.reduceTaskState[args.TaskNum] = 2
			fmt.Printf("[reduce done!] %v\n", args.TaskNum)
		} else if c.reduceTaskState[args.TaskNum] == 2 {
			return nil
		}
	default:
		return fmt.Errorf("wrong args content")
	}
	return nil
}

func (c *Coordinator) server() {
	err := rpc.Register(c)
	if err != nil {
		log.Fatal("rpc register fail!")
	}
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()

	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	for _, v := range c.reduceTaskState {
		if v == 1 || v == 0 {
			return false
		}
	}
	return true
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		mapTaskState:     map[string]int{},
		mapTask:          files,
		mapDone:          false,
		mapTaskExpire:    make([]bool, 10),
		mu:               sync.Mutex{},
		nReduce:          nReduce,
		reduceTaskState:  make([]int, 10),
		reduceTaskExpire: make([]bool, 10),
	}
	for _, v := range files {
		c.mapTaskState[v] = 0
	}
	c.server()
	return &c
}
