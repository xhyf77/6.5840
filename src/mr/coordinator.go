package mr

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

type State int

const (
	Avilable State = iota
	Inprogess
	Completed
	Uninit
)

type Task struct {
	Filename  string
	State     State
	StartTime  time.Time
	Tasknumber int
}

type Coordinator struct {
	// Your definitions here.
	Worker_total uint32
	map_done     uint32
	reduce_done  uint32
	nMap         []Task
	reduce       []Task
	reduce_file  [][]string
	nreduce 	 int
	Worker_pid   []uint32
	mutex		 sync.RWMutex
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) GetTask(args *GetTask, reply *Reply) error {
	//concurrency mutex TODO
	reply.Nreduce = c.nreduce
	reply.Need_delete = false
	reply.Need_wait = false
	c.mutex.Lock()
	defer c.mutex.Unlock()
	count := 0
	if c.map_done == 0 {
		for i := 0; i < len(c.nMap); i++ {
			if c.nMap[i].State == Avilable {
				c.nMap[i].State = Inprogess
				c.nMap[i].StartTime = time.Now()
				reply.Task = c.nMap[i]
				reply.Do_what = 0
				reply.Need_exit = 0
				return nil
			} else if c.nMap[i].State == Completed {
				count++
			} else if c.nMap[i].State == Inprogess{
				task := c.nMap[i]
				Now := time.Now()
				duration := Now.Sub(task.StartTime)
				if duration > 10*time.Second {
					task.State = Inprogess
					task.StartTime = time.Now()
					reply.Task = task
					reply.Do_what = 0
					reply.Need_exit = 0
					return nil
				}
			}
		}
	}

	if c.map_done == 0 && count == len(c.nMap) {
		c.map_done = 1
		for i := 0; i < len(c.reduce_file); i++ {
			kva := []KeyValue{}
			//ToDO
			for j := 0 ; j < len(c.reduce_file[i]) ; j ++ {
				file , err := os.Open(c.reduce_file[i][j])
				if err != nil {
					fmt.Printf("coordinator open file error : %v\n", err )
				}
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
					  break
					}
					kva = append(kva, kv)
				}
				file.Close()
			}
			path := "coordinator-reduce-" + strconv.Itoa(i)
			file , err := os.Create(path)
			if err != nil{
				fmt.Printf("coordinator Create file error : %v\n", err )
			}
			enc := json.NewEncoder(file)
			for _, kv := range kva {
				err := enc.Encode(&kv)
				if err != nil {
					fmt.Printf("coordinator encode data error : %v\n", err )
				}
			}
			file.Close()
			absPath, err := filepath.Abs(path)
			if err != nil {
				fmt.Printf("coordinator get abs path error : %v\n", err)
			}
			c.reduce[i].Filename = absPath
			c.reduce[i].State = Avilable
			c.reduce[i].Tasknumber = i
		}
	}

	count = 0

	for i := 0; i < len(c.reduce); i++ {
		if c.reduce[i].State == Avilable {
			c.reduce[i].State = Inprogess
			c.reduce[i].StartTime = time.Now()
			reply.Task = c.reduce[i]
			reply.Do_what = 1
			reply.Need_exit = 0
			return nil
		} else if c.reduce[i].State == Completed {
			count++
		} else if c.reduce[i].State == Inprogess {
			task := c.reduce[i]
			Now := time.Now()
			duration := Now.Sub(task.StartTime)
			if duration > 5*time.Second {
				task.State = Inprogess
				task.StartTime = time.Now()
				reply.Task = task
				reply.Do_what = 1
				reply.Need_exit = 0
				return nil
			}
		}
	}

	if count == len(c.reduce) {
		reply.Need_exit = 1
		c.reduce_done = 1
		return nil
	}

	reply.Need_wait = true
	return nil
}

func (c *Coordinator) Notifycomplete(args *Taskcomplete, reply *Reply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	reply.Need_delete = false
	if args.Do_what == 0 {
		if c.nMap[args.Mrnum].State == Completed {
			reply.Need_delete = true
		} else{
			for i := 0 ; i < len(c.reduce_file) ; i ++ {
				c.reduce_file[i] = append( c.reduce_file[i] , args.Outputfile[i] )
			}
			c.nMap[args.Mrnum].State = Completed
		}
	} else { //reduce
		if c.reduce[args.Mrnum].State == Completed {
			reply.Need_delete = true
		} else {
			outputfilename := "mr-out-" + strconv.Itoa(args.Mrnum)
			tempfilename := args.Outputfile[0]
			err := os.Rename( tempfilename , outputfilename )
			if err != nil {
				fmt.Printf("coordinator reduce rename error: %v\n", err)
				if os.IsNotExist(err) {
					// 检查哪个路径不存在
					if _, statErr := os.Stat(tempfilename); os.IsNotExist(statErr) {
						fmt.Printf("Source file does not exist: %v\n", tempfilename)
					} 
					if _, statErr := os.Stat("~/6.5840/src/main" + outputfilename); os.IsNotExist(statErr) {
						fmt.Printf("Target directory does not exist: %v\n", "~/6.5840/src/main" + outputfilename)
					}
				}
				reply.Need_delete = true
				return nil
			}
			c.reduce[args.Mrnum].State = Completed
		}
	}
	return nil
}


// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	if c.map_done == 1 && c.reduce_done == 1 {
		ret = true
	}

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.nreduce = nReduce
	c.nMap = make([]Task, len(files) )
	c.reduce = make([]Task , nReduce )
	c.reduce_file = make( [][]string, nReduce )
	for i := 0 ; i < len(c.nMap) ; i ++ {
		c.nMap[i].Filename = files[i]
		c.nMap[i].State = Avilable
		c.nMap[i].Tasknumber = i
	}

	for i := 0 ; i < nReduce ; i ++ {
		c.reduce[i].State = Uninit
	}
	c.map_done = 0
	// Your code here.

	c.server()
	return &c
}
