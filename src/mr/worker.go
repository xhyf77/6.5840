package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	pid := os.Getpid()
	for {
		time.Sleep(time.Second / 100 )
		reply, err := Call_for_work()
		if reply.Need_exit == 1 {
			break
		}
		if err {
			continue
		}
		if reply.Need_wait {
			continue
		}
		notify := Taskcomplete{}
		if reply.Do_what == 0 {
			task := reply.Task
			prefix := "mr-worker" + strconv.Itoa(task.Tasknumber)
			intermediatefilename := make([]string, reply.Nreduce)
			intermediafile := make([]*os.File, reply.Nreduce)
			enc := make([]*json.Encoder, reply.Nreduce)

			for i := 0; i < reply.Nreduce; i++ {
				prefix = prefix + "-" + strconv.Itoa(i) + "-" + strconv.Itoa(pid)
				file, err := os.Create(prefix)
				if err != nil {
					fmt.Printf("can not creat the file : %v", err)
					continue
				}
				enc[i] = json.NewEncoder(file)
				absPath, err := filepath.Abs(prefix)
				if err != nil {
					fmt.Println("get the abs path error:", err)
					continue
				}
				intermediatefilename[i] = absPath
				intermediafile[i] = file
			}
			mapfilename := task.Filename
			mapfile, err := os.Open(mapfilename)
			if err != nil {
				fmt.Printf("[worker] cannot open mapfile %v--\n", mapfilename)
				fmt.Printf("[worker] maptask number %v--\n", task.Tasknumber)
				continue
			}
			content, err := ioutil.ReadAll(mapfile)
			if err != nil {
				fmt.Printf("cannot read %v", mapfilename)
				continue
			}
			mapfile.Close()

			kva := mapf(mapfilename, string(content))
			for _, kv := range kva {
				num := ihash(kv.Key) % reply.Nreduce
				err := enc[num].Encode(&kv)
				if err != nil {
					fmt.Printf("can not encode the data: %v", err)
					continue
				}
			}

			for i := 0; i < len(intermediafile); i++ {
				intermediafile[i].Close()
			}

			notify.Outputfile = intermediatefilename
			notify.Do_what = 0
			notify.Mrnum = task.Tasknumber

			notifyreply, isfail := Call_for_noticy(&notify)
			if isfail {
				continue
			}
			if notifyreply.Need_delete {
				for i := 0; i < len(intermediatefilename); i++ {
					err := os.Remove(intermediatefilename[i])
					if err != nil {
						fmt.Printf("can not delete file  %s: %v\n", intermediatefilename[i], err)
						continue
					}
				}
			}
		} else {
			task := reply.Task
			filename := task.Filename
			file, err := os.Open(filename)
			if err != nil {
				fmt.Printf("reduce worker can not open file , filename:%s --- error:%v \n", filename , err)
				fmt.Println(reply.Need_wait)
				continue
			}
			dec := json.NewDecoder(file)
			kva := []KeyValue{}
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				kva = append(kva, kv)
			}
			sort.Sort(ByKey(kva))

			tempFile, err := os.CreateTemp("", "reduce-*.tmp")
			if err != nil {
				fmt.Printf("woker reduce can not create tempfile: %v\n", err)
				continue
			}
			i := 0
			for i < len(kva) {
				j := i + 1
				for j < len(kva) && kva[j].Key == kva[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, kva[k].Value)
				}
				output := reducef(kva[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(tempFile, "%v %v\n", kva[i].Key, output)

				i = j
			}

			tempFileName := tempFile.Name()
			tempFile.Close()
			absPath, _ := filepath.Abs(tempFileName)
			notify.Outputfile = append(notify.Outputfile, absPath)
			notify.Do_what = 1
			notify.Mrnum = task.Tasknumber
			notifyreply, isfail := Call_for_noticy(&notify)
			if isfail {
				continue
			}

			if notifyreply.Need_delete {
				err := os.Remove(absPath)
				if err != nil {
					fmt.Printf("worker reduce can not delete file  %s: %v\n", absPath, err)
					continue
				}
			}
		}
	}

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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

func Call_for_work() (Reply, bool) {

	// declare an argument structure.
	args := GetTask{}
	// fill in the argument(s).
	args.X = 1
	// declare a reply structure.
	reply := Reply{}
	
	reply.Need_exit = 0

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.GetTask", &args, &reply)
	if ok {
		return reply, false
	} else {
		fmt.Printf("call failed!\n")
		return reply, true
	}
}

func Call_for_noticy(sendargs *Taskcomplete) (Reply, bool) {
	reply := Reply{}
	ok := call("Coordinator.Notifycomplete", sendargs, &reply)
	if ok {
		return reply, false
	} else {
		fmt.Printf("call failed!\n")
		return reply, true
	}
}
