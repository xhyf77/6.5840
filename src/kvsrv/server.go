package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}


type KVServer struct {
	mu sync.Mutex
	kvMap map[string]string
	processedOps   map[int64]string
	processedkey   map[int64]int64
	// Your definitions here.
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	key := args.Key
	value := kv.kvMap[key]
	reply.Value = value
	if args.Get_reply {
		_ , exist := kv.processedOps[args.Clientid]
		if exist {
			delete( kv.processedOps , args.Clientid )
			delete( kv.processedkey , args.Clientid )
		}
	}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	key := args.Key
	value := args.Value
	kv.kvMap[key] = value
	if args.Get_reply {
		_ , exist := kv.processedOps[args.Clientid]
		if exist {
			delete( kv.processedOps , args.Clientid )
			delete( kv.processedkey , args.Clientid )
		}
	}
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	record, exists := kv.processedkey[args.Clientid]
	if exists {
		if record == args.Processedkey {
			reply.Value = kv.processedOps[args.Clientid]
			return 
		} else {
			delete( kv.processedOps , args.Clientid )
			delete( kv.processedkey , args.Clientid )
		}
	}
	oldValue, exists := kv.kvMap[args.Key]
    if !exists {
        oldValue = ""
    }
	newValue := oldValue + args.Value
	kv.kvMap[args.Key] = newValue
    reply.Value = oldValue

	kv.processedOps[args.Clientid] = oldValue
	kv.processedkey[args.Clientid] = args.Processedkey
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.kvMap = make(map[string]string)
	kv.processedOps = make(map[int64]string)
	kv.processedkey = make(map[int64]int64)
	// You may need initialization code here.

	return kv
}
