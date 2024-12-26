package kvsrv

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.5840/labrpc"
)



type Clerk struct {
	server *labrpc.ClientEnd
	id int64
	// You will have to modify this struct.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.server = server
	ck.id = time.Now().UnixNano();
	// You'll have to add code here.
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	reply := GetReply{}
	args := GetArgs{}
	args.Key = key
	args.Clientid = ck.id
	args.Get_reply = true
	for {
		ok := ck.server.Call("KVServer.Get", &args, &reply)
		if ok {
			return reply.Value
		}
		args.Get_reply = false
	}
	// You will have to modify this function.
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) string {
	reply := PutAppendReply{}
	args := PutAppendArgs{}
	args.Key = key
	args.Value = value
	args.Clientid = ck.id

	args.Get_reply = true
	args.Processedkey = time.Now().UnixNano()
	if op == "Put" {
		for {
			ok := ck.server.Call("KVServer.Put", &args, &reply)
			if ok {
				return reply.Value
			}
			args.Get_reply = false
		}
	} else if op == "Append" {
		for {
			ok := ck.server.Call("KVServer.Append", &args, &reply)
			if ok {
				return reply.Value
			}
			args.Get_reply = false
		}
	}
	// You will have to modify this function.
	return ""
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
	return ck.PutAppend(key, value, "Append")
}
