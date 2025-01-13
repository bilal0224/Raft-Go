package raftkv

import (
	"crypto/rand"
	"labrpc"
	"math/big"
)

type Clerk struct {
	servers   []*labrpc.ClientEnd
	id        int64
	requestId int
	// You will have to modify this struct.
	//initilze client id and sequence number here
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.id = nrand()
	ck.requestId = 0
	// You'll have to add code here.
	// to consider clerk ids list
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	ck.requestId = ck.requestId + 1
	args := GetArgs{
		Key:       key,
		Id:        ck.id,
		RequestId: ck.requestId,
	}
	for {
		for i := range ck.servers {
			reply := GetReply{}
			ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
			if ok && reply.Err == OK && !reply.WrongLeader {
				return reply.Value
			}
		}
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.requestId = ck.requestId + 1
	args := PutAppendArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		Id:        ck.id,
		RequestId: ck.requestId,
	}
	for {
		for i := range ck.servers {
			reply := PutAppendReply{}
			ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
			if ok && reply.Err == OK && !reply.WrongLeader {
				return
			}
		}
	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
