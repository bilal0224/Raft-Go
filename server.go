package raftkv

import (
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Command   string
	Key       string
	Value     string
	ClientId  int64
	RequestId int
	Term      int
}

// type Ids struct {
// 	ClientId  int64
// 	RequestId int
// }

type RaftKV struct {
	mu             sync.Mutex
	me             int
	rf             *raft.Raft
	applyCh        chan raft.ApplyMsg
	store          map[string]string
	responseMap    map[int]chan string
	caterdRequests map[int64]int
	clientRequests map[int64][]int
	Commands       []Op
	responseSent   map[int]string

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// time.Sleep(1000 * time.Millisecond)
	_, isLeader := kv.rf.GetState()

	if isLeader {

		reply.WrongLeader = false
		OpInstance := Op{
			Key:       args.Key,
			Value:     "",
			Command:   "Get",
			ClientId:  args.Id,
			RequestId: args.RequestId,
			// Term:      term,
		}
		kv.mu.Lock()
		list := kv.clientRequests[args.Id]
		kv.mu.Unlock()

		for _, reqId := range list {
			if reqId == args.RequestId {
				kv.mu.Lock()
				// fmt.Println("GET duplicate request!")
				value := kv.responseSent[args.RequestId+int(args.Id)]
				reply.Value = value
				kv.mu.Unlock()
				reply.Err = OK
				reply.WrongLeader = false
				return
			}
		}

		index, _, _ := kv.rf.Start(OpInstance)

		kv.mu.Lock()
		_, ok := kv.responseMap[index]
		if !ok {
			kv.responseMap[index] = make(chan string, 1)
		}

		responseChan := kv.responseMap[index]
		kv.mu.Unlock()

		select {
		case <-responseChan:
			// fmt.Println(OpInstance)
			kv.mu.Lock()
			delete(kv.responseMap, index)
			value, ok := kv.store[args.Key]
			kv.mu.Unlock()
			reply.Value = value
			if ok {
				reply.Err = OK
			} else {
				reply.Err = ErrNoKey
			}

			return
		case <-time.After(1000 * time.Millisecond):
			reply.Value = ""
			reply.Err = ErrNoKey
			return
		}

	} else {
		reply.WrongLeader = true
		return
	}

}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {

	_, isLeader := kv.rf.GetState()

	if isLeader {
		reply.WrongLeader = false
		OpInstance := Op{
			Key:       args.Key,
			Value:     args.Value,
			Command:   args.Op,
			ClientId:  args.Id,
			RequestId: args.RequestId,
			// Term:      term,
		}
		kv.mu.Lock()
		list := kv.clientRequests[args.Id]
		kv.mu.Unlock()

		for _, reqId := range list {
			if reqId == args.RequestId {
				// fmt.Println("Appeend duplicate request")
				reply.Err = OK
				reply.WrongLeader = false
				return
			}
		}

		// fmt.Println(OpInstance)
		index, _, _ := kv.rf.Start(OpInstance)

		kv.mu.Lock()
		_, ok := kv.responseMap[index]
		if !ok {
			kv.responseMap[index] = make(chan string, 1)
		}
		responseChan := kv.responseMap[index]
		kv.mu.Unlock()
		select {
		case <-responseChan:
			// fmt.Println(OpInstance)
			kv.mu.Lock()
			delete(kv.responseMap, index)
			kv.mu.Unlock()
			reply.Err = OK
			return
		case <-time.After(1000 * time.Millisecond):
			reply.Err = ErrNoKey
			return
		}
		// kv.mu.Unlock()
	} else {
		reply.Err = ErrNoKey
		reply.WrongLeader = true
		return
	}

}

// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.store = make(map[string]string)
	kv.caterdRequests = make(map[int64]int)

	// Your initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg, 3000)
	kv.responseMap = make(map[int]chan string)
	kv.clientRequests = make(map[int64][]int)
	kv.Commands = make([]Op, 0)
	kv.responseSent = make(map[int]string)

	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go func() {
		for {
			value := <-kv.applyCh
			kv.mu.Lock()
			index := value.Index
			attr := value.Command.(Op)

			if attr.Command == "Put" {
				kv.store[attr.Key] = attr.Value

			} else if attr.Command == "Append" {
				value1 := kv.store[attr.Key]
				kv.store[attr.Key] = value1 + attr.Value
			} else {
				kv.responseSent[attr.RequestId+int(attr.ClientId)] = kv.store[attr.Key]
			}

			kv.clientRequests[attr.ClientId] = append(kv.clientRequests[attr.ClientId], attr.RequestId)

			if _, ok := kv.responseMap[index]; ok {
				select {
				case <-kv.responseMap[index]:
				default:
				}
				kv.responseMap[index] <- OK
			}
			kv.mu.Unlock()
		}

	}()

	return kv
}
