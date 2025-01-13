package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "encoding/gob"

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for Assignment2; only used in Assignment3
	Snapshot    []byte // ignore for Assignment2; only used in Assignment3
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu              sync.Mutex
	peers           []*labrpc.ClientEnd
	persister       *Persister
	me              int // index into peers[]
	currentTerm     int
	votedFor        int
	isLeader        bool
	state           string
	electionTimeout time.Duration
	votesReceived   int
	ticker          *time.Ticker
	nextIndex       []int
	matchIndex      []int
	commitIndex     int // index og highest commited so far  //sees reply from majority and commit
	serverLog       []Log
	lastApplied     int //if its less thn commit loop to apply all of tht (loop mechanism)
	applyCommand    chan ApplyMsg
	applied         map[int]interface{}
	count           int
	replyChan       chan AppendEntriesReply

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

type Log struct {
	Term    int
	Command interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	var term int = rf.currentTerm
	var isleader bool = rf.isLeader
	rf.mu.Unlock()
	// Your code here.

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
}

// example RequestVote RPC arguments structure.
type RequestVoteArgs struct {
	// Your data here.
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
type RequestVoteReply struct {
	// Your data here.
	Term        int
	VoteGranted bool
}

// append entries trigger to send entires wehn see a log
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int //updates commit index on each server
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	if rf.lastApplied < len(rf.serverLog)-1 {
		rf.serverLog = rf.serverLog[:rf.lastApplied]
	}

	if args.Term > rf.currentTerm && args.LastLogIndex >= len(rf.serverLog)-1 {
		rf.currentTerm = args.Term
		rf.isLeader = false
		rf.state = "follower"
		rf.votedFor = args.CandidateId
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
	} else if args.Term > rf.currentTerm && args.LastLogIndex < len(rf.serverLog)-1 {
		rf.currentTerm = args.Term
		rf.isLeader = false
		rf.state = "follower"
		rf.votedFor = -1
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	}

	if args.Term < rf.currentTerm || args.Term == rf.currentTerm && args.LastLogIndex < len(rf.serverLog)-1 {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	}
	rf.mu.Unlock()
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	reply.Term = rf.currentTerm

	if !rf.isLeader {
		if len(rf.serverLog) < len(args.Entries) {
			rf.serverLog = args.Entries
			reply.Success = true
			// fmt.Println("reply from append", reply)
		}

		if len(rf.serverLog) > 0 {
			// fmt.Println("server Log", len(rf.serverLog), " lastApplied", rf.lastApplied, "of server ", rf.me, "leader commit ", args.LeaderCommit, "server commit ", rf.commitIndex)

			if args.LeaderCommit > rf.commitIndex {
				if args.LeaderCommit > rf.lastApplied {
					for i := rf.lastApplied; i <= args.LeaderCommit-1; i++ {
						rf.applyCommand <- ApplyMsg{Command: rf.serverLog[i].Command, Index: i + 1}
						rf.applied[i] = rf.serverLog[i].Command
						rf.lastApplied = len(rf.applied)
					}
					rf.commitIndex = args.LeaderCommit
				}
			}
		}
	}

	rf.mu.Unlock()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok {
		resetMu.Lock()
		resetAll[server] <- struct{}{}
		resetMu.Unlock()
	}
	return ok
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	index := -2
	term := rf.currentTerm
	isLeader := rf.isLeader

	// var flag bool

	if isLeader {
		rf.count = 0
		index = len(rf.serverLog)
		// fmt.Println("command", command)
		log := Log{Command: command, Term: rf.currentTerm}
		// for _, existingLog := range rf.serverLog {
		// 	if existingLog.Command == log.Command {
		// 		flag = true
		// 	}
		// }
		// if !flag {
		rf.serverLog = append(rf.serverLog, log)
		// 	fmt.Println("leader log", len(rf.serverLog))
		// }
	}
	rf.mu.Unlock()
	return index + 1, term, isLeader
}

// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

func generateElectionTimeout() time.Duration {
	randomTimeout := 250 + rand.Intn(350-250)
	return time.Duration(randomTimeout) * time.Millisecond
}

func (rf *Raft) send() {
	go func() {
		for {
			rf.mu.Lock()
			if !rf.isLeader {
				rf.mu.Unlock()
				return
			}

			term := rf.currentTerm

			prevIndex := len(rf.serverLog) - 2
			var prevTerm int
			if len(rf.serverLog) <= 1 {
				prevTerm = -1
			} else {
				prevTerm = rf.serverLog[len(rf.serverLog)-2].Term
			}
			replyChan := make(chan AppendEntriesReply)
			// Create a copy of the data you intend to use inside the goroutines
			args := AppendEntriesArgs{
				Term:         term,
				LeaderId:     rf.me,
				PrevLogIndex: prevIndex,
				PrevLogTerm:  prevTerm,
				Entries:      rf.serverLog,
				LeaderCommit: rf.commitIndex,
			}

			for server := 0; server < len(rf.peers); server++ {
				go func(server int) {
					reply := AppendEntriesReply{}
					rf.sendAppendEntries(server, &args, &reply)
					replyChan <- reply
				}(server)
			}

			rf.mu.Unlock()
			time.Sleep(100 * time.Millisecond)

			for i := 0; i < len(rf.peers); i++ {
				reply := <-replyChan

				if reply.Term > term {
					rf.mu.Lock()
					rf.currentTerm = reply.Term
					rf.state = "follower"
					rf.isLeader = false
					rf.votedFor = -1
					rf.mu.Unlock()
				}

				if reply.Success {
					rf.mu.Lock()
					rf.count++
					// fmt.Println("count", rf.count)
					// fmt.Println("peers", len(rf.peers)/2)
					if rf.count+1 > (len(rf.peers))/2 {
						if len(rf.serverLog) > rf.lastApplied {
							for i := rf.lastApplied; i < len(rf.serverLog); i++ {
								rf.applyCommand <- ApplyMsg{Command: rf.serverLog[i].Command, Index: i + 1}
								rf.applied[i] = rf.serverLog[i].Command
								rf.lastApplied = len(rf.applied)
								rf.commitIndex++
							}
							rf.count = 0
						}
					}
					rf.mu.Unlock()
				}

			}
		}
	}()
}

func (rf *Raft) startElection() {

	rf.mu.Lock()
	rf.currentTerm++
	rf.state = "candidate"
	rf.isLeader = false
	rf.votedFor = rf.me
	electionTerm := rf.currentTerm

	var lastTerm int
	if len(rf.serverLog) > 0 {
		lastTerm = rf.serverLog[len(rf.serverLog)-1].Term
	} else {
		lastTerm = -1
	}
	rf.mu.Unlock()

	for server := 0; server < len(rf.peers); server++ {
		go func(server int) {
			// rf.mu.Lock()
			args := RequestVoteArgs{
				Term:         electionTerm,
				CandidateId:  rf.me,
				LastLogIndex: len(rf.serverLog) - 1,
				LastLogTerm:  lastTerm,
			}
			reply := RequestVoteReply{}
			// rf.mu.Unlock()
			if rf.sendRequestVote(server, &args, &reply) {
				rf.mu.Lock()

				if args.Term != electionTerm {
					return
				}

				// Handle the response based on the current term.
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.state = "follower"
					rf.isLeader = false
					rf.votedFor = -1
				} else if reply.VoteGranted {
					rf.votesReceived++
					// fmt.Println("VOTES RECEIEVD", rf.votesReceived)
					if rf.votesReceived > len(rf.peers)/2 {
						// fmt.Println("LEADER", rf.me)
						// for _, raft := range raftServers {
						// 	if raft.isLeader {
						// 		rf.isLeader = false
						// 		rf.state = "follower"
						// 		rf.votesReceived = 0
						// 	}
						// }
						rf.state = "leader"
						rf.isLeader = true
						rf.votesReceived = 0
						rf.send() // send append entries

					}
				}
				rf.mu.Unlock()
			}
		}(server)
	}
}

func (raft *Raft) heartbeatListener() {
	for {
		select {
		case <-raft.ticker.C:
			// fmt.Println("election started!")
			raft.mu.Lock()
			if raft.ticker != nil {
				raft.ticker.Stop()
			}

			raft.ticker = time.NewTicker(generateElectionTimeout())
			raft.mu.Unlock()

			raft.startElection()

		case <-getResetChannel(raft.me):
			raft.mu.Lock()
			if raft.ticker != nil {
				raft.ticker.Stop()
			}
			raft.ticker = time.NewTicker(generateElectionTimeout())
			raft.mu.Unlock()
		}
	}

}

func getResetChannel(raftID int) chan struct{} {
	resetMu.Lock()
	defer resetMu.Unlock()
	ch, ok := resetAll[raftID]
	if !ok {
		ch = make(chan struct{})
		resetAll[raftID] = ch
	}
	return ch
}

var resetMu sync.Mutex
var resetAll = make(map[int]chan struct{})

// var raftServers = make([]*Raft, 0)
// var raftServersMu sync.Mutex

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {

	// assignment 4 part
	// we pass commands to state machine, using applyChan struct
	//var applyCommand ApplyMsg
	//applyCommand.Index = 1
	//applyCommand.Command = cmd
	//applyChan <- applyCommand

	rf := &Raft{
		currentTerm:     -1,
		votedFor:        0,
		isLeader:        false,
		state:           "follower",
		electionTimeout: generateElectionTimeout(),
		votesReceived:   0,
		commitIndex:     0,
		lastApplied:     0,
		nextIndex:       make([]int, len(peers)),
		matchIndex:      make([]int, len(peers)),
		applyCommand:    make(chan ApplyMsg),
		applied:         make(map[int]interface{}, len(peers)),
		count:           0,
	}
	rf.mu.Lock()
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.ticker = time.NewTicker(rf.electionTimeout)
	rf.mu.Unlock()

	// raftServersMu.Lock()
	// raftServers = append(raftServers, rf)
	// raftServersMu.Unlock()

	resetMu.Lock()
	resetAll[me] = make(chan struct{})
	resetMu.Unlock()

	go rf.heartbeatListener()

	go func() {
		for {
			value := <-rf.applyCommand
			applyCh <- value
		}
	}()

	// initialize from state persisted before a crash
	// rf.readPersist(persister.ReadRaftState())

	return rf
}
