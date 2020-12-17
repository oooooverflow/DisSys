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
	"sync"
)
import "labrpc"

// import "bytes"
// import "encoding/gob"

import "time"
import "math/rand"



//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}


type LogEntry struct {
	Index int
	Term int
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// record heart beats
	ElectionTimeOut *time.Timer
	HeartBeat *time.Timer

	// Persistent state on all servers
	Role int 		// follower 0, candidate 1, leader 2
	CurrentTerm int
	VotedFor int
	Logs []LogEntry

	// Volatile state on all servers
	CommitIndex int
	LastApplied int

	// Volatile state on leaders
	NextIndex []int
	MatchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.CurrentTerm
	isleader = rf.Role == 2
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
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

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
}




//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term int
	CandidateId	int
	LastLogIndex int
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	DPrintf("peer %d with role %d enter function request vote\n", rf.me, rf.Role)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.CurrentTerm {
		//DPrintf("..............\n")
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
	} else {
		if rf.Role == 0 || (rf.Role == 1 && args.Term > rf.CurrentTerm) {
			//DPrintf("-----------\n")
			rf.VotedFor = -1
			rf.Role = 0
		}
		if rf.VotedFor == -1 || rf.VotedFor == args.CandidateId {
			if args.LastLogIndex == 0 || (args.LastLogIndex <= len(rf.Logs) && rf.Logs[len(rf.Logs)-1].Term <= args.LastLogTerm) {
				//DPrintf("*************\n")
				rf.VotedFor = args.CandidateId
				reply.Term = args.Term
				reply.VoteGranted = true
			} else {
				//DPrintf("++++++++++++++\n")
				rf.VotedFor = -1
				reply.Term = rf.CurrentTerm
				reply.VoteGranted = false
			}
		}
	}
	DPrintf("peer %d with term %d vote peer %d with term %d by %t, args.LastLogIndex = %d\n", rf.me, rf.CurrentTerm, args.CandidateId, args.Term, reply.VoteGranted, args.LastLogIndex)
}

//
// example AppendEntries RPC arguments structure.
//
type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries []LogEntry
	LeaderCommit int
}

//
// example AppendEntries RPC reply structure.
//
type AppendEntriesReply struct {
	Term int
	Success bool
}

//
// example AppendEntries RPC handler.
//
func (rf *Raft) AppendEntries (args AppendEntriesArgs, reply *AppendEntriesReply) {
	DPrintf("peer %d with role %d append entry from leader %d\n", rf.me, rf.Role, args.LeaderId)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		reply.Success = false
		return
	} else if args.Term > rf.CurrentTerm {
		rf.Role = 0
		rf.CurrentTerm = args.Term
	}
	if rf.Role == 1 {
		rf.Role = 0
		rf.CurrentTerm = args.Term
	}
	if args.PrevLogIndex != -1 && args.PrevLogIndex <= len(rf.Logs) && rf.Logs[args.PrevLogIndex-1].Term != args.PrevLogTerm {
		reply.Term = -1
		reply.Success = false
		return
	}
	if rf.Role == 0 {
		// reset heart beat
		rf.HeartBeat.Reset(time.Duration(rand.Int63n(200)+400) * time.Millisecond)
		DPrintf("follower peer %d with term %d receive heartbeat from leader %d with term %d\n", rf.me, rf.CurrentTerm, args.LeaderId, args.Term)
		for _, entry := range args.Entries {
			if entry.Index == 0 || (entry.Index <= len(rf.Logs) && rf.Logs[entry.Index-1].Term != entry.Term) {
				rf.Logs = rf.Logs[0:entry.Index]
			}
		}
		for _, entry := range args.Entries {
			if entry.Command != nil {
				rf.Logs = append(rf.Logs, entry)
			}
		}

	}
	if args.LeaderCommit > rf.CommitIndex {
		rf.CommitIndex = args.LeaderCommit
	}
	reply.Term = args.Term
	reply.Success = true
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}


//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	term := -1
	isLeader := true
	if rf.Role != 2 {
		isLeader = false
		index = len(rf.Logs)
		term = rf.CurrentTerm
	} else {
		entry := LogEntry{
			Index:   rf.Logs[len(rf.Logs)-1].Index+1,
			Term:    rf.CurrentTerm,
			Command: command,
		}
		rf.Logs = append(rf.Logs, entry)
		for i := 0; i < len(rf.peers); i++ {
			rf.NextIndex[i] = len(rf.Logs)
		}
		//args := AppendEntriesArgs{
		//	Term: rf.CurrentTerm,
		//	LeaderId: rf.me,
		//	PrevLogIndex: rf.Logs[len(rf.Logs)-1].Index,
		//	PrevLogTerm: rf.Logs[len(rf.Logs)-1].Term,
		//	Entries: entries,
		//	LeaderCommit: rf.commitIndex,
		//}
		//reply := &AppendEntriesReply{
		//	Term:    0,
		//	Success: false,
		//}
		//
		//count := 0
		//for i := 0; i < len(rf.peers); i++ {
		//	if i != rf.me {
		//		rf.sendAppendEntries(rf.me, args, reply)
		//		if reply.Success == true {
		//			count++
		//		}
		//	}
		//}
		//if count > len(rf.peers)/2 {
		//
		//}
		index = rf.Logs[len(rf.Logs)-1].Index+1
		term = rf.CurrentTerm
	}

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	//fmt.Printf("peer len is %d at rf num = %d\n", len(rf.peers), rf.me)

	// Your initialization code here.
	rf.Role = 0
	rf.VotedFor = -1
	rf.HeartBeat = time.NewTimer(time.Duration(rand.Int63n(200) + 400)*time.Millisecond)
	rf.CurrentTerm = 0
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.FollowerLoop()
	return rf
}

func (rf *Raft) FollowerLoop() {
	DPrintf("peer %d becomes follower\n", rf.me)
	for rf.Role == 0 {
		select {
		 	case <-rf.HeartBeat.C :
		 		rf.mu.Lock()
				rf.ElectionTimeOut = time.NewTimer(time.Duration(rand.Int63n(200) + 100)*time.Millisecond)
				rf.mu.Unlock()
		 		rf.CandidateLoop()
	 		default:
		}
	}
}

func (rf *Raft) CandidateLoop() {
	DPrintf("peer %d becomes candidate \n", rf.me)
	rf.mu.Lock()
	rf.Role = 1
	rf.VotedFor = rf.me
	rf.mu.Unlock()
	for rf.Role == 1 {
		rf.mu.Lock()
		rf.CurrentTerm += 1
		rf.mu.Unlock()
		rep := make(chan *RequestVoteReply, len(rf.peers)-1)
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				go func(i int) {
					args := RequestVoteArgs{
						Term:         rf.CurrentTerm,
						CandidateId:  rf.me,
						LastLogIndex: len(rf.Logs),
					}
					if args.LastLogIndex > 0 {
						args.LastLogTerm = rf.Logs[len(rf.Logs)-1].Term
					} else {
						args.LastLogTerm = 0
					}
					reply := &RequestVoteReply{
						Term:        0,
						VoteGranted: false,
					}
					DPrintf("peer %d request vote to peer %d", rf.me, i)
					ok := rf.sendRequestVote(i, args, reply)
					DPrintf("peer %d receive vote from peer %d with ok = %t", rf.me, i, ok)
					if ok {
						DPrintf("candidate %d receive vote from %d with vote result %t\n", rf.me, i, reply.VoteGranted)
						rep <- reply
					}
				}(i)
			}
		}
		count := 1
		maxTerm := -1
		flag := 0
		for i := 0; i < len(rf.peers)-1; i++ {
			select {
				case <-rf.ElectionTimeOut.C:
					DPrintf("peer %d with role %d election timeout retry election\n", rf.me, rf.Role)
					if rf.Role == 1 {
						flag = 1
						break
					}
					break
				case result := <-rep :
					DPrintf("peer %d role %d receive one vote\n", rf.me, rf.Role)
					if result.VoteGranted {
						count++
						if count > len(rf.peers)/2 && rf.Role == 1 {
							flag = 2
							break
						}
					} else {
						if result.Term > maxTerm {
							maxTerm = result.Term
						}
					}
					break
			}
			if flag != 0 {
				break
			}
		}
		if flag == 1 {
			rf.ElectionTimeOut.Reset(time.Duration(rand.Int63n(200)+100) * time.Millisecond)
			continue
		}
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if count > len(rf.peers)/2 && rf.Role == 1 {
			go rf.LeaderLoop()
			break
		} else {
			rf.VotedFor = -1
			rf.CurrentTerm = maxTerm
			break
		}
	}
	DPrintf("peer %d leave candidate loop\n", rf.me)
}

func (rf *Raft) LeaderLoop() {
	DPrintf("peer %d becomes the leader\n", rf.me)
	rf.mu.Lock()
	rf.Role = 2
	rf.NextIndex = []int{}
	rf.MatchIndex = []int{}
	for i := 0; i < len(rf.peers); i++ {
		rf.NextIndex = append(rf.NextIndex, -1)
		rf.MatchIndex = append(rf.MatchIndex, -1)
	}
	rf.mu.Unlock()
	go rf.HeartbeatLoop()
}

func (rf *Raft) HeartbeatLoop () {
	//DPrintf("leader %d broadcast heartbeat\n", rf.me)
	for rf.Role == 2 {
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			if rf.NextIndex[i] == -1 {
				go func(i int) {
					args := AppendEntriesArgs{
						Term:         rf.CurrentTerm,
						LeaderId:     rf.me,
						PrevLogIndex: rf.NextIndex[i],
						PrevLogTerm:  0,
						Entries: []LogEntry{{
							Index:   0,
							Term:    0,
							Command: nil,
						}},
						LeaderCommit: 0,
					}
					reply := &AppendEntriesReply{
						Term:    0,
						Success: false,
					}
					DPrintf("leader %d broadcast heartbeat to peer %d\n", rf.me, i)
					ok := rf.sendAppendEntries(i, args, reply)
					DPrintf("peer %d append entry from peer %d with ok = %t", rf.me, i, ok )
					if ok {
						DPrintf("leader %d receive reply from peer %d\n", rf.me, i)
					}
				}(i)
			} else {
				go func(i int) {
					args := AppendEntriesArgs{
						Term:         rf.CurrentTerm,
						LeaderId:     rf.me,
						PrevLogIndex: rf.NextIndex[i],
						PrevLogTerm:  rf.Logs[rf.NextIndex[i]-1].Term,
						Entries:      rf.Logs[rf.NextIndex[i]-1:],
						LeaderCommit: 0,
					}
					reply := &AppendEntriesReply{
						Term:    0,
						Success: false,
					}
					rf.sendAppendEntries(i, args, reply)
				}(i)
			}
		}
		time.Sleep(time.Duration(100 * time.Millisecond))
	}
}