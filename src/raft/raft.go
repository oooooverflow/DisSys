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
	"sort"
	"sync"
)
import "labrpc"

// import "bytes"
// import "encoding/gob"

import "time"
import "math/rand"

const Heartbeat = 400
const Election = 1000
const Interval = 200



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
	isKilled bool

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
	Logs []*LogEntry

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
	if args.Term < rf.CurrentTerm || (rf.Role == 1 && rf.CurrentTerm == args.Term) {
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
	} else {
		if args.Term > rf.CurrentTerm {
			rf.VotedFor = -1
			rf.Role = 0
		}
		if rf.VotedFor == -1 || rf.VotedFor == args.CandidateId {
			if args.LastLogIndex == 0 || (args.LastLogIndex <= len(rf.Logs) && rf.Logs[len(rf.Logs)-1].Term <= args.LastLogTerm) {
				rf.VotedFor = args.CandidateId
				reply.Term = args.Term
				reply.VoteGranted = true
			} else {
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
	Entries []*LogEntry
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
		DPrintf("peer %d with term %d reject peer %d with term %d because of args.Term < rf.CurrentTerm\n", rf.me, rf.CurrentTerm, args.LeaderId, args.Term)
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
		DPrintf("peer %d with term %d reject peer %d with term %d because of log mismatch\n", rf.me, rf.CurrentTerm, args.LeaderId, args.Term)
		return
	}
	if rf.Role == 0 {
		// reset heart beat
		rf.HeartBeat.Reset(time.Duration(rand.Int63n(Interval)+Heartbeat) * time.Millisecond)
		DPrintf("follower peer %d with term %d receive heartbeat from leader %d with term %d\n", rf.me, rf.CurrentTerm, args.LeaderId, args.Term)
		for _, entry := range args.Entries {
			if entry.Command != nil && entry.Index != 0 && entry.Index <= len(rf.Logs) && rf.Logs[entry.Index-1].Term != entry.Term {
				rf.Logs = rf.Logs[0:entry.Index]
			}
		}
		// append entries
		for _, entry := range args.Entries {
			if entry.Command != nil {
				DPrintf("peer %d append new logs from leader %d with index %d", rf.me, args.LeaderId, entry.Index)
				rf.Logs = append(rf.Logs, entry)
			}
		}
	}
	if args.LeaderCommit > rf.CommitIndex {
		rf.CommitIndex = args.LeaderCommit
	}
	DPrintf("peer %d with committed index %d logs: ", rf.me, rf.CommitIndex)
	for j := 0; j < len(rf.Logs); j++ {
		DPrintf("%d ", rf.Logs[j].Index)
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
		DPrintf("leader %d receive a new command from the client\n", rf.me)
		entry := &LogEntry{
			Index:   len(rf.Logs)+1,
			Term:    rf.CurrentTerm,
			Command: command,
		}
		rf.Logs = append(rf.Logs, entry)
		rf.MatchIndex[rf.me] = len(rf.Logs)
		for i := 0; i < len(rf.peers); i++ {
			if rf.NextIndex[i] == -1 {
				rf.NextIndex[i] = len(rf.Logs)
			}
			//} else {
			//	rf.NextIndex = append(rf.NextIndex, len(rf.Logs))
			//}
		}
		index = len(rf.Logs)
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
	rf.isKilled = true
	DPrintf("peer %d with commited index %d logs: ", rf.me, rf.CommitIndex)
	for j := 0; j < len(rf.Logs); j++ {
		DPrintf("%d ", rf.Logs[j].Index)
	}
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
	rf.isKilled = false
	rf.Role = 0
	rf.VotedFor = -1
	rf.CurrentTerm = 0
	rf.CommitIndex = 0
	rf.LastApplied = 0
	rf.Logs = []*LogEntry{}
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.FollowerLoop()
	return rf
}

func (rf *Raft) FollowerLoop() {
	defer DPrintf("peer %d leave follower loop\n", rf.me)
	DPrintf("peer %d becomes follower\n", rf.me)
	rf.mu.Lock()
	rf.Role = 0
	rf.VotedFor = -1
	rf.HeartBeat = time.NewTimer(time.Duration(rand.Int63n(Interval) + Heartbeat)*time.Millisecond)
	rf.mu.Unlock()
	for !rf.isKilled && rf.Role == 0 {
		select {
		 	case <-rf.HeartBeat.C :
		 		rf.mu.Lock()
				rf.ElectionTimeOut = time.NewTimer(time.Duration(rand.Int63n(Interval) + Election)*time.Millisecond)
				rf.Role = 1
				rf.mu.Unlock()
		 		go rf.CandidateLoop()
				return
	 		default : break
		}
	}
}

func (rf *Raft) CandidateLoop() {
	defer DPrintf("peer %d leave candidate loop\n", rf.me)
	DPrintf("peer %d becomes candidate \n", rf.me)
	rf.mu.Lock()
	rf.VotedFor = rf.me
	rf.mu.Unlock()
	for !rf.isKilled && rf.Role == 1 {
		rf.mu.Lock()
		rf.CurrentTerm += 1
		rf.mu.Unlock()
		rep := make(chan *RequestVoteReply, len(rf.peers)-1)
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				go func(i int) {
					args := RequestVoteArgs {
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
			rf.ElectionTimeOut.Reset(time.Duration(rand.Int63n(Interval)+Election) * time.Millisecond)
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
	//DPrintf("peer %d leave candidate loop\n", rf.me)
}

func (rf *Raft) LeaderLoop() {
	defer DPrintf("peer %d leave leader loop\n", rf.me)
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
	for !rf.isKilled && rf.Role == 2 {
		rf.mu.Lock()
		var matchNum chan int
		if len(rf.Logs) > 0 {
			matchNum = make(chan int, len(rf.Logs)-1)
		}
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			if rf.NextIndex[i] == -1 {
				go func(i int) {
					//rf.mu.Lock()
					args := AppendEntriesArgs{
						Term:         rf.CurrentTerm,
						LeaderId:     rf.me,
						PrevLogIndex: rf.NextIndex[i],
						PrevLogTerm:  0,
						Entries: []*LogEntry{{
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
					//rf.mu.Unlock()
					DPrintf("leader %d broadcast heartbeat to peer %d\n", rf.me, i)
					ok := rf.sendAppendEntries(i, args, reply)
					DPrintf("peer %d receive heartbeat reply from peer %d with ok = %t", rf.me, i, ok )
				}(i)
			} else {
				go func(i int) {
					//rf.mu.Lock()
					args := AppendEntriesArgs{
						Term:         rf.CurrentTerm,
						LeaderId:     rf.me,
						PrevLogIndex: rf.NextIndex[i],
						PrevLogTerm:  rf.Logs[rf.NextIndex[i]-1].Term,
						Entries:      rf.Logs[rf.NextIndex[i]-1:],
						LeaderCommit: rf.CommitIndex,
					}
					reply := &AppendEntriesReply{
						Term:    0,
						Success: false,
					}
					//rf.mu.Unlock()
					DPrintf("leader %d send entries to peer %d\n", rf.me, i)
					ok := rf.sendAppendEntries(i, args, reply)
					DPrintf("peer %d receive append entry reply from peer %d with ok = %t", rf.me, i, ok)
					//rf.mu.Lock()
					if ok && !reply.Success {
						if reply.Term > rf.CurrentTerm {
							go rf.FollowerLoop()
							rf.mu.Unlock()
							return
						} else {
							// log mismatch reply.Term == -1
							rf.NextIndex[i]--
						}
					} else if ok && reply.Success {
						rf.MatchIndex[i] = len(rf.Logs)
						matchNum <- rf.MatchIndex[i]
						rf.NextIndex[i] = -1
						if rf.MatchIndex[i] > rf.CommitIndex {
							rf.CommitIndex = rf.MatchIndex[i]
						}
					}
					//rf.mu.Unlock()
				}(i)
			}
		}
		if len(rf.Logs) > 0 {
			go func(matchNum chan int) {
				//rf.mu.Lock()
				array := []int{rf.MatchIndex[rf.me]}
				//rf.mu.Unlock()
				flag := 0
				for i := 0; i < len(rf.Logs)-1; i++ {
					select {
						case <-matchNum:
							//rf.mu.Lock()
							array = append(array, rf.MatchIndex[i])
							if len(array) > len(rf.peers)/2 {
								sort.Ints(array)
								if rf.CommitIndex < array[len(rf.peers)/2] {
									rf.CommitIndex = array[len(rf.peers)/2]
									flag = 1
								}
							}
							//rf.mu.Unlock()
							break
					}
					if flag == 1 {
						break
					}
				}
			}(matchNum)
		}
		rf.mu.Unlock()
		time.Sleep(time.Duration(100 * time.Millisecond))
	}
}