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
	"bytes"
	"encoding/gob"
	"sort"
	"sync"
)
import "labrpc"

// import "bytes"
// import "encoding/gob"

import "time"
import "math/rand"

const Heartbeat = 400
const Election = 400
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
	applyMsg chan ApplyMsg

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
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Logs)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
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
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.CurrentTerm)
	d.Decode(&rf.VotedFor)
	d.Decode(&rf.Logs)
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
	rf.mu.Lock()
	DPrintf("peer %d with role %d receive request vote from candidate %d\n", rf.me, rf.Role, args.CandidateId)
	defer rf.mu.Unlock()
	if args.Term < rf.CurrentTerm || (rf.Role == 1 && rf.CurrentTerm == args.Term) {
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
	} else {
		if args.Term > rf.CurrentTerm {
			rf.CurrentTerm = args.Term
			rf.VotedFor = -1
			rf.persist()
			rf.Role = 0
		}
		if rf.VotedFor == -1 {
			if len(rf.Logs) != 0 {
				DPrintf("peer %d at RequestVote, rf.Logs[len(rf.Logs)-1].Term=%d, args.LastLogTerm=%d, len(rf.Logs)=%d, args.LastLogIndex=%d", rf.me, rf.Logs[len(rf.Logs)-1].Term, args.LastLogTerm, len(rf.Logs), args.LastLogIndex)
			}
			if len(rf.Logs) == 0 || rf.Logs[len(rf.Logs)-1].Term < args.LastLogTerm || (rf.Logs[len(rf.Logs)-1].Term == args.LastLogTerm && len(rf.Logs) <= args.LastLogIndex) {
				rf.VotedFor = args.CandidateId
				rf.persist()
				reply.Term = args.Term
				reply.VoteGranted = true
			} else {
				rf.VotedFor = -1
				rf.persist()
				reply.Term = rf.CurrentTerm
				reply.VoteGranted = false
			}
		}
	}
	DPrintf("peer %d vote peer %d with term %d by %t, args.LastLogIndex = %d\n", rf.me, args.CandidateId, args.Term, reply.VoteGranted, args.LastLogIndex)
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
	FirstIndex int
	Success bool
}

//
// example AppendEntries RPC handler.
//
func (rf *Raft) AppendEntries (args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	DPrintf("peer %d with role %d append entry from leader %d\n", rf.me, rf.Role, args.LeaderId)
	defer rf.mu.Unlock()
	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		reply.Success = false
		DPrintf("peer %d with term %d reject peer %d with term %d because of args.Term < rf.CurrentTerm\n", rf.me, rf.CurrentTerm, args.LeaderId, args.Term)
		return
	} else if args.Term > rf.CurrentTerm {
		rf.Role = 0
		rf.CurrentTerm = args.Term
		rf.persist()
	}
	if rf.Role == 1 {
		rf.Role = 0
		rf.CurrentTerm = args.Term
		rf.persist()
	}
	if rf.Role == 0 {
		// reset heart beat
		DPrintf("follower peer %d with term %d receive heartbeat from leader %d with term %d\n", rf.me, rf.CurrentTerm, args.LeaderId, args.Term)
		DPrintf("peer %d with role %d reset heart beat timer", rf.me, rf.Role)
		rf.HeartBeat.Reset(time.Duration(rand.Int63n(Interval)+Heartbeat) * time.Millisecond)
	}
	if args.PrevLogIndex != 0 && args.PrevLogIndex <= len(rf.Logs) && rf.Logs[args.PrevLogIndex-1].Term != args.PrevLogTerm {
		reply.Term = rf.Logs[args.PrevLogIndex-1].Term
		k := args.PrevLogIndex
		for ; k > 0; k-- {
			if rf.Logs[k-1].Term != reply.Term {
				break
			}
		}
		DPrintf("peer %d k = %d", rf.me, k)
		reply.FirstIndex = k+1
		reply.Success = false
		DPrintf("peer %d with term %d reject peer %d with term %d because of log mismatch in index %d, reply.term=%d, reply.FirstIndex=%d\n", rf.me, rf.CurrentTerm, args.LeaderId, args.Term, args.PrevLogIndex, reply.Term, reply.FirstIndex)
		return
	}
	if args.PrevLogIndex > len(rf.Logs) {
		if len(rf.Logs) == 0 {
			DPrintf("peer %d reject leader %d because of log mismatch, len(rf.Logs)==0", rf.me, args.LeaderId)
			reply.Term = 0
			reply.FirstIndex = 0
			reply.Success = false
			return
		} else {
			reply.Term = rf.Logs[len(rf.Logs)-1].Term
			k := len(rf.Logs)
			for ; k > 0; k-- {
				if rf.Logs[k-1].Term != reply.Term {
					break
				}
			}
			DPrintf("peer %d k = %d", rf.me, k)
			reply.FirstIndex = k+1
			reply.Success = false
			DPrintf("peer %d with term %d reject peer %d with term %d because of log mismatch, len(log)=%d, args.PrevLogIndex=%d, reply.term=%d, reply.FirstIndex=%d", rf.me, rf.CurrentTerm, args.LeaderId, args.Term, len(rf.Logs), args.PrevLogIndex, reply.Term, reply.FirstIndex)
			return
		}
	}
	if rf.Role == 0 {
		// append entries
		if len(rf.Logs) != 0 {
			if args.PrevLogIndex != 0 {
				rf.Logs = rf.Logs[:args.PrevLogIndex]
				rf.persist()
			} else {
				rf.Logs = []*LogEntry{}
				rf.persist()
			}
		}
		for _, entry := range args.Entries {
				DPrintf("peer %d append new logs from leader %d with index %d", rf.me, args.LeaderId, entry.Index)
				rf.Logs = append(rf.Logs, entry)
		}
		rf.persist()
	}
	if args.LeaderCommit > rf.CommitIndex {
		DPrintf("ready to update %d 's commitIndex, args.LeaderCommit=%d , len(rf.Logs)=%d", rf.me, args.LeaderCommit, len(rf.Logs))
		if args.LeaderCommit < len(rf.Logs) {
			rf.CommitIndex = args.LeaderCommit
			DPrintf("update %d 's commitIndex to LeaderCommit with value %d", rf.me, rf.CommitIndex)
		} else {
			rf.CommitIndex = len(rf.Logs)
			DPrintf("update %d 's commitIndex to len(rf.Logs) with value %d", rf.me, len(rf.Logs))
		}
	}
	// DPrintf("peer %d with committed index %d logs: ", rf.me, rf.CommitIndex)
	// for j := 0; j < len(rf.Logs); j++ {
	// 	DPrintf("peer %d: index: %d term: %d cmd: %d",rf.me  ,rf.Logs[j].Index, rf.Logs[j].Term, rf.Logs[j].Command.(int))
	// }
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
		rf.persist()
		rf.MatchIndex[rf.me] = len(rf.Logs)
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
		DPrintf("peer %d: index: %d term: %d cmd: %d",rf.me  ,rf.Logs[j].Index, rf.Logs[j].Term, rf.Logs[j].Command.(int))
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
	rf.applyMsg = applyCh

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
	go rf.Apply()
	return rf
}

func (rf *Raft) FollowerLoop() {
	defer DPrintf("peer %d leave follower loop\n", rf.me)
	DPrintf("peer %d becomes follower\n", rf.me)
	rf.Role = 0
	rf.VotedFor = -1
	rf.persist()
	rf.HeartBeat = time.NewTimer(time.Duration(rand.Int63n(Interval) + Heartbeat)*time.Millisecond)
	for !rf.isKilled && rf.Role == 0 {
		select {
		 	case <-rf.HeartBeat.C :
		 		rf.mu.Lock()
				rf.Role = 1
				rf.mu.Unlock()
		 		rf.CandidateLoop()
				rf.mu.Lock()
				rf.Role = 0
				rf.HeartBeat.Reset(time.Duration(rand.Int63n(Interval) + Heartbeat)*time.Millisecond)
				rf.mu.Unlock()
				break
		}
	}
}

func (rf *Raft) CandidateLoop() {
	defer DPrintf("peer %d leave candidate loop\n", rf.me)
	rf.mu.Lock()
	DPrintf("peer %d becomes candidate \n", rf.me)
	rf.ElectionTimeOut = time.NewTimer(time.Duration(rand.Int63n(Interval) + Election)*time.Millisecond)
	rf.VotedFor = rf.me
	rf.persist()
	peerNum := len(rf.peers)
	rf.mu.Unlock()
	for !rf.isKilled && rf.Role == 1 {
		rf.mu.Lock()
		rf.CurrentTerm += 1
		rf.persist()
		rep := make(chan *RequestVoteReply, len(rf.peers)-1)
		rf.mu.Unlock()
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				go rf.Vote(i, rep)
			}
		}
		flag, maxTerm, count := rf.Collect(peerNum, rep)
		if flag == 1 {
			rf.mu.Lock()
			if rf.Role == 0 {
				DPrintf("peer %d become follower from candidate", rf.me)
				rf.mu.Unlock()
				return
			}
			DPrintf("peer %d reset election timeout", rf.me)
			rf.ElectionTimeOut.Reset(time.Duration(rand.Int63n(Interval)+Election) * time.Millisecond)
			rf.mu.Unlock()
			continue
		}
		rf.mu.Lock()
		if count > len(rf.peers)/2 && rf.Role == 1 {
			rf.Role = 2
			rf.mu.Unlock()
			rf.LeaderLoop()
			break
		} else {
			rf.VotedFor = -1
			rf.CurrentTerm = maxTerm
			rf.persist()
			rf.mu.Unlock()
			DPrintf("peer %d become follower from candidate", rf.me)
			break
		}
	}
}

func (rf *Raft)Vote (i int, rep chan *RequestVoteReply) {
	rf.mu.Lock()
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
	var ok bool
	if rf.Role == 1 {
		rf.mu.Unlock()
		DPrintf("peer %d request vote to peer %d", rf.me, i)
		ok = rf.sendRequestVote(i, args, reply)
	} else {
		rf.mu.Unlock()
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.Role == 1 {
		DPrintf("peer %d receive vote from peer %d with ok = %t", rf.me, i, ok)
	}
	if ok && rf.Role == 1 {
		DPrintf("candidate %d receive vote from %d with vote result %t\n", rf.me, i, reply.VoteGranted)
		rep <- reply
	}
}

func (rf *Raft) Collect (peerNum int, rep chan *RequestVoteReply) (int, int, int) {
	flag := 0
	count := 1
	maxTerm := -1
	for i := 0; i < peerNum-1; i++ {
		select {
		case <-rf.ElectionTimeOut.C:
			DPrintf("peer %d election timeout retry election\n", rf.me)
			flag = 1
			break
		case result := <-rep :
			rf.mu.Lock()
			DPrintf("peer %d role %d receive one vote\n", rf.me, rf.Role)
			if result.VoteGranted {
				count++
				if count > len(rf.peers)/2 && rf.Role == 1 {
					flag = 2
				}
			} else {
				if result.Term > maxTerm {
					maxTerm = result.Term
				}
			}
			rf.mu.Unlock()
			break
		}
		if flag != 0 {
			break
		}
	}
	return flag, maxTerm, count
}

func (rf *Raft) LeaderLoop() {
	defer DPrintf("peer %d leave leader loop\n", rf.me)
	rf.mu.Lock()
	DPrintf("peer %d becomes the leader\n", rf.me)
	rf.NextIndex = []int{}
	rf.MatchIndex = []int{}
	for i := 0; i < len(rf.peers); i++ {
		rf.NextIndex = append(rf.NextIndex, len(rf.Logs)+1)
		rf.MatchIndex = append(rf.MatchIndex, 0)
	}
	rf.mu.Unlock()
	rf.HeartbeatLoop()
}

func (rf *Raft) HeartbeatLoop () {
	defer DPrintf("peer %d become follower from leader", rf.me)
	go rf.Match()
	for !rf.isKilled && rf.Role == 2 {
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			rf.mu.Lock()
			args := AppendEntriesArgs{
				Term:         rf.CurrentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: rf.NextIndex[i]-1,
				PrevLogTerm:  0,
				Entries:      nil,
				LeaderCommit: rf.CommitIndex,
			}
			args.Entries = rf.Logs[rf.NextIndex[i]-1:]
			if args.PrevLogIndex > 0 {
				args.PrevLogTerm = rf.Logs[args.PrevLogIndex-1].Term
			}
			length := len(rf.Logs)
			rf.mu.Unlock()
			go rf.SendEntry(i, args, length)
		}
		time.Sleep(time.Duration(100 * time.Millisecond))
	}
}

func (rf *Raft) SendEntry (i int, args AppendEntriesArgs, length int) {
	//rf.mu.Lock()
	//length := len(rf.Logs)
	//args := AppendEntriesArgs{
	//	Term:         rf.CurrentTerm,
	//	LeaderId:     rf.me,
	//	PrevLogIndex: rf.NextIndex[i]-1,
	//	PrevLogTerm:  0,
	//	Entries:      nil,
	//	LeaderCommit: rf.CommitIndex,
	//}
	reply := &AppendEntriesReply{
		Term:       0,
		FirstIndex: 0,
		Success:    false,
	}
	DPrintf("leader %d with term %d broadcast heartbeat to peer %d with next index=%d\n", rf.me, rf.CurrentTerm, i, rf.NextIndex[i])
	//rf.mu.Unlock()
	ok := rf.sendAppendEntries(i, args, reply)
	rf.mu.Lock()
	DPrintf("peer %d receive with term %d heartbeat reply from peer %d with ok = %t", rf.me, rf.CurrentTerm, i, ok)
	defer rf.mu.Unlock()
	if rf.CurrentTerm == args.Term && rf.Role == 2 && ok {
		if !reply.Success {
			DPrintf("peer %d rf.CurrentTerm=%d, reply.Term=%d", rf.me, rf.CurrentTerm, reply.Term)
			if reply.Term > rf.CurrentTerm  { //|| (reply.Term > 0 && reply.FirstIndex == 0)
				DPrintf("reply term > rf.current term, become follower")
				rf.Role = 0
				return
			} else {
				// deal with log mismatch
				DPrintf("leader %d log mismatch with peer %d, reply.Term=%d, reply.FirstIndex=%d, len(rf.Logs)=%d", rf.me, i, reply.Term, reply.FirstIndex, len(rf.Logs))
				if reply.Term == 0 {
					rf.NextIndex[i] = 1
					rf.MatchIndex[i] = 0
				} else {
					if reply.FirstIndex <= length {
						if rf.Logs[reply.FirstIndex-1].Term == reply.Term {
							rf.NextIndex[i] = reply.FirstIndex + 1
						} else {
							rf.NextIndex[i] = reply.FirstIndex
						}
						//rf.NextIndex[i] = reply.FirstIndex
						rf.MatchIndex[i] = rf.NextIndex[i]-1
					} else {
						DPrintf("peer %d error: reply.FirstIndex > len(rf.Logs)", rf.me)
					}
				}
			}
		} else if rf.NextIndex[i] <= length {
			DPrintf("append entry reply success with ok=true, update %d 's match index to %d", i, len(rf.Logs))
			rf.MatchIndex[i] = length
			rf.NextIndex[i] = length+1
		}
	}
}

func (rf *Raft) Apply () {
	for !rf.isKilled {
		rf.mu.Lock()
		if rf.LastApplied < rf.CommitIndex {
			DPrintf("peer %d LastApplied %d < CommitIndex %d", rf.me, rf.LastApplied, rf.CommitIndex)
			rf.LastApplied++
			tempMsg := ApplyMsg{
				Index:   rf.LastApplied,
				Command: rf.Logs[rf.LastApplied-1].Command,
			}
			rf.mu.Unlock()
			rf.applyMsg <- tempMsg
			DPrintf("peer %d apply message successfully", rf.me)
		} else {
			rf.mu.Unlock()
		}
		//time.Sleep(time.Duration(50 * time.Millisecond))
	}
}

func (rf *Raft) Match () {
	for !rf.isKilled && rf.Role == 2 {
		rf.mu.Lock()
		array := []int{}
		for i := 0; i < len(rf.peers); i++ {
			array = append(array, rf.MatchIndex[i])
		}
		sort.Ints(array)
		if rf.CommitIndex < array[len(rf.peers)/2] {
			rf.CommitIndex = array[len(rf.peers)/2]
			DPrintf("update leader %d 's commit index to %d", rf.me, rf.CommitIndex)
		}
 		rf.mu.Unlock()
		time.Sleep(time.Duration(50 * time.Millisecond))
	}
}