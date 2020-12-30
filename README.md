# 分布式系统——Raft算法实践



## 设计与分析

### 实验一 Leader Election

- 节点状态

  - 节点状态分成三个角色：Follower，Candidate 和 Leader。除与 Client 通讯所需的信息外，每个节点存储以下信息：

    ```go
    // record heart beats and election timeout
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
    ```

  - 初始化后，每个节点都处于 Follower 状态下。当节点的心跳计时器触发时，节点由 Follower 状态转变为 Candiadte 状态，并给其他节点发送 RPC 调用，请求其他节点为它投票，若收到半数以上的选票赞同其成为 Leader ，则立刻成为 Leader。若在规定时间内未完成投票环节则重启开启投票 。Leader 节点接收 Client 发出的 Log ，并定期向其他节点发送 RPC 调用（心跳），重置其他节点的心跳计时器。

    ![image-20201228150947179](C:\Users\ASUS\AppData\Roaming\Typora\typora-user-images\image-20201228150947179.png)

- 选主过程

  - Client 启动所有的节点，每个节点初始化节点信息（包括随机初始化心跳计时器），并进入 Follower 循环，等待心跳计时器触发，则修改为 Candidate 状态，进入 Candidate 循环。除此之外，Follower 节点接收来自其他节点的信息，这些信息以RPC 调用的形式传递给 Follower，因此会在接收到合法 Leader 的 append entry 信息后随机地重置心跳计时器。具体 Follower 循环代码如下：

    ```go
    func (rf *Raft) FollowerLoop() {
    	rf.Role = 0
    	rf.VotedFor = -1
    	rf.persist()
    	for !rf.isKilled && rf.Role == 0 {
    		select {
                // wait for heart beat timeout
    		 	case <-rf.HeartBeat.C :
    		 		rf.mu.Lock()
    				rf.Role = 1
    				rf.mu.Unlock()
                	// enter candidate loop
    		 		rf.CandidateLoop()
    				rf.mu.Lock()
    				rf.Role = 0
                	// reset heart beat
    				rf.HeartBeat.Reset(time.Duration(rand.Int63n(Interval) + Heartbeat)*time.Millisecond)
    				rf.mu.Unlock()
    				break
    		}
    	}
    }
    ```

  - Candidate 节点使用 RPC 调用的方式请求其他节点为它投票。因为 RPC 调用不一定发送成功，存在丢包以及延迟的影响，因此不能同步的处理发送选票与收集选票两个环节。代码实现中，采用异步的方式处理发送选票和收集选票两个环节。首先使用 goroutine 给每一个其他节点发送 RPC 请求，即逐一开启子协程，每个子协程填写相应的形参，并调用 ``` sendRequestVote``` 函数请求对应节点投票。等待 RPC 调用返回后，使用 channel 收集每一张选票（带缓存channel）。具体发送选票环节的代码如下：

    ```go
    func (rf *Raft)Vote (i int, rep chan *RequestVoteReply) {
    	rf.mu.Lock()
        // prepare params for sendRequestVote
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
    		ok = rf.sendRequestVote(i, args, reply)
    	} else {
    		rf.mu.Unlock()
    		return
    	}
    	rf.mu.Lock()
    	defer rf.mu.Unlock()
    	if ok && rf.Role == 1 {
            // use channel to collect votes
    		rep <- reply
    	}
    }
    ```

  - 选票全部发送出去后，Candidate 收集选票并统计支持者的数量，确认自己是否当选为 Leader。如上文提到的，实现中采用 channel 存储 RPC 调用返回的选票信息，因此采用循环等待的方式，一旦统计过程中发现支持者数量（包括自己投给自己的那一张选票）超过了总节点的半数，就跳出等待循环，成为 Leader。如果在等待循环中，发现等待时间超过了 ElectionTimeOut，则跳出等待循环重新开启投票。收集选票功能的部分代码如下所示：

    ```go
    for i := 0; i < peerNum-1; i++ {
        select {
            case <-rf.ElectionTimeOut.C:
                // election rime out handler
                flag = 1
                break
            case result := <-rep :
            	// receive a vote
                rf.mu.Lock()
                if result.VoteGranted {
                    count++
                    if count > len(rf.peers)/2 && rf.Role == 1 {
                        // recieve votes of the majority
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
    ```

  - 节点接收到其他节点的选票请求后，首先比较两者的 ```CurrentTerm``` 大小，若己方更大，则直接拒绝对方的投票请求，回复 ```false```。此外，如果己方也是 Candidate，即出现竞选竞争的现象，且两者的 ```CurrentTerm``` 一样大，也拒绝对方的投票请求。若对方的 ```CurrentTerm``` 比己方的 ```CurrentTerm``` 大，则更新己方的 ```CurrentTerm``` ，并将己方的状态改为 Follower。最后，比较对方的 Log 与自己的 Log，若对方的 Log 没有自己的新，也拒绝对方的投票。反之，接受对方为 Leader，并为对方投票。比较两个 Log 谁更新的标准为：最后一个日志的 Term 较大者日志较新，或者在最后一个日志的 Term 一样大的情况下，日志长度较长者日志较新。另外，边界情况如己方 Log 长度为 0 需单独处理。处理选票请求的代码如下：

    ```go
    func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
    	rf.mu.Lock()
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
                // leader's log is at least as fresh as mine
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
    }
    ```

- 心跳机制

  - 成功当选 Leader 后，节点需要每隔一段时间为其他节点发送心跳，若针对某个其他节点有待发送的日志需要同步，则会在心跳报文的 Entry 段添加 Log 信息（这个会在下一章节提到），否则心跳报文的 Entry 段为空（```nil```）。而其他节点接收到心跳报文后，重置心跳计时器，并保持自身状态为 Follower。Leader 发送心跳的方式与 Candidate 发送投票请求相似，开启多个子协程，每个协程单独处理与一个节点的通信，包括构建 RPC 调用所需的参数，以及收到对方回复后的处理，若发现对方的 ```CurrentTerm``` 比己方的 ```CurrentTerm``` 大，则将自己的状态改为 Follower，并提前退出。这一模块大部分内容与后续 Log Replication 相关，会在后续详细展开介绍，并展示代码实现。

### 实验二 Log Replication

- Log 同步机制

  - Leader 从 Client 那里得到要同步的 command，会先将该 command 打包成 LogEntry 添加到现有的 Log 后面（根据 append-only 机制）。作为 Leader 要维护两个数组 `NextIndex` 和 `MatchIndex`， `NextIndex[i]` 存储第 i 个节点下一步需要同步的 Log 的起始索引，`MatchIndex[i]` 存储第 i 个节点已经匹配的 Log 的索引。当接收到节点的心跳回复后，首先判断当前自身状态是否还是 Leader，同时判断发送前的 ```CurrentTerm``` 与当前 ```CurrentTerm``` 是否保持一致（比较 `rf.CurrentTerm` 与 `args.Term`），这样做的原因在于在不可靠网络下，因为网络时延的缘故，可能会在等待 RPC 调用的过程中发生了多次 Leader 的转变并且自身依然是 Leader 的情况，但是此时的  ```CurrentTerm``` 已经发生了很大的变化，不应该处理以前发送的 RPC 回复。满足这两个条件后，若 RPC 调用成功即 ok 为 `true` 的话，开始处理节点的回复信息。处理节点的回复信息时，若 `reply.Success` 为 `true` ，则说明对方将己方之前发送过去的待发送的 Log 同步成功，则相应的，需要更新该节点的 `NextIndex` 和 `MatchIndex` 信息，首先将  `MatchIndex[i]` 更新为 `args.PrevLogIndex + len(args.Entries)` ，而  `NextIndex[i]` 即为下一个要发送的索引位置 `rf.MatchIndex[i]+1`。若 `reply.Success` 为 `false` ，则说明发生了 Log 的冲突，这个在后续讨论。具体 Log 发送的代码如下（包括心跳的发送）：

    ```go
    func (rf *Raft) HeartbeatLoop () {
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
                // if have some entries to send, args.Entries != nil
    			args.Entries = rf.Logs[rf.NextIndex[i]-1:]
    			if args.PrevLogIndex > 0 {
    				args.PrevLogTerm = rf.Logs[args.PrevLogIndex-1].Term
    			}
    			rf.mu.Unlock()
    			go rf.SendEntry(i, args)
    		}
    		time.Sleep(time.Duration(100 * time.Millisecond))
    	}
    }
    
    func (rf *Raft) SendEntry (i int, args AppendEntriesArgs) {
    	reply := &AppendEntriesReply{
    		Term:       0,
    		FirstIndex: 0,
    		Success:    false,
    	}
    	ok := rf.sendAppendEntries(i, args, reply)
    	rf.mu.Lock()
    	defer rf.mu.Unlock()
    	if rf.CurrentTerm == args.Term && rf.Role == 2 && ok {
    		if reply.Success { //  assert rf.NextIndex[i] <= len(rf.Logs)
    			rf.MatchIndex[i] = args.PrevLogIndex + len(args.Entries)
    			rf.NextIndex[i] = rf.MatchIndex[i]+1
    		}
    		rf.Match()
    	}
    }
    ```

  - Leader 节点更新了自己的 `MatchIndex` 数组后，需要更新自己的 `CommitIndex`，将其他节点的 `MatchIndex` 信息加上子集的 Log 长度，对该新数组排序后，发现中位数比原先的 `CommitIndex` 大的话，则更新自身的 `CommitIndex`。因为次数说明系统中有超过半数的节点都提交了新 `CommitIndex` 前的所有日志。具体更新 Leader `CommitIndex` 的具体代码如下：

    ```go
    func (rf *Raft) Match () {
    	array := []int{}
    	for i := 0; i < len(rf.peers); i++ {
    		array = append(array, rf.MatchIndex[i])
    	}
    	sort.Ints(array)
    	if rf.CommitIndex < array[len(rf.peers)/2] && rf.Logs[array[len(rf.peers)/2] - 1].Term == rf.CurrentTerm {
    		rf.CommitIndex = array[len(rf.peers)/2]
    	}
    }
    ```

    

  - Follower 节点接收到心跳报文后，首先判断双方的  ```CurrentTerm``` 谁较大，若己方较大，则拒绝对方，回复 `false` 并设置 `reply.Term = rf.CurrentTerm`，立刻返回。反之，若对方较大，则更新自己的  ```CurrentTerm```，并设置己方的状态为 `Follower`。若己方状态为 Candidate，则也将己方状态设置为 `Follower`，并更新自己的  ```CurrentTerm```。经过上述关于  ```CurrentTerm``` 的比较检查后，随机重置己方的心跳计时器，确认收到了 Leader 的心跳报文。下面判断是否发生了 Log 冲突：冲突一，己方与 Leader 的 `PrevLogIndex` 处 LogEntry 的 Term 不一致；冲突二，己方 Log 长度比 Leader 的  `PrevLogIndex` 小。这两者冲突都需要拒绝 Leader 的心跳报文，设置 reply 中的 Term 信息为 -1，并立刻返回。若既没有发生上述两种冲突，则更新自身的 Log，将 Leader 发送的日志从 `PrevLogIndex` 后一位处开始覆盖。最后，若己方 `CommitIndex` 小于 Leader 的  `CommitIndex`，则更新自身的 `CommitIndex` 为 己方日志长度和 Leader 的 `CommitIndex` 两者中较小的一个。Follower 节点具体同步自身日志的代码如下：

    ```go
    if rf.Role == 0 {
        // update logs and append entries
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
            rf.Logs = append(rf.Logs, entry)
        }
        rf.persist()
    }
    // update commit index
    if args.LeaderCommit > rf.CommitIndex {
        if args.LeaderCommit < len(rf.Logs) {
            rf.CommitIndex = args.LeaderCommit
        } else {
            rf.CommitIndex = len(rf.Logs)
        }
    }
    ```

- Log 冲突处理

  - Leader 节点处理 `reply.Success` 为 `false` 的情况，主要分为两种：第一种若对方的 ```CurrentTerm``` 比己方的 ```CurrentTerm``` 大，则将自己的状态改为 Follower，并提前退出。第二种为 Log 冲突的情况，即 `PrevLogIndex` 位置的 LogEntry 不一致或 Leader 发出的 `PrevLogIndex` 大于对应 Follower 的 Log 长度。此时需将 Leader 节点信息中`NextIndex[i]`回退，保守回退的方法为将 `NextIndex[i]` 自减一，并更新对应的 `MatchIndex[i]` 为 `NextIndex[i]-1`，这里先采用 trivial 的回退方案，在下一章展开讨论回退优化，以应对复杂的测试样例，减少系统的运行时间。

- Apply Message 线程

  - 每个节点在初始化（调用 Make）后，都会开启一个 Apply Message 子协程，用于将标记为已提交的 LogEntry 逐一实际提交到 Client 。为了避免该协程频繁抢占 Raft 节点中的互斥锁，虽然每一次调用时都会将未提交的 LogEntry 全都提交，但由于频繁的抢占共享资源，实际运行时实际只提交一个 LogEntry，极大地降低了系统运行时间，导致某些复杂用例运行时间过长，甚至出现超时的现象。因此在每一次成功提交 Log 后，都会将该线程挂起 50 毫秒，将资源释放给其他线程。根据实验结果，这一改变极大地降低了提交 Log 所占用的时间。具体实现的代码如下：

    ```go
    func (rf *Raft) Apply () {
    	for !rf.isKilled {
    		rf.mu.Lock()
    		if rf.LastApplied < rf.CommitIndex {
    			prevApllied := rf.LastApplied+1
    			var commands []interface{}
    			rf.LastApplied = rf.CommitIndex
    			for i := prevApllied ; i <= rf.CommitIndex; i++ {
    				commands = append(commands, rf.Logs[i-1].Command)
    			}
    			rf.mu.Unlock()
                // apply messages one by one
    			for i, command := range commands {
    				tempMsg := ApplyMsg{
    					Index:   prevApllied+i,
    					Command: command,
    				}
                    // use channel to apply message
    				rf.applyMsg <- tempMsg
    			}
    		} else {
    			rf.mu.Unlock()
    		}
    		time.Sleep(time.Duration(50 * time.Millisecond))
    	}
    }
    ```

    

### 实验三 Persist 与 Unreliable Network

- Persist 处理

  - Persist 机制用于节点 crash 后的状态恢复，涉及到三个持久化信息的存储与读取，分别是 `currentTerm`，`votedFor` 和 `log[]`，表示节点当前的 term，leader 投票选择的节点序号，以及当前所存储的日志信息。因此在设计到这三个变量的写操作时，需要调用 persist 函数，对节点的这三个信息进行编码存储，而当节点重启后，在 Make 函数中会调用 readPersist 函数读取 crash 前最后一次存储的持久化信息（如果该节点未 crash 过，则不会读取到持久化信息）。具体 persist 函数和 readPersist 函数的代码实现如下：

    ```go
    // save Raft's persistent state to stable storage
    func (rf *Raft) persist() {
    	w := new(bytes.Buffer)
    	e := gob.NewEncoder(w)
    	e.Encode(rf.CurrentTerm)
    	e.Encode(rf.VotedFor)
    	e.Encode(rf.Logs)
    	data := w.Bytes()
    	rf.persister.SaveRaftState(data)
    }
    
    // restore previously persisted state.
    func (rf *Raft) readPersist(data []byte) {
    	r := bytes.NewBuffer(data)
    	d := gob.NewDecoder(r)
    	d.Decode(&rf.CurrentTerm)
    	d.Decode(&rf.VotedFor)
    	d.Decode(&rf.Logs)
    }
    ```

- Log 冲突的回退优化

  - 考虑到实验三中部分测试样例较为复杂，且随机性较大，例如 Figure8 Unreliable，在某些随机情况下，先前提到过的 Leader 应对 Log 冲突的方法，选择将 `NextIndex[i]`（ i 为冲突的节点序号）每次递减1，在极端情况和不可靠网络下，会出现大范围的 Log 冲突以及大概率的丢包现象。因此每次只是将待发送的 Log 序号减一，会继续频繁地发生冲突，因此参考 raft 论文修订版 page 8 ，扩增 Follower 节点对于 Leader 节点的 reply 结构体信息，新增冲突索引项 FirstIndex，用于记录冲突的 LogEntry 的 Term 第一次出现的索引位置。Leader 接收到 reply 信息后，设置对应的 `NextIndex[i]` 为 `reply.FirstIndex` ，测试结果表明，极大的降低了测试样例的运行时间，使得系统能在规定时间内达成一致（Agreement）。具体实现的代码如下：

    ```go
    type AppendEntriesReply struct {
    	Term int
        // optimization for log mismatch
    	FirstIndex int
    	Success bool
    }
    
    func (rf *Raft) AppendEntries (args AppendEntriesArgs, reply *AppendEntriesReply) {
        // log mismatch
        if args.PrevLogIndex != 0 && args.PrevLogIndex <= len(rf.Logs) && rf.Logs[args.PrevLogIndex-1].Term != args.PrevLogTerm {
            reply.Term = rf.Logs[args.PrevLogIndex-1].Term
            k := args.PrevLogIndex
            for ; k > 0; k-- {
                // the first index
                if rf.Logs[k-1].Term != reply.Term {
                    break
                }
            }
            reply.FirstIndex = k+1
            reply.Success = false
            return
        }
    }
    ```

## 功能展示

- 执行 `go test` 测试所有的测试用例，测试结果全部 PASS。考虑到有些测试用例存在随机性，于是编写脚本执行 `go test` 指令 100 遍，结果全部正确，验证了系统的正确性。以下为执行所有测试用例的结果以及测试 100 次后全部通过的截图。

  <img src="C:\Users\ASUS\AppData\Roaming\Typora\typora-user-images\image-20201229152752590.png" alt="image-20201229152752590" style="zoom: 67%;" />

  ![image-20201229160041662](C:\Users\ASUS\AppData\Roaming\Typora\typora-user-images\image-20201229160041662.png)



## 实验总结

- 本次实验通过阅读 Raft 论文，对分布式一致性算法有了由浅入深的了解。主从机制对于分布式系统的一致性维护有着至关重要的作用， Raft 在 Paxos 的基础上，简化了原本复杂的共识算法，使得每一个 Term 只会有一个 Leader 出现，极大地简化了系统的复杂度，使得算法便于理解，同时保证了分布式存储系统的强一致性。
- Go 语言作为百万级并发的程序语言，为实验的顺利推进提供了便利的数据结构和语言机制，例如 channel 数据结构和 goroutine 机制，前者方便了 Candidate 节点发送选票和收集选票的过程，后者使得发送心跳以及发送选票与原来的父线程分离开来，实现异步执行。
- 观察所实现的算法在各个测试用例下的表现，相比于 Paxos 算法，Raft 算法在网络环境较好的情况下更有优势，因为无需频繁的更换 Leader 节点，所以可以很快捷的达成共识。而在网络环境不可靠且时常丢包的情况下， Raft 会时常出现 Leader 重选的情况，因此极大地延长了系统达成共识的时间，而在此种环境下， Paxos 算法就体现了其优势，因为其拥有多个 Leader 节点，可以很好的应对极端的网络环境。
- 本实验成功完成了任务一，任务二和任务三，并循环测试了 100 次，全部通过。