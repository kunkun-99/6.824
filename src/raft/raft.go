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
	//	"bytes"

	"bytes"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"

	"6.824/labgob"
	"6.824/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

type LogEntry struct {
	Command  interface{}
	Term     int
	LogIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm       int
	votedFor          int
	log               []LogEntry
	lastIncludedIndex int
	lastIncludedTerm  int

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	state        State
	electionTime time.Time
	heatBeat     time.Duration
	leadId       int

	isApplier bool
	applyCh   chan ApplyMsg
	cond      *sync.Cond
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isleader = false
	term = rf.currentTerm
	if rf.state == Leader {
		isleader = true
	}
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs []LogEntry
	var lastIncludedIndex int
	var lastIncludedTerm int
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&logs) != nil ||
		d.Decode(&lastIncludedIndex) != nil || d.Decode(&lastIncludedTerm) != nil {
		log.Fatal("failed to read persist\n")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = logs
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
		rf.lastApplied = lastIncludedIndex
		rf.commitIndex = lastIncludedIndex
	}
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index < rf.lastIncludedIndex {
		return
	}
	for i, log := range rf.log {
		if log.LogIndex == index {
			rf.lastIncludedIndex = index
			rf.lastIncludedTerm = log.Term
			emptyLog := make([]LogEntry, 1)
			rf.log = rf.log[i+1:]
			rf.log = append(emptyLog, rf.log...)
		}
	}
	rf.log[0].LogIndex = rf.lastIncludedIndex
	rf.log[0].Term = rf.lastIncludedTerm
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	data := w.Bytes()
	DPrintf("rf %d Snapshot,loglen: %d,lastIncludedIndex:%d\n", rf.me, len(rf.log), rf.lastIncludedIndex)
	rf.persister.SaveStateAndSnapshot(data, snapshot)
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	// Your data here (2A, 2B).
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	// Your data here (2A, 2B).
	Term    int
	Success bool
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) GetRaftStateSize() int {
	return rf.persister.RaftStateSize()
}

func (rf *Raft) GetSnapShot() []byte {
	return rf.persister.ReadSnapshot()
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if args.Term < rf.currentTerm {
		return
	}
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
		rf.persist()
	}
	lastRfLogIndex := rf.log[len(rf.log)-1].LogIndex
	lastRfLogTerm := rf.log[len(rf.log)-1].Term
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && (lastRfLogTerm < args.LastLogTerm || lastRfLogTerm == args.LastLogTerm && lastRfLogIndex <= args.LastLogIndex) {
		DPrintf("Raft %d granted vote to %d,lastRfLogIndex:%d,lastRfLogTerm:%d,args.LastLogIndex:%d,args.Term:%d", rf.me, args.CandidateId, lastRfLogIndex, lastRfLogTerm, args.LastLogIndex, args.Term)
		reply.Term = args.Term
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.persist()
		rf.electionTime = time.Now().Add(time.Duration((200 + rand.Intn(200))) * time.Millisecond)
		return
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// AppendEntries RPC:1
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	// here may be need modified!
	DPrintf("Raft %d receive %d's AE and rest election time\n", rf.me, args.LeaderId)
	rf.electionTime = time.Now().Add(time.Duration((200 + rand.Intn(200))) * time.Millisecond)
	reply.Term = args.Term
	reply.Success = true
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = Follower
		rf.persist()
		return
	}
	// AppendEntries RPC:2
	if rf.log[len(rf.log)-1].LogIndex < args.PrevLogIndex {
		reply.Success = false
		return
	}
	if args.PrevLogIndex < rf.lastIncludedIndex {
		return
	}
	// AppendEntries RPC:3
	DPrintf("args.PrevLogIndex:%d,rf.lastIncludedIndex:%d\n", args.PrevLogIndex, rf.lastIncludedIndex)
	if rf.log[args.PrevLogIndex-rf.lastIncludedIndex].Term != args.PrevLogTerm {
		reply.Success = false
		return
	}
	// AppendEntries RPC:4
	for i, entry := range args.Entries {
		if entry.LogIndex <= rf.log[len(rf.log)-1].LogIndex && entry.Term != rf.log[entry.LogIndex-rf.lastIncludedIndex].Term {
			rf.log = rf.log[:entry.LogIndex-rf.lastIncludedIndex]
			rf.persist()
		}
		if entry.LogIndex > rf.log[len(rf.log)-1].LogIndex {
			rf.log = append(rf.log, args.Entries[i:]...)
			rf.persist()
			break
		}
	}
	DPrintf("args.PrevLogIndex:%d\n", args.PrevLogIndex)
	DPrintf("%d append success,length %d and log length is %d\n", rf.me, len(args.Entries), len(rf.log))
	// AppendEntries RPC:5
	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit > rf.log[len(rf.log)-1].LogIndex {
			rf.commitIndex = rf.log[len(rf.log)-1].LogIndex
		} else {
			rf.commitIndex = args.LeaderCommit
		}
		rf.cond.Broadcast()
	}
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		return
	}
	reply.Term = args.Term
	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		return
	}
	if rf.isApplier == true {
		return
	}
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	if rf.log[len(rf.log)-1].LogIndex < rf.lastIncludedIndex {
		rf.log = rf.log[0:1]
	} else {
		for i, log := range rf.log {
			if log.LogIndex == rf.lastIncludedIndex {
				emptyLog := make([]LogEntry, 1)
				rf.log = rf.log[i+1:]
				rf.log = append(emptyLog, rf.log...)
			}
		}
	}
	rf.log[0].LogIndex = rf.lastIncludedIndex
	rf.log[0].Term = rf.lastIncludedTerm
	if rf.lastApplied < rf.lastIncludedIndex {
		rf.lastApplied = rf.lastIncludedIndex
	}
	if rf.commitIndex < rf.lastIncludedIndex {
		rf.commitIndex = rf.lastIncludedIndex
	}
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	data := w.Bytes()

	rf.persister.SaveStateAndSnapshot(data, args.Data)
	applyMsg := ApplyMsg{
		CommandValid:  false,
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotIndex: rf.lastIncludedIndex,
		SnapshotTerm:  rf.lastIncludedTerm,
	}
	DPrintf("in InstallSnapshot,rf.lastApplied = %d\n", rf.lastApplied)
	// fmt.Printf("InstallSnapshot before applyMsg\n")
	rf.mu.Unlock()
	DPrintf("before InstallSnapshot\n")
	rf.applyCh <- applyMsg
	DPrintf("after InstallSnapshot\n")
	rf.mu.Lock()
	DPrintf("Raft %d InstallSnapshot,rf.lastApplied = %d\n", rf.me, rf.lastApplied)
	// fmt.Printf("InstallSnapshot after applyMsg\n")
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
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2B).
	if rf.state != Leader {
		return -1, rf.currentTerm, false
	}
	index = rf.log[len(rf.log)-1].LogIndex + 1
	term = rf.currentTerm
	entry := LogEntry{
		Command:  command,
		Term:     term,
		LogIndex: index,
	}
	rf.log = append(rf.log, entry)
	DPrintf("rf%d append one log\n", rf.me)
	rf.LeaderAppendEntries()
	rf.persist()
	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) LeaderElection() bool {
	DPrintf("Raft %d begin election because overtime at term %d\n", rf.me, rf.currentTerm)
	rf.electionTime = time.Now().Add(time.Duration((200 + rand.Intn(200))) * time.Millisecond)
	rf.currentTerm++
	rf.state = Candidate
	rf.votedFor = rf.me
	voteCount := 1
	rf.persist()
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.log[len(rf.log)-1].LogIndex,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}
	var once sync.Once
	for i, _ := range rf.peers {
		if i != rf.me {
			go func(i int) {
				reply := RequestVoteReply{}
				ok := rf.sendRequestVote(i, &args, &reply)
				if !ok {
					return
				}
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.state = Follower
					rf.votedFor = -1
					rf.persist()
					return
				}
				if !reply.VoteGranted {
					return
				}
				voteCount++
				if voteCount > len(rf.peers)/2 && rf.currentTerm == args.Term && rf.state == Candidate {
					once.Do(func() {
						rf.state = Leader
						DPrintf("%d become leader at term  %d,and log index is %d\n", rf.me, rf.currentTerm, rf.log[len(rf.log)-1].LogIndex)
						for j, _ := range rf.peers {
							rf.nextIndex[j] = rf.log[len(rf.log)-1].LogIndex + 1
							rf.matchIndex[j] = 0
						}
						rf.LeaderAppendEntries()
					})
				}

			}(i)
		}
	}

	return true
}

func (rf *Raft) CommitRules() {
	if rf.state != Leader {
		return
	}
	for n := rf.commitIndex + 1; n <= rf.log[len(rf.log)-1].LogIndex; n++ {
		if rf.log[n-rf.lastIncludedIndex].Term != rf.currentTerm {
			continue
		}
		counter := 1
		for serverId, _ := range rf.peers {
			if serverId != rf.me && rf.matchIndex[serverId] >= n {
				DPrintf("rf%d.matchIndex[%d]==%d >= %d\n", rf.me, serverId, rf.matchIndex[serverId], n)
				counter++
			}
			if counter > len(rf.peers)/2 {
				rf.commitIndex = n
				rf.cond.Broadcast()
				DPrintf("Raft %d,update commitIndex:%d and broadcast!\n", rf.me, n)
				break
			}
		}
	}

}

func (rf *Raft) LeaderSendSnapShot(id int) {
	rf.mu.Lock()
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Data:              rf.persister.ReadSnapshot(),
	}
	reply := InstallSnapshotReply{}
	rf.mu.Unlock()
	ok := rf.sendInstallSnapshot(id, &args, &reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = Follower
		rf.votedFor = -1
		rf.persist()
		rf.mu.Unlock()
		return
	}
	rf.matchIndex[id] = rf.lastIncludedIndex
	rf.nextIndex[id] = rf.lastIncludedIndex + 1
	DPrintf("leader %d send Snapshot to raft %d, rf[%d].lastIncludedIndex:%d\n", rf.me, id, rf.me, rf.lastIncludedIndex)
	rf.mu.Unlock()
	return

}

func (rf *Raft) LeaderAppendEntries() {
	for i, _ := range rf.peers {
		if i != rf.me {
			go func(id int) {
				retry := true
				rf.mu.Lock()
				defer rf.mu.Unlock()
				for {
					if !retry {
						break
					}
					retry = false
					if rf.state != Leader {
						return
					}
					// 宕机后的恢复,好像没啥用...不在这里恢复
					if rf.nextIndex[id] == 0 {
						rf.nextIndex[id] = rf.log[len(rf.log)-1].LogIndex + 1
					}
					if rf.nextIndex[id] <= rf.lastIncludedIndex {
						go rf.LeaderSendSnapShot(id)
						return
					}
					logLen := len(rf.log)
					// fmt.Printf("entry len %d\n", rf.log[len(rf.log)-1].LogIndex-rf.nextIndex[id]+1)
					args := AppendEntriesArgs{
						Term:         rf.currentTerm,
						LeaderId:     rf.me,
						PrevLogIndex: rf.log[rf.nextIndex[id]-1-rf.lastIncludedIndex].LogIndex,
						PrevLogTerm:  rf.log[rf.nextIndex[id]-1-rf.lastIncludedIndex].Term,
						Entries:      make([]LogEntry, rf.log[len(rf.log)-1].LogIndex-rf.nextIndex[id]+1),
						LeaderCommit: rf.commitIndex,
					}
					copy(args.Entries, rf.log[rf.nextIndex[id]-rf.lastIncludedIndex:])
					DPrintf("Raft %d PrevLogIndex %d,loglen %d\n", rf.me, rf.log[rf.nextIndex[id]-1-rf.lastIncludedIndex].LogIndex, len(rf.log))
					DPrintf("Raft %d append length %d to Raft %d,logindex:%d,nextindex:%d\n", rf.me, len(args.Entries), id, rf.log[len(rf.log)-1].LogIndex, rf.nextIndex[id])
					rf.mu.Unlock()
					reply := AppendEntriesReply{}
					// 注意，在调用RPC前不能加锁，如果联系不上对面，有一个超时时间，那么这段时间锁会一直加上，那么该服务器做不了其他事情
					// 例如有follower1和2，1宕机或者网络分区了，若给1发送心跳，会一直卡住，导致2无法接收到心跳包。2发起选举，同样得不到leader的投票
					ok := rf.sendAppendEntries(id, &args, &reply)
					if !ok {
						// 否则会出现unlock 已经unlock的锁的情况
						rf.mu.Lock()
						return
					}
					rf.mu.Lock()
					DPrintf("Raft %d send AE to %d\n", rf.me, id)
					if reply.Term > rf.currentTerm {
						DPrintf("%d become follower because peers' term bigger then it,%d < %d\n", rf.me, rf.currentTerm, reply.Term)
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						rf.state = Follower
						rf.persist()
						return
					}
					// 防止出现RPC延迟时，log增加了导致的前后不一致。
					// RPC消息也可能时乱序的，这次接收可能收到的是以前的RPC恢复
					if logLen != len(rf.log) {
						retry = true
					}
					if !reply.Success {
						rf.nextIndex[id]--
						retry = true
					}

				}
				rf.nextIndex[id] = rf.log[len(rf.log)-1].LogIndex + 1
				rf.matchIndex[id] = rf.nextIndex[id] - 1
				DPrintf("rf%d.log length==%d\n", rf.me, len(rf.log))
				DPrintf("rf%d.nextIndex[%d]==%d\n", rf.me, id, rf.nextIndex[id])
				DPrintf("rf%d.matchIndex[%d]==%d\n", rf.me, id, rf.matchIndex[id])
				// if rf.matchIndex[id] > rf.commitIndex {
				rf.CommitRules()

			}(i)
		}
	}

}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) heatBeatTicker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		time.Sleep(rf.heatBeat)
		rf.mu.Lock()
		if rf.state == Leader {
			rf.LeaderAppendEntries()
		}
		rf.mu.Unlock()

	}
}
func (rf *Raft) LeaderElectionTicker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		rf.mu.Lock()
		sleepTime := rf.electionTime.Sub(time.Now())
		rf.mu.Unlock()

		time.Sleep(sleepTime)
		rf.mu.Lock()
		if rf.state != Leader && time.Now().After(rf.electionTime) {
			rf.LeaderElection()
		}
		rf.mu.Unlock()

	}
}

func (rf *Raft) applier() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.mu.Lock()

		if rf.lastApplied < rf.commitIndex {
			rf.isApplier = true
			rf.lastApplied++
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.lastApplied-rf.lastIncludedIndex].Command,
				CommandIndex: rf.log[rf.lastApplied-rf.lastIncludedIndex].LogIndex,
				CommandTerm:  rf.log[rf.lastApplied-rf.lastIncludedIndex].Term,
			}
			DPrintf("in applier rf.lastApplied = %d\n", rf.lastApplied)
			rf.mu.Unlock()
			DPrintf("before applier\n")
			rf.applyCh <- applyMsg
			// fmt.Printf("-----------send applyCh---------\n")
			DPrintf("after applier\n")
			rf.mu.Lock()
			DPrintf("Raft %d apply:%d commitIndex:%d,logindex:%d\n", rf.me, applyMsg.CommandIndex, rf.commitIndex, rf.log[len(rf.log)-1].LogIndex)
		} else {
			rf.isApplier = false
			rf.cond.Wait()
		}
		rf.mu.Unlock()

	}
}

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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0
	rf.heatBeat = 100 * time.Millisecond
	rf.log = make([]LogEntry, 1)
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.commitIndex = 0
	rf.lastApplied = 0
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.applyCh = applyCh
	rf.cond = sync.NewCond(&rf.mu)

	// start ticker goroutine to start elections
	go rf.heatBeatTicker()
	go rf.LeaderElectionTicker()
	go rf.applier()

	return rf
}
