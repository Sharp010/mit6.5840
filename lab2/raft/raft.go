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
	"6.5840/labgob"
	"bytes"
	"fmt"
	"log"
	"os"
	"strconv"

	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

type logTopic string

const (
	dClient  logTopic = "CLNT"
	dCommit  logTopic = "CMIT"
	dDrop    logTopic = "DROP"
	dError   logTopic = "ERRO"
	dInfo    logTopic = "INFO"
	dLeader  logTopic = "LEAD"
	dLog     logTopic = "LOG1"
	dLog2    logTopic = "LOG2"
	dPersist logTopic = "PERS"
	dSnap    logTopic = "SNAP"
	dTerm    logTopic = "TERM"
	dTest    logTopic = "TEST"
	dTimer   logTopic = "TIMR"
	dTrace   logTopic = "TRCE"
	dVote    logTopic = "VOTE"
	dWarn    logTopic = "WARN"
)

var debugStart time.Time
var debugVerbosity int
var debug int

func getVerbosity() int {
	v := os.Getenv("VERBOSE")
	level := 0
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	return level
}
func init() {
	debugVerbosity = getVerbosity()
	debugStart = time.Now()

	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func Debug_(topic logTopic, format string, a ...interface{}) {
	if debug == 1 {
		time := time.Since(debugStart).Microseconds()
		time /= 1000
		prefix := fmt.Sprintf("%06d %v ", time, string(topic))
		format = prefix + format
		log.Printf(format, a...)
	}
}

type Entry struct {
	Term    int
	Command interface{}
}
type State int

const (
	Follower State = iota + 1
	Candidate
	Leader
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

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
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
	currentTerm int
	votedFor    int
	log         []Entry

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	state      State
	applyCh    chan<- ApplyMsg
	newCommit  chan interface{}
	newCommand chan interface{}
	tick       *time.Timer

	// snapshot
	lastIndexSnap int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	term = rf.currentTerm
	isleader = rf.state == Leader
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).

func (rf *Raft) Encode(CurrentTerm int, VotedFor int, LastIndexSnap int, Log []Entry) []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(CurrentTerm) != nil ||
		e.Encode(VotedFor) != nil ||
		e.Encode(LastIndexSnap) != nil ||
		e.Encode(Log) != nil {
		log.Fatalf("Encode Persist Error !\n")
	}
	raftstate := w.Bytes()
	return raftstate
}
func (rf *Raft) persist() {
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	raftState := rf.Encode(rf.currentTerm, rf.votedFor, rf.lastIndexSnap, rf.log)
	snapshot := rf.persister.ReadSnapshot()
	rf.persister.Save(raftState, snapshot)
}
func (rf *Raft) Decode(data []byte) (int, int, int, []Entry) {
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var DTerm int
	var DVoteFor int
	var DLog []Entry
	var DLastIndexSnap int
	if d.Decode(&DTerm) != nil ||
		d.Decode(&DVoteFor) != nil ||
		d.Decode(&DLastIndexSnap) != nil ||
		d.Decode(&DLog) != nil {
		log.Fatalf("Read Persist Error!\n")
	}
	return DTerm, DVoteFor, DLastIndexSnap, DLog
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	rf.currentTerm, rf.votedFor, rf.lastIndexSnap, rf.log = rf.Decode(data)
	rf.commitIndex = rf.lastIndexSnap
	rf.lastApplied = rf.lastIndexSnap
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	Debug_(dSnap, "S%d [Snapshot] Idx %d LSI %d", rf.me, index, rf.lastIndexSnap)
	// trim: 将Index处及以前的日志切掉
	// 找到index在log中的index
	trimIdx := index - rf.lastIndexSnap
	if trimIdx >= 0 {
		// 切掉日志 同时保留哨兵 -> 使用lastSnap作为哨兵
		rf.log = rf.log[trimIdx:]
	}
	// persist:  lastIndexSnap和snap需要同时Save 不然crash后恢复会不一致
	rf.lastIndexSnap = index
	raftState := rf.Encode(rf.currentTerm, rf.votedFor, rf.lastIndexSnap, rf.log)
	rf.persister.Save(raftState, snapshot)
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
type AppendArgs struct {
	Term         int
	LeaderId     int
	PreLogIndex  int
	PreLogTerm   int
	Entries      []Entry
	LeaderCommit int
}

type AppendReply struct {
	Term    int
	Success bool
	XTerm   int
	XIndex  int
	XLen    int
}
type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Offset            int
	Data              []byte
	Done              bool
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) AppendEntries(args *AppendArgs, reply *AppendReply) {
	Debug_(dTimer, "S%d <-[AE] S%d 0", rf.me, args.LeaderId)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	Debug_(dTimer, "S%d <-[AE] S%d 1", rf.me, args.LeaderId)
	// 小于自己的任期 拒绝
	if args.Term < rf.currentTerm || args.PreLogIndex < rf.lastIndexSnap {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	// 大于自己的任期  不管自己是leader还是candidate 都要变成follower 再检查日志
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	} else if args.Term == rf.currentTerm && rf.state == Candidate {
		rf.becomeFollower(args.Term)
	}
	// 等于自己的任期
	var xTerm int
	if args.PreLogIndex <= len(rf.log)-1+rf.lastIndexSnap {
		xTerm = rf.log[args.PreLogIndex-rf.lastIndexSnap].Term
	} else {
		xTerm = 0
	}
	Debug_(dLog2, "S%d [AE] =-> S%d PRETerm %d xTerm %d LSI %d Len %d", rf.me, args.LeaderId, args.PreLogTerm, xTerm, rf.lastIndexSnap, len(rf.log))
	// 日志多于自己 或者 pre处日志不相同  =-> false
	if args.PreLogTerm != xTerm || args.PreLogIndex > len(rf.log)-1+rf.lastIndexSnap {
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.XTerm = xTerm
		reply.XLen = len(rf.log)
		reply.XIndex = 0
		// find first
		for i := 1; i <= len(rf.log)-1; i++ {
			if rf.log[i].Term == xTerm {
				reply.XIndex = i + rf.lastIndexSnap
				break
			}
		}
		if reply.XIndex == 0 {
			reply.XIndex = rf.lastIndexSnap
		}
		rf.tick.Reset(randomElectionTimeout())
		return
	}

	// pre处日志相同,将follower pre后面的位置与leader对齐 =->true
	rf.log = rf.log[:args.PreLogIndex+1-rf.lastIndexSnap]
	rf.log = append(rf.log, args.Entries...)
	// 检查是否apply
	oldCommit := rf.commitIndex
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = intMin(args.LeaderCommit, len(rf.log)-1+rf.lastIndexSnap)
	}
	if rf.commitIndex > oldCommit {
		go func() {
			//Debug_(dCommit, "S%d newCommit signal!", rf.me)
			rf.newCommit <- struct{}{}
		}()
	}
	if rf.votedFor == -1 {
		rf.votedFor = args.LeaderId
	}
	reply.Success = true
	reply.Term = rf.currentTerm
	reply.XLen = len(rf.log)
	rf.persist()
	Debug_(dLog2, "S%d [AE Success] =-> S%d  XLen %d OC %d NC %d", rf.me, args.LeaderId, reply.XLen, oldCommit, rf.commitIndex)
	rf.tick.Reset(randomElectionTimeout())
	return
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	Debug_(dVote, "S%d [RV]-> S%d  T%d -> T%d LI %d LT %d li %d lt %d",
		args.CandidateId, rf.me, args.Term, rf.currentTerm, args.LastLogIndex, args.LastLogTerm, len(rf.log)-1, rf.log[len(rf.log)-1].Term)
	// 任期小于自己直接拒绝
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}
	// args.Term = rf.currentTerm
	// 自己是candidate或leader  拒绝
	if rf.state == Leader || rf.state == Candidate {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	} else if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		lastIndex := len(rf.log) - 1 + rf.lastIndexSnap
		lastLogTerm := rf.log[len(rf.log)-1].Term
		// 检查对方日志是否更新 yes
		if args.LastLogTerm > lastLogTerm ||
			args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastIndex {

			reply.VoteGranted = true
			reply.Term = rf.currentTerm
			rf.votedFor = args.CandidateId

			rf.persist()
			rf.tick.Reset(randomElectionTimeout())
			return
		}
		// no
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	} else {
		// 自己已经投票,有新的term相同的candidate请求自己的投票直接拒绝
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	Debug_(dSnap, "S%d <-=[SN] S%d PRE", rf.me, args.LeaderId)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	Debug_(dSnap, "S%d <-=[SN] S%d LSI %d ArgsLSI %d", rf.me, args.LeaderId, rf.lastIndexSnap, args.LastIncludedIndex)
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}
	reply.Term = rf.currentTerm
	// 接受到旧Snapshot,直接返回
	if args.Term < rf.currentTerm ||
		args.LastIncludedIndex < rf.lastIndexSnap ||
		args.LastIncludedIndex <= rf.commitIndex {
		return
	}

	// 第一次InstallSnapshot -> 清空 snapshot 和 entry
	if rf.lastIndexSnap != args.LastIncludedIndex {
		rf.persister.Save(rf.persister.ReadRaftState(), nil)
		rf.lastIndexSnap = args.LastIncludedIndex
		// 清空并设置哨兵
		rf.log = rf.log[:0]
		rf.log = append(rf.log, Entry{
			Term:    args.LastIncludedTerm,
			Command: nil,
		})
	}
	// 将接受的Snapshot保存
	snapshot := rf.persister.ReadSnapshot()
	if len(snapshot) == args.Offset {
		snapshot = append(snapshot, args.Data...)
		rf.persister.Save(rf.persister.ReadRaftState(), snapshot)
		rf.tick.Reset(randomElectionTimeout())
	} else {
		// 接受到相同的Snapshot
		return
	}
	// 接收完毕,更新状态
	if args.Done {
		rf.lastApplied = rf.lastIndexSnap
		rf.applyCh <- ApplyMsg{
			CommandValid:  false,
			Command:       nil,
			CommandIndex:  0,
			SnapshotValid: true,
			Snapshot:      snapshot,
			SnapshotTerm:  args.LastIncludedTerm,
			SnapshotIndex: args.LastIncludedIndex,
		}
	}
	return
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

func (rf *Raft) sendAppendEntries(server int, args *AppendArgs, reply *AppendReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
func (rf *Raft) sendInstallSnapshot(peerId int) {
	offset := 0
	Term := rf.currentTerm
	snapshot := rf.persister.ReadSnapshot()
	LastIndexSnap := rf.lastIndexSnap
	LastIncludedTerm := rf.log[0].Term
	Len := len(snapshot)
	sliceLen := Len
	var data []byte
	// 分段发送,这里设置一段为整个Snapshot
	for {
		// 判断段尾
		if offset+sliceLen <= Len {
			data = snapshot[offset : offset+sliceLen]
		} else {
			data = snapshot[offset:]
		}

		args := &InstallSnapshotArgs{
			Term:              Term,
			LeaderId:          rf.me,
			LastIncludedIndex: LastIndexSnap,
			LastIncludedTerm:  LastIncludedTerm,
			Offset:            offset,
			Data:              data,
			Done:              true,
		}
		reply := &InstallSnapshotReply{}
		Debug_(dSnap, "S%d  [SN]-> S%d LSI %d LST %d NI %d", rf.me, peerId, LastIndexSnap, LastIncludedTerm, rf.nextIndex[peerId])
		ok := rf.peers[peerId].Call("Raft.InstallSnapshot", args, reply)

		if reply.Term > rf.currentTerm {
			rf.becomeFollower(reply.Term)
			return
		}
		// 特殊标志,直接返回
		if reply.Term == 0 {
			return
		}
		if ok {
			offset += len(data)
			// 发送完毕/自己的snapshot发生变化 -> 结束
			rf.mu.Lock()
			if offset == Len && rf.lastIndexSnap == LastIndexSnap {
				rf.nextIndex[peerId] = rf.lastIndexSnap + 1
				rf.matchIndex[peerId] = rf.nextIndex[peerId] - 1
				Debug_(dSnap, "S%d  [SN]-> S%d Done", rf.me, peerId)
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
		} else {
			Debug_(dSnap, "S%d  [SN]-> S%d Fail!", rf.me, peerId)
		}
		time.Sleep(randomHeartBeatTimeout())
	}
}

func intMin(a int, b int) int {
	if a >= b {
		return b
	}
	return a
}

func (rf *Raft) becomeFollower(term int) {
	Debug_(dTerm, "S%d -> Follower term %d", rf.me, term)
	rf.state = Follower
	rf.votedFor = -1
	rf.currentTerm = term
	rf.tick.Reset(randomElectionTimeout())
	rf.persist()
}

func (rf *Raft) detectCommit() {
	for range rf.newCommit {
		rf.mu.Lock()
		if rf.commitIndex == rf.lastApplied {
			rf.mu.Unlock()
			continue
		}
		entris := rf.log[rf.lastApplied+1-rf.lastIndexSnap : rf.commitIndex+1-rf.lastIndexSnap]
		lastapplied := rf.lastApplied
		rf.lastApplied = rf.commitIndex
		me := rf.me
		rf.mu.Unlock()
		// 解耦慢操作
		for k, v := range entris {
			if lastapplied+k+1 <= rf.lastIndexSnap {
				continue
			}
			Debug_(dCommit, "S%d Commit [%d]: %v", me, lastapplied+k+1, v.Command)
			rf.applyCh <- ApplyMsg{
				CommandValid:  true,
				Command:       v.Command,
				CommandIndex:  lastapplied + k + 1,
				SnapshotValid: false,
				Snapshot:      nil,
				SnapshotTerm:  0,
				SnapshotIndex: 0,
			}
		}
	}
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
	//Debug_(dClient, "S%d START! ", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		return -1, -1, false
	}
	//Debug_(dClient, "S%d START2222!", rf.me)
	newE := Entry{
		Term:    rf.currentTerm,
		Command: command,
	}
	rf.log = append(rf.log, newE)
	rf.persist()
	rf.newCommand <- struct{}{}

	Debug_(dClient, "S%d NEW ENTRY! %v", rf.me, command)
	index := len(rf.log) - 1 + rf.lastIndexSnap
	term := rf.currentTerm
	isLeader := rf.state == Leader

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

func (rf *Raft) heartBeat() {

	Debug_(dLeader, "S%d -> Leader term%d", rf.me, rf.currentTerm)
	tick := time.NewTimer(0)
	defer tick.Stop()
	// 双事件触发
	for {
		if rf.state != Leader || rf.killed() {
			return
		}
		select {
		case <-tick.C:
			rf.sendAEs(rf.currentTerm)
		case <-rf.newCommand:
			rf.sendAEs(rf.currentTerm)
		}
		tick.Reset(randomHeartBeatTimeout())
	}
}
func (rf *Raft) sendAEs(savedTerm int) {
	if rf.state != Leader || rf.killed() {
		return
	}
	// 广播
	for k, _ := range rf.peers {
		if k == rf.me {
			continue
		}
		go func(peerId int) {
			rf.mu.Lock()
			if rf.state != Leader || rf.killed() {
				rf.mu.Unlock()
				return
			}

			// 检查
			rf.nextIndex[peerId] = intMin(rf.nextIndex[peerId], len(rf.log)+rf.lastIndexSnap)
			if rf.nextIndex[peerId] <= rf.lastIndexSnap {
				go rf.sendInstallSnapshot(peerId)
				rf.mu.Unlock()
				return
			}
			// 发送AE
			preLogIndex := rf.nextIndex[peerId] - 1
			preLogTerm := rf.log[preLogIndex-rf.lastIndexSnap].Term
			savedLastIndex := rf.lastIndexSnap
			oldNi := rf.nextIndex[peerId]
			// 新日志+正常心跳
			Debug_(dWarn, "S%d [AE] -> S%d NextIndex %d Len %d CI %d LA %d LSI %d",
				rf.me, peerId, rf.nextIndex[peerId], len(rf.log), rf.commitIndex, rf.lastApplied, rf.lastIndexSnap)
			entris := rf.log[rf.nextIndex[peerId]-rf.lastIndexSnap:]
			Debug_(dWarn, "S%d [AE] -> S%d ELen %d", rf.me, peerId, len(entris))
			args := &AppendArgs{
				Term:        rf.currentTerm,
				LeaderId:    rf.me,
				PreLogIndex: preLogIndex,
				//PreLogTerm:   rf.log[preLogIndex].Term,
				PreLogTerm:   preLogTerm,
				LeaderCommit: rf.commitIndex,
				Entries:      entris,
			}
			reply := &AppendReply{}
			rf.mu.Unlock()

			ok := rf.sendAppendEntries(peerId, args, reply)
			//Debug_(dDrop, "S%d PRE LOCK!  -> S%d", rf.me, peerId)

			rf.mu.Lock()
			defer rf.mu.Unlock()
			Debug_(dDrop, "S%d [AE] -> S%d PreIndex %d Len %d ELen %d %v LSI %d",
				rf.me, peerId, preLogIndex, len(rf.log), len(entris), reply.Success, rf.lastIndexSnap)
			// 一致检查
			if rf.state != Leader || rf.currentTerm != savedTerm ||
				rf.nextIndex[peerId] != oldNi || rf.lastIndexSnap != savedLastIndex {
				return
			}
			if ok == false {
				Debug_(dWarn, "S%d [AE] -> S%d Fail!", rf.me, peerId)
				return
			}
			//Debug_(dTrace, "S%d <- S%d [AE] %v term %d", rf.me, peerId, reply.Success, reply.Term)
			if reply.Term > rf.currentTerm {
				rf.becomeFollower(reply.Term)
				return
			}
			if reply.Success == false {
				Debug_(dError, "S%d  [AE Fail] <-= S%d XTerm %d XLen %d XIndex %d Pre %d PreLogTerm %d",
					rf.me, peerId, reply.XTerm, reply.XLen, reply.XIndex, preLogIndex, rf.log[preLogIndex-rf.lastIndexSnap].Term)

				// follower日志短，检查确认需要回退nextIndex还是发送InstallSnapshot
				if reply.XTerm == 0 {
					// leader当前log不能更新follower,发送InstallSnapshot
					if reply.XIndex < rf.lastIndexSnap {
						go rf.sendInstallSnapshot(peerId)
						return
					}
					// 回退nextIndex
					rf.nextIndex[peerId] = reply.XLen + rf.lastIndexSnap
					rf.matchIndex[peerId] = rf.nextIndex[peerId] - 1
					Debug_(dWarn, "S%d  <- [AE FAIL] S%d NextIndex %d Len %d",
						rf.me, peerId, rf.nextIndex[peerId], len(rf.log))
					return
				}
				// 日志冲突,查看是否有与冲突处日志相同term的日志
				xTerm := -1
				// 避开哨兵
				for i := 1; i <= len(rf.log)-1; i++ {
					if rf.log[i].Term > reply.XTerm {
						break
					} else if rf.log[i].Term == reply.XTerm {
						xTerm = i
					}
				}
				// 回退
				if xTerm == -1 {
					rf.nextIndex[peerId] = reply.XIndex
				} else {
					rf.nextIndex[peerId] = xTerm + rf.lastIndexSnap
				}
				rf.matchIndex[peerId] = rf.nextIndex[peerId] - 1
				Debug_(dWarn, "S%d  <- [AE FAIL] S%d NextIndex %d Len %d XLen %d XIndex %d LSI %d",
					rf.me, peerId, rf.nextIndex[peerId], len(rf.log), reply.XLen, reply.XIndex, rf.lastIndexSnap)
				return
			}
			// reply.Success = true
			// 更新follower nextIndex
			rf.nextIndex[peerId] += len(entris)
			rf.matchIndex[peerId] = rf.nextIndex[peerId] - 1
			// 检查是否apply
			if len(rf.log)-1+rf.lastIndexSnap > rf.commitIndex {
				// 新日志的commit
				// 查看是否有需要确认给client的日志
				oldcommit := rf.commitIndex
				for i := rf.commitIndex + 1; i <= len(rf.log)-1+rf.lastIndexSnap; i++ {
					// 不允许提交往期的log
					if rf.log[i-rf.lastIndexSnap].Term != rf.currentTerm {
						continue
					}
					grant := 1
					for pId := range rf.peers {
						if pId == rf.me {
							continue
						}
						if rf.matchIndex[pId] >= i {
							grant++
						}
						if grant >= len(rf.peers)/2+1 {
							// 大多数通过
							rf.commitIndex = i
							break
						}
					}
				}
				if rf.commitIndex != oldcommit {
					go func() {
						rf.newCommit <- struct{}{}
						rf.newCommand <- struct{}{}
					}()
				}
			}
			Debug_(dTest, "S%d  <- [AE Success] S%d NextIndex %d Len %d XLen %d XIndex %d CI %d LA %d",
				rf.me, peerId, rf.nextIndex[peerId], len(rf.log), reply.XLen, reply.XIndex, rf.commitIndex, rf.lastApplied)
		}(k)
	}
}
func (rf *Raft) startElection() {
	tot := len(rf.peers)
	grantCount := 1
	savedTerm := rf.currentTerm
	for k, _ := range rf.peers {
		if k == rf.me {
			continue
		}
		// 每个request一个goroutine
		go func(peerID int) {
			for {
				//Debug_(dTest, "S%d Get Lock Election ST%d!", rf.me, rf.state)
				// 检查
				if rf.state != Candidate || rf.currentTerm != savedTerm || rf.killed() {
					//Debug_(dTest, "S%d Election exit Term %d!", rf.me, savedTerm)
					return
				}
				// 发送RV
				lastLogIndex := len(rf.log) - 1 + rf.lastIndexSnap
				lastLogTerm := rf.log[len(rf.log)-1].Term
				args := &RequestVoteArgs{
					Term:         rf.currentTerm,
					CandidateId:  rf.me,
					LastLogIndex: lastLogIndex,
					LastLogTerm:  lastLogTerm,
				}
				reply := &RequestVoteReply{}
				ok := rf.sendRequestVote(peerID, args, reply)

				// 一致检查
				if rf.state != Candidate || rf.currentTerm != savedTerm || rf.killed() {
					//Debug_(dTest, "S%d Election exit Term %d!", rf.me, savedTerm)
					return
				}
				if ok == false {
					//Debug_(dTest, "S%d [RV] -> S%d Fail!", rf.me, peerID)
					time.Sleep(randomRequestVoteTimeout())
					continue
				}
				if reply.Term > rf.currentTerm {
					// quit election immediately
					rf.becomeFollower(reply.Term)
					return
				}
				if reply.VoteGranted {
					// 获取选票,在这一轮中不需要在给该peer发送请求
					Debug_(dVote, "S%d <- S%d Vote", rf.me, peerID)
					rf.mu.Lock()
					grantCount++
					// 一致检查
					if grantCount >= tot/2+1 && rf.state == Candidate && rf.currentTerm == savedTerm {
						rf.state = Leader
						go rf.heartBeat()
					}
					rf.mu.Unlock()
					return
				}
				time.Sleep(randomRequestVoteTimeout())
			}
		}(k)
	}
}

func (rf *Raft) ticker() {
	go rf.detectCommit()
	defer rf.tick.Stop()
	// kill 退出
	for rf.killed() == false {
		<-rf.tick.C
		rf.mu.Lock()
		// kill 退出
		if rf.killed() {
			rf.mu.Unlock()
			return
		}
		if rf.state != Leader {
			rf.currentTerm++
			rf.persist()
			Debug_(dInfo, "S%d -> Candidate term %d Len %d LT %d", rf.me, rf.currentTerm, len(rf.log), rf.log[len(rf.log)-1].Term)
			rf.state = Candidate
			rf.tick.Reset(randomElectionTimeout())
			// 开始选举
			go rf.startElection()
		} else {
			// 重置ticker
			rf.tick.Reset(randomElectionTimeout())
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
	debug = 0
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh
	rf.votedFor = -1
	rf.state = Follower
	rf.currentTerm = 1
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.newCommit = make(chan interface{})
	rf.log = make([]Entry, 1)
	rf.newCommand = make(chan interface{})
	rf.mu = sync.Mutex{}
	rf.lastIndexSnap = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	for i := 0; i < len(peers); i++ {
		rf.nextIndex[i] = len(rf.log) + rf.lastIndexSnap
	}
	Debug_(dClient, "S%d Start at T:%d VF:%d", rf.me, rf.currentTerm, rf.votedFor)
	// start ticker goroutine to start elections
	rf.tick = time.NewTimer(randomElectionTimeout())
	go rf.ticker()

	return rf
}

// 150 - 300 ms
func randomElectionTimeout() time.Duration {
	ms := 150 + (rand.Int63() % 151)
	return time.Duration(ms) * time.Millisecond
}

// 50 - 70
func randomHeartBeatTimeout() time.Duration {
	ms := 50 + (rand.Int63() % 21)
	return time.Duration(ms) * time.Millisecond
}

// 50 - 100
func randomRequestVoteTimeout() time.Duration {
	ms := 50 + (rand.Int63() % 21)
	return time.Duration(ms) * time.Millisecond
}
