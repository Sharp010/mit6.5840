package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	Seq      int
	ClientId int64

	Type  string
	Key   string
	Value string
}

type Ans struct {
	Seq   int
	Reply string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	MaxSeq     int
	ReplyTable map[int64]Ans
	KVStore    map[string]string

	persister   *raft.Persister
	SnapIndex   int
	CommitQueue []Op
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	// Leader检查
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		Debug_(dDrop, "S%d <-[Get] %d NotLeader!", kv.me, args.ClientId%10000)
		kv.mu.Unlock()
		return
	}
	Debug_(dClient, "S%d Leader!", kv.me)
	replySeq := kv.ReplyTable[args.ClientId].Seq
	replyValue := kv.ReplyTable[args.ClientId].Reply
	Debug_(dVote, "S%d <-[Get] Cid %d Seq %d Key [%s] RSeq %d Value [%s]",
		kv.me, args.ClientId%10000, args.Seq, args.Key, replySeq, replyValue)
	// 操作已完成
	// replyTable对应处的Seq等于args的Seq才说明apply成功
	if args.Seq <= replySeq {
		if replyValue != "" {
			reply.Err = OK
			reply.Value = replyValue
			kv.mu.Unlock()
			return
		} else {
			reply.Err = ErrNoKey
			reply.Value = ""
			kv.mu.Unlock()
			return
		}
	}

	// 发送给Raft,指令可以重复，后续检查是否执行
	OP := Op{
		Seq:      args.Seq,
		ClientId: args.ClientId,
		Type:     "Get",
		Key:      args.Key,
		Value:    "",
	}
	Debug_(dCommit, "S%d [ADD] %v", kv.me, OP)
	//kv.rf.Start(OP)
	kv.CommitQueue = append(kv.CommitQueue, OP)
	Debug_(dLog2, "S%d SUBMIT [Get] Cid %d Seq %d Key [%s] ", kv.me, args.ClientId%10000, args.Seq, args.Key)

	// 等待操作完成
	t0 := time.Now()
	kv.mu.Unlock()
	for {
		kv.mu.Lock()
		// 可能出现网络分区
		if time.Since(t0).Milliseconds() > 100 {
			reply.Err = ErrWrongLeader
			kv.mu.Unlock()
			return
		}
		if args.Seq <= kv.ReplyTable[args.ClientId].Seq {
			ReplyValue := kv.ReplyTable[args.ClientId].Reply
			if ReplyValue != "" {
				reply.Err = OK
				reply.Value = ReplyValue
				kv.mu.Unlock()
				return
			} else {
				reply.Err = ErrNoKey
				reply.Value = ""
				kv.mu.Unlock()
				return
			}
		}
		kv.mu.Unlock()
		//time.Sleep(5 * time.Millisecond)
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	// 判断Leader
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		Debug_(dDrop, "S%d <-[%s] %d NotLeader!", kv.me, args.Op, args.ClientId%10000)
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	Debug_(dClient, "S%d Leader!", kv.me)
	replySeq := kv.ReplyTable[args.ClientId].Seq
	Debug_(dClient, "S%d <-[%s] Cid %d Seq %d Key [%s] Value [%v] RSeq %d",
		kv.me, args.Op, args.ClientId%10000, args.Seq, args.Key, args.Value, replySeq)
	// 操作已完成
	if args.Seq <= replySeq {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	// 新请求
	OP := Op{
		Seq:      args.Seq,
		ClientId: args.ClientId,
		Type:     args.Op,
		Key:      args.Key,
		Value:    args.Value,
	}
	Debug_(dCommit, "S%d [ADD] %v", kv.me, OP)
	// 加入raft
	//kv.rf.Start(OP)
	kv.CommitQueue = append(kv.CommitQueue, OP)
	Debug_(dLog2, "S%d SUBMIT [%s] Cid %d Seq %d Key [%s] ", kv.me, args.Op, args.ClientId%10000, args.Seq, args.Key)

	kv.mu.Unlock()
	// 等待操作完成
	t0 := time.Now()
	for {
		kv.mu.Lock()
		// 超时重试
		if time.Since(t0).Milliseconds() > 100 {
			//reply.Err = ErrNoKey
			reply.Err = ErrWrongLeader
			kv.mu.Unlock()
			return
		}
		if args.Seq <= kv.ReplyTable[args.ClientId].Seq {
			reply.Err = OK
			kv.mu.Unlock()
			return
		}
		kv.mu.Unlock()
		//time.Sleep(5 * time.Millisecond)
	}
	//return
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) Decode(serverState []byte) (int, map[int64]Ans, map[string]string) {
	r := bytes.NewBuffer(serverState)
	d := labgob.NewDecoder(r)
	var index int
	var table map[int64]Ans
	var store map[string]string
	if d.Decode(&index) != nil || d.Decode(&table) != nil ||
		d.Decode(&store) != nil {
		log.Fatalf("Read Persist Error!\n")
	}
	return index, table, store
}
func (kv *KVServer) readPersister(serverState []byte) {
	if len(serverState) < 1 || serverState == nil {
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.SnapIndex, kv.ReplyTable, kv.KVStore = kv.Decode(serverState)
}

func (kv *KVServer) Encode(index int, replyTable map[int64]Ans, kvStore map[string]string) []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(index) != nil || e.Encode(replyTable) != nil ||
		e.Encode(kvStore) != nil {
		log.Fatalf("Encode Persist Error !\n")
	}
	raftstate := w.Bytes()
	return raftstate
}

func (kv *KVServer) persist() {
	serverState := kv.Encode(kv.SnapIndex, kv.ReplyTable, kv.KVStore)
	//go kv.rf.Snapshot(kv.SnapIndex, serverState)
	kv.rf.Snapshot(kv.SnapIndex, serverState)
}

func (kv *KVServer) detectSnapshot(persister *raft.Persister) {
	for !kv.killed() {
		kv.mu.Lock()
		if persister.RaftStateSize() >= kv.maxraftstate {
			kv.persist()
		}
		kv.mu.Unlock()
	}
}

func (kv *KVServer) detectCommitQueue() {
	for !kv.killed() {
		if len(kv.CommitQueue) == 0 {
			// 可以使用条件变量
			time.Sleep(2 * time.Millisecond)
			continue
		}
		kv.rf.Start(kv.CommitQueue[0])
		kv.CommitQueue = kv.CommitQueue[1:]
	}
}

func (kv *KVServer) detectCommit() {
	for apply := range kv.applyCh {
		if apply.SnapshotValid {
			kv.readPersister(apply.Snapshot)
			continue
		}
		// nop 忽略
		if apply.Command == -1 {
			if apply.CommandIndex > kv.SnapIndex {
				kv.SnapIndex++
			}
			continue
		}
		Debug_(dCommit, "S%d [Fetch] %v", kv.me, apply.Command)
		command := apply.Command.(Op)

		kv.mu.Lock()
		if kv.persister.RaftStateSize() >= kv.maxraftstate && kv.maxraftstate != -1 {
			kv.persist()
		}
		kv.SnapIndex++
		// 重复 -> 忽略
		if command.Seq <= kv.ReplyTable[command.ClientId].Seq {
			kv.mu.Unlock()
			continue
		}
		// 执行
		switch command.Type {
		case "Get":
		case "Put":
			kv.KVStore[command.Key] = command.Value
		case "Append":
			kv.KVStore[command.Key] += command.Value
		}
		Debug_(dPersist, "S%d COMMIT [%s] Cid %d Seq %d Key [%s] Value [%s] extra [%s]", kv.me, command.Type, command.ClientId%10000, command.Seq, command.Key, kv.KVStore[command.Key], command.Value)
		kv.ReplyTable[command.ClientId] = Ans{
			Seq:   command.Seq,
			Reply: kv.KVStore[command.Key],
		}
		kv.mu.Unlock()
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.mu = sync.Mutex{}
	kv.maxraftstate = maxraftstate
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.MaxSeq = 1
	kv.KVStore = make(map[string]string)
	kv.ReplyTable = make(map[int64]Ans)
	kv.SnapIndex = 0
	kv.persister = persister
	kv.CommitQueue = make([]Op, 0)

	kv.readPersister(persister.ReadSnapshot())
	Debug_(dSnap, "S%d snap : table: %v store: %v", kv.ReplyTable, kv.KVStore)
	go kv.detectCommitQueue()
	if maxraftstate != -1 {
		go kv.detectSnapshot(persister)
	}
	go kv.detectCommit()
	return kv
}
