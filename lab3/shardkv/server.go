package shardkv

import (
	"6.5840/labrpc"
	"6.5840/shardctrler"
	"bytes"
	"log"
	"sync/atomic"
	"time"
)
import "6.5840/raft"
import "sync"
import "6.5840/labgob"

type Op struct {
	Seq      int
	ClientId int64

	Type  string
	Key   string
	Value string

	ConfigNum int
	// accept
	ShardIdx     int
	Shards2Group map[int]int
	Kv           map[string]string
	Table        map[int64]Ans
}
type Ans struct {
	Seq   int
	Reply string
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big
	sm           *shardctrler.Clerk
	dead         int32 // set by Kill()
	once         bool

	config     shardctrler.Config
	reConfigs  []shardctrler.Config
	auth       []bool
	replyTable []map[int64]Ans
	kvStore    []map[string]string

	persister   *raft.Persister
	SnapIndex   int
	CommitQueue []Op
}

func (kv *ShardKV) ShardKvs(args *ShardKvsArgs, reply *ShardKvsReply) {
	kv.mu.Lock()
	defer func() {
		Debug_(dClient, "G%d S%d  <-[SKV] Err %s",
			kv.gid, kv.me, reply.Err)
		kv.mu.Unlock()
	}()
	Debug_(dClient, "G%d S%d  <-[SKV] Sid %d auth %v ACfg %d NCfg %d",
		kv.gid, kv.me, args.Shard, kv.auth[args.Shard], args.ConfigNum, kv.config.Num)
	kv.checkUpdate()
	// config检查
	if args.ConfigNum > kv.config.Num {
		reply.Err = ErrWrongGroup
		return
	}
	// 操作权检查 + 识别重复
	if kv.auth[args.Shard] == true || args.ConfigNum < kv.config.Num {
		reply.Err = OK
		return
	}
	// Leader检查,只允许leader发送Kv
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	// 需要集群major将Kv存下
	OP := Op{
		Type:      Accept,
		ConfigNum: args.ConfigNum,
		ShardIdx:  args.Shard,
		Kv:        args.Kv,
		Table:     args.Table,
	}
	// 队列保证顺序+破除死锁
	//kv.rf.Start(OP)
	kv.CommitQueue = append(kv.CommitQueue, OP)
	Debug_(dLog, "G%d S%d Leader {Accept Log} SID %d CfgId %d", kv.gid, kv.me, args.Shard, kv.config.Num)

	//等待回复
	kv.mu.Unlock()
	t0 := time.Now()
	for {
		kv.mu.Lock()
		// 超过100ms,可能是网络分区了,为少数派的leader
		if time.Since(t0).Milliseconds() > 100 {
			reply.Err = ErrWrongLeader
			return
		}
		if kv.auth[args.Shard] {
			reply.Err = OK
			return
		}
		kv.mu.Unlock()
		time.Sleep(20 * time.Millisecond)
	}
}

// 将shard对应的Kv pairs发送给对应的group
func (kv *ShardKV) deliverShardKvs(configNum int, shards map[int]int) {
	for shard, gid := range shards {
		// 一个任务对应一个goroutine
		go func(shardId int, gid int) {
			if servers, ok := kv.config.Groups[gid]; ok {
				// 寻找group中的leader
				for si := 0; si < len(servers); si++ {
					kv.mu.Lock()
					// 结束条件
					if configNum != kv.config.Num || kv.auth[shardId] == false || kv.killed() {
						kv.mu.Unlock()
						return
					}
					// 拷贝
					Table := make(map[int64]Ans)
					Kv := make(map[string]string)
					for cid, ans := range kv.replyTable[shardId] {
						// 保留put结果,Get结果可以丢弃
						if ans.Reply == "" {
							Table[cid] = Ans{
								Seq:   ans.Seq,
								Reply: "",
							}
						}
					}
					for k, v := range kv.kvStore[shardId] {
						Kv[k] = v
					}
					// 准备
					srv := kv.make_end(servers[si])
					args := ShardKvsArgs{
						ConfigNum: configNum,
						Shard:     shardId,
						Kv:        Kv,
						Table:     Table,
					}
					var reply ShardKvsReply
					kv.mu.Unlock()
					// 解耦
					Debug_(dDrop, "G%d S%d [SKV]-> G%d S%d Sid %d CfgId %d auth %v",
						kv.gid, kv.me, gid, si, shardId, kv.config.Num, kv.auth[shardId])
					ok := srv.Call("ShardKV.ShardKvs", &args, &reply)
					Debug_(dDrop, "G%d S%d [SKV]-> G%d S%d Sid %d CfgId %d Err %s kv %v table %v",
						kv.gid, kv.me, gid, si, shardId, kv.config.Num, reply.Err, Kv, Table)

					kv.mu.Lock()
					if ok && reply.Err == OK && kv.config.Num == configNum && kv.auth[shardId] {
						// 放权log  保证集群状态一致
						OP := Op{
							Type:      Release,
							ConfigNum: configNum,
							ShardIdx:  shardId,
						}
						kv.CommitQueue = append(kv.CommitQueue, OP)
					}
					kv.mu.Unlock()
				}
			}
			//}
		}(shard, gid)
	}
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	Debug_(dInfo, "G%d S%d Get LOCK Sid %d CfgId %d auth %v",
		kv.gid, kv.me, key2shard(args.Key), kv.config.Num, kv.auth[key2shard(args.Key)])
	kv.mu.Lock()
	defer kv.mu.Unlock()
	shard := key2shard(args.Key)
	// config检查
	if kv.config.Shards[shard] != kv.gid {
		reply.Err = ErrWrongGroup
		Debug_(dDrop, "G%d S%d <-[Get] %d WrongGroup!", kv.gid, kv.me, args.ClientId%10000)
		return
	}
	// leader状态检查
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		Debug_(dDrop, "G%d S%d <-[Get] %d NotLeader!", kv.gid, kv.me, args.ClientId%10000)
		return
	}
	// 此时config正确但是还没有操作权
	// 需要等待获取KVs
	if kv.auth[shard] == false {
		t0 := time.Now()
		kv.mu.Unlock()
		for {
			kv.mu.Lock()
			// 超过100ms,可能是网络分区了,为少数派的leader
			if time.Since(t0).Milliseconds() > 100 {
				reply.Err = ErrWrongLeader
				Debug_(dClient, "G%d S%d NoAuth! Sid %d", kv.gid, kv.me, shard)
				return
			}
			if kv.auth[shard] {
				break
			}
			kv.mu.Unlock()
			//time.Sleep(5 * time.Millisecond)
		}
	}
	Debug_(dClient, "G%d S%d Leader!", kv.gid, kv.me)
	replySeq := kv.replyTable[shard][args.ClientId].Seq
	replyValue := kv.replyTable[shard][args.ClientId].Reply
	Debug_(dVote, "G%d S%d <-[Get] Cid %d Seq %d Key [%s] RSeq %d Value [%s] Sid %d",
		kv.gid, kv.me, args.ClientId%10000, args.Seq, args.Key, replySeq, replyValue, shard)
	// 操作已完成
	// replyTable对应处的Seq等于args的Seq才说明apply成功
	if args.Seq <= replySeq {
		if replyValue != "" {
			reply.Err = OK
			reply.Value = replyValue
			return
		} else {
			reply.Err = ErrNoKey
			reply.Value = ""
			return
		}
	}
	// 发送给Raft,指令可以重复，后续检查是否执行
	OP := Op{
		Seq:       args.Seq,
		ClientId:  args.ClientId,
		Type:      Get,
		Key:       args.Key,
		ConfigNum: kv.config.Num,
	}
	Debug_(dCommit, "G%d S%d [ADD] %v", kv.gid, kv.me, OP)
	kv.CommitQueue = append(kv.CommitQueue, OP)
	Debug_(dLog2, "G%d S%d SUBMIT [Get] Cid %d Seq %d Key [%s] Sid %d Len %d",
		kv.gid, kv.me, args.ClientId%10000, args.Seq, args.Key, key2shard(args.Key), len(kv.CommitQueue))

	if kv.persister.RaftStateSize() >= kv.maxraftstate && kv.maxraftstate != -1 {
		kv.persist(true, true)
	}
	kv.mu.Unlock()

	// 等待操作完成
	t0 := time.Now()
	for {
		kv.mu.Lock()
		if kv.config.Shards[key2shard(args.Key)] != kv.gid {
			reply.Err = ErrWrongGroup
			return
		}
		// 超过100ms,可能是网络分区了,为少数派的leader
		if time.Since(t0).Milliseconds() > 100 {
			reply.Err = ErrWrongLeader
			Debug_(dClient, "G%d S%d Delay! Sid %d", kv.gid, kv.me, shard)
			//kv.mu.Unlock()
			return
		}
		if args.Seq <= kv.replyTable[shard][args.ClientId].Seq {
			ReplyValue := kv.replyTable[shard][args.ClientId].Reply
			if ReplyValue != "" {
				reply.Err = OK
				reply.Value = ReplyValue
				Debug_(dVote, "G%d S%d [Check] Cid %d Seq %d ",
					kv.gid, kv.me, args.ClientId%10000, args.Seq)
				//kv.mu.Unlock()
				return
			} else {
				reply.Err = ErrNoKey
				reply.Value = ""
				//kv.mu.Unlock()
				return
			}
		}
		kv.mu.Unlock()
		//time.Sleep(5 * time.Millisecond)
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	Debug_(dInfo, "G%d S%d PutAppend LOCK Key %v SId %d CfgId %d auth %v shards %v",
		kv.gid, kv.me, args.Key, key2shard(args.Key), kv.config.Num, kv.auth[key2shard(args.Key)], kv.config.Shards)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// 判断config
	shard := key2shard(args.Key)
	// config检查
	if kv.config.Shards[shard] != kv.gid {
		reply.Err = ErrWrongGroup
		//kv.mu.Unlock()
		return
	}
	// Leader检查
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		Debug_(dDrop, "G%d S%d <-[%s] %d NotLeader!", kv.gid, kv.me, args.Op, args.ClientId%10000)
		reply.Err = ErrWrongLeader
		//kv.mu.Unlock()
		return
	}
	// config正确但是还没有操作权
	if kv.auth[shard] == false {
		kv.mu.Unlock()
		t0 := time.Now()
		for {
			kv.mu.Lock()
			// 超时重试
			if time.Since(t0).Milliseconds() > 100 {
				reply.Err = ErrWrongGroup
				return
			}
			// 有操作权了
			if kv.auth[shard] {
				break
			}
			kv.mu.Unlock()
			//time.Sleep(5 * time.Millisecond)
		}
	}
	Debug_(dClient, "G%d S%d Leader!", kv.gid, kv.me)
	replySeq := kv.replyTable[shard][args.ClientId].Seq
	Debug_(dClient, "G%d S%d <-[%s] Cid %d Seq %d Key [%s] Value [%v] RSeq %d Sid %d",
		kv.gid, kv.me, args.Op, args.ClientId%10000, args.Seq, args.Key, args.Value, replySeq, shard)
	// 操作已完成
	if args.Seq <= replySeq {
		reply.Err = OK
		return
	}
	// 新请求
	OP := Op{
		Seq:       args.Seq,
		ClientId:  args.ClientId,
		Type:      args.Op,
		Key:       args.Key,
		Value:     args.Value,
		ConfigNum: kv.config.Num,
	}
	Debug_(dCommit, "G%d S%d [ADD] %v", kv.gid, kv.me, OP)
	// 加入raft
	kv.CommitQueue = append(kv.CommitQueue, OP)
	Debug_(dLog2, "G%d S%d SUBMIT [%s] Cid %d Seq %d Key [%s] Sid %d Len %",
		kv.gid, kv.me, args.Op, args.ClientId%10000, args.Seq, args.Key, key2shard(args.Key), len(kv.CommitQueue))

	if kv.persister.RaftStateSize() >= kv.maxraftstate && kv.maxraftstate != -1 {
		kv.persist(true, true)
	}
	kv.mu.Unlock()
	// 等待操作完成
	t0 := time.Now()
	for {
		kv.mu.Lock()
		if kv.config.Shards[key2shard(args.Key)] != kv.gid {
			reply.Err = ErrWrongGroup
			return
		}
		// 超时重试
		if time.Since(t0).Milliseconds() > 100 {
			reply.Err = ErrWrongLeader
			return
		}
		if args.Seq <= kv.replyTable[shard][args.ClientId].Seq {
			reply.Err = OK
			return
		}
		kv.mu.Unlock()
		//time.Sleep(5 * time.Millisecond)
	}
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&kv.dead, 1)
}
func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *ShardKV) Decode(serverState []byte) (int, []map[int64]Ans, []map[string]string, shardctrler.Config, []bool) {
	r := bytes.NewBuffer(serverState)
	d := labgob.NewDecoder(r)
	var index int
	var table []map[int64]Ans
	var store []map[string]string
	var auth []bool
	var Config shardctrler.Config
	if d.Decode(&index) != nil || d.Decode(&table) != nil ||
		d.Decode(&store) != nil || d.Decode(&Config) != nil || d.Decode(&auth) != nil {
		log.Fatalf("Read Persist Error!\n")
	}
	return index, table, store, Config, auth
}
func (kv *ShardKV) Encode(index int, replyTable []map[int64]Ans, kvStore []map[string]string, Config shardctrler.Config, auth []bool) []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(index) != nil || e.Encode(replyTable) != nil ||
		e.Encode(kvStore) != nil || e.Encode(Config) != nil || e.Encode(auth) != nil {
		log.Fatalf("Encode Persist Error !\n")
	}
	raftstate := w.Bytes()
	return raftstate
}

// 持久化 SnapIndex replyTable kvStore reConfigs auth
// snapshotValid==false -> 代表maxraftstate==-1,不持久化snapshot
// raftValid==true -> 更新底层raft的commitIndex,raft可以触发InstallSnapshot
// raftValid==false -> 不更新底层raft的commitIndex,节省网络带宽
func (kv *ShardKV) persist(raftValid bool, snapshotValid bool) {
	switch snapshotValid {
	case true:
		serverState := kv.Encode(kv.SnapIndex, kv.replyTable, kv.kvStore, kv.config, kv.auth)
		if raftValid == false {
			go kv.rf.Snapshot(-1, serverState)
		} else {
			Ni := kv.SnapIndex
			go kv.rf.Snapshot(Ni, serverState)
		}
	case false:
		if raftValid == false {
			go kv.rf.Snapshot(-1, nil)
		}
	}
}

func (kv *ShardKV) readPersister(serverState []byte) {
	if len(serverState) < 1 || serverState == nil {
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.SnapIndex, kv.replyTable, kv.kvStore, kv.config, kv.auth = kv.Decode(serverState)
}

// 检查需要发送的Kvs
func (kv *ShardKV) checkAccept(shards [shardctrler.NShards]int) map[int]int {
	df := make(map[int]int)
	oldconfig := kv.sm.Query(kv.config.Num - 1)
	for i := 0; i < shardctrler.NShards; i++ {
		if shards[i] == kv.gid && kv.auth[i] == false {
			df[i] = oldconfig.Shards[i]
		}
	}
	return df
}

// 检查需要接受的Kvs
func (kv *ShardKV) checkSend(shards [shardctrler.NShards]int) map[int]int {
	df := make(map[int]int)
	for i := 0; i < shardctrler.NShards; i++ {
		if shards[i] != kv.gid && kv.auth[i] == true {
			df[i] = shards[i]
		}
	}
	return df
}

func (kv *ShardKV) detectCommit() {
	for apply := range kv.applyCh {
		if apply.SnapshotValid {
			Debug_(dSnap, "G%d S%d [Snap] SnapId %d NSI %d CfgId %d snapshot %d",
				kv.gid, kv.me, apply.SnapshotIndex, kv.SnapIndex, kv.config.Num, len(apply.Snapshot))
			kv.readPersister(apply.Snapshot)
			// ignore this condition?
			if kv.maxraftstate != -1 {
				if kv.persister.RaftStateSize() >= kv.maxraftstate {
					kv.persist(true, true)
				} else {
					kv.persist(false, true)
				}
			}
			continue
		}
		// nop
		if apply.Command == -1 {
			if apply.CommandIndex > kv.SnapIndex {
				kv.SnapIndex++
			}
			continue
		}
		//Debug_(dSnap, "S%d COMMIT %v %v", kv.me, command.Key, command.Value)
		kv.mu.Lock()
		command := apply.Command.(Op)
		Debug_(dCommit, "G%d S%d [Fetch] %v CfgId %d auth %v SD %d K2S %d config %v SNI %d CMI %d",
			kv.gid, kv.me, apply.Command, kv.config.Num, kv.auth, command.ShardIdx, key2shard(command.Key),
			kv.config, kv.SnapIndex, apply.CommandIndex)
		//if kv.persister.RaftStateSize() >= kv.maxraftstate && kv.maxraftstate != -1 {
		//	kv.persist()
		//}
		if apply.CommandIndex > kv.SnapIndex {
			kv.SnapIndex++
		}
		if command.ConfigNum > kv.config.Num {
			kv.checkNewConfig()
			kv.checkUpdate()
		}
		shard := key2shard(command.Key)
		// 执行
		switch command.Type {
		case Deliver:
			_, isLeader := kv.rf.GetState()
			if isLeader && command.ConfigNum == kv.config.Num {
				kv.deliverShardKvs(command.ConfigNum, command.Shards2Group)
			}
			kv.mu.Unlock()
			continue
		case Accept:
			// 检查 添加kvs
			if kv.config.Shards[command.ShardIdx] == kv.gid &&
				kv.auth[command.ShardIdx] == false &&
				kv.config.Num == command.ConfigNum {

				kv.auth[command.ShardIdx] = true
				kv.replyTable[command.ShardIdx] = make(map[int64]Ans)
				kv.kvStore[command.ShardIdx] = make(map[string]string)
				if command.Kv != nil {
					for k, v := range command.Kv {
						kv.kvStore[command.ShardIdx][k] = v
					}
				}
				if command.Table != nil {
					for cid, ans := range command.Table {
						kv.replyTable[command.ShardIdx][cid] = ans
					}
				}
				Debug_(dDrop, "G%d S%d [Accept] Sid %d CfgId %d store %v table %v",
					kv.gid, kv.me, command.ShardIdx, kv.config.Num, kv.kvStore, kv.replyTable)
				if kv.maxraftstate != -1 {
					//go kv.persist()
					if kv.persister.RaftStateSize() >= kv.maxraftstate {
						kv.persist(true, true)
					} else {
						kv.persist(false, true)
					}
				} else {
					kv.persist(false, false)
				}
			}
			kv.mu.Unlock()
			continue
		case Release:
			if command.ConfigNum == kv.config.Num && kv.auth[command.ShardIdx] {
				Debug_(dDrop, "G%d S%d [Release] Sid %d CfgId %d", kv.gid, kv.me, command.ShardIdx, kv.config.Num)
				kv.auth[command.ShardIdx] = false
				kv.kvStore[command.ShardIdx] = nil
				kv.replyTable[command.ShardIdx] = nil
				if kv.maxraftstate != -1 {
					//go kv.persist()
					if kv.persister.RaftStateSize() >= kv.maxraftstate {
						kv.persist(true, true)
					} else {
						kv.persist(false, true)
					}
				} else {
					kv.persist(false, false)
				}
			}
			kv.mu.Unlock()
			continue
		case Get:
			// 重复检测
			Debug_(dPersist, "G%d S%d COMMIT-PRE [%s] Cid %d Seq %d Key [%s] Value [%s] extra [%s] sseq %d auth %v sbelog %d",
				kv.gid, kv.me, command.Type, command.ClientId%10000, command.Seq, command.Key, kv.kvStore[shard][command.Key], command.Value,
				kv.replyTable[shard][command.ClientId].Seq, kv.auth[shard], kv.config.Shards[shard])
			if command.Seq <= kv.replyTable[shard][command.ClientId].Seq || !kv.auth[shard] {
				kv.mu.Unlock()
				continue
			}
			if command.ConfigNum > kv.config.Num {

			}
		case Put:
			Debug_(dPersist, "G%d S%d COMMIT-PRE [%s] Cid %d Seq %d Key [%s] Value [%s] extra [%s] sseq %d auth %v sbelog %d",
				kv.gid, kv.me, command.Type, command.ClientId%10000, command.Seq, command.Key, kv.kvStore[shard][command.Key], command.Value,
				kv.replyTable[shard][command.ClientId].Seq, kv.auth[shard], kv.config.Shards[shard])
			if command.Seq <= kv.replyTable[shard][command.ClientId].Seq || !kv.auth[shard] {
				kv.mu.Unlock()
				continue
			}
			kv.kvStore[shard][command.Key] = command.Value
		case Append:
			Debug_(dPersist, "G%d S%d COMMIT-PRE [%s] Cid %d Seq %d Key [%s] Value [%s] extra [%s] sseq %d auth %v sbelog %d",
				kv.gid, kv.me, command.Type, command.ClientId%10000, command.Seq, command.Key, kv.kvStore[shard][command.Key], command.Value,
				kv.replyTable[shard][command.ClientId].Seq, kv.auth[shard], kv.config.Shards[shard])
			if command.Seq <= kv.replyTable[shard][command.ClientId].Seq || !kv.auth[shard] {
				kv.mu.Unlock()
				continue
			}
			kv.kvStore[shard][command.Key] += command.Value
		}
		Debug_(dPersist, "G%d S%d COMMIT [%s] Cid %d Seq %d Key [%s] Value [%s] extra [%s]",
			kv.gid, kv.me, command.Type, command.ClientId%10000, command.Seq, command.Key, kv.kvStore[shard][command.Key], command.Value)
		// 填充replyTable
		reply := ""
		if command.Type == Get {
			reply = kv.kvStore[shard][command.Key]
		}
		kv.replyTable[shard][command.ClientId] = Ans{
			Seq:   command.Seq,
			Reply: reply,
		}
		// 分情况持久化
		if kv.maxraftstate != -1 {
			//go kv.persist()
			if kv.persister.RaftStateSize() >= kv.maxraftstate {
				kv.persist(true, true)
			} else {
				kv.persist(false, true)
			}
		} else {
			kv.persist(false, false)
		}
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) detectSnapshot() {
	for !kv.killed() {
		kv.mu.Lock()
		if kv.persister.RaftStateSize() >= kv.maxraftstate {
			//go kv.persist()
			kv.persist(true, true)
		}
		kv.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}

func (kv *ShardKV) checkUpdate() {
	deliver := kv.checkSend(kv.config.Shards)
	accept := kv.checkAccept(kv.config.Shards)
	// 当前config未完成,不能继续
	if len(deliver) == 0 && len(accept) == 0 {
		// 当前config已完成,查看是否有新config需要执行
		if len(kv.reConfigs) > 0 {
			kv.config = kv.reConfigs[0]
			kv.reConfigs = kv.reConfigs[1:]
			kv.once = true
			//kv.persist()
			Debug_(dLeader, "G%d S%d NEW CONFIG! CfgId %d config %v auth %v reconfig %v deliver %v accept %v SNI %v",
				kv.gid, kv.me, kv.config.Num, kv.config, kv.auth, kv.reConfigs, deliver, accept, kv.SnapIndex)
			//serverState := kv.Encode(kv.SnapIndex, kv.replyTable, kv.kvStore, kv.config, kv.auth)
			//if kv.SnapIndex == 0 {
			//	kv.persister.Save(nil, serverState)
			//}
		}
	} else if len(deliver) != 0 {
		_, isLeader := kv.rf.GetState()
		//if isLeader && kv.once {
		//	kv.deliverShardKvs(kv.config.Num, deliver)
		//	kv.once = false
		//}
		if isLeader {
			//kv.deliverShardKvs(kv.config.Num, deliver)
			OP := Op{
				Seq:          0,
				ClientId:     0,
				Type:         Deliver,
				Key:          "",
				Value:        "",
				ConfigNum:    kv.config.Num,
				ShardIdx:     0,
				Shards2Group: deliver,
				Kv:           nil,
				Table:        nil,
			}
			kv.CommitQueue = append(kv.CommitQueue, OP)
		}
	}
	//else if len(accept) != 0 {
	//	//_, isLeader := kv.rf.GetState()
	//	//if isLeader {
	//	kv.confirmShardKvs(accept)
	//	//}
	//}
}

// 检查新config config补全 auth初始化
func (kv *ShardKV) checkNewConfig() {
	newConfig := kv.sm.Query(-1)
	// 确保config连续性
	for len(kv.reConfigs) > 0 && kv.reConfigs[0].Num != kv.config.Num+1 {
		kv.reConfigs = kv.reConfigs[1:]
	}
	// 确认最新config的config Num
	var lastConfig int
	if len(kv.reConfigs) >= 1 {
		lastConfig = kv.reConfigs[len(kv.reConfigs)-1].Num
	} else {
		lastConfig = kv.config.Num
	}
	// 有新config
	if newConfig.Num != lastConfig {
		// 有新config且需要config补全
		if newConfig.Num != lastConfig+1 {
			for i := lastConfig + 1; i <= newConfig.Num-1; i++ {
				lostConfig := kv.sm.Query(i)
				kv.reConfigs = append(kv.reConfigs, lostConfig)
			}
		}
		kv.reConfigs = append(kv.reConfigs, newConfig)
		// 原始config变化时需要初始化auth
		if kv.config.Num == 0 {
			// config Num 变成1
			kv.config = kv.reConfigs[0]
			kv.reConfigs = kv.reConfigs[1:]
			kv.once = true
			Debug_(dLeader, "G%d S%d NEW CONFIG! CfgId %d %v", kv.gid, kv.me, kv.config.Num, kv.config)
			for k, v := range kv.config.Shards {
				if v == kv.gid {
					kv.auth[k] = true
				}
				kv.replyTable[k] = make(map[int64]Ans)
				kv.kvStore[k] = make(map[string]string)
			}
			//kv.persist()
		}
	}
}
func (kv *ShardKV) detectReconfiguration() {
	for !kv.killed() {
		kv.mu.Lock()
		kv.checkNewConfig()
		// config0 为全0
		if kv.config.Num == 0 {
			kv.mu.Unlock()
			time.Sleep(100 * time.Millisecond)
			continue
		}
		kv.checkUpdate()
		kv.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) detectCommitQueue() {
	for !kv.killed() {
		if len(kv.CommitQueue) == 0 {
			time.Sleep(2 * time.Millisecond)
			continue
		}
		Debug_(dInfo, "G%d S%d [Commit] OP %v ", kv.gid, kv.me, kv.CommitQueue[0])
		kv.rf.Start(kv.CommitQueue[0])
		kv.CommitQueue = kv.CommitQueue[1:]
	}
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	kv.CommitQueue = make([]Op, 0)
	kv.persister = persister
	kv.sm = shardctrler.MakeClerk(ctrlers)
	kv.maxraftstate = maxraftstate
	kv.replyTable = make([]map[int64]Ans, shardctrler.NShards)
	kv.kvStore = make([]map[string]string, shardctrler.NShards)
	for i := 0; i < shardctrler.NShards; i++ {
		kv.replyTable[i] = nil
		kv.kvStore[i] = nil
	}
	kv.auth = make([]bool, shardctrler.NShards)
	kv.once = true

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.config = kv.sm.Query(0)

	kv.readPersister(persister.ReadSnapshot())

	Debug_(dClient, "G%d S%d Start CfgId %d auth %v SNI %d",
		kv.gid, kv.me, kv.config.Num, kv.auth, kv.SnapIndex)

	go kv.detectCommit()
	//if maxraftstate != -1 {
	//	go kv.detectSnapshot()
	//}
	go kv.detectCommitQueue()
	go kv.detectReconfiguration()

	return kv
}
