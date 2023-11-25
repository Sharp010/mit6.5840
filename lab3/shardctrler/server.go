package shardctrler

import (
	"6.5840/raft"
	"bytes"
	"log"
	"sort"
	"sync/atomic"
	"time"
)
import "6.5840/labrpc"
import "sync"
import "6.5840/labgob"

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	done      map[int64]int
	query     map[int64]Config
	configs   []Config // indexed by config num
	nextIndex int
	persister *raft.Persister

	CommitQueue []Op
	dead        int32
}

type Op struct {
	// Your data here.
	ClientId int64
	Seq      int
	Type     string
	Cfg      Config
}

// 拷贝最新的config,在拷贝上进行新分配
func (sc *ShardCtrler) copyLatest(reply *Config) {
	lastConfig := sc.configs[len(sc.configs)-1]
	for k, v := range lastConfig.Shards {
		reply.Shards[k] = v
	}
	for k := range lastConfig.Groups {
		reply.Groups[k] = lastConfig.Groups[k]
	}
	return
}

// 平均分配shard
func (sc *ShardCtrler) balance(reply *Config) {
	totG := len(reply.Groups)
	if totG == 0 {
		return
	}
	//Debug_(dTrace, "S%d totG %d config %v", sc.me, totG, reply)
	// map遍历不是deterministic的
	groups := make([]int, 0)
	for id := range reply.Groups {
		groups = append(groups, id)
	}
	sort.Ints(groups)
	//Debug_(dCommit, "S%d group %v", sc.me, groups)
	avg := NShards / totG
	i := 0
	// 平均分配一轮
	for _, id := range groups {
		cnt := avg
		for j := i; j < NShards; j++ {
			reply.Shards[j] = id
			cnt--
			if cnt == 0 {
				i = j + 1
				break
			}
		}
	}
	// 多余的再平均一轮
	if i == NShards {
		return
	}
	for _, id := range groups {
		reply.Shards[i] = id
		i++
		if i == NShards {
			return
		}
	}

}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	sc.mu.Lock()
	// 不是leader不能响应join
	_, isLeader := sc.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}
	// 已完成,返回
	if args.Seq <= sc.done[args.ClientId] {
		reply.Err = OK
		sc.mu.Unlock()
		return
	}
	// 发送指令到底层raft
	OP := Op{
		ClientId: args.ClientId,
		Seq:      args.Seq,
		Type:     Join,
		Cfg: Config{
			Num:    0,
			Shards: [10]int{},
			Groups: args.Servers,
		},
	}
	sc.CommitQueue = append(sc.CommitQueue, OP)
	sc.mu.Unlock()
	// 等待操作完成
	t0 := time.Now()
	for {
		sc.mu.Lock()
		if sc.done[args.ClientId] <= args.Seq {
			reply.Err = OK
			sc.mu.Unlock()
			return
		}
		if time.Since(t0).Milliseconds() > 40 {
			reply.WrongLeader = true
			sc.mu.Unlock()
			return
		}
		sc.mu.Unlock()
	}

}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	sc.mu.Lock()
	// 不是leader不能响应leave
	_, isLeader := sc.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}
	// 已完成,返回
	if args.Seq <= sc.done[args.ClientId] {
		reply.Err = OK
		sc.mu.Unlock()
		return
	}
	// 发送指令到底层raft
	groups := make(map[int][]string)
	for _, v := range args.GIDs {
		groups[v] = make([]string, 0)
	}
	OP := Op{
		ClientId: args.ClientId,
		Seq:      args.Seq,
		Type:     Leave,
		Cfg: Config{
			Num:    0,
			Shards: [10]int{},
			Groups: groups,
		},
	}

	sc.CommitQueue = append(sc.CommitQueue, OP)
	sc.mu.Unlock()
	// 等待操作完成
	t0 := time.Now()
	for {
		sc.mu.Lock()
		if args.Seq <= sc.done[args.ClientId] {
			reply.Err = OK
			sc.mu.Unlock()
			return
		}
		if time.Since(t0).Milliseconds() > 40 {
			reply.WrongLeader = true
			sc.mu.Unlock()
			return
		}
		sc.mu.Unlock()
	}

}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// 不是leader不能响应Move
	sc.mu.Lock()
	_, isLeader := sc.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}
	// 已完成,返回
	if args.Seq <= sc.done[args.ClientId] {
		reply.Err = OK
		sc.mu.Unlock()
		return
	}
	// 发送指令到底层raft
	groups := make(map[int][]string)
	groups[args.GID] = make([]string, 0)
	OP := Op{
		ClientId: args.ClientId,
		Seq:      args.Seq,
		Type:     Move,
		Cfg: Config{
			Num:    0,
			Shards: [10]int{args.Shard},
			Groups: groups,
		},
	}
	//sc.rf.Start(OP)
	sc.CommitQueue = append(sc.CommitQueue, OP)
	sc.mu.Unlock()
	// 等待操作完成
	t0 := time.Now()
	for {
		sc.mu.Lock()
		if args.Seq <= sc.done[args.ClientId] {
			reply.Err = OK
			Debug_(dLeader, "S%d [Move] Shard %v -> G%v", sc.me, args.Shard, args.GID)
			sc.mu.Unlock()
			return
		}
		if time.Since(t0).Milliseconds() > 40 {
			reply.WrongLeader = true
			sc.mu.Unlock()
			return
		}
		sc.mu.Unlock()
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	sc.mu.Lock()
	Debug_(dTrace, "S%d <-[Query] Cid %d Num %d Seq %d", sc.me, args.ClientId, args.Num, args.Seq)
	// follower可以返回往期的config，但不能返回最新config
	for _, cfg := range sc.configs {
		//Debug_(dSnap, "S%d cgfNum %d argsNum %d", sc.me, cfg.Num, args.Num)
		if cfg.Num == args.Num {
			reply.Err = OK
			reply.Config = cfg
			sc.mu.Unlock()
			return
		}
	}
	_, isLeader := sc.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}
	// 发送指令到底层raft
	OP := Op{
		ClientId: args.ClientId,
		Seq:      args.Seq,
		Type:     Query,
		Cfg:      Config{},
	}
	//sc.rf.Start(OP)
	sc.CommitQueue = append(sc.CommitQueue, OP)
	sc.mu.Unlock()
	// 等待操作完成
	t0 := time.Now()
	for {
		sc.mu.Lock()
		if args.Seq <= sc.done[args.ClientId] {
			reply.Err = OK
			reply.Config = sc.query[args.ClientId]
			//Debug_(dDrop, "S%d [%s] Cid %d NewConfig %v Num %d configs %v", sc.me, OP.Type, args.ClientId, reply.Config, args.Num, sc.configs)
			sc.mu.Unlock()
			return
		}
		if time.Since(t0).Milliseconds() > 40 {
			reply.WrongLeader = true
			sc.mu.Unlock()
			return
		}
		sc.mu.Unlock()
	}
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&sc.dead, 1)
}
func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func (sc *ShardCtrler) detectCommit() {
	for apply := range sc.applyCh {
		if apply.SnapshotValid {
			sc.readPersister(apply.Snapshot)
			continue
		}
		// nop 忽略
		if apply.Command == -1 {
			continue
		}
		OP := apply.Command.(Op)
		// 重复apply都忽略
		sc.mu.Lock()
		if OP.Seq <= sc.done[OP.ClientId] {
			sc.mu.Unlock()
			continue
		}
		// 按类型处理apply的reConfig
		switch OP.Type {
		case Join:
			var newConfig Config
			newConfig.Groups = make(map[int][]string)
			sc.copyLatest(&newConfig)
			for k, v := range OP.Cfg.Groups {
				newConfig.Groups[k] = v
			}
			// 新Group加入,平均shard,获取新config
			sc.balance(&newConfig)
			newConfig.Num = sc.nextIndex
			sc.nextIndex++
			Debug_(dClient, "S%d [%s] NewConfig %v", sc.me, OP.Type, newConfig)
			sc.configs = append(sc.configs, newConfig)
			sc.done[OP.ClientId] = OP.Seq
			sc.persist(apply.CommandIndex)
		case Leave:
			var newConfig Config
			newConfig.Groups = make(map[int][]string)
			sc.copyLatest(&newConfig)
			for k, _ := range OP.Cfg.Groups {
				delete(newConfig.Groups, k)
			}
			// 平均shard,获取新config
			sc.balance(&newConfig)
			newConfig.Num = sc.nextIndex
			sc.nextIndex++
			Debug_(dClient, "S%d [%s] NewConfig %v", sc.me, OP.Type, newConfig)
			sc.configs = append(sc.configs, newConfig)
			sc.done[OP.ClientId] = OP.Seq
			sc.persist(apply.CommandIndex)
		case Move:
			var newConfig Config
			newConfig.Groups = make(map[int][]string)
			sc.copyLatest(&newConfig)
			GId := -1
			for k := range OP.Cfg.Groups {
				GId = k
			}
			// 将指定shard分配给指定Group
			newConfig.Shards[OP.Cfg.Shards[0]] = GId
			// 不再平均shard,获取新config
			newConfig.Num = sc.nextIndex
			sc.nextIndex++
			Debug_(dClient, "S%d [%s] NewConfig %v", sc.me, OP.Type, newConfig)
			sc.configs = append(sc.configs, newConfig)
			sc.done[OP.ClientId] = OP.Seq
			sc.persist(apply.CommandIndex)
		case Query:
			sc.query[OP.ClientId] = sc.configs[len(sc.configs)-1]
			sc.done[OP.ClientId] = OP.Seq
		}
		//Debug_(dPersist, "S%d [%s] Commit Len %d GLen %d", sc.me, OP.Type, len(sc.configs), len(sc.configs[len(sc.configs)-1].Groups))
		sc.mu.Unlock()
	}
}

func (sc *ShardCtrler) Decode(serverState []byte) (int, map[int64]int, []Config) {
	r := bytes.NewBuffer(serverState)
	d := labgob.NewDecoder(r)
	var nextIndex int
	var done map[int64]int
	var configs []Config
	if d.Decode(&nextIndex) != nil || d.Decode(&done) != nil ||
		d.Decode(&configs) != nil {
		log.Fatalf("Read Persist Error!\n")
	}
	return nextIndex, done, configs
}

func (sc *ShardCtrler) readPersister(serverState []byte) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if serverState == nil {
		serverState = sc.persister.ReadSnapshot()
	}
	if len(serverState) < 1 || serverState == nil {
		return
	}
	sc.nextIndex, sc.done, sc.configs = sc.Decode(serverState)
}

func (sc *ShardCtrler) Encode(nextIndex int, done map[int64]int, configs []Config) []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(nextIndex) != nil || e.Encode(done) != nil ||
		e.Encode(configs) != nil {
		log.Fatalf("Encode Persist Error !\n")
	}
	raftstate := w.Bytes()
	return raftstate
}

func (sc *ShardCtrler) persist(index int) {
	serverState := sc.Encode(sc.nextIndex, sc.done, sc.configs)
	go sc.rf.Snapshot(index, serverState)
}
func (sc *ShardCtrler) detectCommitQueue() {
	for !sc.killed() {
		if len(sc.CommitQueue) == 0 {
			time.Sleep(2 * time.Millisecond)
			continue
		}
		sc.rf.Start(sc.CommitQueue[0])
		sc.CommitQueue = sc.CommitQueue[1:]
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me
	sc.nextIndex = 1
	sc.persister = persister

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}
	sc.configs[0].Num = 0
	sc.done = make(map[int64]int)
	sc.query = make(map[int64]Config)

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)
	sc.CommitQueue = make([]Op, 0)

	sc.readPersister(nil)

	// Your code here.
	go sc.detectCommitQueue()
	go sc.detectCommit()

	return sc
}
