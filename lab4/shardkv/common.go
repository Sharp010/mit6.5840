package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	Accept         = "Accept"
	Deliver        = "Deliver"
	Release        = "Release"
	Put            = "Put"
	Append         = "Append"
	Get            = "Get"
)
const (
	YES = iota
	NO
	Sending
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key       string
	Value     string
	Op        string // "Put" or "Append"
	ClientId  int64
	Seq       int
	ConfigNum int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key       string
	ClientId  int64
	Seq       int
	ConfigNum int
}

type GetReply struct {
	Err   Err
	Value string
}

type ShardKvsArgs struct {
	ConfigNum int
	Shard     int
	Kv        map[string]string
	Table     map[int64]Ans
}
type ShardKvsReply struct {
	Err string
}
type ConfirmArgs struct {
	ConfigNum int
	Shard     int
}
type ConfirmReply struct {
	Err string
}
