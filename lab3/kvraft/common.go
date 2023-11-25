package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

//type Type string

//const (
//	Get    Type = "Get"
//	Put    Type = "Put"
//	Append Type = "Append"
//)
//
type Err string

// Put or Append
type PutAppendArgs struct {
	Key      string
	Value    string
	Op       string // "Put" or "Append"
	Seq      int
	ClientId int64
}

type PutAppendReply struct {
	Err Err
	Seq int
}

type GetArgs struct {
	Key      string
	Seq      int
	ClientId int64
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
	Seq   int
}
