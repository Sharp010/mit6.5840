package kvraft

import (
	"6.5840/labrpc"
	"sync"
	"time"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	ClientId int64
	Seq      int
	mu       sync.Mutex
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.ClientId = nrand()
	ck.mu = sync.Mutex{}
	ck.Seq = 1
	return ck
}

var retry_interval = 80 * time.Millisecond

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	args := &GetArgs{
		Key:      key,
		Seq:      ck.Seq,
		ClientId: ck.ClientId,
	}
	for {
		for _, server := range ck.servers {
			reply := &GetReply{}
			ok := server.Call("KVServer.Get", args, reply)
			// OK or ErrNoKey
			if ok && reply.Err != ErrWrongLeader {
				ck.Seq++
				return reply.Value
			}
		}
		time.Sleep(80 * time.Millisecond)
	}

	//ck.mu.Lock()
	//ret := ""
	////quit := false
	//wg := sync.WaitGroup{}
	//wg.Add(1)
	//for k := range ck.servers {
	//	go func(server int, seq int) {
	//		for {
	//			ck.mu.Lock()
	//			if seq != ck.Seq {
	//				ck.mu.Unlock()
	//				return
	//			}
	//			args := &GetArgs{
	//				Key:      key,
	//				Seq:      seq,
	//				ClientId: ck.ClientId,
	//			}
	//			reply := &GetReply{}
	//			ck.mu.Unlock()
	//			Debug_(dLeader, "S%d <-=[Get] Cid %d Seq %d Key [%s] Value %s", server, ck.ClientId%10000, seq, key, reply.Value)
	//			ok := ck.servers[server].Call("KVServer.Get", args, reply)
	//
	//			ck.mu.Lock()
	//			if seq != ck.Seq {
	//				ck.mu.Unlock()
	//				return
	//			}
	//			if !ok {
	//				ck.mu.Unlock()
	//				time.Sleep(10 * time.Millisecond)
	//				continue
	//			}
	//			switch reply.Err {
	//			case OK:
	//				// 成功返回
	//				Debug_(dClient, "S%d <-=[Get] Cid %d Seq %d Key [%s] value %s SUCCESS!", server, ck.ClientId%10000, seq, key, reply.Value)
	//				ret = reply.Value
	//				wg.Done()
	//				ck.Seq++
	//				ck.mu.Unlock()
	//				return
	//			case ErrNoKey:
	//				// Get到空
	//				Debug_(dClient, "S%d <-=[Get] Cid %d Seq %d Key [%s] value %s SUCCESS!", server, ck.ClientId%10000, seq, key, reply.Value)
	//				wg.Done()
	//				ck.Seq++
	//				ck.mu.Unlock()
	//				return
	//			case ErrWrongLeader:
	//				// 不是leader
	//				Debug_(dClient, "S%d <-=[Get] Cid %d Seq %d Wait!1", server, ck.ClientId%10000, seq)
	//				ck.mu.Unlock()
	//				time.Sleep(retry_interval)
	//				continue
	//			}
	//		}
	//	}(k, ck.Seq)
	//}
	//ck.mu.Unlock()
	//wg.Wait()
	//return ret
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := &PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		Seq:      ck.Seq,
		ClientId: ck.ClientId,
	}
	for {
		for _, server := range ck.servers {
			reply := &PutAppendReply{}
			ok := server.Call("KVServer.PutAppend", args, reply)
			if ok && reply.Err != ErrWrongLeader {
				ck.Seq++
				return
			}
		}
		time.Sleep(80 * time.Millisecond)
	}

	//ck.mu.Lock()
	////quit := false
	//wg := sync.WaitGroup{}
	//wg.Add(1)
	//for k := range ck.servers {
	//	go func(server int, seq int) {
	//		for {
	//			ck.mu.Lock()
	//			if ck.Seq != seq {
	//				ck.mu.Unlock()
	//				return
	//			}
	//			args := &PutAppendArgs{
	//				Key:      key,
	//				Value:    value,
	//				Op:       op,
	//				Seq:      seq,
	//				ClientId: ck.ClientId,
	//			}
	//			reply := &PutAppendReply{}
	//			ck.mu.Unlock()
	//			ok := ck.servers[server].Call("KVServer.PutAppend", args, reply)
	//			Debug_(dLeader, "S%d <-=[%s] Cid %d Seq %d Key [%s] Value [%s]", server, op, ck.ClientId%10000, seq, key, value)
	//			ck.mu.Lock()
	//			if ck.Seq != seq {
	//				ck.mu.Unlock()
	//				return
	//			}
	//			if !ok {
	//				ck.mu.Unlock()
	//				time.Sleep(10 * time.Millisecond)
	//				continue
	//			}
	//			switch reply.Err {
	//			case OK:
	//				Debug_(dClient, "S%d <-=[%s] Cid %d Seq %d Key [%s] Value [%s] SUCCESS!", server, op, ck.ClientId%10000, seq, key, value)
	//				wg.Done()
	//				ck.Seq++
	//				ck.mu.Unlock()
	//				return
	//			case ErrWrongLeader:
	//				ck.mu.Unlock()
	//				time.Sleep(retry_interval)
	//				continue
	//			}
	//		}
	//	}(k, ck.Seq)
	//}
	//ck.mu.Unlock()
	//wg.Wait()
	//return
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
