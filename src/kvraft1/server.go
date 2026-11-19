package kvraft

import (
	"sync"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/tester1"
)

type entry struct {
	value   string
	version rpc.Tversion
}

type KVServer struct {
	mu   sync.Mutex
	me   int
	rsm  *rsm.RSM
	data map[string]entry
}

func (kv *KVServer) DoOp(req any) any {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	switch args := req.(type) {
	case rpc.GetArgs:
		item, ok := kv.data[args.Key]
		if !ok {
			return rpc.GetReply{Err: rpc.ErrNoKey}
		}
		return rpc.GetReply{Value: item.value, Version: item.version, Err: rpc.OK}
	case rpc.PutArgs:
		item, ok := kv.data[args.Key]
		if !ok && args.Version != 0 {
			return rpc.PutReply{Err: rpc.ErrNoKey}
		}
		if item.version != args.Version {
			return rpc.PutReply{Err: rpc.ErrVersion}
		}
		kv.data[args.Key] = entry{value: args.Value, version: args.Version + 1}
		return rpc.PutReply{Err: rpc.OK}
	default:
		panic("unsupported key/value operation")
	}
}

func (kv *KVServer) Snapshot() []byte {
	return nil
}

func (kv *KVServer) Restore(data []byte) {
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
}

func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []any {
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})

	kv := &KVServer{me: me, data: make(map[string]entry)}

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	return []any{kv, kv.rsm.Raft()}
}

func NewServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, grp tester.Tgid, srv int, persister *tester.Persister) []any {
	return StartKVServer(ends, Gid, srv, persister, tester.MaxRaftState)
}
