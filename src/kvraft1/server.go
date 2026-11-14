package kvraft

import (
	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/tester1"
)

type KVServer struct {
	me  int
	rsm *rsm.RSM
}

func (kv *KVServer) DoOp(req any) any {
	return nil
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

	kv := &KVServer{me: me}

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	return []any{kv, kv.rsm.Raft()}
}

func NewServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, grp tester.Tgid, srv int, persister *tester.Persister) []any {
	return StartKVServer(ends, Gid, srv, persister, tester.MaxRaftState)
}
