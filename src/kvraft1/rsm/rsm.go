package rsm

import (
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/raft1"
	"6.5840/raftapi"
	"6.5840/tester1"
)

type Op struct {
	Me  int
	Id  uint64
	Req any
}

type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type pendingRequest struct {
	index int
	done  chan any
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int
	sm           StateMachine
	nextID       uint64
	pending      map[uint64]pendingRequest
}

func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,
		pending:      make(map[uint64]pendingRequest),
	}
	if !tester.UseRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}
	go rsm.readApply()
	return rsm
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

func (rsm *RSM) Submit(req any) (rpc.Err, any) {
	rsm.mu.Lock()
	rsm.nextID++
	op := Op{Me: rsm.me, Id: rsm.nextID, Req: req}
	index, _, leader := rsm.rf.Start(op)
	if !leader {
		rsm.mu.Unlock()
		return rpc.ErrWrongLeader, nil
	}
	done := make(chan any, 1)
	rsm.pending[op.Id] = pendingRequest{index: index, done: done}
	rsm.mu.Unlock()

	return rpc.OK, <-done
}

func (rsm *RSM) readApply() {
	for msg := range rsm.applyCh {
		if !msg.CommandValid {
			continue
		}
		op := msg.Command.(Op)
		result := rsm.sm.DoOp(op.Req)

		rsm.mu.Lock()
		if pending, ok := rsm.pending[op.Id]; ok && op.Me == rsm.me && msg.CommandIndex == pending.index {
			pending.done <- result
			delete(rsm.pending, op.Id)
		}
		rsm.mu.Unlock()
	}
}
