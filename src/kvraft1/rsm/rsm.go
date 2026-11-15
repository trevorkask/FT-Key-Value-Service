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

type RSM struct {
	mu           sync.Mutex
	submitMu     sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int
	sm           StateMachine
	nextID       uint64
	pendingIndex int
	pending      chan any
}

func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,
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
	rsm.submitMu.Lock()
	defer rsm.submitMu.Unlock()

	rsm.mu.Lock()
	rsm.nextID++
	op := Op{Me: rsm.me, Id: rsm.nextID, Req: req}
	index, _, leader := rsm.rf.Start(op)
	if !leader {
		rsm.mu.Unlock()
		return rpc.ErrWrongLeader, nil
	}
	done := make(chan any, 1)
	rsm.pendingIndex = index
	rsm.pending = done
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
		if rsm.pending != nil && msg.CommandIndex == rsm.pendingIndex && op.Me == rsm.me && op.Id == rsm.nextID {
			rsm.pending <- result
			rsm.pending = nil
		}
		rsm.mu.Unlock()
	}
}
