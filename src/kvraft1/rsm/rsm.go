package rsm

import (
	"sync"
	"time"

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
	term  int
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
	stopped      bool
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
	if rsm.stopped {
		rsm.mu.Unlock()
		return rpc.ErrWrongLeader, nil
	}
	rsm.nextID++
	op := Op{Me: rsm.me, Id: rsm.nextID, Req: req}
	index, term, leader := rsm.rf.Start(op)
	if !leader {
		rsm.mu.Unlock()
		return rpc.ErrWrongLeader, nil
	}
	done := make(chan any, 1)
	rsm.pending[op.Id] = pendingRequest{index: index, term: term, done: done}
	rsm.mu.Unlock()

	result, ok := <-done
	if !ok {
		return rpc.ErrWrongLeader, nil
	}
	return rpc.OK, result
}

func (rsm *RSM) readApply() {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	defer func() {
		rsm.mu.Lock()
		defer rsm.mu.Unlock()
		rsm.stopped = true
		for id, pending := range rsm.pending {
			close(pending.done)
			delete(rsm.pending, id)
		}
	}()
	for {
		select {
		case msg, ok := <-rsm.applyCh:
			if !ok {
				return
			}
			if !msg.CommandValid {
				continue
			}
			op := msg.Command.(Op)
			result := rsm.sm.DoOp(op.Req)

			rsm.mu.Lock()
			for id, pending := range rsm.pending {
				if msg.CommandIndex != pending.index {
					continue
				}
				if op.Me == rsm.me && op.Id == id {
					pending.done <- result
				} else {
					close(pending.done)
				}
				delete(rsm.pending, id)
			}
			rsm.mu.Unlock()
		case <-ticker.C:
			rsm.mu.Lock()
			if len(rsm.pending) > 0 {
				term, leader := rsm.rf.GetState()
				for id, pending := range rsm.pending {
					if !leader || term != pending.term {
						close(pending.done)
						delete(rsm.pending, id)
					}
				}
			}
			rsm.mu.Unlock()
		}
	}
}
