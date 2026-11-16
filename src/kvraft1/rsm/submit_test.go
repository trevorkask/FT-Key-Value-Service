package rsm

import (
	"reflect"
	"testing"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/raftapi"
)

type submissionRaft struct {
	started chan raftapi.ApplyMsg
	index   int
}

func (rf *submissionRaft) Start(command any) (int, int, bool) {
	rf.index++
	rf.started <- raftapi.ApplyMsg{CommandValid: true, Command: command, CommandIndex: rf.index}
	return rf.index, 1, true
}

func (rf *submissionRaft) GetState() (int, bool) {
	return 1, true
}

func (rf *submissionRaft) Snapshot(int, []byte) {}

func (rf *submissionRaft) PersistBytes() int {
	return 0
}

type echoStateMachine struct{}

func (sm *echoStateMachine) DoOp(req any) any {
	return req
}

func (sm *echoStateMachine) Snapshot() []byte {
	return nil
}

func (sm *echoStateMachine) Restore([]byte) {}

func TestConcurrentSubmissionResults(t *testing.T) {
	const count = 32
	rf := &submissionRaft{started: make(chan raftapi.ApplyMsg, count)}
	rsm := &RSM{
		rf:      rf,
		applyCh: make(chan raftapi.ApplyMsg, count),
		sm:      &echoStateMachine{},
		pending: make(map[uint64]pendingRequest),
	}
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		rsm.readApply()
	}()
	defer func() {
		close(rsm.applyCh)
		<-readerDone
	}()

	type result struct {
		want []int
		got  any
		err  rpc.Err
	}
	results := make(chan result, count)
	for i := 0; i < count; i++ {
		go func(i int) {
			req := []int{i, i * i}
			err, reply := rsm.Submit(req)
			results <- result{want: req, got: reply, err: err}
		}(i)
	}

	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	commands := make([]raftapi.ApplyMsg, 0, count)
	for len(commands) < count {
		select {
		case msg := <-rf.started:
			commands = append(commands, msg)
		case <-deadline.C:
			t.Fatalf("only %d of %d requests reached Raft before committing", len(commands), count)
		}
	}
	select {
	case <-results:
		t.Fatal("Submit returned before its command was committed")
	default:
	}

	for _, msg := range commands {
		rsm.applyCh <- msg
	}
	for i := 0; i < count; i++ {
		select {
		case result := <-results:
			if result.err != rpc.OK || !reflect.DeepEqual(result.got, result.want) {
				t.Fatalf("request %v returned %v, %v", result.want, result.got, result.err)
			}
		case <-deadline.C:
			t.Fatal("Submit did not return after its command was applied")
		}
	}
	rsm.mu.Lock()
	remaining := len(rsm.pending)
	rsm.mu.Unlock()
	if remaining != 0 {
		t.Fatalf("%d completed requests are still pending", remaining)
	}
}
