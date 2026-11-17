package rsm

import (
	"reflect"
	"sync"
	"testing"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/raftapi"
)

type submissionRaft struct {
	mu      sync.Mutex
	started chan raftapi.ApplyMsg
	index   int
	term    int
	leader  bool
}

func (rf *submissionRaft) Start(command any) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !rf.leader {
		return -1, rf.term, false
	}
	rf.index++
	rf.started <- raftapi.ApplyMsg{CommandValid: true, Command: command, CommandIndex: rf.index}
	return rf.index, rf.term, true
}

func (rf *submissionRaft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.term, rf.leader
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

func makeSubmissionRSM(t *testing.T, count int) (*RSM, *submissionRaft, func()) {
	t.Helper()
	rf := &submissionRaft{started: make(chan raftapi.ApplyMsg, count), term: 1, leader: true}
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
	var once sync.Once
	stop := func() {
		once.Do(func() {
			close(rsm.applyCh)
			<-readerDone
		})
	}
	t.Cleanup(stop)
	return rsm, rf, stop
}

func TestConcurrentSubmissionResults(t *testing.T) {
	const count = 32
	rsm, rf, _ := makeSubmissionRSM(t, count)

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

func TestSubmissionInterrupted(t *testing.T) {
	for _, reason := range []string{"follower", "new term", "replaced entry", "shutdown"} {
		t.Run(reason, func(t *testing.T) {
			rsm, rf, stop := makeSubmissionRSM(t, 1)
			result := make(chan rpc.Err, 1)
			go func() {
				err, _ := rsm.Submit("old request")
				result <- err
			}()
			var msg raftapi.ApplyMsg
			select {
			case msg = <-rf.started:
			case <-time.After(time.Second):
				t.Fatal("request did not reach Raft")
			}
			switch reason {
			case "follower":
				rf.mu.Lock()
				rf.leader = false
				rf.mu.Unlock()
			case "new term":
				rf.mu.Lock()
				rf.term++
				rf.mu.Unlock()
			case "replaced entry":
				op := msg.Command.(Op)
				op.Me++
				op.Req = "replacement"
				msg.Command = op
				rsm.applyCh <- msg
			case "shutdown":
				stop()
			}
			select {
			case err := <-result:
				if err != rpc.ErrWrongLeader {
					t.Fatalf("interrupted request returned %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("interrupted request did not return")
			}
			rsm.mu.Lock()
			remaining := len(rsm.pending)
			rsm.mu.Unlock()
			if remaining != 0 {
				t.Fatalf("%d interrupted requests are still pending", remaining)
			}
			if reason == "shutdown" {
				if err, _ := rsm.Submit("after shutdown"); err != rpc.ErrWrongLeader {
					t.Fatalf("request after shutdown returned %v", err)
				}
				select {
				case <-rf.started:
					t.Fatal("request after shutdown reached Raft")
				default:
				}
			}
		})
	}
}
