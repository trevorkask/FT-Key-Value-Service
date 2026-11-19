package kvraft

import (
	"fmt"
	"sync"
	"testing"

	"6.5840/kvsrv1/rpc"
)

func TestStateMachineVersions(t *testing.T) {
	kv := &KVServer{data: make(map[string]entry)}
	missing := kv.DoOp(rpc.GetArgs{Key: "k"}).(rpc.GetReply)
	if missing != (rpc.GetReply{Err: rpc.ErrNoKey}) {
		t.Fatalf("missing Get returned %+v", missing)
	}

	steps := []struct {
		value   string
		version rpc.Tversion
		err     rpc.Err
		stored  string
		current rpc.Tversion
	}{
		{"bad", 1, rpc.ErrNoKey, "", 0},
		{"first", 0, rpc.OK, "first", 1},
		{"first", 0, rpc.ErrVersion, "first", 1},
		{"future", 2, rpc.ErrVersion, "first", 1},
		{"second", 1, rpc.OK, "second", 2},
		{"first", 0, rpc.ErrVersion, "second", 2},
		{"", 2, rpc.OK, "", 3},
	}
	for i, step := range steps {
		put := kv.DoOp(rpc.PutArgs{Key: "k", Value: step.value, Version: step.version}).(rpc.PutReply)
		if put.Err != step.err {
			t.Fatalf("step %d: Put returned %v, want %v", i, put.Err, step.err)
		}
		get := kv.DoOp(rpc.GetArgs{Key: "k"}).(rpc.GetReply)
		want := rpc.GetReply{Value: step.stored, Version: step.current, Err: rpc.OK}
		if step.current == 0 {
			want.Err = rpc.ErrNoKey
		}
		if get != want {
			t.Fatalf("step %d: Get returned %+v, want %+v", i, get, want)
		}
	}

	put := kv.DoOp(rpc.PutArgs{Key: "", Value: "other key"}).(rpc.PutReply)
	if put.Err != rpc.OK {
		t.Fatalf("Put with an empty key returned %v", put.Err)
	}
	for _, check := range []struct {
		key  string
		want rpc.GetReply
	}{
		{"k", rpc.GetReply{Value: "", Version: 3, Err: rpc.OK}},
		{"", rpc.GetReply{Value: "other key", Version: 1, Err: rpc.OK}},
		{"missing", rpc.GetReply{Err: rpc.ErrNoKey}},
	} {
		if get := kv.DoOp(rpc.GetArgs{Key: check.key}).(rpc.GetReply); get != check.want {
			t.Fatalf("Get(%q) returned %+v, want %+v", check.key, get, check.want)
		}
	}
}

func TestStateMachineConcurrentPut(t *testing.T) {
	kv := &KVServer{data: make(map[string]entry)}
	for version := rpc.Tversion(0); version < 10; version++ {
		start := make(chan struct{})
		winners := make(chan string, 32)
		var wg sync.WaitGroup
		for i := 0; i < 32; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				<-start
				value := fmt.Sprintf("%d/%d", version, i)
				put := kv.DoOp(rpc.PutArgs{Key: "k", Value: value, Version: version}).(rpc.PutReply)
				if put.Err == rpc.OK {
					winners <- value
				} else if put.Err != rpc.ErrVersion {
					t.Errorf("concurrent Put returned %v", put.Err)
				}
				get := kv.DoOp(rpc.GetArgs{Key: "k"}).(rpc.GetReply)
				if get.Err != rpc.OK || get.Version != version+1 {
					t.Errorf("concurrent Get returned %+v", get)
				}
			}(i)
		}
		close(start)
		wg.Wait()
		close(winners)
		if len(winners) != 1 {
			t.Fatalf("version %d: got %d successful writes, want 1", version, len(winners))
		}
		get := kv.DoOp(rpc.GetArgs{Key: "k"}).(rpc.GetReply)
		if winner := <-winners; get.Value != winner || get.Version != version+1 {
			t.Fatalf("version %d: Get returned %+v, winner %q", version, get, winner)
		}
	}
}
