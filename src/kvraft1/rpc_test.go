package kvraft

import (
	"testing"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/tester1"
)

func findHandlerLeader(t *testing.T, clnt *tester.Clnt, group *tester.ServerGrp, skip int) (int, rpc.GetReply) {
	t.Helper()
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); {
		for i, server := range group.SrvNames() {
			if i == skip {
				continue
			}
			var reply rpc.GetReply
			if clnt.Call(server, "KVServer.Get", &rpc.GetArgs{Key: "k"}, &reply) {
				if reply.Err == rpc.OK || reply.Err == rpc.ErrNoKey {
					return i, reply
				}
				if reply.Err != rpc.ErrWrongLeader {
					t.Fatalf("server %d returned unexpected Get error %v", i, reply.Err)
				}
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("no leader completed Get")
	return -1, rpc.GetReply{}
}

func TestReplicatedHandlers(t *testing.T) {
	cfg := tester.MakeConfig(t, 3, true, "kvraft1d", []string{"--max-raft-state=-1"})
	defer cfg.Cleanup()
	group := cfg.Group(Gid)
	clnt := cfg.MakeClient()
	leader, get := findHandlerLeader(t, clnt, group, -1)
	if get != (rpc.GetReply{Err: rpc.ErrNoKey}) {
		t.Fatalf("missing key returned %+v", get)
	}

	for i, server := range group.SrvNames() {
		if i == leader {
			continue
		}
		var get rpc.GetReply
		if !clnt.Call(server, "KVServer.Get", &rpc.GetArgs{Key: "k"}, &get) || get.Err != rpc.ErrWrongLeader {
			t.Fatalf("follower %d Get returned %+v", i, get)
		}
		var put rpc.PutReply
		if !clnt.Call(server, "KVServer.Put", &rpc.PutArgs{Key: "k", Value: "follower"}, &put) || put.Err != rpc.ErrWrongLeader {
			t.Fatalf("follower %d Put returned %+v", i, put)
		}
	}

	for _, step := range []struct {
		args rpc.PutArgs
		err  rpc.Err
	}{
		{rpc.PutArgs{Key: "k", Value: "bad", Version: 1}, rpc.ErrNoKey},
		{rpc.PutArgs{Key: "k", Value: "first"}, rpc.OK},
		{rpc.PutArgs{Key: "k", Value: "first"}, rpc.ErrVersion},
	} {
		var reply rpc.PutReply
		if !clnt.Call(group.SrvName(leader), "KVServer.Put", &step.args, &reply) || reply.Err != step.err {
			t.Fatalf("Put(%+v) returned %+v, want %v", step.args, reply, step.err)
		}
	}
	get = rpc.GetReply{}
	if !clnt.Call(group.SrvName(leader), "KVServer.Get", &rpc.GetArgs{Key: "k"}, &get) ||
		get != (rpc.GetReply{Value: "first", Version: 1, Err: rpc.OK}) {
		t.Fatalf("Get after Put returned %+v", get)
	}

	group.DisconnectAll(leader)
	newLeader, get := findHandlerLeader(t, clnt, group, leader)
	if get != (rpc.GetReply{Value: "first", Version: 1, Err: rpc.OK}) {
		t.Fatalf("new leader lost the committed value: %+v", get)
	}
	var put rpc.PutReply
	if !clnt.Call(group.SrvName(newLeader), "KVServer.Put", &rpc.PutArgs{Key: "k", Value: "second", Version: 1}, &put) || put.Err != rpc.OK {
		t.Fatalf("majority Put returned %+v", put)
	}

	type getResult struct {
		ok    bool
		reply rpc.GetReply
	}
	done := make(chan getResult, 1)
	go func() {
		var reply rpc.GetReply
		ok := clnt.Call(group.SrvName(leader), "KVServer.Get", &rpc.GetArgs{Key: "k"}, &reply)
		done <- getResult{ok: ok, reply: reply}
	}()
	select {
	case result := <-done:
		if !result.ok || result.reply.Err != rpc.ErrWrongLeader {
			t.Fatalf("isolated server answered Get without a majority: %+v", result)
		}
		done <- result
	case <-time.After(200 * time.Millisecond):
	}
	group.ConnectAll()
	select {
	case result := <-done:
		if !result.ok {
			t.Fatal("Get RPC failed after reconnecting")
		}
		if result.reply.Err != rpc.ErrWrongLeader && result.reply != (rpc.GetReply{Value: "second", Version: 2, Err: rpc.OK}) {
			t.Fatalf("Get after reconnecting returned %+v", result.reply)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Get did not finish after reconnecting")
	}
}
