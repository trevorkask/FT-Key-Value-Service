RACE ?= -race
RUN ?=

.PHONY: all raft1-build rsm1-build kvraft1-build raft1 rsm1 kvraft1

all: rsm1 kvraft1

raft1-build:
	go build $(RACE) -o main/raft1d main/raft1d.go

rsm1-build:
	go build $(RACE) -o main/rsm1d main/rsm1d.go

kvraft1-build:
	go build $(RACE) -o main/kvraft1d main/kvraft1d.go

raft1: raft1-build
	cd raft1 && go test -v $(RACE) $(RUN)

rsm1: rsm1-build
	cd kvraft1/rsm && go test -v $(RACE) $(RUN)

kvraft1: kvraft1-build
	cd kvraft1 && go test -v $(RACE) $(RUN)
