SHELL := /bin/bash
PROJECT=tinykv
GOPATH ?= $(shell go env GOPATH)

# Ensure GOPATH is set before running build process.
ifeq "$(GOPATH)" ""
  $(error Please set the environment variable GOPATH before running `make`)
endif

GO                  := GO111MODULE=on go
GOBUILD             := $(GO) build $(BUILD_FLAG) -tags codes
GOTEST              := $(GO) test -v --count=1 --parallel=1 -p=1
TEST_CLEAN          := rm -rf /tmp/*test-raftstore*

TEST_LDFLAGS        := ""

PACKAGE_LIST        := go list ./...| grep -vE "cmd"
PACKAGES            := $$($(PACKAGE_LIST))

# Targets
.PHONY: clean test proto kv scheduler dev

default: kv scheduler

dev: default test

test:
	@echo "Running tests in native mode."
	@export TZ='Asia/Shanghai'; \
	LOG_LEVEL=fatal $(GOTEST) -cover $(PACKAGES)

CURDIR := $(shell pwd)
export PATH := $(CURDIR)/bin/:$(PATH)
proto:
	mkdir -p $(CURDIR)/bin
	(cd proto && ./generate_go.sh)
	GO111MODULE=on go build ./proto/pkg/...

kv:
	$(GOBUILD) -o bin/tinykv-server kv/main.go

scheduler:
	$(GOBUILD) -o bin/tinyscheduler-server scheduler/main.go

ci: default
	@echo "Checking formatting"
	@test -z "$$(gofmt -s -l $$(find . -name '*.go' -type f -print) | tee /dev/stderr)"
	@echo "Running Go vet"
	@go vet ./...

format:
	@gofmt -s -w `find . -name '*.go' -type f ! -path '*/_tools/*' -print`

project1:
	$(GOTEST) ./kv/server -run 1

project2: project2a project2b project2c

project2a:
	$(GOTEST) ./raft -run 2A

project2aa:
	$(GOTEST) ./raft -run 2AA

project2ab:
	$(GOTEST) ./raft -run 2AB

project2ac:
	$(GOTEST) ./raft -run 2AC

project2b:
	$(TEST_CLEAN)
	$(GOTEST) ./kv/test_raftstore -run ^TestBasic2B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestConcurrent2B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestUnreliable2B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestOnePartition2B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestManyPartitionsOneClient2B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestManyPartitionsManyClients2B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestPersistOneClient2B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestPersistConcurrent2B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestPersistConcurrentUnreliable2B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestPersistPartition2B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestPersistPartitionUnreliable2B$ || true
	$(TEST_CLEAN)
#project2b:
#	$(TEST_CLEAN)
#	$(GOTEST) -v ./kv/test_raftstore -run ^TestBasic2B$ | grep PASS || true
#	$(GOTEST) -v ./kv/test_raftstore -run ^TestConcurrent2B$ | grep PASS || true
#	$(GOTEST) -v ./kv/test_raftstore -run ^TestUnreliable2B$ | grep PASS || true
#	$(GOTEST) -v ./kv/test_raftstore -run ^TestOnePartition2B$ | grep PASS || true
#	$(GOTEST) -v ./kv/test_raftstore -run ^TestManyPartitionsOneClient2B$ | grep PASS || true
#	$(GOTEST) -v ./kv/test_raftstore -run ^TestManyPartitionsManyClients2B$ | grep PASS || true
#	$(GOTEST) -v ./kv/test_raftstore -run ^TestPersistOneClient2B$ | grep PASS || true
#	$(GOTEST) -v ./kv/test_raftstore -run ^TestPersistConcurrent2B$ | grep PASS || true
#	$(GOTEST) -v ./kv/test_raftstore -run ^TestPersistConcurrentUnreliable2B$ | grep PASS || true
#	$(GOTEST) -v ./kv/test_raftstore -run ^TestPersistPartition2B$ | grep PASS || true
#	$(GOTEST) -v ./kv/test_raftstore -run ^TestPersistPartitionUnreliable2B$ | grep PASS || true
#	$(TEST_CLEAN)
#project2b:
#	$(TEST_CLEAN)
#	$(GOTEST) -v ./kv/test_raftstore -run ^TestBasic2B$ | grep -E "PASS|ok|FAIL" || true
#	$(GOTEST) -v ./kv/test_raftstore -run ^TestConcurrent2B$ | grep -E "PASS|ok|FAIL" || true
#	$(GOTEST) -v ./kv/test_raftstore -run ^TestUnreliable2B$ | grep -E "PASS|ok|FAIL" || true
#	$(GOTEST) -v ./kv/test_raftstore -run ^TestOnePartition2B$ | grep -E "PASS|ok|FAIL" || true
#	$(GOTEST) -v ./kv/test_raftstore -run ^TestManyPartitionsOneClient2B$ | grep -E "PASS|ok|FAIL" || true
#	$(GOTEST) -v ./kv/test_raftstore -run ^TestManyPartitionsManyClients2B$ | grep -E "PASS|ok|FAIL" || true
#	$(GOTEST) -v ./kv/test_raftstore -run ^TestPersistOneClient2B$ | grep -E "PASS|ok|FAIL" || true
#	$(GOTEST) -v ./kv/test_raftstore -run ^TestPersistConcurrent2B$ | grep -E "PASS|ok|FAIL" || true
#	$(GOTEST) -v ./kv/test_raftstore -run ^TestPersistConcurrentUnreliable2B$ | grep -E "PASS|ok|FAIL" || true
#	$(GOTEST) -v ./kv/test_raftstore -run ^TestPersistPartition2B$ | grep -E "PASS|ok|FAIL" || true
#	$(GOTEST) -v ./kv/test_raftstore -run ^TestPersistPartitionUnreliable2B$ | grep -E "PASS|ok|FAIL" || true
#	$(TEST_CLEAN)



project2c:
	$(TEST_CLEAN)
	$(GOTEST) ./raft -run 2C || true
	$(GOTEST) ./kv/test_raftstore -run ^TestOneSnapshot2C$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestSnapshotRecover2C$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestSnapshotRecoverManyClients2C$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestSnapshotUnreliable2C$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestSnapshotUnreliableRecover2C$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestSnapshotUnreliableRecoverConcurrentPartition2C$ || true
	$(TEST_CLEAN)

project2c_SnapshotRecover:
	$(TEST_CLEAN)
	$(GOTEST) ./kv/test_raftstore -run ^TestSnapshotRecover2C$ || true
	$(TEST_CLEAN)

project2c_TestSnapshotUnreliable5Times:
	$(TEST_CLEAN)
	$(GOTEST) ./kv/test_raftstore -run ^TestSnapshotUnreliable2C$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestSnapshotUnreliable2C$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestSnapshotUnreliable2C$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestSnapshotUnreliable2C$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestSnapshotUnreliable2C$ || true
	$(TEST_CLEAN)

project3: project3a project3b project3c

project3a:
	$(GOTEST) ./raft -run 3A

project3b:
	$(TEST_CLEAN)
	$(GOTEST) ./kv/test_raftstore -run ^$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestBasicConfChange3B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestConfChangeRemoveLeader3B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestConfChangeRecover3BTestTransferLeader3B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestConfChangeRecoverManyClients3B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestConfChangeUnreliable3B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestConfChangeUnreliableRecover3B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestConfChangeSnapshotUnreliableRecover3B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestConfChangeSnapshotUnreliableRecoverConcurrentPartition3B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestOneSplit3B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestSplitRecover3B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestSplitRecoverManyClients3B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestSplitUnreliable3B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestSplitUnreliableRecover3B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestSplitConfChangeSnapshotUnreliableRecover3B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestSplitConfChangeSnapshotUnreliableRecoverConcurrentPartition3B$ || true
	$(TEST_CLEAN)

project3b_multiTests:
	$(TEST_CLEAN)
	for i in {1..10}; do \
		$(GOTEST) ./kv/test_raftstore -run ^$(TEST_NAME)$$ | grep -E "PASS|FAIL|run" || true; \
	done
	$(TEST_CLEAN)

project3b_multiTestsWithError:
	$(TEST_CLEAN)
	for i in {1..10}; do \
		$(GOTEST) ./kv/test_raftstore -run ^$(TEST_NAME)$$ | grep -v "info" || true; \
	done
	$(TEST_CLEAN)

project3b_multiTestsWithErrors:
	$(TEST_CLEAN)
	PASS_COUNT=0; \
	for i in {1..10}; do \
		RESULT=$$($(GOTEST) ./kv/test_raftstore -run ^$(TEST_NAME)$$ | grep -v "info" || true); \
		echo "$$RESULT"; \
		PASS_COUNT=$$(($$PASS_COUNT + $$(echo "$$RESULT" | grep -c "PASS"))); \
	done; \
	HALF_PASS_COUNT=$$(($$PASS_COUNT / 2)); \
	GREEN='\033[0;32m'; \
	NC='\033[0m'; \
	echo -e "$$GREEN TOTAL tests: 10 $$NC"; \
	echo -e "$$GREEN PASS  tests: $$HALF_PASS_COUNT $$NC"
	$(TEST_CLEAN)


project3b_TransferLeader:
	$(TEST_CLEAN)
	$(GOTEST) ./kv/test_raftstore -run ^TestTransferLeader3B$ || true
	$(TEST_CLEAN)
project3b_ConfChange:
	$(TEST_CLEAN)
	$(GOTEST) ./kv/test_raftstore -run ^TestBasicConfChange3B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestConfChangeRemoveLeader3B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestConfChangeRecover3B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestConfChangeRecoverManyClients3B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestConfChangeUnreliable3B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestConfChangeUnreliableRecover3B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestConfChangeSnapshotUnreliableRecover3B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestConfChangeSnapshotUnreliableRecoverConcurrentPartition3B$ || true
	$(TEST_CLEAN)
project3b_TestConfChangeRemoveLeader3B10s:
	$(TEST_CLEAN)
	$(GOTEST) ./kv/test_raftstore -run ^TestConfChangeRemoveLeader3B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestConfChangeRemoveLeader3B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestConfChangeRemoveLeader3B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestConfChangeRemoveLeader3B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestConfChangeRemoveLeader3B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestConfChangeRemoveLeader3B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestConfChangeRemoveLeader3B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestConfChangeRemoveLeader3B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestConfChangeRemoveLeader3B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestConfChangeRemoveLeader3B$ || true
	$(TEST_CLEAN)
project3b_TestConfChangeUnreliableRecover3B10s:
	$(TEST_CLEAN)
	$(GOTEST) ./kv/test_raftstore -run ^TestConfChangeUnreliableRecover3B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestConfChangeUnreliableRecover3B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestConfChangeUnreliableRecover3B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestConfChangeUnreliableRecover3B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestConfChangeUnreliableRecover3B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestConfChangeUnreliableRecover3B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestConfChangeUnreliableRecover3B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestConfChangeUnreliableRecover3B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestConfChangeUnreliableRecover3B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestConfChangeUnreliableRecover3B$ || true
	$(TEST_CLEAN)
project3b_Split:
	$(TEST_CLEAN)
	$(GOTEST) ./kv/test_raftstore -run ^TestOneSplit3B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestSplitRecover3B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestSplitRecoverManyClients3B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestSplitUnreliable3B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestSplitUnreliableRecover3B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestSplitConfChangeSnapshotUnreliableRecover3B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestSplitConfChangeSnapshotUnreliableRecoverConcurrentPartition3B$ || true
	$(TEST_CLEAN)
project3b_TestOneSplit3B10s:
	$(TEST_CLEAN)
	$(GOTEST) ./kv/test_raftstore -run ^TestOneSplit3B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestOneSplit3B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestOneSplit3B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestOneSplit3B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestOneSplit3B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestOneSplit3B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestOneSplit3B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestOneSplit3B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestOneSplit3B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestOneSplit3B$ || true
	$(TEST_CLEAN)
project3b_TestSplitConfChangeSnapshotUnreliableRecoverConcurrentPartition3B10s:
	$(TEST_CLEAN)
	$(GOTEST) ./kv/test_raftstore -run ^TestSplitConfChangeSnapshotUnreliableRecoverConcurrentPartition3B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestSplitConfChangeSnapshotUnreliableRecoverConcurrentPartition3B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestSplitConfChangeSnapshotUnreliableRecoverConcurrentPartition3B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestSplitConfChangeSnapshotUnreliableRecoverConcurrentPartition3B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestSplitConfChangeSnapshotUnreliableRecoverConcurrentPartition3B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestSplitConfChangeSnapshotUnreliableRecoverConcurrentPartition3B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestSplitConfChangeSnapshotUnreliableRecoverConcurrentPartition3B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestSplitConfChangeSnapshotUnreliableRecoverConcurrentPartition3B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestSplitConfChangeSnapshotUnreliableRecoverConcurrentPartition3B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestSplitConfChangeSnapshotUnreliableRecoverConcurrentPartition3B$ || true
	$(TEST_CLEAN)
project3c:
	$(GOTEST) ./scheduler/server ./scheduler/server/schedulers -check.f="3C"

project4: project4a project4b project4c

project4a:
	$(GOTEST) ./kv/transaction/... -run 4A

project4b:
	$(GOTEST) ./kv/transaction/... -run 4B

project4c:
	$(GOTEST) ./kv/transaction/... -run 4C
