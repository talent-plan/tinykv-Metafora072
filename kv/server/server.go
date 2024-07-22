package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"

	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4B)
	Latches *latches.Latches

	// coprocessor API handler, out of course scope
	copHandler *coprocessor.CopHandler
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// The below functions are Server's gRPC API (implements TinyKvServer).

// Raft commands (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

// Snapshot stream (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

// KvGet Transactional API.
// KvGet 在提供的时间戳处从数据库中读取一个值。如果在 KvGet 请求的时候，要读取的 key 被另一个事务锁定，那么 TinyKV 应该返回一个错误。
// 否则，TinyKV 必须搜索该 key 的版本以找到最新的、有效的值。
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	// Your Code Here (4B).
	/*
	type GetRequest struct {
		Context              *Context // 一个指向 Context 结构体的指针，包含请求的上下文信息。
		Key                  []byte   // 一个字节切片，表示要读取的键。
		Version              uint64   // 一个无符号 64 位整数，表示读取的时间点（版本）。
	}
	*/
	/*
	type GetResponse struct {
		RegionError *errorpb.Error ''
		Error       *KeyError
		Value       []byte
		// True if the requested key doesn't exist; another error will not be signalled.
		NotFound     bool
	}
	 */
	response := &kvrpcpb.GetResponse{}

	// 获取 Reader
	// 如果获取 Reader 时发生区域错误（RegionError），则将错误信息返回给客户端。
	reader, err := server.storage.Reader(req.GetContext())
	// 使用类型断言将 err 转换为 *raft_storage.RegionError 类型，并将结果赋值给 regionErr。
	// 同时，ok 是一个布尔值，表示类型断言是否成功。如果 err 是 *raft_storage.RegionError 类型，ok 将为 true，否则为 false。
	regionError, ok := err.(*raft_storage.RegionError)
	if ok { // 发生区域错误（RegionError）
		/*
		type RegionError struct {
			RequestErr *errorpb.Error
		}
		 */
		response.RegionError = regionError.RequestErr
		return response, nil
	}

	defer reader.Close()
	// 创建事务，获取 Lock
	txn := mvcc.NewMvccTxn(reader,req.GetVersion())
	lock, err := txn.GetLock(req.GetKey())
	regionError, ok = err.(*raft_storage.RegionError)
	if ok {
		response.RegionError = regionError.RequestErr
		return response, nil
	}

	// 如果锁存在且请求的版本号大于或等于锁的时间戳，则返回锁信息，表示需要等待锁释放。
	if lock != nil && req.Version >= lock.Ts {
		keyError := &kvrpcpb.KeyError{
			Locked:		&kvrpcpb.LockInfo{
				Key:			req.Key,
				PrimaryLock: 	lock.Primary,
				LockVersion:    lock.Ts,
				LockTtl:        lock.Ttl,
			},
		}
		response.Error = keyError
	}

	// 尝试获取指定键的值。如果获取值时发生区域错误，则将错误信息返回给客户端。如果值不存在，则设置 NotFound 标志。
	value, err := txn.GetValue(req.GetKey())
	if err != nil {
		regionError, ok = err.(*raft_storage.RegionError)
		if ok {
			response.RegionError = regionError.RequestErr
			return response, nil
		}
		return nil, err
	}

	if value == nil {
		response.NotFound = true
		response.Value = nil
	} else {
		response.NotFound = false
		response.Value = value
	}

	return response, nil
}

// KvPrewrite 是一个值被实际写入数据库的地方。一个 key 被锁定，一个 key 被存储。我们必须检查另一个事务没有锁定或写入同一个 key 。
func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	/*
	type PrewriteRequest struct {
		Context   *Context
		// 一个 Mutation 结构体的切片，包含客户端希望作为事务一部分进行的所有写操作（变更）。
		Mutations []*Mutation
		// Key of the primary lock.
		PrimaryLock          []byte
		StartVersion         uint64
		LockTtl              uint64
	}
	type PrewriteResponse struct {
		RegionError          *errorpb.Error
		Errors               []*KeyError
	}
	type Mutation struct {
		Op
		Key                  []byte
		Value                []byte
	}
	const (
		Op_Put      Op = 0
		Op_Del      Op = 1
		Op_Rollback Op = 2
		// Used by TinySQL but not TinyKV.
		Op_Lock     Op = 3
	)
	*/
	response := &kvrpcpb.PrewriteResponse{
		Errors: make([]*kvrpcpb.KeyError, 0),
	}

	// 获取 Reader
	// 如果获取 Reader 时发生区域错误（RegionError），则将错误信息返回给客户端。
	reader, err := server.storage.Reader(req.GetContext())
	// 使用类型断言将 err 转换为 *raft_storage.RegionError 类型，并将结果赋值给 regionErr。
	// 同时，ok 是一个布尔值，表示类型断言是否成功。如果 err 是 *raft_storage.RegionError 类型，ok 将为 true，否则为 false。
	regionError, ok := err.(*raft_storage.RegionError)
	if ok { // 发生区域错误（RegionError）
		/*
			type RegionError struct {
				RequestErr *errorpb.Error
			}
		*/
		response.RegionError = regionError.RequestErr
		return response, nil
	}

	defer reader.Close()
	// 创建事务并检测冲突
	txn := mvcc.NewMvccTxn(reader,req.GetStartVersion())
	for _, mutation := range req.Mutations {
		// MostRecentWrite 查询传入 key 的最新 Write
		write, commitTS, err := txn.MostRecentWrite(mutation.GetKey())
		if err != nil {
			regionError, ok := err.(*raft_storage.RegionError)
			if ok {
				response.RegionError = regionError.RequestErr
				return response, nil
			}
			return nil, err
		}

		// 如果这两个条件都满足，说明在当前事务开始之后已经有提交的写操作，这会导致写冲突。
		if write != nil && req.GetStartVersion() <= commitTS {
			keyError := &kvrpcpb.KeyError{
				Conflict:	&kvrpcpb.WriteConflict{
					StartTs:	req.StartVersion,
					ConflictTs: commitTS,
					Key:		mutation.GetKey(),
					Primary:    req.GetPrimaryLock(),
				},
			}
			response.Errors = append(response.Errors, keyError)
			continue
		}

		// 检测 Key 是否有 Lock 锁住，如果有的话则说明别的事务可能正在修改
		lock, err := txn.GetLock(mutation.GetKey())
		if err != nil {
			regionError, ok := err.(*raft_storage.RegionError)
			if ok {
				response.RegionError = regionError.RequestErr
				return response, nil
			}
			return nil, err
		}
		if lock != nil {
			keyError := &kvrpcpb.KeyError{
				Locked:   &kvrpcpb.LockInfo{
					Key:		 mutation.GetKey(),
					PrimaryLock: req.GetPrimaryLock(),
					LockVersion: lock.Ts,
					LockTtl:     lock.Ttl,
				},
			}
			response.Errors = append(response.Errors, keyError)
			continue
		}

		var writeKind mvcc.WriteKind

		switch mutation.GetOp() {
		case kvrpcpb.Op_Put:
			writeKind = mvcc.WriteKindPut
			txn.PutValue(mutation.GetKey(), mutation.GetValue())
		case kvrpcpb.Op_Del:
			writeKind = mvcc.WriteKindDelete
			txn.DeleteValue(mutation.GetKey())
		case kvrpcpb.Op_Rollback:
			continue
		case kvrpcpb.Op_Lock:
			continue
		}

		// 加锁
		addLock := &mvcc.Lock{
			Primary:	req.GetPrimaryLock(),
			Ts:			req.GetStartVersion(),
			Ttl:		req.GetLockTtl(),
			Kind: 		writeKind,
		}
		txn.PutLock(mutation.GetKey(), addLock)
	}

	// 如果有出错，需要 abort 事务
	if len(response.Errors) > 0 {
		return response, nil
	}

	// 写入事务中暂存的修改到 storage 中
	err = server.storage.Write(req.GetContext(),txn.Writes())
	if err != nil {
		regionError, ok := err.(*raft_storage.RegionError)
		if ok {
			response.RegionError = regionError.RequestErr
			return response, nil
		}
		return nil, err
	}
	return response, nil
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	/*
	type CommitRequest struct {
		Context *Context
		// Identifies the transaction, must match the start_version in the transaction's
		// prewrite request.
		StartVersion uint64
		// Must match the keys mutated by the transaction's prewrite request.
		Keys [][]byte
		// Must be greater than start_version.
		CommitVersion        uint64
	}
	// Empty if the commit is successful.
	type CommitResponse struct {
		RegionError          *errorpb.Error ''
		Error                *KeyError
	}
	 */
	response := &kvrpcpb.CommitResponse{}

	// 获取 Reader
	// 如果获取 Reader 时发生区域错误（RegionError），则将错误信息返回给客户端。
	reader, err := server.storage.Reader(req.GetContext())
	// 使用类型断言将 err 转换为 *raft_storage.RegionError 类型，并将结果赋值给 regionErr。
	// 同时，ok 是一个布尔值，表示类型断言是否成功。如果 err 是 *raft_storage.RegionError 类型，ok 将为 true，否则为 false。
	regionError, ok := err.(*raft_storage.RegionError)
	if ok { // 发生区域错误（RegionError）
		/*
			type RegionError struct {
				RequestErr *errorpb.Error
			}
		*/
		response.RegionError = regionError.RequestErr
		return response, nil
	}

	defer reader.Close()
	// 创建事务并等待锁
	txn := mvcc.NewMvccTxn(reader,req.GetStartVersion())
	// 等待所有需要提交的键的锁，确保事务的原子性。使用 defer 确保在函数结束时释放锁。
	server.Latches.WaitForLatches(req.GetKeys())
	defer server.Latches.ReleaseLatches(req.GetKeys())

	for _, curKey := range req.GetKeys() {
		// CurrentWrite 查询当前事务(根据 start timestamp)下，传入 key 的最新 Write。
		write,_ ,err := txn.CurrentWrite(curKey)
		if err != nil {
			regionError, ok := err.(*raft_storage.RegionError)
			if ok {
				response.RegionError = regionError.RequestErr
				return response, nil
			}
			return nil, err
		}

		// 检查是否重复提交
		if write != nil && write.Kind != mvcc.WriteKindRollback && req.GetStartVersion() == write.StartTS {
			return response, nil
		}

		// 检查 curKey 的 lock 是否还存在
		lock, err := txn.GetLock(curKey)
		if err != nil {
			regionError, ok := err.(*raft_storage.RegionError)
			if ok {
				response.RegionError = regionError.RequestErr
				return response, nil
			}
			return nil, err
		}
		if lock == nil {
			keyError := &kvrpcpb.KeyError{
				Retryable: "true",
			}
			response.Error = keyError
			return response, nil
		}

		if lock.Ts != req.GetStartVersion() {
			keyError := &kvrpcpb.KeyError{
				Retryable: "true",
			}
			response.Error = keyError
			return response, nil
		}

		// 正常提交事务
		addWrite := &mvcc.Write{
			StartTS:	req.GetStartVersion(),
			Kind:       lock.Kind,
		}
		// 将提交版本的写入记录写入事务，并删除键的锁。
		txn.PutWrite(curKey,req.GetCommitVersion(),addWrite)
		txn.DeleteLock(curKey)
	}

	// 写入事务中暂存的修改到 storage 中
	err = server.storage.Write(req.GetContext(),txn.Writes())
	if err != nil {
		regionError, ok := err.(*raft_storage.RegionError)
		if ok {
			response.RegionError = regionError.RequestErr
			return response, nil
		}
		return nil, err
	}
	return response, nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coppb.Request) (*coppb.Response, error) {
	resp := new(coppb.Response)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return server.copHandler.HandleCopDAGRequest(reader, req), nil
	case kv.ReqTypeAnalyze:
		return server.copHandler.HandleCopAnalyzeRequest(reader, req), nil
	}
	return nil, nil
}
