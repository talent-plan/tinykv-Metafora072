package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"

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
	// 读取某一个 key 的数据，检查其是否存在小于或等于 startTs 的 lock，如果存在说明在本次读取时还存在未 commit 的事务，先等一会，
	// 如果等超时了 lock 还在，则尝试 rollback。如果直接强行读会产生脏读，读取了未 commit 的数据。
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
		// Lock 的 startTs 小于当前事务的 startTs：如果你读了，就会产生脏读，因为前一个事务都没有 commit 你就读了。
		// Lock 的 startTs 大于当前事务的 startTs：如果你读了并修改了然后提交，拥有这个 lock 的事务会产生不可重复读。
		// Lock 的 startTs 等于当前事务的 startTs：不可能发生，因为当你重启事务之后，是分配一个新的 startTs，你不可能使用一个过去的 startTs 去执行重试操作。
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

		// 检查 key 的 lock 的时间戳是否为事务的 startTs，不是直接 abort。
		// 因为存在一种可能，在你 commit 的时候，你前面的 prewrite 操作因为过于缓慢，超时，
		// 导致你的 lock 被其他事务 rollback 了，然后你这里读取到的 lock 实际上不属于你的，是别的事务的。
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

// KvScan 相当于 RawScan 的事务性工作，它从数据库中读取许多值。但和 KvGet 一样，它是在一个时间点上进行的。
// 由于 MVCC 的存在，KvScan 明显比 RawScan 复杂得多 - - 由于多个版本和 key 编码的存在，你不能依靠底层存储来迭代值。
func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	/*
	type ScanRequest struct {
		Context  *Context
		StartKey []byte
		// The maximum number of values read.
		Limit                uint32
		Version              uint64
	}
	type ScanResponse struct {
		RegionError *errorpb.Error
		// Other errors are recorded for each key in pairs.
		Pairs                []*KvPair
	}
	 */
	response := &kvrpcpb.ScanResponse{
		Pairs: make([]*kvrpcpb.KvPair, 0),
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

	txn := mvcc.NewMvccTxn(reader,req.GetVersion())

	scanner := mvcc.NewScanner(req.GetStartKey(),txn)
	defer scanner.Close()

	var scanCount uint32 = 0
	for ; scanCount < req.GetLimit() ; {
		curKey, curValue, err := scanner.Next()
		if err != nil {
			regionError, ok := err.(*raft_storage.RegionError)
			if ok {
				response.RegionError = regionError.RequestErr
				return response, nil
			}
			return nil, err
		}

		if curKey == nil { // 没有值了
			break
		}

		lock, err := txn.GetLock(curKey)
		if err != nil {
			regionError, ok := err.(*raft_storage.RegionError)
			if ok {
				response.RegionError = regionError.RequestErr
				return response, nil
			}
			return nil, err
		}

		// 读取某一个 key 的数据，检查其是否存在小于或等于 startTs 的 lock，如果存在说明在本次读取时还存在未 commit 的事务，先等一会，
		// 如果等超时了 lock 还在，则尝试 rollback。如果直接强行读会产生脏读，读取了未 commit 的数据。
		if lock != nil && lock.Ts <= req.GetVersion() {
			keyError := &kvrpcpb.KeyError{
				Locked:		&kvrpcpb.LockInfo{
					Key:			curKey,
					PrimaryLock: 	lock.Primary,
					LockVersion:    lock.Ts,
					LockTtl:        lock.Ttl,
				},
			}
			pair := &kvrpcpb.KvPair{
				Error: keyError,
				Key:   curKey,
			}
			response.Pairs = append(response.Pairs, pair)
			scanCount++
			continue
		}

		if curValue != nil {
			pair := &kvrpcpb.KvPair{
				Error: nil,
				Key:   curKey,
				Value: curValue,
			}
			response.Pairs = append(response.Pairs, pair)
			scanCount++
		}
	}

	return response, nil
}

// KvCheckTxnStatus 检查超时，删除过期的锁并返回锁的状态。
// 如果事务之前已回滚或提交，则返回该信息。
// 如果事务的 TTL 耗尽，则中止该事务并回滚主锁。
// 否则，返回TTL信息。
func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	/*
	type CheckTxnStatusRequest struct {
		Context              *Context
		PrimaryKey           []byte
		LockTs               uint64
		CurrentTs            uint64
	}
	type CheckTxnStatusResponse struct {
		RegionError *errorpb.Error
		// Three kinds of txn status:
		// locked: lock_ttl > 0
		// committed: commit_version > 0
		// rolled back: lock_ttl == 0 && commit_version == 0
		LockTtl       uint64
		CommitVersion uint64
		// The action performed by TinyKV in response to the CheckTxnStatus request.
		Action
	}
	const (
		Action_NoAction Action = 0
		// The lock is rolled back because it has expired.
		Action_TTLExpireRollback Action = 1
		// The lock does not exist, TinyKV left a record of the rollback, but did not
		// have to delete a lock.
		Action_LockNotExistRollback Action = 2
	)
	*/
	response := &kvrpcpb.CheckTxnStatusResponse{}

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

	txn := mvcc.NewMvccTxn(reader,req.GetLockTs())

	// CurrentWrite 查询当前事务(根据 start timestamp)下，传入 key 的最新 Write。
	write, commitTS, err := txn.CurrentWrite(req.GetPrimaryKey())
	if err != nil {
		regionError, ok := err.(*raft_storage.RegionError)
		if ok {
			response.RegionError = regionError.RequestErr
			return response, nil
		}
		return nil, err
	}

	// 事务已经 commit
	if write != nil && write.Kind != mvcc.WriteKindRollback {
		response.CommitVersion = commitTS
		return response, nil
	}

	if write != nil && write.Kind == mvcc.WriteKindRollback {
		return response, nil
	}

	lock, err := txn.GetLock(req.GetPrimaryKey())
	if err != nil {
		regionError, ok := err.(*raft_storage.RegionError)
		if ok {
			response.RegionError = regionError.RequestErr
			return response, nil
		}
		return nil, err
	}
	// lock 不存在，表明 primary key 已回滚
	if lock == nil {
		addWrite := &mvcc.Write{
			StartTS: req.GetLockTs(),
			Kind:    mvcc.WriteKindRollback,
		}

		txn.PutWrite(req.GetPrimaryKey(),req.GetLockTs(),addWrite)

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

		response.Action = kvrpcpb.Action_LockNotExistRollback
		return response, nil
	}

	// lock 不为空，检查 lock 是否超时，如果超时则移除 Lock 和 Value，创建一个 WriteKindRollback
	if mvcc.PhysicalTime(req.GetCurrentTs()) >= mvcc.PhysicalTime(lock.Ts) + lock.Ttl {
		txn.DeleteLock(req.GetPrimaryKey())
		txn.DeleteValue(req.GetPrimaryKey())

		addWrite := &mvcc.Write{
			StartTS: req.GetLockTs(),
			Kind:    mvcc.WriteKindRollback,
		}
		txn.PutWrite(req.GetPrimaryKey(),req.GetLockTs(),addWrite)

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

		response.Action = kvrpcpb.Action_TTLExpireRollback
		return response, nil
	}

	return response, nil
}

// KvBatchRollback 检查一个 key 是否被当前事务锁定，如果是，则删除该锁，删除任何值，并留下一个回滚指示器作为写入。
func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	/*
	type BatchRollbackRequest struct {
		Context              *Context
		StartVersion         uint64
		Keys                 [][]byte
	}
	type BatchRollbackResponse struct{
		RegionError
		Error                *KeyError
	}
	 */
	response := &kvrpcpb.BatchRollbackResponse{}

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

	txn := mvcc.NewMvccTxn(reader,req.GetStartVersion())

	server.Latches.WaitForLatches(req.Keys)
	defer server.Latches.ReleaseLatches(req.Keys)

	for _, curKey := range req.GetKeys() {
		// CurrentWrite 查询当前事务(根据 start timestamp)下，传入 key 的最新 Write。
		curWrite, _, err := txn.CurrentWrite(curKey)
		if err != nil {
			regionError, ok := err.(*raft_storage.RegionError)
			if ok {
				response.RegionError = regionError.RequestErr
				return response, nil
			}
			return nil, err
		}

		// 如果 key 的 write 是 WriteKindRollback，则说明已经回滚完毕，跳过该 key；
		if curWrite != nil && curWrite.Kind == mvcc.WriteKindRollback {
			continue
		}
		// 不是回滚操作，说明存在 key 已经提交，则拒绝回滚，将 Abort 赋值为 true，然后返回即可；
		if curWrite != nil && curWrite.Kind != mvcc.WriteKindRollback {
			keyError := &kvrpcpb.KeyError{
				Abort: "true",
			}
			response.Error = keyError
			return response, nil
		}

		// 如果上两者都没有，则说明需要被回滚，那么就删除 lock 和 Value，同时打上 rollback 标记，然后返回即可；
		lock, err := txn.GetLock(curKey)
		if err != nil {
			regionError, ok := err.(*raft_storage.RegionError)
			if ok {
				response.RegionError = regionError.RequestErr
				return response, nil
			}
			return nil, err
		}

		// 如果 Lock 被清除或者 Lock 不是当前事务的 Lock，则中止操作
		// 这个时候说明 key 被其他事务占用
		if lock == nil || req.GetStartVersion() != lock.Ts {
			addWrite := &mvcc.Write{
				StartTS: req.GetStartVersion(),
				Kind:    mvcc.WriteKindRollback,
			}
			txn.PutWrite(curKey,req.GetStartVersion(),addWrite)
			continue
		}

		// 移除 Lock、删除 Value，写入 WriteKindRollback 的 Write
		txn.DeleteLock(curKey)
		txn.DeleteValue(curKey)
		addWrite := &mvcc.Write{
			StartTS: req.GetStartVersion(),
			Kind:    mvcc.WriteKindRollback,
		}
		txn.PutWrite(curKey,req.GetStartVersion(),addWrite)
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

// KvResolveLock 检查一批锁定的 key ，并将它们全部回滚或全部提交。
// 当客户端已经通过 KvCheckTxnStatus() 检查了 primary key 的状态，这里打算要么全部回滚，要么全部提交，具体取决于 ResolveLockRequest 的 CommitVersion。
// 如果 req.CommitVersion == 0，则调用 KvBatchRollback() 将这些 key 全部回滚；
// 如果 req.CommitVersion > 0，则调用 KvCommit() 将这些 key 全部提交；
func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	/*
	// Resolve lock will find all locks belonging to the transaction with the given start timestamp.
	// If commit_version is 0, TinyKV will rollback all locks. If commit_version is greater than
	// 0 it will commit those locks with the given commit timestamp.
	// The client will make a resolve lock request for all secondary keys once it has successfully
	// committed or rolled back the primary key.
	type ResolveLockRequest struct {
		Context              *Context
		StartVersion         uint64
		CommitVersion        uint64
	}
	// Empty if the lock is resolved successfully.
	type ResolveLockResponse struct {
		RegionError
		Error                *KeyError
	}
	 */
	response := &kvrpcpb.ResolveLockResponse{}

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

	it := reader.IterCF(engine_util.CfLock)

	defer reader.Close()
	defer it.Close()

	var optKeys [][]byte = make([][]byte, 0)

	for ; it.Valid(); it.Next() {
		curValue, err := it.Item().ValueCopy(nil)
		if err != nil {
			return response, err
		}

		// 将字节流反序列化为 lock 结构体
		lock, err := mvcc.ParseLock(curValue)
		if err != nil {
			return response, err
		}

		// 获取到含有 Lock 的所有 key
		if lock.Ts == req.GetStartVersion() {
			curKey := it.Item().KeyCopy(nil)
			optKeys = append(optKeys, curKey)
		}

	}

	if len(optKeys) == 0 {
		return response, nil
	}

	// 如果 req.CommitVersion == 0，则调用 KvBatchRollback() 将这些 key 全部回滚；
	var responseErr error
	if req.GetCommitVersion() == 0 {
		rollbackRequest := &kvrpcpb.BatchRollbackRequest{
			Context:			req.GetContext(),
			StartVersion:		req.GetStartVersion(),
			Keys: 				optKeys,
		}
		rollbackResponse, curErr := server.KvBatchRollback(nil, rollbackRequest)
		response.RegionError = rollbackResponse.RegionError
		response.Error = rollbackResponse.Error
		responseErr = curErr
	}
	// 如果 req.CommitVersion > 0，则调用 KvCommit() 将这些 key 全部提交；
	if req.GetCommitVersion() > 0 {
		commitRequest := &kvrpcpb.CommitRequest{
			Context: 			req.GetContext(),
			StartVersion: 		req.GetStartVersion(),
			Keys: 				optKeys,
			CommitVersion: 		req.GetCommitVersion(),
		}
		commitResponse, curErr := server.KvCommit(nil, commitRequest)
		response.RegionError = commitResponse.RegionError
		response.Error = commitResponse.Error
		responseErr = curErr
	}

	return response, responseErr
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
