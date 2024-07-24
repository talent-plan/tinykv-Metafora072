package mvcc

import (
	"bytes"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

// Scanner is used for reading multiple sequential key/value pairs from the storage layer. It is aware of the implementation
// of the storage layer and returns results suitable for users.
// Invariant: either the scanner is finished and cannot be used, or it is ready to return a value immediately.
// Scanner 用于从存储层读取多个连续的键/值对。它了解存储层的实现，并返回适合用户的结果。
// 不变性：要么扫描器已完成且不能使用，要么它准备立即返回一个值。
type Scanner struct {
	// Your Data Here (4C).
	it			engine_util.DBIterator
	txn 		*MvccTxn
	nextKey 	[]byte
	finished	bool
}

// NewScanner creates a new scanner ready to read from the snapshot in txn.
// NewScanner 创建一个新的 Scanner. 准备从 txn 中的快照读取。
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	// Your Code Here (4C).
	scanner := &Scanner{
		it:		  txn.Reader.IterCF(engine_util.CfWrite),
		txn:	  txn,
		nextKey:  startKey,
		finished: false,
	}
	return scanner
}

func (scan *Scanner) Close() {
	// Your Code Here (4C).
	if scan.it == nil {
		return
	}
	scan.it.Close()
}

// Next returns the next key/value pair from the scanner. If the scanner is exhausted, then it will return `nil, nil, nil`.
// Next 返回扫描器中的下一个键/值对。如果扫描器已耗尽，则返回 `nil, nil, nil`。
func (scan *Scanner) Next() ([]byte, []byte, error) {
	// Your Code Here (4C).
	if scan.finished {
		return nil, nil, nil
	}

	// 查找 nextKey
	scan.it.Seek(EncodeKey(scan.nextKey,scan.txn.StartTS))
	if scan.it.Valid() == false {
		scan.finished = true
		return nil, nil, nil
	}

	//// 获取当前迭代到的 curKey
	item := scan.it.Item()
	initKey := scan.nextKey
	curKey := scan.it.Item().KeyCopy(nil)
	userKey := DecodeUserKey(curKey)

	if bytes.Equal(userKey, scan.nextKey) == false { // ？？？
		scan.nextKey = userKey
		return scan.Next()
	}

	for scan.it.Next(); scan.it.Valid(); scan.it.Next() {
		curKey := scan.it.Item().KeyCopy(nil)
		userKey := DecodeUserKey(curKey)
		if bytes.Equal(userKey, scan.nextKey) == false { // 找到了下一个不同的 key
			scan.nextKey = userKey
			break
		}
	}

	if scan.it.Valid() == false {
		scan.finished = true
		//return scan.nextKey, nil, nil
	}

	value, err := item.ValueCopy(nil)
	if err != nil {
		return initKey, nil, err
	}
	write, err := ParseWrite(value)
	if err != nil {
		return initKey, nil, err
	}

	if write.Kind == WriteKindDelete {
		return initKey, nil, nil
	}

	// 查找对应的 CfDefault
	goatValue, err := scan.txn.Reader.GetCF(engine_util.CfDefault,EncodeKey(initKey,write.StartTS))


	return initKey, goatValue, err
}
