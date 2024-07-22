package mvcc

import (
	"bytes"
	"encoding/binary"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/codec"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/tsoutil"
)

// KeyError is a wrapper type so we can implement the `error` interface.
type KeyError struct {
	kvrpcpb.KeyError
}

func (ke *KeyError) Error() string {
	return ke.String()
}

// MvccTxn groups together writes as part of a single transaction. It also provides an abstraction over low-level
// storage, lowering the concepts of timestamps, writes, and locks into plain keys and values.
// MvccTxn 结构体用于将多个写操作组合成一个事务。它还提供了对低级存储的抽象，将时间戳、写操作和锁的概念简化为普通的键和值。
type MvccTxn struct {
	// 事务的开始时间戳。
	StartTS uint64
	// 用于读取存储的接口.
	Reader  storage.StorageReader
	// 存储修改操作的切片。
	writes  []storage.Modify
}

func NewMvccTxn(reader storage.StorageReader, startTs uint64) *MvccTxn {
	return &MvccTxn{
		Reader:  reader,
		StartTS: startTs,
	}
}

// Writes returns all changes added to this transaction.
func (txn *MvccTxn) Writes() []storage.Modify {
	return txn.writes
}

// PutWrite records a write at key and ts.
// PutWrite 函数的作用是记录一个写操作到指定的键和时间戳。
func (txn *MvccTxn) PutWrite(key []byte, ts uint64, write *Write) {
	// Your Code Here (4A).
	txn.writes = append(txn.writes, storage.Modify{
		Data: storage.Put {
			Key:   EncodeKey(key,ts),
			Cf:	   engine_util.CfWrite,
			Value: write.ToBytes(),
		},
	})
}

// GetLock returns a lock if key is locked. It will return (nil, nil) if there is no lock on key, and (nil, err)
// if an error occurs during lookup.
// GetLock 函数的作用是检查指定的键是否被锁定，并返回相应的锁信息。
func (txn *MvccTxn) GetLock(key []byte) (*Lock, error) {
	// Your Code Here (4A).
	value, err := txn.Reader.GetCF(engine_util.CfLock,key)
	if err != nil {
		return nil, err
	}

	// It will return (nil, nil) if there is no lock on key
	if value == nil {
		return nil, nil
	}

	lock, err := ParseLock(value)
	if err != nil {
		return nil, err
	}
	return lock, nil
}

// PutLock adds a key/lock to this transaction.
// PutLock 函数的作用是将一个键和锁添加到当前事务中。
func (txn *MvccTxn) PutLock(key []byte, lock *Lock) {
	// Your Code Here (4A).
	txn.writes = append(txn.writes, storage.Modify{
		Data: storage.Put {
			Key:   key,
			Cf:    engine_util.CfLock,
			Value: lock.ToBytes(),
		},
	})
}

// DeleteLock adds a delete lock to this transaction.
// DeleteLock 函数的作用是将一个删除锁操作添加到当前事务中。
func (txn *MvccTxn) DeleteLock(key []byte) {
	// Your Code Here (4A).
	txn.writes = append(txn.writes, storage.Modify{
		Data: storage.Delete {
			Key: key,
			Cf:  engine_util.CfLock,
		},
	})
}

// GetValue finds the value for key, valid at the start timestamp of this transaction.
// I.e., the most recent value committed before the start of this transaction.
// GetValue 查询当前事务下，传入 key 对应的 Value。
func (txn *MvccTxn) GetValue(key []byte) ([]byte, error) {
	// Your Code Here (4A).
	it := txn.Reader.IterCF(engine_util.CfWrite)
	defer it.Close()

	encodeKey := EncodeKey(key,txn.StartTS)
	// Seek 函数的作用是将迭代器定位到提供的键。如果该键存在，迭代器将指向该键；如果不存在，迭代器将指向大于提供键的下一个最小键。
	it.Seek(encodeKey)
	if it.Valid() == false {
		return nil, nil
	}

	// 判断找到的 key 是不是预期的 key
	// KeyCopy(dst []byte) []byte: 返回键的副本。如果传入的切片为 nil 或容量不足，将分配一个新的切片并返回。
	resultKey := DecodeUserKey(it.Item().KeyCopy(nil))

	if bytes.Equal(resultKey,key) == false { // key 不相等
		return nil, nil
	}

	// ValueCopy(dst []byte) ([]byte, error): 返回值的副本。如果传入的切片为 nil 或容量不足，将分配一个新的切片并返回。
	value, err := it.Item().ValueCopy(nil)
	if err != nil {
		return nil, err
	}

	// 根据 value 获取其对应的 write 结构体
	write, err := ParseWrite(value)
	if err != nil {
		return nil, err
	}

	if write.Kind == WriteKindPut {
		goatKey := EncodeKey(key, write.StartTS)
		return txn.Reader.GetCF(engine_util.CfDefault,goatKey)
	}
	return nil, nil
}

// PutValue adds a key/value write to this transaction.
// PutValue 函数将一个键/值写入操作添加到事务中
func (txn *MvccTxn) PutValue(key []byte, value []byte) {
	// Your Code Here (4A).
	txn.writes = append(txn.writes, storage.Modify{
		Data: storage.Put {
			Key:   EncodeKey(key,txn.StartTS),
			Cf:    engine_util.CfDefault,
			Value: value,
		},
	})
}

// DeleteValue removes a key/value pair in this transaction.
// DeleteValue 函数将一个键/值删除操作添加到事务中
func (txn *MvccTxn) DeleteValue(key []byte) {
	// Your Code Here (4A).
	txn.writes = append(txn.writes, storage.Modify{
		Data: storage.Delete {
			Key:   EncodeKey(key,txn.StartTS),
			Cf:    engine_util.CfDefault,
		},
	})
}

// CurrentWrite searches for a write with this transaction's start timestamp. It returns a Write from the DB and that
// write's commit timestamp, or an error.
// CurrentWrite 查询当前事务(根据 start timestamp)下，传入 key 的最新 Write。
func (txn *MvccTxn) CurrentWrite(key []byte) (*Write, uint64, error) {
	// Your Code Here (4A).
	it := txn.Reader.IterCF(engine_util.CfWrite)
	defer it.Close()

	for it.Seek(EncodeKey(key,0xFFFFFFFFFFFFFFFF)); it.Valid(); it.Next() {
		item := it.Item()
		curKey := item.KeyCopy(nil)
		userKey := DecodeUserKey(curKey)
		if bytes.Equal(userKey,key) == false {
			return nil, 0, nil
		}

		value, err := item.ValueCopy(nil)
		if err != nil {
			return nil, 0, err
		}
		write, err := ParseWrite(value)
		if err != nil {
			return nil, 0, err
		}

		if write.StartTS == txn.StartTS {
			return write, decodeTimestamp(curKey), nil
		}
	}
	return nil, 0, nil
}

// MostRecentWrite finds the most recent write with the given key. It returns a Write from the DB and that
// write's commit timestamp, or an error.
// MostRecentWrite 查询传入 key 的最新 Write
func (txn *MvccTxn) MostRecentWrite(key []byte) (*Write, uint64, error) {
	// Your Code Here (4A).
	it := txn.Reader.IterCF(engine_util.CfWrite)
	defer it.Close()

	it.Seek(EncodeKey(key,0xFFFFFFFFFFFFFFFF))
	if it.Valid() == false {
		return nil, 0, nil
	}

	curKey := it.Item().KeyCopy(nil)
	userKey := DecodeUserKey(curKey)

	if bytes.Equal(userKey,key) == false {
		return nil, 0, nil
	}

	value, err := it.Item().ValueCopy(nil)
	if err != nil {
		return nil, 0, err
	}

	write, err := ParseWrite(value)
	if err != nil {
		return nil, 0, err
	}

	return write, decodeTimestamp(curKey), nil
}

// EncodeKey encodes a user key and appends an encoded timestamp to a key. Keys and timestamps are encoded so that
// timestamped keys are sorted first by key (ascending), then by timestamp (descending). The encoding is based on
// https://github.com/facebook/mysql-5.6/wiki/MyRocks-record-format#memcomparable-format.
// EncodeKey 的作用是对用户的键进行编码，并附加一个编码的时间戳。这样做的目的是使带有时间戳的键首先按键（升序）排序，然后按时间戳（降序）排序。
func EncodeKey(key []byte, ts uint64) []byte {
	encodedKey := codec.EncodeBytes(key)
	newKey := append(encodedKey, make([]byte, 8)...)
	binary.BigEndian.PutUint64(newKey[len(encodedKey):], ^ts)
	return newKey
}

// DecodeUserKey takes a key + timestamp and returns the key part.
// DecodeUserKey 函数从包含键和时间戳的编码键中提取并返回用户键部分。
func DecodeUserKey(key []byte) []byte {
	_, userKey, err := codec.DecodeBytes(key)
	if err != nil {
		panic(err)
	}
	return userKey
}

// decodeTimestamp takes a key + timestamp and returns the timestamp part.
// decodeTimestamp 函数从包含键和时间戳的编码键中提取并返回时间戳部分。
func decodeTimestamp(key []byte) uint64 {
	left, _, err := codec.DecodeBytes(key)
	if err != nil {
		panic(err)
	}
	return ^binary.BigEndian.Uint64(left)
}

// PhysicalTime returns the physical time part of the timestamp.
func PhysicalTime(ts uint64) uint64 {
	return ts >> tsoutil.PhysicalShiftBits
}
