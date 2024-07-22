package mvcc

import (
	"encoding/binary"
	"fmt"

	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// Write is a representation of a committed write to backing storage.
// A serialized version is stored in the "write" CF of our engine when a write is committed. That allows MvccTxn to find
// the status of a key at a given timestamp.
// Write 结构体表示提交到后端存储的写操作。序列化版本存储在引擎的 “write” 列族（CF）中，当写操作提交时，这允许 MvccTxn 在给定时间戳找到键的状态。
type Write struct {
	// 写操作的开始时间戳。
	StartTS uint64
	// 写操作的类型，使用 WriteKind 枚举表示。
	Kind    WriteKind
}

// ToBytes 方法将 Write 结构体序列化为字节切片。
func (wr *Write) ToBytes() []byte {
	buf := append([]byte{byte(wr.Kind)}, 0, 0, 0, 0, 0, 0, 0, 0)
	binary.BigEndian.PutUint64(buf[1:], wr.StartTS)
	return buf
}

// ParseWrite 函数将字节切片反序列化为 Write 结构体。
func ParseWrite(value []byte) (*Write, error) {
	if value == nil {
		return nil, nil
	}
	if len(value) != 9 {
		return nil, fmt.Errorf("mvcc/write/ParseWrite: value is incorrect length, expected 9, found %d", len(value))
	}
	kind := value[0]
	startTs := binary.BigEndian.Uint64(value[1:])

	return &Write{startTs, WriteKind(kind)}, nil
}

type WriteKind int

const (
	// WriteKindPut 表示写入操作。
	WriteKindPut      WriteKind = 1
	// WriteKindDelete 表示删除操作。
	WriteKindDelete   WriteKind = 2
	// WriteKindRollback 表示回滚操作。
	WriteKindRollback WriteKind = 3
)

func (wk WriteKind) ToProto() kvrpcpb.Op {
	switch wk {
	case WriteKindPut:
		return kvrpcpb.Op_Put
	case WriteKindDelete:
		return kvrpcpb.Op_Del
	case WriteKindRollback:
		return kvrpcpb.Op_Rollback
	}

	return -1
}

func WriteKindFromProto(op kvrpcpb.Op) WriteKind {
	switch op {
	case kvrpcpb.Op_Put:
		return WriteKindPut
	case kvrpcpb.Op_Del:
		return WriteKindDelete
	case kvrpcpb.Op_Rollback:
		return WriteKindRollback
	default:
		panic("unsupported type")
	}
}
