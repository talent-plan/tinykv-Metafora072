package standalone_storage

import (
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"

	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/Connor1996/badger"
	"log"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	db *badger.DB
	// Your Data Here (1).
}


func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	opts := badger.DefaultOptions
	opts.Dir = conf.DBPath
	opts.ValueDir = conf.DBPath

	db,err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}

	return &StandAloneStorage{
		db : db,
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return s.db.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	txn := s.db.NewTransaction(false)
	return &BadgerReader{txn}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	for _, modify := range batch {
		key, value, cf := modify.Key(), modify.Value(), modify.Cf()
		var err error
		switch modify.Data.(type) {
		case storage.Put:
			// func PutCF(engine *badger.DB, cf string, key []byte, val []byte) error {
			// 	return engine.Update(func(txn *badger.Txn) error {
			// 		return txn.Set(KeyWithCF(cf, key), val)
			// 	})
			// }
			err = engine_util.PutCF(s.db,cf,key,value)
		case storage.Delete:
			// func DeleteCF(engine *badger.DB, cf string, key []byte) error {
			// 	return engine.Update(func(txn *badger.Txn) error {
			// 		return txn.Delete(KeyWithCF(cf, key))
			// 	})
			// }
			err = engine_util.DeleteCF(s.db,cf,key)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// BadgerReader <StorageReader>
type BadgerReader struct {
	txn *badger.Txn
}


// type StorageReader interface {
// 	// When the key doesn't exist, return nil for the value
// 	GetCF(cf string, key []byte) ([]byte, error)
// 	IterCF(cf string) engine_util.DBIterator
// 	Close()
// }
func (br *BadgerReader) GetCF(cf string, key []byte) ([]byte, error) {
	//func GetCFFromTxn(txn *badger.Txn, cf string, key []byte) (val []byte, err error)
	value, err := engine_util.GetCFFromTxn(br.txn,cf,key)

	if err == badger.ErrKeyNotFound {
		return nil,nil
	}
	return value,err
}

// type BadgerIterator struct {
// 	iter   *badger.Iterator
// 	prefix string
// }
// BadgerIterator <DBIterator>

// func NewCFIterator(cf string, txn *badger.Txn) *BadgerIterator {
// 	return &BadgerIterator{
// 		iter:   txn.NewIterator(badger.DefaultIteratorOptions),
// 		prefix: cf + "_",
// 	}
// }

// type DBIterator interface {
// 	// Item returns pointer to the current key-value pair.
// 	Item() DBItem
// 	// Valid returns false when iteration is done.
// 	Valid() bool
// 	// Next would advance the iterator by one. Always check it.Valid() after a Next()
// 	// to ensure you have access to a valid it.Item().
// 	Next()
// 	// Seek would seek to the provided key if present. If absent, it would seek to the next smallest key
// 	// greater than provided.
// 	Seek([]byte)

// 	// Close the iterator
// 	Close()
// }

func (br *BadgerReader) IterCF(cf string) engine_util.DBIterator {
	it := engine_util.NewCFIterator(cf,br.txn)
	return it
}

func (br *BadgerReader) Close() {
	br.txn.Discard()
}




