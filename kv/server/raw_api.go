package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"

	"github.com/pingcap-incubator/tinykv/kv/storage"
)

// // Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
// type Server struct {
// 	storage storage.Storage

// 	// (Used in 4B)
// 	Latches *latches.Latches

// 	// coprocessor API handler, out of course scope
// 	copHandler *coprocessor.CopHandler
// }

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	} 
	value, err := reader.GetCF(req.Cf, req.Key)
	if err != nil {
		return nil, err
	}
	rawGetResponse := &kvrpcpb.RawGetResponse{
		Value : value,
		NotFound : (value == nil),
	}
	return rawGetResponse, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	batch := storage.Modify {
		Data : storage.Put{ Key: req.Key, Value: req.Value, Cf: req.Cf },
	}
	err := server.storage.Write(req.Context,[]storage.Modify{batch})
	if err != nil {
		return nil,err
	}

	rawPutResponse := &kvrpcpb.RawPutResponse{}
	return rawPutResponse, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	batch := storage.Modify {
		Data : storage.Delete{ Key: req.Key, Cf: req.Cf }, 
	}
	err := server.storage.Write(req.Context,[]storage.Modify{batch})
	if err != nil {
		return nil,err
	}
	rawDeleteResponse := &kvrpcpb.RawDeleteResponse{}
	return rawDeleteResponse, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil,err
	}


	var kvs []*kvrpcpb.KvPair

	it := reader.IterCF(req.Cf)
	defer it.Close()

	limit := req.Limit

	for it.Seek(req.StartKey); it.Valid(); it.Next() {
		item := it.Item()

		value, _ := item.Value()

		cur := &kvrpcpb.KvPair{
			Key : item.Key(),
			Value : value,
		}

		kvs = append(kvs,cur)
		limit = limit - 1

		if limit == 0 {
			break
		}
	}

	rawScanResponse := &kvrpcpb.RawScanResponse{
		Kvs : kvs,
	}
	return rawScanResponse, nil
}
