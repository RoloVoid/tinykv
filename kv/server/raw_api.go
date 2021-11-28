package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	resp := new(kvrpcpb.RawGetResponse)
	errs := server.storage.Start()
	if errs != nil {
		return nil, errs
	}
	defer server.storage.Stop()
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	data, err := reader.GetCF(req.Cf, req.Key)
	if err != nil {
		return nil, err
	}
	if len(data) < 1 {
		resp.NotFound = true
	} else {
		resp.Value = data
		resp.NotFound = false
	}
	return resp, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	err := server.storage.Start()
	if err != nil {
		return nil, err
	}
	defer server.storage.Stop()
	//seal data based on modify
	// Hint: Consider using Storage.Modify to store data to be modified
	batch := make([]storage.Modify, 0)
	modify := storage.Modify{}
	put := storage.Put{
		Key:   req.Key,
		Value: req.Value,
		Cf:    req.Cf,
	}
	modify.Data = put
	batch = append(batch, modify)
	err = server.storage.Write(req.Context, batch)
	resp := new(kvrpcpb.RawPutResponse)
	if err != nil {
		resp.Error = err.Error()
		return resp, nil
	}
	return nil, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Hint: Consider using Storage.Modify to store data to be deleted
	err := server.storage.Start()
	if err != nil {
		return nil, err
	}
	defer server.storage.Stop()
	batch := make([]storage.Modify, 0)
	modify := storage.Modify{}
	delete := storage.Delete{
		Key: req.Key,
		Cf:  req.Cf,
	}
	modify.Data = delete
	batch = append(batch, modify)
	err = server.storage.Write(req.Context, batch)
	resp := new(kvrpcpb.RawDeleteResponse)
	if err != nil {
		resp.Error = err.Error()
		return resp, nil
	}
	return nil, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Hint: Consider using reader.IterCF
	err := server.storage.Start()
	if err != nil {
		return nil, err
	}
	defer server.storage.Stop()
	resp := new(kvrpcpb.RawScanResponse)
	reader, err := server.storage.Reader(req.Context)
	iterator := reader.IterCF(req.Cf)
	counter, i := int(req.Limit), 0
	iterator.Seek(req.StartKey)
	kvs := make([]*kvrpcpb.KvPair, 0)
	for ; iterator.Valid(); iterator.Next() {
		if i >= counter {
			break
		}
		key := iterator.Item().Key()
		data, err := reader.GetCF(req.Cf, key)
		if err != nil {
			resp.Error = err.Error()
			break
		}
		kv := &kvrpcpb.KvPair{
			Key:   key,
			Value: data,
		}
		kvs = append(kvs, kv)
		i++
	}
	resp.Kvs = kvs
	return resp, nil
}
