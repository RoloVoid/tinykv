package standalone_storage

import (
	"os"
	"path/filepath"
	"sync"

	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	engine *engine_util.Engines
	config *config.Config

	wg sync.WaitGroup
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	dbPath := conf.DBPath
	kvPath := filepath.Join(dbPath, "kv")

	os.MkdirAll(kvPath, os.ModePerm)

	kvDB := engine_util.CreateDB(kvPath, false)
	engine := engine_util.NewEngines(kvDB, nil, kvPath, "")
	wg := sync.WaitGroup{}
	return &StandAloneStorage{engine: engine, config: conf, wg: wg}
}

func (s *StandAloneStorage) Start() error {
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// err := s.engine.Close()
	// if err != nil {
	// 	return err
	// }
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	txn := s.engine.Kv.NewTransaction(true)
	return NewStandAloneReader(txn), nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	wb := &engine_util.WriteBatch{}
	for _, item := range batch {
		wb.SetCF(item.Cf(), item.Key(), item.Value())
	}
	err := s.engine.WriteKV(wb)
	if err != nil {
		return err
	}
	return nil
}
