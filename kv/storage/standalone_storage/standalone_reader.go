package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

type StandAloneReader struct {
	txn *badger.Txn
}

func NewStandAloneReader(txn *badger.Txn) *StandAloneReader {
	return &StandAloneReader{
		txn: txn,
	}
}

func (s *StandAloneReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(s.txn, cf, key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return val, err
}

func (s *StandAloneReader) IterCF(cf string) engine_util.DBIterator {
	return NewStandAloneIterator(engine_util.NewCFIterator(cf, s.txn))
}

func (s *StandAloneReader) Close() {
	s.txn.Discard()
}

//standalone iterator
type StandAloneIterator struct {
	iter *engine_util.BadgerIterator
}

func NewStandAloneIterator(iter *engine_util.BadgerIterator) *StandAloneIterator {
	return &StandAloneIterator{
		iter: iter,
	}
}

func (it *StandAloneIterator) Item() engine_util.DBItem {
	return it.iter.Item()
}

func (it *StandAloneIterator) Valid() bool {
	if !it.iter.Valid() {
		return false
	}
	return true
}

func (it *StandAloneIterator) ValidForPrefix(prefix []byte) bool {
	if !it.iter.ValidForPrefix(prefix) {
		return false
	}
	return true
}

func (it *StandAloneIterator) Close() {
	it.iter.Close()
}

func (it *StandAloneIterator) Next() {
	it.iter.Next()
}

func (it *StandAloneIterator) Seek(key []byte) {
	it.iter.Seek(key)
}

func (it *StandAloneIterator) Rewind() {
	it.iter.Rewind()
}
