package bitcaskgo

import (
	"bitcask-go/index"
	"bytes"
)

type Iterator struct {
	indexIter index.Iterator // 索引迭代器
	db        *DB            // 数据库实例
	options   IteratorOptions
}

func (db *DB) NewIterator(opts IteratorOptions) *Iterator {
	indexIter := db.index.Iterator(opts.Reverse)
	return &Iterator{
		indexIter: indexIter,
		db:        db,
		options:   opts,
	}
}

func (it *Iterator) Rewind() {
	it.indexIter.Rewind()
	it.skipToNext()
}

func (it *Iterator) Seek(key []byte) {
	it.indexIter.Seek(key)
	it.skipToNext()
}

func (it *Iterator) Next() {
	it.indexIter.Next()
	it.skipToNext()
}

func (it *Iterator) Valid() bool {
	return it.indexIter.Valid()
}

func (it *Iterator) Key() []byte {
	return it.indexIter.Key()
}

func (it *Iterator) Value() ([]byte, error) {
	logRecordPos := it.indexIter.Value()
	it.db.mu.RLock()
	defer it.db.mu.RUnlock()
	return it.db.getValueByPosition(logRecordPos)
}

func (it *Iterator) Close() {
	it.indexIter.Close()
}

func (it *Iterator) skipToNext() {
	prefixlen := len(it.options.Prefix)
	if prefixlen == 0 {
		return
	}

	for ; it.indexIter.Valid(); it.indexIter.Next() {
		key := it.indexIter.Key()
		if prefixlen <= len(key) && bytes.Equal(it.options.Prefix, key[:prefixlen]) {
			break
		}
	}
}
