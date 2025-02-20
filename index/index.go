package index

import "bitcask-go/data"

// Indexer 抽象索引接口，如需加入其他数据结构可在这直接实现
type Indexer interface {
	Put(key []byte, pos *data.LogRecordPos) bool
	Get(key []byte) *data.LogRecordPos
	Delete(key []byte) bool
}
