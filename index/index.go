package index

import (
	"bitcask-go/data"
	"bytes"

	"github.com/google/btree"
)

// Indexer 抽象索引接口，如需加入其他数据结构可在这直接实现
type Indexer interface {
	// Put 向索引中存储 key 对应的位置信息
	Put(key []byte, pos *data.LogRecordPos) bool
	// Get 根据 key 取出对应的位置信息
	Get(key []byte) *data.LogRecordPos
	// Delete 根据 key 删除对应的位置信息
	Delete(key []byte) bool
}

type Item struct {
	key []byte
	pos *data.LogRecordPos
}

func (ai *Item) Less(bi btree.Item) bool {
	return bytes.Compare(ai.key, bi.(*Item).key) == -1
}
