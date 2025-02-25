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
	// Iterator 初始化一个迭代器，用于遍历索引中的 key
	Iterator(reverse bool) Iterator
	// Size 索引中的数据量
	Size() int

	// Close 关闭索引
	Close() error
}

type IndexType = int8

const (
	// Btree 索引
	Btree IndexType = iota + 1

	// ART 自适应基数树索引
	ART

	// BPTree B+树索引
	BPTree
)

// NewIndexer 根据类型初始化索引
func NewIndexer(typ IndexType, dirPath string, sync bool) Indexer {
	switch typ {
	case Btree:
		return NewBTree()
	case ART:
		return NewART()
	case BPTree:
		return NewBPlusTree(dirPath, sync)
	default:
		panic("unsupported index type")
	}
}

type Item struct {
	key []byte
	pos *data.LogRecordPos
}

func (ai *Item) Less(bi btree.Item) bool {
	return bytes.Compare(ai.key, bi.(*Item).key) == -1
}

type Iterator interface {
	Rewind()                   // 重新回到迭代器的起点
	Seek(key []byte)           // 查找第一个大于(或小于)等于目标的key
	Next()                     // 移动到下一个key
	Valid() bool               // 是否有效,即是否遍历完了所有的 Key, 用于退出遍历
	Key() []byte               // 返回当前遍历位置的 Key 数据
	Value() *data.LogRecordPos // 返回当前遍历位置的 Value 数据
	Close()                    // 关闭迭代器并释放相应资源
}
