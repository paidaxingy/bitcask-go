package bitcaskgo

type Options struct {
	DirPath string // 数据库数据目录

	DataFileSize int64 // 数据文件大小

	SyncWrites bool // 每次写数据是否持久化

	BytesPerSync uint // 触发一次持久化的字节数

	IndexType IndexerType // 索引类型

	MMapAtStartup bool // 启动时是否使用 mmap 加载数据

	DataFileMerGeRatio float32 // 数据文件合并的阈值
}

// 迭代器选项
type IteratorOptions struct {
	Prefix []byte // 遍历前缀为指定值的 Key，默认为空

	Reverse bool // 是否反向遍历，默认 false 是正向
}

// 批量写配置选项
type WriteBatchOptions struct {
	MaxBatchNum uint // 最大批量写入的数量

	SyncWrites bool // 每次写数据是否持久化
}

type IndexerType = int8

const (
	// BTree 索引
	BTree IndexerType = iota + 1

	// ART 自适应基数树索引
	ART

	// BPTree B+树索引
	BPTree
)

var DefaultOptions = Options{
	DirPath:            "./tmp",
	DataFileSize:       256 * 1024 * 1024, // 256MB
	SyncWrites:         false,
	BytesPerSync:       0,
	IndexType:          BTree,
	MMapAtStartup:      true,
	DataFileMerGeRatio: 0.5,
}

var DefaultIteratorOptions = IteratorOptions{
	Prefix:  nil,
	Reverse: false,
}

var DefaultWriteBatchOptions = WriteBatchOptions{
	MaxBatchNum: 1000,
	SyncWrites:  true,
}
