package data

import (
	"encoding/binary"
	"hash/crc32"
)

type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
	LogRecordTxnFinished
)

// crc type keySize valueSize
// 4 + 1 + 5 + 5
const maxLogRecordHeaderSize = binary.MaxVarintLen32*2 + 5

// LogRecord 写入到数据文件的记录
// 之所以叫日志，是因为数据文件中的数据是追加写入的，类似日志的格式
type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType
}

// 头部信息
type logRecordHeader struct {
	crc        uint32        // crc 校验值
	recordType LogRecordType // 标识 LogRecord 的类型
	keySize    uint32        // key 的长度
	valueSize  uint32        // value 的长度
}

// LogRecordPos 数据内存索引，主要是描述数据在磁盘上的位置
type LogRecordPos struct {
	Fid    uint32 // 文件id，表示将数据存储到哪个文件当中
	Offset int64  // 偏移，表示将数据存储到文件的哪个位置
}

// TransactionRecord 暂存的事务相关的数据
type TransactionRecord struct {
	Record *LogRecord
	Pos    *LogRecordPos
}

// EncodeLogRecord 对 LogRecord 进行编码，返回字节数组和长度
func EncodeLogRecord(logRecord *LogRecord) ([]byte, int64) {

	header := make([]byte, maxLogRecordHeaderSize)

	header[4] = logRecord.Type
	var index = 5
	// 5 字节之后，存储的是 key 和 value 的长度
	index += binary.PutVarint(header[index:], int64(len(logRecord.Key)))
	index += binary.PutVarint(header[index:], int64(len(logRecord.Value)))

	var size = index + len(logRecord.Key) + len(logRecord.Value)

	encBytes := make([]byte, size)

	// 将 header 拷贝到 encBytes 中
	copy(encBytes[:index], header[:index])
	// 将 key 和 value 拷贝到 encBytes 中
	copy(encBytes[index:], logRecord.Key)
	copy(encBytes[index+len(logRecord.Key):], logRecord.Value)

	// 对 encBytes 进行 crc 校验
	crc := crc32.ChecksumIEEE(encBytes[4:])
	// 将 crc 校验值写入到 header 中
	binary.LittleEndian.PutUint32(encBytes[:4], crc)

	return encBytes, int64(size)

}

func decodeLogRecordHeader(buf []byte) (*logRecordHeader, int64) {
	if len(buf) <= 4 {
		return nil, 0
	}

	header := &logRecordHeader{
		crc:        binary.LittleEndian.Uint32(buf[:4]),
		recordType: buf[4],
	}

	var index = 5
	// 取出实际的 key 和 value 的长度
	keySize, n := binary.Varint(buf[index:])
	header.keySize = uint32(keySize)
	index += n

	valueSize, n := binary.Varint(buf[index:])
	header.valueSize = uint32(valueSize)
	index += n
	return header, int64(index)
}

func getLogRecordCRC(lr *LogRecord, header []byte) uint32 {
	if lr == nil {
		return 0
	}
	crc := crc32.ChecksumIEEE(header[:])
	crc = crc32.Update(crc, crc32.IEEETable, lr.Key)
	crc = crc32.Update(crc, crc32.IEEETable, lr.Value)
	return crc
}
