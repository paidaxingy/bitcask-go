package redis

import (
	"bitcask-go/utils"
	"encoding/binary"
	"math"
)

const (
	maxMetaDataSize   = 1 + binary.MaxVarintLen64*2 + binary.MaxVarintLen32*2
	extraListMetaSize = binary.MaxVarintLen64 * 2

	initialListMark = math.MaxUint64 / 2
)

// 元数据
type metadata struct {
	dataType byte   //数据类型
	expire   int64  //过期时间
	version  int64  //版本号
	size     uint32 //数据大小
	head     uint64 //List 数据头
	tail     uint64 //List 数据尾
}

func (md *metadata) encode() []byte {
	var size = maxMetaDataSize
	if md.dataType == List {
		size += extraListMetaSize
	}
	buf := make([]byte, size)
	var index = 1
	buf[0] = md.dataType
	index += binary.PutVarint(buf[index:], md.expire)
	index += binary.PutVarint(buf[index:], md.version)
	index += binary.PutVarint(buf[index:], int64(md.size))

	if md.dataType == List {
		index += binary.PutUvarint(buf[index:], md.head)
		index += binary.PutUvarint(buf[index:], md.tail)
	}
	return buf[:index]
}

func decodeMetadata(buf []byte) *metadata {
	dataType := buf[0]
	var index = 1
	expire, n := binary.Varint(buf[index:])
	index += n
	version, n := binary.Varint(buf[index:])
	index += n
	size, n := binary.Varint(buf[index:])
	index += n

	var head, tail uint64
	if dataType == List {
		head, n = binary.Uvarint(buf[index:])
		index += n
		tail, _ = binary.Uvarint(buf[index:])
	}
	return &metadata{
		dataType: dataType,
		expire:   expire,
		version:  version,
		size:     uint32(size),
		head:     head,
		tail:     tail,
	}
}

type hashInternalKey struct {
	key     []byte
	version int64
	field   []byte
}

func (hk *hashInternalKey) encode() []byte {
	buf := make([]byte, len(hk.key)+8+len(hk.field))
	var index = 0
	copy(buf[index:index+len(hk.key)], hk.key)
	index += len(hk.key)
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(hk.version))
	index += 8
	copy(buf[index:index+len(hk.field)], hk.field)
	return buf
}

type setInternalKey struct {
	key     []byte
	version int64
	member  []byte
}

func (sk *setInternalKey) encode() []byte {
	buf := make([]byte, len(sk.key)+8+len(sk.member)+4)
	var index = 0
	copy(buf[index:index+len(sk.key)], sk.key)
	index += len(sk.key)
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(sk.version))
	index += 8
	copy(buf[index:index+len(sk.member)], sk.member)
	index += len(sk.member)

	binary.LittleEndian.PutUint32(buf[index:index+4], uint32(len(sk.member)))
	return buf
}

type listInternalKey struct {
	key     []byte
	version int64
	index   uint64
}

func (lk *listInternalKey) encode() []byte {
	buf := make([]byte, len(lk.key)+8+8)
	var index = 0
	copy(buf[index:index+len(lk.key)], lk.key)
	index += len(lk.key)
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(lk.version))
	index += 8
	binary.LittleEndian.PutUint64(buf[index:index+8], lk.index)
	return buf
}

type zsetInternalKey struct {
	key     []byte
	version int64
	score   float64
	member  []byte
}

func (zk *zsetInternalKey) encodeWithMember() []byte {
	buf := make([]byte, len(zk.key)+8+len(zk.member)+8)
	var index = 0
	copy(buf[index:index+len(zk.key)], zk.key)
	index += len(zk.key)
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(zk.version))
	index += 8
	copy(buf[index:index+len(zk.member)], zk.member)
	return buf
}
func (zk *zsetInternalKey) encodeWithScore() []byte {
	scoreBuf := utils.Float64ToBytes(zk.score)
	buf := make([]byte, len(zk.key)+8+len(zk.member)+len(scoreBuf)+4)
	var index = 0
	copy(buf[index:index+len(zk.key)], zk.key)
	index += len(zk.key)
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(zk.version))
	index += 8
	copy(buf[index:index+len(scoreBuf)], scoreBuf)
	index += len(scoreBuf)
	copy(buf[index:index+len(zk.member)], zk.member)
	index += len(zk.member)
	binary.LittleEndian.PutUint32(buf[index:index+4], uint32(len(zk.member)))
	return buf
}
