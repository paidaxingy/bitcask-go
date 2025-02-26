package index

import (
	"bitcask-go/data"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBPlusTree_Put(t *testing.T) {
	path := "./tmp"

	tree := NewBPlusTree(path, false)
	defer func() {
		if err := tree.Close(); err != nil {
			panic(err)
		}
		if err := os.RemoveAll(path); err != nil {
			panic(err)
		}
	}()
	res1 := tree.Put([]byte("aac"), &data.LogRecordPos{Fid: 123, Offset: 999})
	assert.Nil(t, res1)
	res2 := tree.Put([]byte("abc"), &data.LogRecordPos{Fid: 123, Offset: 999})
	assert.Nil(t, res2)
	res3 := tree.Put([]byte("acc"), &data.LogRecordPos{Fid: 123, Offset: 999})
	assert.Nil(t, res3)
	res4 := tree.Put([]byte("aac"), &data.LogRecordPos{Fid: 1, Offset: 9})
	assert.Equal(t, res4.Fid, uint32(123))
	assert.Equal(t, res4.Offset, int64(999))
}

func TestBPlusTree_Get(t *testing.T) {
	path := "./tmp"

	tree := NewBPlusTree(path, false)
	defer func() {
		if err := tree.Close(); err != nil {
			panic(err)
		}
		if err := os.RemoveAll(path); err != nil {
			panic(err)
		}
	}()
	pos := tree.Get([]byte("not exist"))
	assert.Nil(t, pos)
	tree.Put([]byte("acc"), &data.LogRecordPos{Fid: 123, Offset: 999})
	pos1 := tree.Get([]byte("acc"))
	assert.NotNil(t, pos1)
	assert.Equal(t, pos1.Fid, uint32(123))
	assert.Equal(t, pos1.Offset, int64(999))
	tree.Put([]byte("aac"), &data.LogRecordPos{Fid: 9884, Offset: 1232})
	pos2 := tree.Get([]byte("aac"))
	assert.NotNil(t, pos2)
	assert.Equal(t, pos2.Fid, uint32(9884))
	assert.Equal(t, pos2.Offset, int64(1232))
}

func TestBPlusTree_Delete(t *testing.T) {
	path := "./tmp"
	tree := NewBPlusTree(path, false)
	defer func() {
		if err := tree.Close(); err != nil {
			panic(err)
		}
		if err := os.RemoveAll(path); err != nil {
			panic(err)
		}
	}()
	res, ok1 := tree.Delete([]byte("not exist"))
	assert.False(t, ok1)
	assert.Nil(t, res)

	res0 := tree.Put([]byte("acc"), &data.LogRecordPos{Fid: 123, Offset: 999})
	assert.Nil(t, res0)
	res1, ok2 := tree.Delete([]byte("acc"))
	assert.True(t, ok2)
	assert.Equal(t, res1.Fid, uint32(123))
	assert.Equal(t, res1.Offset, int64(999))
	pos1 := tree.Get([]byte("acc"))
	assert.Nil(t, pos1)
}

func TestBPlusTree_Size(t *testing.T) {
	path := "./tmp"

	tree := NewBPlusTree(path, false)
	defer func() {
		if err := tree.Close(); err != nil {
			panic(err)
		}
		if err := os.RemoveAll(path); err != nil {
			panic(err)
		}
	}()
	assert.Equal(t, tree.Size(), 0)

	tree.Put([]byte("aac"), &data.LogRecordPos{Fid: 123, Offset: 999})
	tree.Put([]byte("abc"), &data.LogRecordPos{Fid: 123, Offset: 999})
	tree.Put([]byte("acc"), &data.LogRecordPos{Fid: 123, Offset: 999})

	assert.Equal(t, tree.Size(), 3)
}
func TestBPlusTree_Iterator(t *testing.T) {
	path := "./tmp"
	tree := NewBPlusTree(path, false)
	defer func() {
		if err := tree.Close(); err != nil {
			panic(err)
		}
		if err := os.RemoveAll(path); err != nil {
			panic(err)
		}
	}()
	tree.Put([]byte("caac"), &data.LogRecordPos{Fid: 123, Offset: 999})
	tree.Put([]byte("bbca"), &data.LogRecordPos{Fid: 123, Offset: 999})
	tree.Put([]byte("acce"), &data.LogRecordPos{Fid: 123, Offset: 999})
	tree.Put([]byte("ccec"), &data.LogRecordPos{Fid: 123, Offset: 999})
	tree.Put([]byte("bbba"), &data.LogRecordPos{Fid: 123, Offset: 999})

	iter := tree.Iterator(true)
	assert.NotNil(t, iter)
	for iter.Rewind(); iter.Valid(); iter.Next() {
		assert.NotNil(t, iter.Key())
		assert.NotNil(t, iter.Value())
	}
	iter.Close()
}
