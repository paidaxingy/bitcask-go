package index

import (
	"bitcask-go/data"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBPlusTree_Put(t *testing.T) {
	if err := os.MkdirAll("./tmp", os.ModePerm); err != nil {
		panic(err)
	}
	path := filepath.Join("./tmp")
	defer func() {
		_ = os.Remove(path)
	}()
	tree := NewBPlusTree(path, false)
	tree.Put([]byte("aac"), &data.LogRecordPos{Fid: 123, Offset: 999})
	tree.Put([]byte("abc"), &data.LogRecordPos{Fid: 123, Offset: 999})
	tree.Put([]byte("acc"), &data.LogRecordPos{Fid: 123, Offset: 999})
}

func TestBPlusTree_Get(t *testing.T) {
	path := "./tmp"
	defer func() {
		_ = os.RemoveAll(path)
	}()
	tree := NewBPlusTree(path, false)

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
	defer func() {
		_ = os.RemoveAll(path)
	}()
	tree := NewBPlusTree(path, false)

	res := tree.Delete([]byte("not exist"))
	assert.False(t, res)

	tree.Put([]byte("acc"), &data.LogRecordPos{Fid: 123, Offset: 999})
	res1 := tree.Delete([]byte("acc"))
	assert.True(t, res1)
	pos1 := tree.Get([]byte("acc"))
	assert.Nil(t, pos1)
}

func TestBPlusTree_Size(t *testing.T) {
	path := "./tmp"
	defer func() {
		_ = os.RemoveAll(path)
	}()
	tree := NewBPlusTree(path, false)

	assert.Equal(t, tree.Size(), 0)

	tree.Put([]byte("aac"), &data.LogRecordPos{Fid: 123, Offset: 999})
	tree.Put([]byte("abc"), &data.LogRecordPos{Fid: 123, Offset: 999})
	tree.Put([]byte("acc"), &data.LogRecordPos{Fid: 123, Offset: 999})

	assert.Equal(t, tree.Size(), 3)
}
func TestBPlusTree_Iterator(t *testing.T) {
	if err := os.MkdirAll("./tmp", os.ModePerm); err != nil {
		panic(err)
	}
	path := filepath.Join("./tmp")
	defer func() {
		_ = os.RemoveAll(path)
	}()
	tree := NewBPlusTree(path, false)
	tree.Put([]byte("caac"), &data.LogRecordPos{Fid: 123, Offset: 999})
	tree.Put([]byte("bbca"), &data.LogRecordPos{Fid: 123, Offset: 999})
	tree.Put([]byte("acce"), &data.LogRecordPos{Fid: 123, Offset: 999})
	tree.Put([]byte("ccec"), &data.LogRecordPos{Fid: 123, Offset: 999})
	tree.Put([]byte("bbba"), &data.LogRecordPos{Fid: 123, Offset: 999})

	iter := tree.Iterator(true)

	for iter.Rewind(); iter.Valid(); iter.Next() {
		assert.NotNil(t, iter.Key())
		assert.NotNil(t, iter.Value())
	}
}
