package bitcaskgo

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

// 测试完成之后销毁 DB 数据目录
func destroyDB(db *DB) {
	if db != nil {
		if db.activeFile != nil {
			_ = db.activeFile.Close() // todo 实现后用close
		}
		err := os.RemoveAll(db.options.DirPath)
		if err != nil {
			panic(err)
		}
	}
}
func TestOpen(t *testing.T) {
	opts := DefaultOptions
	db, err := Open(opts)
	t.Log(db)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)
}

func TestDB_Put(t *testing.T) {
	opts := DefaultOptions
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 正常情况
	err = db.Put([]byte("key"), []byte("value"))
	assert.Nil(t, err)
	val1, err := db.Get([]byte("key"))
	assert.Nil(t, err)
	assert.Equal(t, []byte("value"), val1)

	// key为空
	err = db.Put(nil, []byte("value"))
	assert.Equal(t, ErrKeyIsEmpty, err)

	// value为空
	err = db.Put([]byte("key2"), nil)
	assert.Nil(t, err)
	val2, err := db.Get([]byte("key2"))
	assert.Nil(t, err)
	assert.Equal(t, []byte{}, val2)

	// 重复 Put 相同的 key
	err = db.Put([]byte("key"), []byte("new-value"))
	assert.Nil(t, err)
	val3, err := db.Get([]byte("key"))
	assert.Nil(t, err)
	assert.Equal(t, []byte("new-value"), val3)
}

func TestDB_Get(t *testing.T) {
	opts := DefaultOptions
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 正常读取一条数据
	err = db.Put([]byte("key"), []byte("value"))
	assert.Nil(t, err)
	val, err := db.Get([]byte("key"))
	assert.Nil(t, err)
	assert.Equal(t, []byte("value"), val)

	// 读取一个不存在的 key
	val, err = db.Get([]byte("non-exist-key"))
	assert.Equal(t, ErrKeyNotFound, err)
	assert.Nil(t, val)

	// 值被重复 Put 后再次读取
	err = db.Put([]byte("key"), []byte("new-value"))
	assert.Nil(t, err)
	val, err = db.Get([]byte("key"))
	assert.Nil(t, err)
	assert.Equal(t, []byte("new-value"), val)

	// 读取一个空的 key
	val, err = db.Get(nil)
	assert.Equal(t, ErrKeyIsEmpty, err)
	assert.Nil(t, val)

	// 值被删除后再 Get
	err = db.Delete([]byte("key"))
	assert.Nil(t, err)
	val, err = db.Get([]byte("key"))
	assert.Equal(t, ErrKeyNotFound, err)
	assert.Nil(t, val)
}

func TestDB_Delete(t *testing.T) {
	opts := DefaultOptions
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 正常删除一个存在的 key
	err = db.Put([]byte("key"), []byte("value"))
	assert.Nil(t, err)
	err = db.Delete([]byte("key"))
	assert.Nil(t, err)
	val, err := db.Get([]byte("key"))
	assert.Equal(t, ErrKeyNotFound, err)
	assert.Nil(t, val)

	// 删除一个不存在的 key
	err = db.Delete([]byte("non-exist-key"))
	assert.Nil(t, err)

	// 删除一个空的 key
	err = db.Delete(nil)
	assert.Equal(t, ErrKeyIsEmpty, err)
}
