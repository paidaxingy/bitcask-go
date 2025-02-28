package redis

import (
	"bitcask-go/utils"
	"os"
	"testing"
	"time"

	bitcask "bitcask-go"

	"github.com/stretchr/testify/assert"
)

// 测试完成之后销毁 Redis 数据目录
func destroyRds(rds *RedisDataStructure, dirPath string) {
	if rds != nil {
		if err := rds.db.Close(); err != nil {
			panic(err)
		}
		err := os.RemoveAll(dirPath)
		if err != nil {
			panic(err)
		}
	}
}
func TestRedisDataStructure_Get(t *testing.T) {
	opts := bitcask.DefaultOptions
	rds, err := NewRedisDataStructure(opts)
	assert.Nil(t, err)
	defer destroyRds(rds, opts.DirPath)
	err = rds.Set(utils.GetTestKey(1), 0, utils.RandomValue(100))
	assert.Nil(t, err)
	err = rds.Set(utils.GetTestKey(2), time.Second*5, utils.RandomValue(100))
	assert.Nil(t, err)

	val1, err := rds.Get(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val1)

	val2, err := rds.Get(utils.GetTestKey(2))
	assert.Nil(t, err)
	assert.NotNil(t, val2)

	_, err = rds.Get(utils.GetTestKey(3))
	assert.Equal(t, err, bitcask.ErrKeyNotFound)
}

func TestRedisDataStructure_Del_Type(t *testing.T) {
	opts := bitcask.DefaultOptions
	rds, err := NewRedisDataStructure(opts)
	assert.Nil(t, err)
	defer destroyRds(rds, opts.DirPath)

	err = rds.Del(utils.GetTestKey(11))
	assert.Nil(t, err)

	err = rds.Set(utils.GetTestKey(1), 0, utils.RandomValue(100))
	assert.Nil(t, err)

	// Type
	typ, err := rds.Type(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.Equal(t, typ, String)

	err = rds.Del(utils.GetTestKey(1))
	assert.Nil(t, err)

	_, err = rds.Get(utils.GetTestKey(1))
	assert.Equal(t, err, bitcask.ErrKeyNotFound)

}

func TestRedisDataStructure_HGet(t *testing.T) {
	opts := bitcask.DefaultOptions
	rds, err := NewRedisDataStructure(opts)
	defer destroyRds(rds, opts.DirPath)
	assert.Nil(t, err)

	ok1, err := rds.HSet(utils.GetTestKey(1), []byte("field1"), utils.RandomValue(100))
	assert.Nil(t, err)
	assert.True(t, ok1)

	ok2, err := rds.HSet(utils.GetTestKey(1), []byte("field1"), utils.RandomValue(100))
	assert.Nil(t, err)
	assert.False(t, ok2)

	ok3, err := rds.HSet(utils.GetTestKey(1), []byte("field2"), utils.RandomValue(100))
	assert.Nil(t, err)
	assert.True(t, ok3)

	val1, err := rds.HGet(utils.GetTestKey(1), []byte("field1"))
	assert.Nil(t, err)
	assert.NotNil(t, val1)

	val2, err := rds.HGet(utils.GetTestKey(1), []byte("field2"))
	assert.Nil(t, err)
	assert.NotNil(t, val2)

	val3, err := rds.HGet(utils.GetTestKey(1), []byte("field3"))
	assert.Equal(t, err, bitcask.ErrKeyNotFound)
	assert.Nil(t, val3)
}

func TestRedisDataStructure_HDel(t *testing.T) {
	opts := bitcask.DefaultOptions
	rds, err := NewRedisDataStructure(opts)
	defer destroyRds(rds, opts.DirPath)
	assert.Nil(t, err)

	del1, err := rds.HDel(utils.GetTestKey(111), nil)
	assert.Nil(t, err)
	assert.False(t, del1)

	ok1, err := rds.HSet(utils.GetTestKey(1), []byte("field1"), utils.RandomValue(100))
	assert.Nil(t, err)
	assert.True(t, ok1)

	ok2, err := rds.HSet(utils.GetTestKey(1), []byte("field1"), utils.RandomValue(100))
	assert.Nil(t, err)
	assert.False(t, ok2)

	ok3, err := rds.HSet(utils.GetTestKey(1), []byte("field2"), utils.RandomValue(100))
	assert.Nil(t, err)
	assert.True(t, ok3)

	del2, err := rds.HDel(utils.GetTestKey(1), []byte("field2"))
	assert.Nil(t, err)
	assert.True(t, del2)
}

func TestRedisDataStructure_SIsMember(t *testing.T) {
	opts := bitcask.DefaultOptions
	rds, err := NewRedisDataStructure(opts)
	defer destroyRds(rds, opts.DirPath)
	assert.Nil(t, err)

	ok, err := rds.SAdd(utils.GetTestKey(1), []byte("member1"))
	assert.Nil(t, err)
	assert.True(t, ok)

	ok, err = rds.SAdd(utils.GetTestKey(1), []byte("member1"))
	assert.Nil(t, err)
	assert.False(t, ok)

	ok, err = rds.SAdd(utils.GetTestKey(1), []byte("member2"))
	assert.Nil(t, err)
	assert.True(t, ok)

	ok, err = rds.SIsMember(utils.GetTestKey(2), []byte("member1"))
	assert.Nil(t, err)
	assert.False(t, ok)

	ok, err = rds.SIsMember(utils.GetTestKey(1), []byte("member1"))
	assert.Nil(t, err)
	assert.True(t, ok)

	ok, err = rds.SIsMember(utils.GetTestKey(1), []byte("member2"))
	assert.Nil(t, err)
	assert.True(t, ok)

	ok, err = rds.SIsMember(utils.GetTestKey(1), []byte("member3"))
	assert.Nil(t, err)
	assert.False(t, ok)
}

func TestRedisDataStructure_SRem(t *testing.T) {
	opts := bitcask.DefaultOptions
	rds, err := NewRedisDataStructure(opts)
	defer destroyRds(rds, opts.DirPath)
	assert.Nil(t, err)

	ok, err := rds.SAdd(utils.GetTestKey(1), []byte("member1"))
	assert.Nil(t, err)
	assert.True(t, ok)

	ok, err = rds.SAdd(utils.GetTestKey(1), []byte("member1"))
	assert.Nil(t, err)
	assert.False(t, ok)

	ok, err = rds.SAdd(utils.GetTestKey(1), []byte("member2"))
	assert.Nil(t, err)
	assert.True(t, ok)

	ok, err = rds.SRem(utils.GetTestKey(2), []byte("member2"))
	assert.Nil(t, err)
	assert.False(t, ok)

	ok, err = rds.SRem(utils.GetTestKey(1), []byte("member2"))
	assert.Nil(t, err)
	assert.True(t, ok)

	ok, err = rds.SRem(utils.GetTestKey(1), []byte("member3"))
	assert.Nil(t, err)
	assert.False(t, ok)
}

func TestRedisDataStructure_LPop(t *testing.T) {
	opts := bitcask.DefaultOptions
	rds, err := NewRedisDataStructure(opts)
	defer destroyRds(rds, opts.DirPath)
	assert.Nil(t, err)

	res, err := rds.LPush(utils.GetTestKey(1), []byte("val-1"))
	assert.Nil(t, err)
	assert.Equal(t, res, uint32(1))
	res, err = rds.LPush(utils.GetTestKey(1), []byte("val-1"))
	assert.Nil(t, err)
	assert.Equal(t, res, uint32(2))
	res, err = rds.LPush(utils.GetTestKey(1), []byte("val-2"))
	assert.Nil(t, err)
	assert.Equal(t, res, uint32(3))

	val, err := rds.LPop(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.Equal(t, val, []byte("val-2"))

	val, err = rds.LPop(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.Equal(t, val, []byte("val-1"))

	val, err = rds.LPop(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.Equal(t, val, []byte("val-1"))
}

func TestRedisDataStructure_RPop(t *testing.T) {
	opts := bitcask.DefaultOptions
	rds, err := NewRedisDataStructure(opts)
	defer destroyRds(rds, opts.DirPath)
	assert.Nil(t, err)

	res, err := rds.RPush(utils.GetTestKey(1), []byte("val-1"))
	assert.Nil(t, err)
	assert.Equal(t, res, uint32(1))
	res, err = rds.RPush(utils.GetTestKey(1), []byte("val-1"))
	assert.Nil(t, err)
	assert.Equal(t, res, uint32(2))
	res, err = rds.RPush(utils.GetTestKey(1), []byte("val-2"))
	assert.Nil(t, err)
	assert.Equal(t, res, uint32(3))

	val, err := rds.RPop(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.Equal(t, val, []byte("val-2"))

	val, err = rds.RPop(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.Equal(t, val, []byte("val-1"))

	val, err = rds.RPop(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.Equal(t, val, []byte("val-1"))
}
