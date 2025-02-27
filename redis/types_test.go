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
