package fio

import (
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMMap_Read(t *testing.T) {
	path := "a.data"
	defer destroyFile(path)

	mmapIO, err := NewMMapIOManager(path)
	assert.Nil(t, err)

	// empty file
	b1 := make([]byte, 10)
	n1, err := mmapIO.Read(b1, 0)
	assert.Equal(t, 0, n1)
	assert.Equal(t, err, io.EOF)

	err = mmapIO.Close()
	assert.Nil(t, err)
	fio, err := NewFileIOManager(path)
	assert.Nil(t, err)
	_, err = fio.Write([]byte("aa"))
	assert.Nil(t, err)
	_, err = fio.Write([]byte("bb"))
	assert.Nil(t, err)
	_, err = fio.Write([]byte("cc"))
	assert.Nil(t, err)
	err = fio.Close()
	assert.Nil(t, err)
	mmapIO2, err := NewMMapIOManager(path)
	assert.Nil(t, err)
	size, err := mmapIO2.Size()
	assert.Nil(t, err)
	assert.Equal(t, 6, int(size))
	b2 := make([]byte, 2)
	n2, err := mmapIO2.Read(b2, 0)
	assert.Equal(t, 2, n2)
	assert.Nil(t, err)
	assert.Equal(t, b2, []byte("aa"))
	mmapIO2.Close()
}
