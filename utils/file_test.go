package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDirSize(t *testing.T) {
	path := "."
	dirSize, err := DirSize(path)
	assert.Nil(t, err)
	assert.True(t, dirSize > 0)
}

func TestAvailableDiskSize(t *testing.T) {
	path := "."
	size, err := AvailableDiskSize(path)
	assert.Nil(t, err)
	assert.True(t, size > 0)
}
