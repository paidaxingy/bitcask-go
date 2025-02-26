package utils

import (
	"os"
	"path/filepath"
)

// DirSize 获取目录大小
func DirSize(dirPath string) (int64, error) {

	var size int64

	err := filepath.Walk(dirPath, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return nil
	})

	return size, err
}

// AvailableDiskSize 获取可用磁盘大小
func AvailableDiskSize(dirPath string) (uint64, error) {
	// todo
	return 10000000000, nil
}
