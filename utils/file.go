package utils

import (
	"os"
	"path/filepath"
	"strings"
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

// 拷贝数据目录
func CopyDir(src, dest string, exclude []string) error {
	// 目标文件夹不存在则创建
	if _, err := os.Stat(dest); os.IsNotExist(err) {
		if err := os.MkdirAll(dest, os.ModePerm); err != nil {
			return err
		}
	}

	return filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		fileName := strings.Replace(path, src, "", 1)
		if fileName == "" {
			return nil
		}
		for _, e := range exclude {
			matched, errMatch := filepath.Match(e, info.Name())
			if errMatch != nil {
				return errMatch
			}
			if matched {
				return nil
			}
		}
		if info.IsDir() {
			if errMKdir := os.Mkdir(filepath.Join(dest, fileName), info.Mode()); errMKdir != nil {
				return errMKdir
			}
			return nil
		}
		data, errRD := os.ReadFile(fileName)
		if errRD != nil {
			return errRD
		}
		return os.WriteFile(fileName, data, info.Mode())
	})
}
