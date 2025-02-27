package utils

import (
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"unsafe"
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

// 取指定目录所在磁盘的剩余空间大小
func AvailableDiskSize(dirPath string) (uint64, error) {
	// 加载 kernel32.dll
	kernel32 := syscall.NewLazyDLL("kernel32.dll")
	// 获取 GetDiskFreeSpaceExW 函数
	procGetDiskFreeSpaceExW := kernel32.NewProc("GetDiskFreeSpaceExW")

	// 将路径转换为 UTF-16
	pathPtr, err := syscall.UTF16PtrFromString(dirPath)
	if err != nil {
		return 0, err
	}

	var freeBytesAvailable, _, _ int64

	// 调用 GetDiskFreeSpaceExW
	ret, _, err := procGetDiskFreeSpaceExW.Call(
		uintptr(unsafe.Pointer(pathPtr)),
		uintptr(unsafe.Pointer(&freeBytesAvailable)),
		0,
		0,
	)

	if ret == 0 {
		return 0, err
	}

	return uint64(freeBytesAvailable), nil
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
