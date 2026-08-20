package main

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	"github.com/cheggaaa/pb/v3"
)

// 计算文件指定范围内的 sha1 值
func hashFileRange(f *os.File, signCheck string) (rangeHash string, e error) {
	var start, end int64
	if _, err := fmt.Sscanf(signCheck, "%d-%d", &start, &end); err != nil {
		return "", fmt.Errorf("解析 sign_check %s 出现错误：%w", signCheck, err)
	}
	if start < 0 || end < 0 || end < start {
		return "", fmt.Errorf("sign_check范围错误：%s", signCheck)
	}

	if _, err := f.Seek(start, io.SeekStart); err != nil {
		return "", fmt.Errorf("移动 %s 的读取位置出现错误：%w", f.Name(), err)
	}
	h := sha1.New()
	if _, err := io.CopyN(h, f, end-start+1); err != nil {
		return "", fmt.Errorf("计算 %s 的指定范围 sha1 值出现错误：%w", f.Name(), err)
	}

	return strings.ToUpper(hex.EncodeToString(h.Sum(nil))), nil
}

// 计算文件的 sha1 值
func hashSHA1(f *os.File) (blockHash, totalHash string, e error) {
	// 计算文件最前面一个区块的 sha1 hash 值
	block := make([]byte, 128*1024)
	n, err := f.Read(block)
	if err != nil {
		return "", "", fmt.Errorf("读取 %s 出现错误：%w", f.Name(), err)
	}
	data := sha1.Sum(block[:n])
	blockHash = strings.ToUpper(hex.EncodeToString(data[:]))
	if _, err = f.Seek(0, io.SeekStart); err != nil {
		return "", "", fmt.Errorf("移动 %s 的读取位置出现错误：%w", f.Name(), err)
	}

	// 计算整个文件的 sha1 hash 值
	// 大文件计算 hash 需要较长时间，显示进度条避免看起来像卡住
	var hashBar *pb.ProgressBar
	if info, serr := f.Stat(); serr == nil && info.Size() > 16*1024*1024 {
		log.Printf("正在计算 %s 的 SHA1 值，文件大小是 %d 字节", f.Name(), info.Size())
		hashBar = newBar(info.Size(), f.Name()+" SHA1")
	}
	h := sha1.New()
	var reader io.Reader = f
	if hashBar != nil {
		reader = hashBar.NewProxyReader(f)
	}
	if _, err = io.Copy(h, reader); err != nil {
		return "", "", fmt.Errorf("计算 %s 的 sha1 值出现错误：%w", f.Name(), err)
	}
	if hashBar != nil {
		hashBar.Finish()
	}
	totalHash = strings.ToUpper(hex.EncodeToString(h.Sum(nil)))

	return blockHash, totalHash, nil
}
