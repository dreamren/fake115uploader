package main

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"strings"
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
func hashSHA1(f *os.File, tb *taskBar) (blockHash, totalHash string, e error) {
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
	info, serr := f.Stat()
	h := sha1.New()
	if serr == nil && info.Size() >= minBarSize && tb != nil && tb.bar != nil {
		// 大文件计算 hash 需要一定时间，在同一行显示校验进度（磁盘读取速度），
		// 校验完成后被上传进度覆盖；小于 1MB 的文件瞬间完成，不打扰进度条显示
		tb.beginPhase("校验 "+info.Name(), info.Size())
		if _, err = io.Copy(h, tb.bar.NewProxyReader(f)); err != nil {
			return "", "", fmt.Errorf("计算 %s 的 sha1 值出现错误：%w", f.Name(), err)
		}
	} else if _, err = io.Copy(h, f); err != nil {
		return "", "", fmt.Errorf("计算 %s 的 sha1 值出现错误：%w", f.Name(), err)
	}
	totalHash = strings.ToUpper(hex.EncodeToString(h.Sum(nil)))

	return blockHash, totalHash, nil
}
