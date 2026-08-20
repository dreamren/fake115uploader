package main

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/cheggaaa/pb/v3"
)

// 进度监听
type multipartProgressListener struct {
	bar  *pb.ProgressBar
	last int64 // 上一次进度事件时该分片已上传的字节数
}

// 实现 oss.ProgressListener 的接口
func (listener *multipartProgressListener) ProgressChanged(event *oss.ProgressEvent) {
	switch event.EventType {
	case oss.TransferStartedEvent:
		listener.last = 0
	case oss.TransferDataEvent:
		// 实时更新进度，避免上传大文件时进度条长时间停留在 0%
		listener.bar.Add64(event.ConsumedBytes - listener.last)
		listener.last = event.ConsumedBytes
	case oss.TransferCompletedEvent:
		listener.bar.Add64(event.ConsumedBytes - listener.last)
		listener.last = 0
	case oss.TransferFailedEvent:
		// 分片上传失败，回滚该分片已计入进度的字节数
		listener.bar.Add64(-listener.last)
		listener.last = 0
	default:
	}
}

// 按文件大小分割分片，优先使用指定的分片数量，否则按分片大小分片
func splitChunks(file string, size int64) ([]oss.FileChunk, error) {
	if config.PartsNum != 0 {
		return oss.SplitFileByPartNum(file, int(config.PartsNum))
	}

	partSize := int64(config.PartSizeMB) * 1024 * 1024
	// 单个分片大小不能小于 100KB
	if partSize < 100*1024 {
		partSize = 100 * 1024
	}
	// 分片数量不能超过 maxParts，超过时按分片数量上限重新计算分片大小
	if size > partSize*maxParts {
		partSize = (size + maxParts - 1) / maxParts
	}

	return oss.SplitFileByPartSize(file, partSize)
}

// 上传单个分片，出现错误就重试，共尝试 3 次
func uploadPartWithRetry(ctx context.Context, tm *ossTokenManager, imur oss.InitiateMultipartUploadResult,
	f *os.File, chunk oss.FileChunk, file string, bar *pb.ProgressBar) (oss.UploadPart, error) {
	var lastErr error
	for retry := 0; retry < 3; retry++ {
		if err := ctx.Err(); err != nil {
			if lastErr != nil {
				return oss.UploadPart{}, lastErr
			}
			return oss.UploadPart{}, err
		}

		ot, bucket := tm.get()
		if _, err := f.Seek(chunk.Offset, io.SeekStart); err != nil {
			return oss.UploadPart{}, fmt.Errorf("移动 %s 的读取位置出现错误：%w", file, err)
		}
		listener := &multipartProgressListener{bar: bar}
		part, err := bucket.UploadPart(imur, f, chunk.Size, chunk.Number,
			oss.SetHeader("x-oss-security-token", ot.SecurityToken),
			oss.UserAgentHeader(aliUserAgent),
			oss.Progress(listener),
		)
		if err == nil {
			return part, nil
		}
		// 回滚失败分片已显示的进度
		bar.Add64(-listener.last)
		listener.last = 0

		lastErr = err
		log.Printf("上传 %s 的第%d个分片时出现错误：%v", file, chunk.Number, err)
		if retry < 2 {
			log.Printf("等待 %d 秒后尝试重新上传第%d个分片", retry+1, chunk.Number)
			select {
			case <-time.After(time.Duration(retry+1) * time.Second):
			case <-ctx.Done():
				return oss.UploadPart{}, lastErr
			}
		}
	}

	return oss.UploadPart{}, lastErr
}

// 中止 OSS 上的分片上传任务，避免残留已上传的分片
func abortUpload(tm *ossTokenManager, imur oss.InitiateMultipartUploadResult, file string) {
	ot, bucket := tm.get()
	if err := bucket.AbortMultipartUpload(imur,
		oss.SetHeader("x-oss-security-token", ot.SecurityToken),
		oss.UserAgentHeader(aliUserAgent),
	); err != nil {
		log.Printf("中止 %s 在 OSS 上的分片上传任务出现错误：%v", file, err)
	}
}

// 利用 oss 的接口以分片并行的方式上传文件
func multipartUploadFile(ctx context.Context, ft *fastToken, file string, parentCID uint64) (e error) {
	log.Println("分片模式上传文件：" + file)

	info, err := os.Stat(file)
	if err != nil {
		return fmt.Errorf("获取 %s 的信息出现错误：%w", file, err)
	}
	// 分片模式上传的文件大小不能小于 1KB（1KB 这个大小属于推测，没详细测试过）
	if info.Size() <= 1024 {
		log.Printf("%s 的大小小于1KB，改用普通模式上传", file)
		return ossUploadFile(ctx, ft, file, parentCID)
	}
	// 上传的文件大小不能超过 115GB
	if info.Size() > 115*1024*1024*1024 {
		return fmt.Errorf("%s 的大小超过115GB，取消上传", file)
	}

	tm, err := newOssTokenManager(ctx, ft.Bucket)
	if err != nil {
		return err
	}

	chunks, err := splitChunks(file, info.Size())
	if err != nil {
		return fmt.Errorf("分割 %s 的分片出现错误：%w", file, err)
	}

	ot, bucket := tm.get()
	// 必须设置 oss.Sequential()：115 定制的 OSS 只在顺序模式下计算整个文件的 SHA1，
	// callback 里的 ${sha1} 才会被正确填充，115 服务端校验才能通过。
	// 顺序模式要求分片按序号上传，因此分片只能串行上传，速度靠多任务并行弥补
	imur, err := bucket.InitiateMultipartUpload(ft.Object,
		oss.SetHeader("x-oss-security-token", ot.SecurityToken),
		oss.UserAgentHeader(aliUserAgent),
		oss.Sequential(),
	)
	if err != nil {
		return fmt.Errorf("初始化 %s 的分片上传出现错误：%w", file, err)
	}

	fmt.Println("按 q 键停止上传并退出程序")
	bar := newBar(info.Size(), filepath.Base(file))
	defer bar.Finish()

	// 顺序模式要求分片按序号依次上传，只能串行上传分片
	f, err := os.Open(file)
	if err != nil {
		abortUpload(tm, imur, file)
		return fmt.Errorf("打开 %s 出现错误：%w", file, err)
	}
	defer f.Close()

	parts := make([]oss.UploadPart, len(chunks))
	if *verbose {
		log.Printf("开始按序上传 %s 的 %d 个分片", file, len(chunks))
	}
	for _, chunk := range chunks {
		if err = ctx.Err(); err != nil {
			// 用户主动停止上传
			abortUpload(tm, imur, file)
			return err
		}
		if *verbose {
			log.Printf("正在上传 %s 的第%d个分片（共%d个分片）", file, chunk.Number, len(chunks))
		}
		part, err := uploadPartWithRetry(ctx, tm, imur, f, chunk, file, bar)
		if err != nil {
			abortUpload(tm, imur, file)
			return err
		}
		// 分片号从 1 开始，按序存放以便合并
		parts[chunk.Number-1] = part
	}

	ot, bucket = tm.get()
	// 顺序模式下 115 定制的 OSS 会计算整个文件的 SHA1 并自动填充 callback 里的
	// ${sha1} 占位符，客户端不能替换它；x-oss-hash-sha1 头用于 OSS 侧的完整性校验
	cb := base64.StdEncoding.EncodeToString([]byte(ft.Callback.Callback))
	cbVar := base64.StdEncoding.EncodeToString([]byte(ft.Callback.CallbackVar))
	var header http.Header
	var callbackBody []byte
	cmur, err := bucket.CompleteMultipartUpload(imur, parts,
		oss.SetHeader("x-oss-security-token", ot.SecurityToken),
		oss.SetHeader("x-oss-hash-sha1", ft.SHA1),
		oss.Callback(cb),
		oss.CallbackVar(cbVar),
		oss.UserAgentHeader(aliUserAgent),
		oss.GetResponseHeader(&header),
		oss.CallbackResult(&callbackBody),
	)
	// EOF 错误是 xml 的 Unmarshal 导致的，响应其实是 json 格式，所以实际上上传是成功的
	if err != nil && !errors.Is(err, io.EOF) {
		// 当文件名含有 &< 这两个字符之一时响应的 xml 解析会出现错误，实际上上传是成功的
		if filename := filepath.Base(file); !strings.ContainsAny(filename, "&<") {
			return fmt.Errorf("完成 %s 的分片上传出现错误：%w", file, err)
		}
	}
	if *verbose {
		log.Printf("CompleteMultipartUpload 的响应头的值是：\n%+v", header)
		log.Printf("callback 的响应体的内容是：%s", callbackBody)
		log.Printf("cmur 的值是：%+v", cmur)
	}
	// OSS 返回 200 不代表 115 入库成功，115 可能在 callback 响应里返回业务错误，
	// 这里解析响应以及时发现入库失败的具体原因
	if filename := filepath.Base(file); !strings.ContainsAny(filename, "&<") {
		if err := checkCallbackResult(callbackBody, file); err != nil {
			return err
		}
	}

	// 验证上传是否成功
	if err = verifyUploaded(parentCID, filepath.Base(file), ft.SHA1); err != nil {
		return err
	}
	log.Printf("分片模式上传 %s 成功", file)
	if *removeFile {
		// Windows 不允许删除被占用的文件，先关闭文件句柄和进度条再删除
		f.Close()
		bar.Finish()
		if err = remove(file); err != nil {
			return err
		}
	}

	return nil
}
