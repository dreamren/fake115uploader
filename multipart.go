package main

import (
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
	last int64 // 上一次进度事件时该分片已上传的字节数
}

// 实现 oss.ProgressListener 的接口
func (listener *multipartProgressListener) ProgressChanged(event *oss.ProgressEvent) {
	switch event.EventType {
	case oss.TransferStartedEvent:
		listener.last = 0
	case oss.TransferDataEvent:
		// 实时更新进度，避免上传大文件时进度条长时间停留在 0%
		bar.Add64(event.ConsumedBytes - listener.last)
		listener.last = event.ConsumedBytes
	case oss.TransferCompletedEvent:
		bar.Add64(event.ConsumedBytes - listener.last)
		listener.last = 0
	case oss.TransferFailedEvent:
		// 分片上传失败，回滚该分片已计入进度的字节数
		bar.Add64(-listener.last)
		listener.last = 0
	default:
	}
}

// 获取 ossToken 和 bucket
func getBucket(bucketName string) (ot *ossToken, bucket *oss.Bucket, e error) {
	defer func() {
		if err := recover(); err != nil {
			e = fmt.Errorf("getBucket() error: %v", err)
		}
	}()

	ot, err := getOSSToken()
	checkErr(err)
	client, err := oss.New(ot.endpoint, ot.AccessKeyID, ot.AccessKeySecret, getClientOptions()...)
	checkErr(err)
	bucket, err = client.Bucket(bucketName)
	checkErr(err)
	return ot, bucket, nil
}

// 利用 oss 的接口以 multipart 的方式上传文件
func multipartUploadFile(ft *fastToken, file string) (e error) {
	defer func() {
		if err := recover(); err != nil {
			e = fmt.Errorf("multipartUploadFile() error: %v", err)
		}
	}()

	log.Println("分片模式上传文件：" + file)

	ot, bucket, err := getBucket(ft.Bucket)
	checkErr(err)
	// ossToken 一小时后就会失效，所以每 50 分钟重新获取一次
	ticker := time.NewTicker(50 * time.Minute)
	defer ticker.Stop()

	cb := base64.StdEncoding.EncodeToString([]byte(ft.Callback.Callback))
	cbVar := base64.StdEncoding.EncodeToString([]byte(ft.Callback.CallbackVar))

	info, err := os.Stat(file)
	checkErr(err)

	// 分片模式上传的文件大小不能小于 1KB（1KB 这个大小属于推测，没详细测试过）
	if info.Size() <= 1024 {
		// 此时不能打开文件，否则文件被占用会导致上传后删除文件失败
		log.Printf("%s 的大小小于1KB，改用普通模式上传", file)
		return ossUploadFile(ft, file)
	}
	// 上传的文件大小不能超过 115GB
	if info.Size() > 115*1024*1024*1024 {
		return fmt.Errorf("%s 的大小超过115GB，取消上传", file)
	}

	var chunks []oss.FileChunk
	// 是否指定分片数量
	if config.PartsNum != 0 {
		chunks, err = oss.SplitFileByPartNum(file, int(config.PartsNum))
		checkErr(err)
	} else {
		for i := int64(1); i < 10; i++ {
			if info.Size() < i*1024*1024*1024 {
				// 文件大小小于 iGB 时分为 i*1000 片
				chunks, err = oss.SplitFileByPartNum(file, int(i*1000))
				checkErr(err)
				break
			}
		}
		if info.Size() > 9*1024*1024*1024 {
			// 文件大小大于 9GB 时分为 10000 片
			chunks, err = oss.SplitFileByPartNum(file, maxParts)
			checkErr(err)
		}
	}
	// 单个分片大小不能小于 100KB
	if chunks[0].Size < 100*1024 {
		chunks, err = oss.SplitFileByPartSize(file, 100*1024)
		checkErr(err)
	}

	imur, err := bucket.InitiateMultipartUpload(ft.Object,
		oss.SetHeader("x-oss-security-token", ot.SecurityToken),
		oss.UserAgentHeader(aliUserAgent),
		oss.Sequential(),
	)
	checkErr(err)

	f, err := os.Open(file)
	checkErr(err)
	defer f.Close()

	fmt.Println("按 q 键停止上传并退出程序")
	// 已成功上传的分片总字节数
	var committed int64
	var parts []oss.UploadPart
	bar = pb.New64(info.Size()).SetTemplate(pb.Full).Set(pb.Bytes, true)
	bar.Start()
	defer bar.Finish()

	// 中止 OSS 上的分片上传任务，避免残留已上传的分片
	abortUpload := func() {
		if err := bucket.AbortMultipartUpload(imur,
			oss.SetHeader("x-oss-security-token", ot.SecurityToken),
			oss.UserAgentHeader(aliUserAgent),
		); err != nil {
			log.Printf("中止 %s 在 OSS 上的分片上传任务出现错误：%v", file, err)
		}
	}

	uploadingPart = true
	defer func() {
		uploadingPart = false
	}()
	for _, chunk := range chunks {
		select {
		case <-multipartCh:
			// 按 q 键停止上传，先中止 OSS 上的上传任务再通知退出程序
			bar.Finish()
			abortUpload()
			multipartCh <- struct{}{}
			return errStopUpload
		default:
			if *verbose {
				log.Printf("正在上传 %s 的第%d个分片（共%d个分片）", file, chunk.Number, len(chunks))
			}
			var part oss.UploadPart
			// 出现错误就继续尝试，共尝试 3 次
			for retry := 0; retry < 3; retry++ {
				select {
				case <-ticker.C:
					// 到时重新获取 ossToken
					ot, bucket, err = getBucket(ft.Bucket)
					checkErr(err)
				default:
				}
				f.Seek(chunk.Offset, io.SeekStart)
				part, err = bucket.UploadPart(imur, f, chunk.Size, chunk.Number,
					oss.SetHeader("x-oss-security-token", ot.SecurityToken),
					oss.UserAgentHeader(aliUserAgent),
					oss.Progress(&multipartProgressListener{}),
				)
				if err == nil {
					break
				} else {
					log.Printf("上传 %s 的第%d个分片时出现错误：%v", file, chunk.Number, err)
					// 回滚失败分片已显示的进度
					bar.SetCurrent(committed)
					if retry != 2 {
						log.Printf("等待 %d 秒后尝试重新上传第%d个分片", retry+1, chunk.Number)
						time.Sleep(time.Duration(retry+1) * time.Second)
					}
				}
			}
			if err != nil {
				bar.Finish()
				abortUpload()
				return fmt.Errorf("上传 %s 的第%d个分片时出现错误：%w", file, chunk.Number, err)
			}
			parts = append(parts, part)
			committed += chunk.Size
		}
	}
	uploadingPart = false
	bar.Finish()

	select {
	case <-ticker.C:
		// 到时重新获取 ossToken
		ot, bucket, err = getBucket(ft.Bucket)
		checkErr(err)
	default:
	}
	var header http.Header
	cmur, err := bucket.CompleteMultipartUpload(imur, parts,
		oss.SetHeader("x-oss-security-token", ot.SecurityToken),
		oss.SetHeader("x-oss-hash-sha1", ft.SHA1),
		oss.Callback(cb),
		oss.CallbackVar(cbVar),
		oss.UserAgentHeader(aliUserAgent),
		oss.GetResponseHeader(&header),
	)
	// EOF 错误是 xml 的 Unmarshal 导致的，响应其实是 json 格式，所以实际上上传是成功的
	if err != nil && !errors.Is(err, io.EOF) {
		// 当文件名含有 &< 这两个字符之一时响应的 xml 解析会出现错误，实际上上传是成功的
		if filename := filepath.Base(file); !strings.ContainsAny(filename, "&<") {
			panic(err)
		}
	}
	if *verbose {
		log.Printf("CompleteMultipartUpload 的响应头的值是：\n%+v", header)
		log.Printf("cmur 的值是：%+v", cmur)
	}

	time.Sleep(time.Second)
	// 验证上传是否成功
	fileURL := fmt.Sprintf(listFileURL, config.CID, 20)
	v, err := getURLJSON(fileURL)
	checkErr(err)
	s := string(v.GetStringBytes("data", "0", "sha"))
	if s == ft.SHA1 {
		log.Printf("分片模式上传 %s 成功", file)
		if *removeFile {
			f.Close()
			err = remove(file)
			checkErr(err)
		}
	} else {
		panic(fmt.Errorf("分片模式上传 %s 失败", file))
	}

	return nil
}
