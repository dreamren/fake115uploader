package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/cheggaaa/pb/v3"
	"github.com/valyala/fastjson"
)

type uploadInfo struct {
	Endpoint    string `json:"endpoint"`
	GetTokenURL string `json:"gettokenurl"`
}

type ossToken struct {
	StatusCode      string
	AccessKeySecret string
	SecurityToken   string
	Expiration      string
	AccessKeyID     string `json:"AccessKeyId"`
	endpoint        string
}

var (
	stdoutIsTTY = func() bool {
		info, err := os.Stdout.Stat()
		return err == nil && info.Mode()&os.ModeCharDevice != 0
	}()

	// 进度条池管理：pb.Pool 在所有进度条完成后的下一次刷新时会停止渲染且无法重启，
	// 多文件依次上传时后面的文件就没有进度条了，因此检测到全部完成后换用新的池
	barMu       sync.Mutex
	curPool     *pb.Pool           // 当前正在渲染的进度条池
	curPoolBars []*pb.ProgressBar  // 已加入当前池的进度条
)

// 等待旧池完成最后一次渲染的时间，略大于 pb 库的默认刷新间隔（200ms）
const barSwitchDelay = 250 * time.Millisecond

// 创建带文件名前缀的进度条
// 进度条统一输出到 stdout，日志输出到 stderr，避免两者在终端里互相覆盖截断
func newBar(total int64, prefix string) *pb.ProgressBar {
	b := pb.New64(total).SetTemplate(pb.Full).Set(pb.Bytes, true).Set("prefix", prefix).SetWriter(os.Stdout)
	if !stdoutIsTTY {
		b.Start()
		return b
	}

	barMu.Lock()
	defer barMu.Unlock()

	// 所有进度条都已完成时，池已停止渲染（或即将停止），换用新池
	if curPool != nil && allBarsFinished() {
		// 等待渲染循环把已完成进度条的最终状态渲染出来再停止
		time.Sleep(barSwitchDelay)
		curPool.Stop()
		curPool = nil
		curPoolBars = nil
	}
	if curPool == nil {
		curPool = pb.NewPool()
		if err := curPool.Start(); err != nil {
			log.Printf("启动进度条池出现错误：%v", err)
			curPool = nil
			b.Start() // 退化为单进度条独立渲染
			return b
		}
	}
	curPool.Add(b)
	curPoolBars = append(curPoolBars, b)
	return b
}

// 判断当前池里的进度条是否全部完成
func allBarsFinished() bool {
	for _, b := range curPoolBars {
		if !b.IsFinished() {
			return false
		}
	}
	return true
}

// 停止进度条池
func stopBarPool() {
	barMu.Lock()
	defer barMu.Unlock()
	if curPool != nil {
		curPool.Stop()
		curPool = nil
	}
}

// 进度监听
type ossProgressListener struct {
	bar *pb.ProgressBar
}

// 实现 oss.ProgressListener 的接口
func (listener *ossProgressListener) ProgressChanged(event *oss.ProgressEvent) {
	switch event.EventType {
	case oss.TransferDataEvent:
		listener.bar.SetCurrent(event.ConsumedBytes)
	case oss.TransferCompletedEvent, oss.TransferFailedEvent:
		listener.bar.Finish()
	default:
	}
}

// 获取网页请求响应的 json
func getURLJSON(url string) (v *fastjson.Value, e error) {
	body, err := getURL(url)
	if err != nil {
		return nil, err
	}
	var p fastjson.Parser
	v, err = p.ParseBytes(body)
	if err != nil {
		return nil, fmt.Errorf("解析 %s 的响应出现错误：%w", url, err)
	}

	return v, nil
}

// 获取 POST 表单请求响应的 json
func postFormJSON(url string, formStr string) (v *fastjson.Value, e error) {
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewBuffer([]byte(formStr)))
	if err != nil {
		return nil, fmt.Errorf("构造请求 %s 出现错误：%w", url, err)
	}
	req.Header.Set("User-Agent", userAgent)
	req.Header.Set("Cookie", config.Cookies)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := doRequest(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("读取 %s 的响应出现错误：%w", url, err)
	}

	var p fastjson.Parser
	v, err = p.ParseBytes(body)
	if err != nil {
		return nil, fmt.Errorf("解析 %s 的响应出现错误：%w", url, err)
	}
	return v, nil
}

// 以 GET 请求获取网页内容
func getURL(url string) (body []byte, e error) {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("构造请求 %s 出现错误：%w", url, err)
	}
	req.Header.Set("User-Agent", userAgent)
	req.Header.Set("Cookie", config.Cookies)
	resp, err := doRequest(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err = io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("读取 %s 的响应出现错误：%w", url, err)
	}

	return body, nil
}

// 获取 oss 的 token
func getOSSToken() (token *ossToken, e error) {
	token = new(ossToken)
	body, err := getURL(getinfoURL)
	if err != nil {
		return nil, err
	}
	var info uploadInfo
	if err = json.Unmarshal(body, &info); err != nil {
		return nil, fmt.Errorf("解析 getuploadinfo 的响应出现错误：%w", err)
	}
	if *internal {
		i := strings.Index(info.Endpoint, ".aliyuncs.com")
		if i < 0 {
			return nil, fmt.Errorf("OSS endpoint %s 无法转换为内网地址", info.Endpoint)
		}
		token.endpoint = info.Endpoint[:i] + "-internal" + info.Endpoint[i:]
	} else {
		token.endpoint = info.Endpoint
	}

	if *verbose {
		log.Printf("info 的值：\n%+v", info)
	}

	body, err = getURL(info.GetTokenURL)
	if err != nil {
		return nil, err
	}
	if err = json.Unmarshal(body, &token); err != nil {
		return nil, fmt.Errorf("解析 OSS token 的响应出现错误：%w", err)
	}

	if *verbose {
		log.Printf("OSS token 的值：\n%+v", token)
	}

	return token, nil
}

// 获取 oss 客户端选项
func getClientOptions() (options []oss.ClientOption) {
	if proxyHost != "" {
		if proxyUser != "" {
			options = append(options, oss.AuthProxy(proxyHost, proxyUser, proxyPassword))
		} else {
			options = append(options, oss.Proxy(proxyHost))
		}
	}

	return options
}

// ossToken 的管理器，在 token 到期前主动刷新并重建 OSS 客户端
type ossTokenManager struct {
	ctx        context.Context
	bucketName string
	mu         sync.Mutex
	ot         *ossToken
	bucket     *oss.Bucket
	expiresAt  time.Time
}

// 创建 ossToken 管理器，并启动后台主动刷新协程
func newOssTokenManager(ctx context.Context, bucketName string) (*ossTokenManager, error) {
	m := &ossTokenManager{ctx: ctx, bucketName: bucketName}
	if err := m.refresh(); err != nil {
		return nil, err
	}
	go m.autoRefresh()

	return m, nil
}

// 刷新 token 并重建 OSS 客户端
func (m *ossTokenManager) refresh() error {
	ot, err := getOSSToken()
	if err != nil {
		return err
	}
	client, err := oss.New(ot.endpoint, ot.AccessKeyID, ot.AccessKeySecret, getClientOptions()...)
	if err != nil {
		return fmt.Errorf("创建 OSS 客户端出现错误：%w", err)
	}
	bucket, err := client.Bucket(m.bucketName)
	if err != nil {
		return fmt.Errorf("获取 OSS bucket 出现错误：%w", err)
	}

	// token 的过期时间解析失败时，默认 50 分钟后过期
	expiresAt := time.Now().Add(50 * time.Minute)
	if t, err := time.Parse(time.RFC3339, ot.Expiration); err == nil {
		expiresAt = t
	}

	m.mu.Lock()
	m.ot = ot
	m.bucket = bucket
	m.expiresAt = expiresAt
	m.mu.Unlock()

	return nil
}

// 获取当前 token 和 bucket，快到期时同步刷新一次兜底
func (m *ossTokenManager) get() (*ossToken, *oss.Bucket) {
	m.mu.Lock()
	expiring := time.Now().After(m.expiresAt.Add(-2 * time.Minute))
	m.mu.Unlock()

	if expiring {
		if err := m.refresh(); err != nil {
			log.Printf("刷新 ossToken 出现错误：%v", err)
		}
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	return m.ot, m.bucket
}

// 后台协程，在 token 到期前 5 分钟主动刷新
func (m *ossTokenManager) autoRefresh() {
	for {
		m.mu.Lock()
		expiresAt := m.expiresAt
		m.mu.Unlock()

		// 到期前 5 分钟刷新，已过期的立刻刷新
		d := time.Until(expiresAt.Add(-5 * time.Minute))
		if d <= 0 {
			d = time.Second
		}
		select {
		case <-m.ctx.Done():
			return
		case <-time.After(d):
		}

		if err := m.refresh(); err != nil {
			log.Printf("主动刷新 ossToken 出现错误：%v", err)
			// 刷新失败后退避重试
			select {
			case <-m.ctx.Done():
				return
			case <-time.After(30 * time.Second):
			}
		}
	}
}

// 在目标文件夹的文件列表里按文件名精确查找文件，返回该文件在服务端的 SHA1
// 使用文件夹列表接口而不是搜索接口，列表实时反映服务端状态，搜索索引对刚上传的文件有延迟
func findUploadedFile(parentCID uint64, filename string) (sha string, found bool, e error) {
	fileURL := fmt.Sprintf(listFileDirURL, parentCID)
	v, err := getURLJSON(fileURL)
	if err != nil {
		return "", false, err
	}

	for _, e := range v.GetArray("data") {
		// 文件夹没有 fid 字段
		if !e.Exists("fid") {
			continue
		}
		// 列表按时间降序排列，先找到的是最新上传的同名文件
		if string(e.GetStringBytes("n")) == filename {
			return strings.ToUpper(string(e.GetStringBytes("sha"))), true, nil
		}
	}

	return "", false, nil
}

// 验证上传是否成功，服务端入库有延迟，共尝试 5 次
func verifyUploaded(parentCID uint64, filename, fileSHA1 string) error {
	var lastSHA string
	found := false
	for i := 0; i < 5; i++ {
		sha, ok, err := findUploadedFile(parentCID, filename)
		if err != nil {
			log.Printf("验证上传 %s 出现错误（第%d次尝试）：%v", filename, i+1, err)
		} else if ok {
			found = true
			lastSHA = sha
			if sha == fileSHA1 {
				return nil
			}
		}
		if i < 4 {
			time.Sleep(2 * time.Second)
		}
	}

	if found {
		return fmt.Errorf("验证上传 %s 失败：文件已上传但 SHA1 不匹配（服务端是 %s，本地是 %s）", filename, lastSHA, fileSHA1)
	}
	return fmt.Errorf("验证上传 %s 失败：目标文件夹里没有找到该文件", filename)
}

// 解析 115 在 callback 里返回的响应，判断上传是否真正入库
// OSS 返回 200 只说明 callback 的 HTTP 请求成功，115 可能在响应里返回业务错误
func checkCallbackResult(callbackBody []byte, file string) error {
	if len(callbackBody) == 0 {
		return fmt.Errorf("完成 %s 的分片上传出现错误：115 没有返回 callback 响应（OSS 可能没有执行 callback）", file)
	}
	var p fastjson.Parser
	v, err := p.ParseBytes(callbackBody)
	if err != nil {
		// callback 正常执行时 115 返回 JSON，响应不是 JSON 说明 OSS 可能没有执行 callback
		// 此时响应是 CompleteMultipartUpload 的标准 XML 结果
		return fmt.Errorf("完成 %s 的分片上传出现错误：callback 的响应不是 JSON（OSS 可能没有执行 callback），响应内容是：%s", file, string(callbackBody))
	}
	if v.Exists("state") && !v.GetBool("state") {
		return fmt.Errorf("完成 %s 的分片上传出现错误：115 返回失败：%s", file, string(v.GetStringBytes("message")))
	}
	if msg := string(v.GetStringBytes("message")); msg != "" {
		return fmt.Errorf("完成 %s 的分片上传出现错误：115 返回错误：%s", file, msg)
	}
	// 成功的响应里包含文件信息
	if string(v.GetStringBytes("data", "file_id")) == "" {
		return fmt.Errorf("完成 %s 的分片上传出现错误：115 的 callback 响应里缺少文件信息，响应内容是：%s", file, string(callbackBody))
	}
	return nil
}

// 利用 oss 的接口上传文件
func ossUploadFile(ctx context.Context, ft *fastToken, file string, parentCID uint64) (e error) {
	log.Println("普通模式上传文件：" + file)

	info, err := os.Stat(file)
	if err != nil {
		return fmt.Errorf("获取 %s 的信息出现错误：%w", file, err)
	}

	tm, err := newOssTokenManager(ctx, ft.Bucket)
	if err != nil {
		return err
	}
	ot, bucket := tm.get()

	cb := base64.StdEncoding.EncodeToString([]byte(ft.Callback.Callback))
	cbVar := base64.StdEncoding.EncodeToString([]byte(ft.Callback.CallbackVar))
	bar := newBar(info.Size(), filepath.Base(file))
	options := []oss.Option{
		oss.SetHeader("x-oss-security-token", ot.SecurityToken),
		oss.Callback(cb),
		oss.CallbackVar(cbVar),
		oss.UserAgentHeader(aliUserAgent),
		oss.Progress(&ossProgressListener{bar: bar}),
	}

	fmt.Println("按 q 键停止上传并退出程序")
	err = bucket.PutObjectFromFile(ft.Object, file, options...)
	if err != nil {
		bar.Finish()
		return fmt.Errorf("普通模式上传 %s 出现错误：%w", file, err)
	}
	bar.Finish()

	if err = verifyUploaded(parentCID, filepath.Base(file), ft.SHA1); err != nil {
		return err
	}
	log.Printf("普通模式上传 %s 成功", file)
	if *removeFile {
		if err = remove(file); err != nil {
			return err
		}
	}

	return nil
}

// 删除文件
func remove(file string) error {
	err := os.Remove(file)
	if err != nil {
		return fmt.Errorf("删除原文件 %s 出现错误：%w", file, err)
	}
	log.Printf("成功删除原文件 %s", file)
	return nil
}
