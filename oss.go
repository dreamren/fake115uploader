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

	// 进度条池，程序启动后只创建一次
	barMu   sync.Mutex
	barPool *pb.Pool
)

// Docker pull 风格的任务进度条模板，同一个文件的校验和上传阶段复用同一行。
// 已完成（进度走满或任务结束）时保留 100% 的进度条，像 docker 拉完镜像那样定格
// 进行中：上传 file.mkv [=============>        ] 45.20 MB/100.00 MB 12.50 MB/s ETA 45s
// 已完成：上传 file.mkv [=====================>] 100.00 MB/100.00 MB
const taskBarTemplate = `{{if string . "prefix"}}{{string . "prefix"}} {{if or .IsFinished (ge .Current .Total)}}{{bar . "[" "=" ">" " " "]"}} {{counters . "%s/%s" "%s"}}{{else}}{{bar . "[" "=" ">" " " "]"}} {{counters . "%s/%s" "%s"}} {{speed . "%s/s" "…"}} {{rtime . "ETA %s" "" ""}}{{end}}{{end}}`

// 小于此大小的文件不显示进度条，只在汇总中体现
const minBarSize = 1 << 20

// 启动进度条池，在上传任务开始前调用
func startBarPool() {
	if !stdoutIsTTY {
		return
	}

	barMu.Lock()
	defer barMu.Unlock()
	barPool = pb.NewPool()
	// 进度条统一输出到 stdout，日志输出到 stderr，避免两者在终端里互相覆盖截断
	//（不设置时 pb 库的池默认输出到 stderr，会和日志混在同一个流里）
	barPool.Output = os.Stdout
	if err := barPool.Start(); err != nil {
		log.Printf("启动进度条池出现错误：%v", err)
		barPool = nil
	}
}

// 停止进度条池。必须在上传任务全部结束后、打印上传结果汇总前调用，
// 否则进度条的重绘会覆盖终端里刚打印的汇总信息
func stopBarPool() {
	barMu.Lock()
	defer barMu.Unlock()
	if barPool == nil {
		return
	}
	barPool.Stop()
	barPool = nil
}

// 每个上传任务固定占用一行进度条，校验和上传阶段复用同一行，
// 避免上传大量文件时终端里的进度条行数无限增长。
// 进度条在第一次显示内容（首个大文件的校验阶段）时才加入进度条池，
// 只处理过小文件的任务不会产生空行。
// 注意：任务进度条不能调用 Finish，pb 库的池在所有进度条完成后的
// 下一次刷新时会停止渲染且无法重启，之后新加入的进度条将无法显示
type taskBar struct {
	bar *pb.ProgressBar
}

// 创建任务进度条
// 进度条池未启动（stdout 不是终端）时返回 nil，所有方法都是安全的空操作
func newTaskBar() *taskBar {
	barMu.Lock()
	defer barMu.Unlock()
	if barPool == nil {
		return nil
	}
	b := pb.New64(0).SetTemplate(taskBarTemplate).
		Set(pb.Bytes, true).Set(pb.SIBytesPrefix, true).SetWriter(os.Stdout)
	return &taskBar{bar: b}
}

// 进度条是否已加入进度条池（池在加入进度条时会设置 Static 标志）
func (t *taskBar) joined() bool {
	return t != nil && t.bar != nil && t.bar.GetBool(pb.Static)
}

// 首次显示内容时把进度条加入进度条池开始渲染
func (t *taskBar) joinPool() {
	if t.joined() {
		return
	}
	barMu.Lock()
	defer barMu.Unlock()
	if barPool != nil {
		barPool.Add(t.bar)
	}
}

// 开始新阶段：更新前缀、重置进度并重新计时，速度只统计当前阶段（校验或上传）
func (t *taskBar) beginPhase(prefix string, total int64) {
	if t == nil || t.bar == nil {
		return
	}
	t.joinPool()
	t.bar.SetTotal(total)
	t.bar.SetCurrent(0)
	t.bar.Set("prefix", truncatePrefix(prefix))
	// Start 会重置完成状态并重新计时；池内的 Static 进度条不会启动独立渲染协程
	t.bar.Start()
}

// 开始上传阶段：小于 1MB 的文件不显示进度条，只在汇总中体现。
// 此时不能清空进度条，该行保持上一个文件完成时的 100% 显示，
// 直到下一个大文件的校验或上传阶段自然覆盖它
func (t *taskBar) beginUpload(filename string, total int64) {
	if total < minBarSize {
		return
	}
	t.beginPhase("上传 "+filename, total)
}

// 截断过长的进度条前缀，避免进度条行超过终端宽度导致换行错乱
func truncatePrefix(s string) string {
	const max = 30
	r := []rune(s)
	if len(r) <= max {
		return s
	}
	return string(r[:max-1]) + "…"
}

// 进度监听
type ossProgressListener struct {
	bar *pb.ProgressBar
}

// 实现 oss.ProgressListener 的接口。
// 注意不能在这里调用 Finish，进度条的生命周期由任务统一管理
func (listener *ossProgressListener) ProgressChanged(event *oss.ProgressEvent) {
	if listener == nil || listener.bar == nil {
		return
	}
	switch event.EventType {
	case oss.TransferDataEvent:
		listener.bar.SetCurrent(event.ConsumedBytes)
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
func ossUploadFile(ctx context.Context, ft *fastToken, file string, parentCID uint64, tb *taskBar) (e error) {
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
	tb.beginUpload(filepath.Base(file), info.Size())
	var uploadBar *pb.ProgressBar
	if tb != nil {
		uploadBar = tb.bar
	}
	options := []oss.Option{
		oss.SetHeader("x-oss-security-token", ot.SecurityToken),
		oss.Callback(cb),
		oss.CallbackVar(cbVar),
		oss.UserAgentHeader(aliUserAgent),
		oss.Progress(&ossProgressListener{bar: uploadBar}),
	}

	err = bucket.PutObjectFromFile(ft.Object, file, options...)
	if err != nil {
		return fmt.Errorf("普通模式上传 %s 出现错误：%w", file, err)
	}

	if err = verifyUploaded(parentCID, filepath.Base(file), ft.SHA1); err != nil {
		return err
	}
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
	return nil
}
