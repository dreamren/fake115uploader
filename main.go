package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/eiannone/keyboard"
	"github.com/orzogc/fake115uploader/cipher"
	"github.com/valyala/fastjson"
)

// const tokenURL = "https://uplb.115.com/3.0/gettoken.php"
// const resumeURL = "https://uplb.115.com/3.0/resumeupload.php?isp=0&appid=0&appversion=%s&format=json&sig=%s"
// downloadURL   = "https://webapi.115.com/files/download?pickcode=%s"
// sampleInitURL = "https://uplb.115.com/3.0/sampleinitupload.php"

const (
	infoURL        = "https://proapi.115.com/app/uploadinfo"
	initURL        = "https://uplb.115.com/4.0/initupload.php?k_ec=%s"
	getinfoURL     = "https://uplb.115.com/3.0/getuploadinfo.php"
	listFileDirURL = "https://webapi.115.com/files?aid=1&cid=%d&o=user_ptime&asc=0&offset=0&show_dir=1&limit=100000&natsort=1&format=json"
	downloadURL    = "https://proapi.115.com/app/chrome/downurl"
	orderURL       = "https://webapi.115.com/files/order"
	createDirURL   = "https://webapi.115.com/files/add"
	searchURL      = "https://webapi.115.com/files/search?offset=0&limit=100000&aid=1&cid=%d&format=json"
	appVer         = "30.5.1"
	userAgent      = "Mozilla/5.0 115disk/" + appVer
	endString      = "000000"
	aliUserAgent   = "aliyun-sdk-android/2.9.1"
	linkPrefix     = "115://"
	targetPrefix   = "U_1_"
	maxParts       = 10000
)

var (
	fastUpload      *bool
	upload          *bool
	multipartUpload *bool
	configFile      *string
	internal        *bool
	removeFile      *bool
	recursive       *bool
	verbose         *bool
	userID          string
	userKey         string
	config          uploadConfig // 设置数据
	result          resultData   // 上传结果
	resultMu        sync.Mutex   // 保护 result 的并发读写
	quit            = make(chan struct{})
	proxyHost       string
	proxyUser       string
	proxyPassword   string
	httpClient      = &http.Client{Timeout: 30 * time.Second}
	ecdhCipher      *cipher.EcdhCipher
)

// 设置数据
type uploadConfig struct {
	Cookies           string `json:"cookies"`           // 115 网页版的 Cookie
	CID               uint64 `json:"cid"`               // 115 里文件夹的 cid
	ResultDir         string `json:"resultDir"`         // 在指定文件夹保存上传结果
	HTTPRetry         uint   `json:"httpRetry"`         // HTTP 请求失败后的重试次数
	HTTPProxy         string `json:"httpProxy"`         // HTTP 代理
	OSSProxy          string `json:"ossProxy"`          // OSS 上传代理
	PartsNum          uint   `json:"partsNum"`          // 分片上传的分片数量
	PartSizeMB        int    `json:"partSizeMB"`        // 分片上传的分片大小（MB）
	ParallelParts     int    `json:"parallelParts"`     // 分片上传时每个文件的并行分片数
	ConcurrentUploads int    `json:"concurrentUploads"` // 同时上传的任务数
}

// 上传失败的文件信息
type failedFile struct {
	Path string `json:"path"` // 上传文件的路径
	CID  uint64 `json:"cid"`  // 上传文件的 115 父文件夹 cid
}

// 上传结果数据
type resultData struct {
	Success []string     `json:"success"` // 上传成功的文件
	Failed  []failedFile `json:"failed"`  // 上传失败的文件
}

// 要上传的文件的信息
type fileInfo struct {
	Path     string `json:"path"`     // 文件路径
	ParentID uint64 `json:"parentID"` // 要上传到的文件夹的 cid
}

// 检查错误
func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}

// 获取时间
func getTime() string {
	t := time.Now()
	return fmt.Sprintf("%d-%02d-%02d %02d-%02d-%02d", t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second())
}

// 处理输入
func getInput(ctx context.Context) {
	defer func() {
		if err := recover(); err != nil {
			log.Printf("getInput() error: %v", err)
		}
	}()

	eventCh, err := keyboard.GetKeys(10)
	checkErr(err)

	for {
		select {
		case <-ctx.Done():
			return
		case event := <-eventCh:
			checkErr(event.Err)
			if string(event.Rune) == "q" || string(event.Rune) == "Q" || event.Key == keyboard.KeyCtrlC {
				// 通知退出信号，接收方可能已退出，不阻塞
				select {
				case quit <- struct{}{}:
				default:
				}
				return
			}
		}
	}
}

func closeKeybord() {
	ch := make(chan struct{}, 1)
	defer close(ch)

	go func() {
		err := keyboard.Close()
		if err != nil {
			log.Printf("关闭 keyboard 出现错误：%v", err)
		}

		ch <- struct{}{}
	}()

	select {
	case <-ch:
		return
	case <-time.After(5 * time.Second):
		log.Println("关闭 keyboard 超时，强制退出")
		os.Exit(1)
	}
}

// 监听退出信号（q 键或系统信号），取消 context 停止所有上传任务
func watchStop(ctx context.Context, cancel context.CancelFunc) {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	select {
	case <-quit: // q 键
	case <-ch: // 系统信号
	case <-ctx.Done():
		return
	}

	signal.Stop(ch)
	signal.Reset(os.Interrupt, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	log.Println("收到退出信号，正在停止上传任务，请等待")
	cancel()
}

// 记录上传成功的文件
func recordSuccess(path string) {
	resultMu.Lock()
	result.Success = append(result.Success, path)
	resultMu.Unlock()
}

// 记录上传失败的文件
func recordFailed(path string, cid uint64) {
	resultMu.Lock()
	result.Failed = append(result.Failed, failedFile{Path: path, CID: cid})
	resultMu.Unlock()
}

// 程序退出时打印信息
func exitPrint() {
	defer func() {
		if err := recover(); err != nil {
			log.Printf("exitPrint() error: %v", err)
			// 不保存上传结果
			config.ResultDir = ""
			exitPrint()
		}
	}()

	if len(result.Success) == 0 && len(result.Failed) == 0 {
		log.Println("本次运行没有上传文件")
		return
	}

	if config.ResultDir != "" {
		resultFile := filepath.Join(config.ResultDir, getTime()+" result.json")
		log.Printf("上传结果保存在 %s", resultFile)
		data, err := json.MarshalIndent(result, "", "    ")
		checkErr(err)
		err = os.WriteFile(resultFile, data, 0644)
		checkErr(err)
	}

	fmt.Printf("上传成功的文件（%d）：\n", len(result.Success))
	for _, s := range result.Success {
		fmt.Println(s)
	}
	fmt.Printf("上传失败的文件（%d）：\n", len(result.Failed))
	for _, f := range result.Failed {
		fmt.Printf("  文件：%s，cid：%d\n", f.Path, f.CID)
	}
}

// 进行 http 请求
func doRequest(req *http.Request) (resp *http.Response, err error) {
	for i := 0; i < int(config.HTTPRetry+1); i++ {
		if i > 0 {
			// 重试前重置请求体
			if req.GetBody != nil {
				if body, e := req.GetBody(); e == nil {
					req.Body = body
				}
			}
			// 重试前退避一段时间
			time.Sleep(time.Duration(i) * time.Second)
		}
		resp, err = httpClient.Do(req)
		if err == nil {
			return resp, nil
		}
		log.Printf("http 请求出现错误（第%d次尝试）：%v", i+1, err)
	}

	return nil, fmt.Errorf("http 请求出现错误：%w", err)
}

// 获取 userID 和 userKey
func getUserKey() (e error) {
	defer func() {
		if err := recover(); err != nil {
			log.Println("请确定网络是否畅通或者 cookies 是否设置好，每一次登陆网页端 115 都要重设一次 cookies")
			e = fmt.Errorf("getUserKey() error: %v", err)
		}
	}()

	req, err := http.NewRequest(http.MethodGet, infoURL, nil)
	checkErr(err)
	req.Header.Set("User-Agent", userAgent)
	req.Header.Set("Cookie", config.Cookies)
	resp, err := doRequest(req)
	checkErr(err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	checkErr(err)

	var p fastjson.Parser
	v, err := p.ParseBytes(body)
	checkErr(err)
	userID = strconv.Itoa(v.GetInt("user_id"))
	userKey = string(v.GetStringBytes("userkey"))

	if userID == "0" {
		panic(fmt.Errorf("获取 userkey 出错，请确定 cookies 是否设置好"))
	}

	if *verbose {
		log.Printf("userID和userKey的值分别是：%s %s", userID, userKey)
	}
	return nil
}

// 根据文件夹名字查找文件夹
func findDir(v *fastjson.Value, pid uint64, name string) (cid uint64, e error) {
	for _, v := range v.GetArray("data") {
		if v.Exists("fid") {
			continue
		}
		parentID, err := strconv.ParseUint(string(v.GetStringBytes("pid")), 10, 64)
		if err != nil {
			continue
		}
		if parentID == pid && string(v.GetStringBytes("n")) == name {
			cid, err = strconv.ParseUint(string(v.GetStringBytes("cid")), 10, 64)
			if err != nil {
				return 0, fmt.Errorf("查找文件夹 %s 失败：%v", name, err)
			}
			if *verbose {
				log.Printf("文件夹 %s 已存在，cid：%d", name, cid)
			}
			orderFile(cid)

			return cid, nil
		}
	}

	return 0, fmt.Errorf("查找文件夹 %s 失败", name)
}

// 在 115 网盘指定文件夹里创建新文件夹
func createDir(pid uint64, name string) (cid uint64, e error) {
	form := url.Values{}
	form.Set("pid", strconv.FormatUint(pid, 10))
	form.Set("cname", name)
	v, err := postFormJSON(createDirURL, form.Encode())
	if err != nil {
		return 0, fmt.Errorf("创建文件夹 %s 出现错误：%w", name, err)
	}

	if v.GetBool("state") {
		cid, err = strconv.ParseUint(string(v.GetStringBytes("cid")), 10, 64)
		if err != nil {
			return 0, fmt.Errorf("解析文件夹 %s 的 cid 出现错误：%v", name, err)
		}
		if *verbose {
			log.Printf("成功创建文件夹 %s ，cid：%d", name, cid)
		}
		orderFile(cid)

		return cid, nil
	}
	// 要创建的文件夹已经存在
	if v.GetInt("errno") == 20004 {
		reqURL, err := url.Parse(fmt.Sprintf(searchURL, pid))
		if err != nil {
			return 0, fmt.Errorf("解析搜索链接出现错误：%w", err)
		}
		query := reqURL.Query()
		query.Set("search_value", name)
		reqURL.RawQuery = query.Encode()
		// 请求有可能返回空 body
		if sv, err := getURLJSON(reqURL.String()); err == nil {
			if cid, err := findDir(sv, pid, name); err == nil {
				return cid, nil
			} else if *verbose {
				log.Printf("搜索文件夹失败，改为直接查找文件夹：%v", err)
			}
		} else if *verbose {
			log.Printf("搜索文件夹失败，改为直接查找文件夹：%v", err)
		}

		// 如果搜索的文件夹不存在，就直接查找
		fileURL := fmt.Sprintf(listFileDirURL, pid)
		v, err = getURLJSON(fileURL)
		if err != nil {
			return 0, fmt.Errorf("创建文件夹 %s 出现错误：%w", name, err)
		}
		if cid, err = findDir(v, pid, name); err == nil {
			return cid, nil
		}
	}

	return 0, fmt.Errorf("创建文件夹 %s 失败", name)
}

// 将 cid 对应文件夹设置为时间降序
func orderFile(cid uint64) {
	orderBody := fmt.Sprintf("user_order=user_ptime&file_id=%d&user_asc=0&fc_mix=0", cid)
	v, err := postFormJSON(orderURL, orderBody)
	if err != nil {
		log.Printf("排序文件夹 %d 出现错误：%v", cid, err)
		return
	}
	if !v.GetBool("state") {
		log.Printf("排序文件夹 %d 出现错误：%s", cid, v.GetStringBytes("error"))
	} else if *verbose {
		log.Printf("排序文件夹 %d 成功", cid)
	}
}

// 读取设置文件
func loadConfig() (e error) {
	defer func() {
		if err := recover(); err != nil {
			e = fmt.Errorf("loadConfig() error: %v", err)
		}
	}()

	if _, err := os.Stat(*configFile); os.IsNotExist(err) {
		log.Printf("设置文件不存在，新建设置文件 %s ，请先设置cookies", *configFile)
		data, err := json.MarshalIndent(config, "", "    ")
		checkErr(err)
		err = os.WriteFile(*configFile, data, 0644)
		checkErr(err)
		os.Exit(1)
	} else {
		data, err := os.ReadFile(*configFile)
		checkErr(err)
		if json.Valid(data) {
			err = json.Unmarshal(data, &config)
			checkErr(err)
		} else {
			panic(fmt.Errorf("设置文件 %s 的内容不符合json格式，请检查其内容", *configFile))
		}
	}

	return nil
}

// 程序初始化
func initialize() (e error) {
	defer func() {
		if err := recover(); err != nil {
			e = fmt.Errorf("initialize() error: %v", err)
		}
	}()

	fastUpload = flag.Bool("f", false, "秒传模式上传`文件`")
	upload = flag.Bool("u", false, "先尝试用秒传模式上传`文件`，失败后改用普通模式上传")
	multipartUpload = flag.Bool("m", false, "先尝试用秒传模式上传`文件`，失败后改用分片模式上传（适合用于上传超大文件）")
	configFile = flag.String("l", "", "指定设置`文件`（json 格式），默认是程序所在的文件夹里的 fake115uploader.json")
	cookies := flag.String("k", "", "使用指定的 115 的`Cookie`")
	cid := flag.Uint64("c", 1, "上传文件到指定的 115 文件夹，`cid`为 115 里的文件夹对应的 cid(默认为 0，即根目录）")
	resultDir := flag.String("r", "", "将上传结果保存在指定`文件夹`")
	noConfig := flag.Bool("n", false, "不读取设置文件，需要和 -k 配合使用")
	internal = flag.Bool("a", false, "利用阿里云内网上传文件，需要在阿里云服务器上运行本程序")
	removeFile = flag.Bool("e", false, "上传成功后自动删除原文件")
	httpProxy := flag.String("http-proxy", "", "指定 HTTP`代理`")
	ossProxy := flag.String("oss-proxy", "", "指定 OSS 上传使用的`代理`")
	httpRetry := flag.Uint("http-retry", 0, "HTTP 请求失败后的`重试次数`，默认为 0（即不重试）")
	recursive = flag.Bool("recursive", false, "递归上传文件夹")
	partsNum := flag.Uint("parts-num", 0, "分片模式上传文件的`分片数量`，范围为 1 到 10000，设置后忽略分片大小")
	partSize := flag.Int("part-size", 0, "分片模式上传文件的`分片大小`，单位为 MB，范围为 1 到 5120，默认为 0（即 128MB）")
	parallelParts := flag.Int("parallel-parts", 0, "已无效：115 要求分片按序上传，分片无法并行，请用 -concurrent-uploads 提升速度")
	concurrentUploads := flag.Int("concurrent-uploads", 0, "同时上传的最大`任务数`，范围为 1 到 10，默认为 0（即 2）")
	logFile := flag.String("log-file", "", "将日志保存到指定`文件`（推荐：进度条会截断终端日志，日志文件里的内容才是完整的）")
	verbose = flag.Bool("v", false, "显示更详细的信息（调试用）")
	help := flag.Bool("h", false, "显示帮助信息")

	flag.Parse()

	// 终端里进度条的重绘会截断日志行，把日志写入文件以保证内容完整
	if *logFile != "" {
		lf, err := os.OpenFile(*logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			log.Fatalf("打开日志文件 %s 出现错误：%v", *logFile, err)
		}
		defer lf.Close()
		log.SetOutput(io.MultiWriter(os.Stderr, lf))
		log.Printf("日志同时保存到 %s", *logFile)
	}

	if *configFile == "" {
		path, err := os.Executable()
		checkErr(err)
		*configFile = filepath.Join(filepath.Dir(path), "fake115uploader.json")
	}

	if !*noConfig {
		err := loadConfig()
		checkErr(err)
	}

	if flag.NFlag() == 0 {
		log.Println("请输入正确的参数")
		flag.PrintDefaults()
		os.Exit(1)
	}
	if *help {
		flag.PrintDefaults()
		os.Exit(0)
	}
	if (*fastUpload && *upload) || (*fastUpload && *multipartUpload) || (*upload && *multipartUpload) {
		log.Println("-f、-u 和-m 这三个参数只能同时使用其中一个")
		os.Exit(1)
	}

	if *partsNum != 0 && !*multipartUpload {
		log.Println("-parts-num 参数只支持分片上传模式")
		os.Exit(1)
	}
	if (*partSize != 0 || *parallelParts != 0) && !*multipartUpload {
		log.Println("-part-size 和 -parallel-parts 参数只支持分片上传模式")
		os.Exit(1)
	}
	// 优先使用参数指定的分片数量
	if *partsNum != 0 {
		config.PartsNum = *partsNum
	}
	if config.PartsNum > maxParts {
		log.Printf("分片数量不能大于%d", maxParts)
		os.Exit(1)
	}

	// 优先使用参数指定的分片大小
	if *partSize != 0 {
		config.PartSizeMB = *partSize
	}
	if config.PartSizeMB == 0 {
		config.PartSizeMB = 128
	}
	if config.PartSizeMB < 1 || config.PartSizeMB > 5120 {
		log.Printf("分片大小不能小于1MB或大于5120MB")
		os.Exit(1)
	}

	// 优先使用参数指定的并行分片数
	if *parallelParts != 0 {
		config.ParallelParts = *parallelParts
	}
	if config.ParallelParts == 0 {
		config.ParallelParts = 4
	}
	if config.ParallelParts < 1 || config.ParallelParts > 100 {
		log.Printf("并行分片数不能小于1或大于100")
		os.Exit(1)
	}

	// 优先使用参数指定的任务数
	if *concurrentUploads != 0 {
		config.ConcurrentUploads = *concurrentUploads
	}
	if config.ConcurrentUploads == 0 {
		config.ConcurrentUploads = 2
	}
	if config.ConcurrentUploads < 1 || config.ConcurrentUploads > 10 {
		log.Printf("同时上传的任务数不能小于1或大于10")
		os.Exit(1)
	}

	// 优先使用参数指定的 Cookie
	if *cookies != "" {
		config.Cookies = *cookies
	}
	if config.Cookies == "" {
		log.Printf("设置文件 %s 里的cookies不能为空字符串，或者用-k指定115的Cookie", *configFile)
		os.Exit(1)
	}
	if *verbose {
		log.Printf("Cookies的值为：%s", config.Cookies)
	}

	// 优先使用参数指定的 cid
	if *cid != 1 {
		config.CID = *cid
	}

	// 优先使用参数指定的文件夹
	if *resultDir != "" {
		config.ResultDir = *resultDir
	}
	if config.ResultDir != "" {
		info, err := os.Stat(config.ResultDir)
		checkErr(err)
		if !info.IsDir() {
			log.Printf("%s 必须是文件夹，请重新设置", config.ResultDir)
			os.Exit(1)
		}
	}

	// 优先使用参数指定的 HTTP 请求重试次数
	if *httpRetry != 0 {
		config.HTTPRetry = *httpRetry
	}

	// HTTP 代理，优先级 httpProxy > 设置文件 > http_proxy/https_proxy
	*httpProxy = strings.TrimSpace(*httpProxy)
	if *httpProxy == "" {
		*httpProxy = strings.TrimSpace(config.HTTPProxy)
	}
	if *httpProxy != "" {
		proxyURL, err := url.Parse(*httpProxy)
		if err == nil {
			httpClient.Transport = &http.Transport{
				Proxy: http.ProxyURL(proxyURL),
				DialContext: (&net.Dialer{
					Timeout:   30 * time.Second,
					KeepAlive: 30 * time.Second,
				}).DialContext,
				ForceAttemptHTTP2:     true,
				MaxIdleConns:          100,
				IdleConnTimeout:       90 * time.Second,
				TLSHandshakeTimeout:   10 * time.Second,
				ExpectContinueTimeout: 1 * time.Second,
			}
		} else {
			log.Printf("解析HTTP代理地址出现错误：%v", err)
		}
	}

	// OSS 代理，优先级 ossProxy > 设置文件 > http_proxy > https_proxy
	*ossProxy = strings.TrimSpace(*ossProxy)
	if *ossProxy == "" {
		*ossProxy = strings.TrimSpace(config.OSSProxy)
	}
	if *ossProxy == "" {
		*ossProxy = strings.TrimSpace(os.Getenv("http_proxy"))
	}
	if *ossProxy == "" {
		*ossProxy = strings.TrimSpace(os.Getenv("https_proxy"))
	}
	if *ossProxy != "" {
		proxyURL, err := url.Parse(*ossProxy)
		if err == nil {
			proxyHost = "//" + proxyURL.Host
			if proxyURL.User != nil {
				proxyUser = proxyURL.User.Username()
				if password, b := proxyURL.User.Password(); b {
					proxyPassword = password
				}
			}
		} else {
			log.Printf("解析OSS代理地址出现错误：%v", err)
		}
	}

	err := getUserKey()
	checkErr(err)

	if len(flag.Args()) != 0 && (*upload || *multipartUpload) {
		orderFile(config.CID)
	}

	ecdhCipher, err = cipher.NewEcdhCipher()
	checkErr(err)

	return nil
}

func main() {
	defer func() {
		stopBarPool()
		if len(result.Failed) != 0 {
			os.Exit(1)
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go watchStop(ctx, cancel)

	if err := initialize(); err != nil {
		log.Fatal(err)
	}

	go getInput(ctx)
	defer closeKeybord()

	defer exitPrint()

	files := make([]fileInfo, 0, len(flag.Args()))
	cidMap := make(map[string]uint64)
	for _, file := range flag.Args() {
		file = filepath.Clean(file)
		info, err := os.Stat(file)
		if err != nil {
			log.Printf("获取 %s 的信息出现错误：%v", file, err)
			continue
		}

		if info.IsDir() {
			// 上传文件夹
			if *recursive {
				err = filepath.WalkDir(file, func(path string, d fs.DirEntry, err error) error {
					if d == nil {
						return fmt.Errorf("获取文件夹 %s 的信息出现错误，取消上传该文件夹：%w", path, err)
					}

					path = filepath.Clean(path)

					if d.IsDir() {

						if err != nil {
							log.Printf("获取文件夹 %s 的信息出现错误，取消上传该文件夹：%v", path, err)
							return fs.SkipDir
						}

						if path == file {
							var filename string
							if path == "." {
								abs, err := filepath.Abs(path)
								if err != nil {
									return fmt.Errorf("获取文件夹 %s 的绝对路径失败，取消上传该文件夹：%w", path, err)
								}
								filename = filepath.Base(abs)
							} else {
								filename = filepath.Base(path)
							}

							cid, err := createDir(config.CID, filename)
							if err != nil {
								return err
							}

							cidMap[path] = cid

							return nil
						}

						pdir := filepath.Dir(path)
						if pid, ok := cidMap[pdir]; ok {
							cid, err := createDir(pid, d.Name())

							if err != nil {
								return err
							}

							cidMap[path] = cid
						} else {
							return fmt.Errorf("没有创建文件夹 %s ，取消上传 %s", filepath.Base(pdir), path)
						}
					} else {
						if err != nil {
							log.Printf("获取文件 %s 的信息出现错误，取消上传该文件：%v", path, err)
							return nil
						}

						pdir := filepath.Dir(path)
						if pid, ok := cidMap[pdir]; ok {
							files = append(files, fileInfo{
								Path:     path,
								ParentID: pid,
							})
						} else {
							return fmt.Errorf("没有创建文件夹 %s ，取消上传 %s", filepath.Base(pdir), path)
						}
					}
					return nil
				})
				if err != nil {
					log.Printf("上传文件夹 %s 出现错误：%v", file, err)
					continue
				}
			} else {
				log.Printf("%s 是文件夹，上传文件夹需要参数 -recursive", file)
				continue
			}
		} else {
			files = append(files, fileInfo{
				Path:     file,
				ParentID: config.CID,
			})
		}
	}

	// 多个任务并行上传，每个任务固定占用一行进度条
	if len(files) > 0 {
		fmt.Println("按 q 键停止上传并退出程序")
		startBarPool()
	}
	tasks := make(chan fileInfo)
	var wg sync.WaitGroup
	go func() {
		defer close(tasks)
		for _, file := range files {
			select {
			case tasks <- file:
			case <-ctx.Done():
				return
			}
		}
	}()
	for i := 0; i < config.ConcurrentUploads; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var tb *taskBar
			for file := range tasks {
				if tb == nil {
					tb = newTaskBar()
				}
				file.uploadFile(ctx, tb)
			}
			// 不调用进度条的 Finish：进度走满时模板已显示 100% 定格，
			// 而 Finish 会让进度条池在所有任务完成后停止渲染，
			// 之后其他任务新加入的进度条将无法显示
		}()
	}
	wg.Wait()

	// 先停止进度条渲染，再打印上传结果汇总，避免进度条重绘覆盖汇总信息
	stopBarPool()
}

// 上传文件
func (file *fileInfo) uploadFile(ctx context.Context, tb *taskBar) {
	switch {
	case *fastUpload:
		if _, err := file.fastUploadFile(tb); err != nil {
			if ctx.Err() != nil {
				return
			}
			// 秒传失败是常态，不打印日志，结果进汇总
			recordFailed(file.Path, file.ParentID)
			return
		}
		recordSuccess(file.Path)
	case *upload:
		token, err := file.fastUploadFile(tb)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			if token == nil {
				// 获取上传 token 失败，无法继续上传
				recordFailed(file.Path, file.ParentID)
				return
			}
			if err := ossUploadFile(ctx, token, file.Path, file.ParentID, tb); err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Printf("普通模式上传 %s 出现错误：%v", file.Path, err)
				recordFailed(file.Path, file.ParentID)
				return
			}
		}
		recordSuccess(file.Path)
	case *multipartUpload:
		token, err := file.fastUploadFile(tb)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			if token == nil {
				// 获取上传 token 失败，无法继续上传
				recordFailed(file.Path, file.ParentID)
				return
			}
			if err := multipartUploadFile(ctx, token, file.Path, file.ParentID, tb); err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Printf("分片模式上传 %s 出现错误：%v", file.Path, err)
				recordFailed(file.Path, file.ParentID)
				return
			}
		}
		recordSuccess(file.Path)
	}
}
