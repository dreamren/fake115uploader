package main

// 上传速度监控：连续若干秒上传速度低于阈值时，取消并跳过当前任务。
//
// 检测在进度回调里喂入累计字节数，由 slowDetector 按滑动时间窗计算平均速度；
// 一旦触发，uploadStats 会置位暂停标志 abortableReader，令正在进行的
// 上传 reader 返回哨兵错误 errUploadTooSlow，从而中断 OSS SDK 的上传调用，
// 上层识别该哨兵错误后取消并跳过该任务（不重试）。

import (
	"errors"
	"io"
	"sync"
	"time"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
)

// 上传速度过慢被跳过的哨兵错误
var errUploadTooSlow = errors.New("上传速度过慢，已取消该任务")

// 慢速检测器。threshold<=0 或 window<=0 时功能关闭（所有方法安全空操作）。
// 用滑动时间窗：窗口满时计算窗口内平均速度，若低于阈值触发，
// 然后以当前时刻为新窗口起点继续滚动检测，实现"连续"语义。
type slowDetector struct {
	threshold float64    // MB/s，<=0 禁用
	window    time.Duration // 连续多长时间，<=0 禁用

	mu         sync.Mutex
	init       bool
	winStartAt time.Time
	winBytes   int64
}

func newSlowDetector(threshold float64, window time.Duration) *slowDetector {
	if threshold <= 0 || window <= 0 {
		return nil
	}
	return &slowDetector{threshold: threshold, window: window}
}

// observe 喂入全局累计字节数；返回 errUploadTooSlow 表示已持续低于阈值
func (d *slowDetector) observe(cur int64, now time.Time) error {
	if d == nil {
		return nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.init {
		d.init = true
		d.winStartAt = now
		d.winBytes = cur
		return nil
	}
	elapsed := now.Sub(d.winStartAt)
	if elapsed < d.window {
		return nil
	}
	rate := float64(cur-d.winBytes) / elapsed.Seconds() / 1024 / 1024
	if d.threshold > 0 && rate < d.threshold {
		return errUploadTooSlow
	}
	// 未触发，滚动窗口继续检测
	d.winStartAt = now
	d.winBytes = cur
	return nil
}

// 中止标志：慢速触发后置位，abortableReader 据此中断上传
type uploadAbort struct {
	mu   sync.Mutex
	sent bool
}

func (a *uploadAbort) mark() {
	if a == nil {
		return
	}
	a.mu.Lock()
	a.sent = true
	a.mu.Unlock()
}

func (a *uploadAbort) triggered() bool {
	if a == nil {
		return false
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.sent
}

// 全局上传统计：累计字节 + 慢速检测 + 中止标志
// 分片/普通模式共用，保证"连续 x 秒速度低于 y"跨分片、跨阶段统计
type uploadStats struct {
	mu       sync.Mutex
	consumed int64
	det      *slowDetector
	abort    *uploadAbort
}

func newUploadStats(det *slowDetector) *uploadStats {
	return &uploadStats{
		det:   det,
		abort: &uploadAbort{},
	}
}

// add 累进累计字节并喂慢速检测，触发时置位中止标志
func (st *uploadStats) add(n int64, now time.Time) error {
	if st == nil || n <= 0 {
		return nil
	}
	st.mu.Lock()
	st.consumed += n
	var err error
	if st.det != nil {
		err = st.det.observe(st.consumed, now)
	}
	st.mu.Unlock()
	if err != nil {
		st.abort.mark()
	}
	return err
}

// 中断用 reader：上传前包装文件读取器，一旦中止标志置位，Read 立即返回
// 哨兵错误，OSS SDK 收到读错误后会中断上传请求
type abortableReader struct {
	r  io.Reader
	ab *uploadAbort
}

func newAbortableReader(r io.Reader, ab *uploadAbort) io.Reader {
	if ab == nil {
		return r
	}
	return &abortableReader{r: r, ab: ab}
}

func (r *abortableReader) Read(p []byte) (int, error) {
	if r.ab.triggered() {
		return 0, errUploadTooSlow
	}
	return r.r.Read(p)
}

// newTaskMonitor 按配置为单个上传任务创建一个速度监控器。
// 速度阈值/持续秒数任一为 0（未在配置或参数中设置）时返回 nil（功能关闭）。
// 每个任务必须各建各的实例，滑动时间窗是任务独立的，共享会串扰。
func newTaskMonitor() *uploadStats {
	det := newSlowDetector(config.SlowSpeedMB, time.Duration(config.SlowSeconds)*time.Second)
	return newUploadStats(det)
}

// taskProgress 实现 oss.ProgressListener，同时承担两件事：
//  1. 把上传进度累进到槽位（slot.advance），驱动屏幕上的进度条和速度显示；
//  2. 把上传字节累进到 uploadStats，喂给慢速检测器，触发时置位中止标志。
//
// OSS 每次上传调用（普通模式一次、分片模式每个分片一次）的 ConsumedBytes
// 都是从 0 开始的独立累计值，因此这里记录每个调用流内上次字节数 last，
// 用差值累进到全局 total 中，保证跨分片的速度/进度统计是连续的。
type taskProgress struct {
	slot *progSlot
	st   *uploadStats
	mu   sync.Mutex
	last int64 // 当前上传调用流内已计入的累计字节
}

func newTaskProgress(slot *progSlot, st *uploadStats) *taskProgress {
	return &taskProgress{slot: slot, st: st}
}

func (p *taskProgress) ProgressChanged(e *oss.ProgressEvent) {
	if p == nil {
		return
	}
	switch e.EventType {
	case oss.TransferStartedEvent:
		p.mu.Lock()
		p.last = 0 // 每个上传调用流从 0 开始
		p.mu.Unlock()
	case oss.TransferDataEvent:
		p.feed(e.ConsumedBytes)
	case oss.TransferCompletedEvent:
		p.feed(e.ConsumedBytes)
	case oss.TransferFailedEvent:
		// 失败的分片不计入，避免把未完成的字节算成上传速度；下次重试重新累计
		p.mu.Lock()
		p.last = 0
		p.mu.Unlock()
	default:
	}
}

func (p *taskProgress) feed(consumed int64) {
	p.mu.Lock()
	delta := consumed - p.last
	if delta < 0 {
		delta = 0
	}
	p.last = consumed
	slot := p.slot
	st := p.st
	p.mu.Unlock()

	if slot != nil {
		slot.advance(delta)
	}
	if st != nil {
		st.add(delta, time.Now())
	}
}

// reset 供上传函数在分片重试前清理当前调用流的累计值
func (p *taskProgress) reset() {
	if p == nil {
		return
	}
	p.mu.Lock()
	p.last = 0
	p.mu.Unlock()
}