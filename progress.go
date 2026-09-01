package main

// 自有终端进度渲染器，彻底取代 pb 库。
//
// 原理：整块屏幕被当作一个固定高度 H 的画布，每次刷新都用 ANSI 光标定位
// 到左上角，逐行用 \033[2K 清掉后重写内容，最后 \033[J 清掉末尾残留。
// 每个并发上传任务固定占用一行（槽位），行数在运行期间绝不变化，
// 因此不同任务的进度条（含校验/上传阶段切换）绝不会互相覆盖错位。
//
// 布局（从顶部往下）：
//   顶部提示行       按 q 键停止上传退出
//   消息区 N 行       最近的消息（错误/跳过/秒传成功等），滚动
//   槽位区 K 行       每个并发任务一行： [阶段] 文件名 [进度条] 45% 速度
//   底部留白 +=1 行
//
// 消息区固定高度，槽位区固定高度=并发任务数，总高度恒定，
// 因此无论消息和进度如何变化，各槽位行号稳定，绝不跳动、绝不覆盖。
//
// 每个 worker 协程在启动时认领一个固定槽位（slot），处理完一个文件后
// 同一个文件的行会被下一个文件的阶段覆盖，绝不变动行号。

import (
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/mattn/go-runewidth"
)

const (
	renderRefreshInterval = 100 * time.Millisecond
	renderMsgLines        = 5    // 消息区固定行数
	renderPaddingLines    = 1    // 底部留白行数
	renderBarWidth        = 22   // 进度条格子数
	renderNameMax         = 34   // 槽位里文件名最大显示宽度
	renderMsgMaxLen       = 110  // 单条消息最大显示宽度
	renderColWidth        = 120  // 每行进渲染宽（超出裁剪，防止换行错乱）
	bottomHint            = "按 q 键停止上传退出"
)

type barRenderer struct {
	mu        sync.Mutex
	out       io.Writer
	ttl       bool         // stdout 是否为终端
	running   bool
	stop      chan struct{}
	rowsTotal int
	msgs      []string     // 消息区内容（从旧到新，最多 renderMsgLines 条）
	slots     []*progSlot  // 每个并发任务一个固定槽位
}

// 全局渲染器。Stdout 是终端时启用，否则不启用。
// barRenderer 未启用时，所有方法都是安全的空操作。
var bar = &barRenderer{}

// initRenderer 初始化渲染器：是否启用渲染由 stdout 是否为终端决定，
// 但无论是否渲染都创建槽位，保证各任务在非终端下也有安全的进度载体。
// 必须在并发 worker 启动前调用一次。
func initRenderer(concurrent int) {
	info, err := os.Stdout.Stat()
	bar.mu.Lock()
	defer bar.mu.Unlock()
	bar.ttl = err == nil && info.Mode()&os.ModeCharDevice != 0
	bar.out = os.Stdout
	bar.slots = bar.slots[:0]
	for i := 0; i < concurrent; i++ {
		bar.slots = append(bar.slots, newSlot(i))
	}
	bar.rowsTotal = 1 + renderMsgLines + concurrent + renderPaddingLines
}

// 渲染器是否已启用（终端）
func rendererEnabled() bool {
	bar.mu.Lock()
	defer bar.mu.Unlock()
	return bar.ttl
}

// 启动后台渲染协程
func startRenderer() {
	if !rendererEnabled() {
		return
	}
	bar.mu.Lock()
	if bar.running {
		bar.mu.Unlock()
		return
	}
	bar.running = true
	bar.stop = make(chan struct{})
	stopCh := bar.stop
	bar.mu.Unlock()

	// 隐藏光标并清理屏幕
	fmt.Fprint(bar.out, "\033[2J\033[H\033[?25l")
	bar.redrawNow()

	go func() {
		t := time.NewTicker(renderRefreshInterval)
		defer t.Stop()
		for {
			select {
			case <-stopCh:
				return
			case <-t.C:
				bar.redrawNow()
			}
		}
	}()
}

// 停止渲染，清理屏幕，恢复光标。幂等，可多次调用。
func stopRenderer() {
	bar.mu.Lock()
	if !bar.running {
		bar.mu.Unlock()
		return
	}
	bar.running = false
	close(bar.stop)
	bar.mu.Unlock()
	fmt.Fprint(bar.out, "\033[2J\033[0m\033[?25h")
}

// 全量重绘画面
func (r *barRenderer) redrawNow() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.ttl {
		return
	}

	var b strings.Builder
	b.Grow((r.rowsTotal + 1) * renderColWidth)
	b.WriteString("\033[H") // 光标定位左上

	// 顶部提示行
	writeLine(&b, r.pad(bottomHint))

	// 消息区
	for i := 0; i < renderMsgLines; i++ {
		if i < len(r.msgs) {
			writeLine(&b, r.pad(r.trunc(r.msgs[i], renderMsgMaxLen)))
		} else {
			writeLine(&b, r.pad(""))
		}
	}

	// 槽位区
	for _, s := range r.slots {
		writeLine(&b, r.pad(s.render()))
	}

	// 底部留白
	for i := 0; i < renderPaddingLines; i++ {
		writeLine(&b, "")
	}

	b.WriteString("\033[J") // 清掉区域末尾残留

	r.out.Write([]byte(b.String()))
}

// 写一行：\033[2K 清行 + \r 回行首 + 内容 + \n
func writeLine(b *strings.Builder, content string) {
	b.WriteString("\033[2K\r")
	b.WriteString(content)
	b.WriteByte('\n')
}

func (r *barRenderer) trunc(s string, max int) string {
	if runewidth.StringWidth(s) <= max {
		return s
	}
	return runewidth.Truncate(s, max-1, "…")
}

func (r *barRenderer) pad(s string) string {
	return runewidth.FillRight(s, renderColWidth)
}

// 槽位渲染文本
func (s *progSlot) render() string {
	s.mu.Lock()
	defer s.mu.Unlock()

	phase := s.phase
	if phase == "" {
		phase = "----"
	}
	name := runewidth.Truncate(s.name, renderNameMax, "…")
	name = runewidth.FillRight(name, renderNameMax)

	var b strings.Builder
	b.WriteByte('[')
	b.WriteString(phase)
	b.WriteString("] ")
	b.WriteString(name)
	b.WriteByte(' ')

	// 进度条
	barWidth := renderBarWidth
	fill := 0
	if s.total > 0 {
		p := s.cur * int64(barWidth) / s.total
		if p > int64(barWidth) {
			p = int64(barWidth)
		}
		fill = int(p)
	}
	b.WriteByte('[')
	b.WriteString(strings.Repeat("█", fill))
	b.WriteString(strings.Repeat("░", barWidth-fill))
	b.WriteByte(']')

	// 百分比
	if s.total > 0 {
		pct := float64(s.cur) * 100 / float64(s.total)
		b.WriteString(fmt.Sprintf(" %3.0f%%", pct))
	}

	// 速度
	if s.speedText != "" {
		b.WriteString(" " + s.speedText)
	}

	return b.String()
}

// 槽位：每个并发任务一个，固定屏幕行号
type progSlot struct {
	r         *barRenderer
	idx       int
	mu        sync.Mutex
	phase     string
	name      string
	cur       int64
	total     int64
	speedText string

	prevBytes int64
	prevTime  time.Time
	ewma      float64 // MB/s
	ewmaInit  bool
}

func newSlot(idx int) *progSlot {
	return &progSlot{r: bar, idx: idx}
}

// beginPhase 开始一个阶段：切换阶段标记、更新文件名、复位进度与速度统计。
func (s *progSlot) beginPhase(phase, name string, total int64) {
	s.mu.Lock()
	s.phase = phase
	s.name = name
	s.total = total
	s.cur = 0
	s.ewma = 0
	s.ewmaInit = false
	s.speedText = ""
	s.mu.Unlock()
}

// advance 累进已上传/已读取字节并重算速度
func (s *progSlot) advance(n int64) {
	if n <= 0 {
		return
	}
	s.mu.Lock()
	s.cur += n
	s.updateSpeedLocked(s.cur)
	s.mu.Unlock()
}

// setProgress 直接设置累计字节（跨分片总计用）
func (s *progSlot) setProgress(cur int64) {
	s.mu.Lock()
	if cur > s.total {
		cur = s.total
	}
	s.cur = cur
	s.updateSpeedLocked(cur)
	s.mu.Unlock()
}

// wrapReader 返回一个读取时自动推进进度的 Reader（用于校验阶段读取文件）
func (s *progSlot) wrapReader(r io.Reader) io.Reader {
	return &slotReader{r: r, s: s}
}

// finish 结束任务：把相位固定为完成/跳过等终态并清掉速度显示
func (s *progSlot) finish(phase string) {
	s.mu.Lock()
	s.phase = phase
	s.speedText = ""
	if s.total > 0 {
		s.cur = s.total
	}
	s.mu.Unlock()
}

func (s *progSlot) updateSpeedLocked(cur int64) {
	now := time.Now()
	if !s.ewmaInit {
		s.ewmaInit = true
		s.prevBytes = cur
		s.prevTime = now
		s.speedText = ""
		return
	}
	dt := now.Sub(s.prevTime).Seconds()
	dx := float64(cur - s.prevBytes)
	if dx < 0 {
		dx = 0
	}
	if dt > 0 {
		inst := dx / dt / 1024 / 1024
		if s.ewma == 0 {
			s.ewma = inst
		} else {
			s.ewma = 0.7*s.ewma + 0.3*inst
		}
	}
	s.prevBytes = cur
	s.prevTime = now
	if s.ewma > 0 {
		s.speedText = fmt.Sprintf("%.2f MB/s", s.ewma)
	} else {
		s.speedText = ""
	}
}

// 读取时推进进度的 Reader
type slotReader struct {
	r io.Reader
	s *progSlot
}

func (sr *slotReader) Read(p []byte) (int, error) {
	n, err := sr.r.Read(p)
	if n > 0 {
		sr.s.advance(int64(n))
	}
	return n, err
}

// barLogger 把 log 库的输出统一捕获到消息区（并可选转发到日志文件）。
// 渲染启用时：日志行进消息区显示，同时写入日志文件（若设）。
// 渲染未启用：原样写入 fallback（stderr）和日志文件（若设），与原来行为一致。
type barLogger struct {
	r        *barRenderer
	fallback io.Writer // 非渲染时写回的日志目标（stderr）
	file     io.Writer // -log-file 指定的日志文件 writer，可为 nil
}

// setupLogSink 把 log 库的输出接到渲染器的消息区/fallback。在 main 里、worker
// 启动前调用一次即可。
func setupLogSink() {
	lb := &barLogger{r: bar, fallback: os.Stderr, file: logFileWriter}
	log.SetOutput(lb)
}

func (b *barLogger) Write(p []byte) (int, error) {
	if !rendererEnabled() {
		if b.fallback != nil {
			b.fallback.Write(p)
		}
		if b.file != nil {
			b.file.Write(p)
		}
		return len(p), nil
	}
	if b.file != nil {
		b.file.Write(p)
	}
	msg := strings.TrimRight(string(p), "\n")
	if msg != "" {
		b.r.pushMsg(msg)
	}
	return len(p), nil
}

// pushMsg 把一条消息送进消息区（滚动）
func (r *barRenderer) pushMsg(msg string) {
	msg = r.trunc(msg, renderMsgMaxLen)
	r.mu.Lock()
	if len(r.msgs) >= renderMsgLines {
		r.msgs = append(r.msgs[1:], msg)
	} else {
		r.msgs = append(r.msgs, msg)
	}
	r.mu.Unlock()
}