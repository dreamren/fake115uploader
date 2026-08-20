package main

import (
	"strings"
	"testing"

	"github.com/cheggaaa/pb/v3"
)

// 渲染辅助：固定宽度避免依赖终端
func renderBar(b *pb.ProgressBar) string {
	if b == nil {
		return ""
	}
	b.SetWidth(100)
	return b.String()
}

func newTestBar() *taskBar {
	b := pb.New64(0).SetTemplate(taskBarTemplate).
		Set(pb.Bytes, true).Set(pb.SIBytesPrefix, true)
	return &taskBar{bar: b}
}

// 场景1：校验阶段显示
func TestVerifyPhaseDisplay(t *testing.T) {
	tb := newTestBar()
	tb.beginPhase("校验 a.mkv", 5<<20)
	tb.bar.Add64(2 << 20)
	s := renderBar(tb.bar)
	for _, want := range []string{"校验 a.mkv", "2.10 MB", "5.24 MB"} {
		if !strings.Contains(s, want) {
			t.Errorf("校验进度里应包含 %q，实际是 %q", want, s)
		}
	}
}

// 场景2：上传进度走满后自动定格100%，不显示时间和速度
func TestUploadFullKeepsBar(t *testing.T) {
	tb := newTestBar()
	tb.beginUpload("a.mkv", 5<<20)
	tb.bar.Add64(3 << 20)
	s := renderBar(tb.bar)
	// 进行中：未走满，速度未知时显示 … 占位（有速度时显示 xx MB/s 和 ETA）
	if !strings.Contains(s, "…") && !strings.Contains(s, "/s") {
		t.Errorf("进行中应显示速度或占位，实际是 %q", s)
	}

	tb.bar.Add64(2 << 20) // 走满，无需 Finish
	s = renderBar(tb.bar)
	for _, want := range []string{"上传 a.mkv", "5.24 MB"} {
		if !strings.Contains(s, want) {
			t.Errorf("完成进度里应包含 %q，实际是 %q", want, s)
		}
	}
	for _, notWant := range []string{"ETA", "/s", "…"} {
		if strings.Contains(s, notWant) {
			t.Errorf("完成进度里不应显示 %q，实际是 %q", notWant, s)
		}
	}
	if !strings.Contains(s, "[") || !strings.Contains(s, "]") {
		t.Errorf("完成时应保留进度条本体，实际是 %q", s)
	}
}

// 场景3：小文件不打扰已有进度（关键：不清空、不重置）
func TestSmallFileKeepsBar(t *testing.T) {
	tb := newTestBar()
	// 先完成一个大文件的上传
	tb.beginUpload("a.mkv", 5<<20)
	tb.bar.Add64(5 << 20)
	before := renderBar(tb.bar)
	if !strings.Contains(before, "上传 a.mkv") {
		t.Fatalf("前置条件失败：%q", before)
	}

	// 小文件：beginUpload 应直接返回，进度条保持
	tb.beginUpload("tiny.txt", 100*1024)
	after := renderBar(tb.bar)
	if after != before {
		t.Errorf("小文件不应改变进度条显示\n之前：%q\n之后：%q", before, after)
	}
}

// 场景4：秒传成功的大文件显示校验100%
func TestFastUploadShowsVerifyDone(t *testing.T) {
	tb := newTestBar()
	tb.beginPhase("校验 b.mkv", 5<<20)
	tb.bar.Add64(5 << 20) // 校验读完
	s := renderBar(tb.bar)
	if !strings.Contains(s, "校验 b.mkv") {
		t.Errorf("秒传成功后应显示校验完成，实际是 %q", s)
	}
	if strings.Contains(s, "ETA") {
		t.Errorf("完成后不应显示时间，实际是 %q", s)
	}
}
